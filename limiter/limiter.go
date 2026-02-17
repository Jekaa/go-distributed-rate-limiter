package limiter

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// NodeID - уникальный идентификатор ноды в кластере.
type NodeID string

// UserConsumption - структура, описывающая потребление пользователя.
type UserConsumption struct {
	PerNodeConsumption map[NodeID]int64
	LastUpdateNanos    int64
	// Версия данных для CAS операций
	Version uint64
}

// GossipMessage - структура, которой обмениваются ноды.
type GossipMessage struct {
	SenderID     NodeID
	VectorClock  map[NodeID]int64
	Updates      map[string]map[NodeID]int64
	Timestamp    int64
	MessageID    [16]byte
	LockRequests map[string]LockRequest
	LockReleases map[string]NodeID
	// Количество известных нод в кластере (для пересчёта квоты)
	KnownPeers int
}

// LockRequest - запрос на блокировку пользователя
type LockRequest struct {
	NodeID    NodeID
	Timestamp int64
	ExpiresAt int64
}

// consumptionWrapper - обертка для безопасного хранения UserConsumption в sync.Map
type consumptionWrapper struct {
	mu   sync.RWMutex
	data *UserConsumption
}

// DistributedRateLimiter - основной тип.
type DistributedRateLimiter struct {
	nodeID         NodeID
	ratePerSecond  float64
	bucketCapacity int64

	Users       sync.Map
	gossipConn  *net.UDPConn
	gossipAddr  *net.UDPAddr
	peers       sync.Map
	vectorClock atomic.Value

	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	droppedMessages uint64
	shutdown        atomic.Bool

	bufferPool sync.Pool

	vectorClockMu sync.Mutex

	updateChan   chan struct{}
	waitingReqs  sync.Map
	messageCache sync.Map

	userLocks      sync.Map
	lockRequestCh  chan LockRequest
	lockReleaseCh  chan string
	localLockMu    sync.Mutex
	lockTimeout    time.Duration
	gossipInterval time.Duration

	lockContentionCount uint64
	lockGrantedCount    uint64

	// Кэш количества нод для пересчёта квоты.
	// Обновляется при получении gossip-сообщений.
	// Защищён атомиком для быстрого чтения в hot path.
	peerCount atomic.Int64
}

// LockInfo - информация о текущей блокировке
type LockInfo struct {
	Holder    NodeID
	Acquired  time.Time
	ExpiresAt time.Time
	Version   uint64
}

func init() {
	gob.Register(GossipMessage{})
	gob.Register(map[NodeID]int64{})
	gob.Register(map[string]map[NodeID]int64{})
	gob.Register(NodeID(""))
	gob.Register(LockRequest{})
	gob.Register(map[string]LockRequest{})
	gob.Register(map[string]NodeID{})
}

// NewDistributedRateLimiter создает новый лимитер с распределенными блокировками.
func NewDistributedRateLimiter(nodeID NodeID, ratePerSecond float64, bucketCapacity int64,
	gossipNetwork string, gossipAddress string) (*DistributedRateLimiter, error) {

	addr, err := net.ResolveUDPAddr(gossipNetwork, gossipAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve multicast address: %w", err)
	}

	conn, err := net.ListenMulticastUDP(gossipNetwork, nil, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on multicast: %w", err)
	}

	if err := conn.SetReadBuffer(8 * 1024 * 1024); err != nil {
		log.Printf("Warning: failed to set read buffer: %v", err)
	}
	if err := conn.SetWriteBuffer(8 * 1024 * 1024); err != nil {
		log.Printf("Warning: failed to set write buffer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	drl := &DistributedRateLimiter{
		nodeID:         nodeID,
		ratePerSecond:  ratePerSecond,
		bucketCapacity: bucketCapacity,
		gossipConn:     conn,
		gossipAddr:     addr,
		ctx:            ctx,
		cancel:         cancel,
		updateChan:     make(chan struct{}, 1000),
		lockRequestCh:  make(chan LockRequest, 1000),
		lockReleaseCh:  make(chan string, 1000),
		lockTimeout:    50 * time.Millisecond,
		gossipInterval: 5 * time.Millisecond,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, 8192))
			},
		},
	}

	// Изначально нода одна — квота = полный bucket.
	drl.peerCount.Store(1)

	initialVC := make(map[NodeID]int64)
	initialVC[nodeID] = 0
	drl.vectorClock.Store(initialVC)

	drl.peers.Store(nodeID, struct{}{})

	drl.wg.Add(6)
	go drl.gossipReceiverLoop()
	go drl.gossipSenderLoop()
	go drl.cleanupLoop()
	go drl.updateHandlerLoop()
	go drl.lockManagerLoop()
	go drl.lockCleanupLoop()

	return drl, nil
}

// nodeQuota возвращает долю bucket, выделенную данной ноде.
//
// Ключевое архитектурное решение: вместо того чтобы каждая нода
// считала весь глобальный bucket и надеялась на своевременный gossip
// (что и приводило к багу 60 > 30), мы делим bucket поровну между
// известными нодами. Это даёт детерминированное разделение без
// необходимости в синхронной координации.
//
// Компромисс: при N нодах каждая видит capacity/N токенов, поэтому
// кратковременно после добавления ноды суммарная пропускная способность
// может немного просесть, пока gossip не обновит peerCount у всех.
// Для production это приемлемо — лучше небольшой undershoot, чем overshoot.
func (drl *DistributedRateLimiter) nodeQuota() int64 {
	n := drl.peerCount.Load()
	if n <= 1 {
		return drl.bucketCapacity
	}
	q := drl.bucketCapacity / n
	if q < 1 {
		return 1
	}
	return q
}

// nodeRate возвращает долю rate, выделенную данной ноде.
func (drl *DistributedRateLimiter) nodeRate() float64 {
	n := drl.peerCount.Load()
	if n <= 1 {
		return drl.ratePerSecond
	}
	return drl.ratePerSecond / float64(n)
}

// Stop останавливает лимитер.
func (drl *DistributedRateLimiter) Stop() {
	if !drl.shutdown.CompareAndSwap(false, true) {
		return
	}

	drl.cancel()

	if drl.gossipConn != nil {
		drl.gossipConn.Close()
	}

	done := make(chan struct{})
	go func() {
		drl.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		log.Printf("Warning: timeout waiting for goroutines to finish for node %s", drl.nodeID)
	}

	close(drl.updateChan)
	close(drl.lockRequestCh)
	close(drl.lockReleaseCh)
}

// lockManagerLoop управляет распределенными блокировками
func (drl *DistributedRateLimiter) lockManagerLoop() {
	defer drl.wg.Done()

	ticker := time.NewTicker(drl.gossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-drl.ctx.Done():
			return
		case req := <-drl.lockRequestCh:
			drl.handleLockRequest(req)
		case userID := <-drl.lockReleaseCh:
			drl.handleLockRelease(userID)
		case <-ticker.C:
			drl.broadcastLockState()
		}
	}
}

// lockCleanupLoop очищает истекшие блокировки
func (drl *DistributedRateLimiter) lockCleanupLoop() {
	defer drl.wg.Done()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-drl.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			drl.userLocks.Range(func(key, value interface{}) bool {
				lockInfo := value.(LockInfo)
				if now.After(lockInfo.ExpiresAt) {
					drl.userLocks.Delete(key)
					atomic.AddUint64(&drl.lockContentionCount, 1)
				}
				return true
			})
		}
	}
}

func (drl *DistributedRateLimiter) generateMessageID() [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[:8], uint64(time.Now().UnixNano()))
	rand.Read(id[8:])
	return id
}

func (drl *DistributedRateLimiter) isMessageDuplicate(msgID [16]byte) bool {
	if _, ok := drl.messageCache.Load(msgID); ok {
		return true
	}
	drl.messageCache.Store(msgID, time.Now())
	return false
}

func (drl *DistributedRateLimiter) cleanupMessageCache() {
	threshold := time.Now().Add(-1 * time.Second)
	drl.messageCache.Range(func(key, value interface{}) bool {
		if timestamp, ok := value.(time.Time); ok && timestamp.Before(threshold) {
			drl.messageCache.Delete(key)
		}
		return true
	})
}

func (drl *DistributedRateLimiter) acquireDistributedLock(ctx context.Context, userID string, timeout time.Duration) bool {
	req := LockRequest{
		NodeID:    drl.nodeID,
		Timestamp: time.Now().UnixNano(),
		ExpiresAt: time.Now().Add(drl.lockTimeout).UnixNano(),
	}

	select {
	case drl.lockRequestCh <- req:
	default:
	}

	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if drl.tryAcquireLocalLock(userID, req) {
			drl.broadcastLockAcquired(userID)
			atomic.AddUint64(&drl.lockGrantedCount, 1)
			return true
		}

		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}

	return false
}

func (drl *DistributedRateLimiter) tryAcquireLocalLock(userID string, req LockRequest) bool {
	drl.localLockMu.Lock()
	defer drl.localLockMu.Unlock()

	if lockInfo, ok := drl.userLocks.Load(userID); ok {
		info := lockInfo.(LockInfo)
		if time.Now().After(info.ExpiresAt) || info.Holder == drl.nodeID {
			// продолжаем
		} else {
			atomic.AddUint64(&drl.lockContentionCount, 1)
			return false
		}
	}

	drl.userLocks.Store(userID, LockInfo{
		Holder:    drl.nodeID,
		Acquired:  time.Now(),
		ExpiresAt: time.Now().Add(drl.lockTimeout),
		Version:   uint64(time.Now().UnixNano()),
	})

	return true
}

func (drl *DistributedRateLimiter) releaseDistributedLock(userID string) {
	drl.localLockMu.Lock()
	defer drl.localLockMu.Unlock()

	if lockInfo, ok := drl.userLocks.Load(userID); ok {
		info := lockInfo.(LockInfo)
		if info.Holder == drl.nodeID {
			drl.userLocks.Delete(userID)
			select {
			case drl.lockReleaseCh <- userID:
			default:
			}
		}
	}
}

func (drl *DistributedRateLimiter) handleLockRequest(req LockRequest) {
	if req.NodeID == drl.nodeID {
		return
	}
}

func (drl *DistributedRateLimiter) handleLockRelease(userID string) {
	drl.notifyWaitingUsersForUser(userID)
}

func (drl *DistributedRateLimiter) broadcastLockState() {
	lockReleases := make(map[string]NodeID)
	drl.userLocks.Range(func(key, value interface{}) bool {
		userID := key.(string)
		lockInfo := value.(LockInfo)
		if time.Now().Before(lockInfo.ExpiresAt) {
			lockReleases[userID] = lockInfo.Holder
		}
		return true
	})

	if len(lockReleases) == 0 {
		return
	}

	msg := GossipMessage{
		SenderID:     drl.nodeID,
		VectorClock:  drl.vectorClock.Load().(map[NodeID]int64),
		Timestamp:    time.Now().UnixNano(),
		MessageID:    drl.generateMessageID(),
		LockReleases: lockReleases,
		KnownPeers:   int(drl.peerCount.Load()),
	}

	drl.encodeAndSendMessage(msg)
}

func (drl *DistributedRateLimiter) broadcastLockAcquired(userID string) {
	lockRequests := map[string]LockRequest{
		userID: {
			NodeID:    drl.nodeID,
			Timestamp: time.Now().UnixNano(),
			ExpiresAt: time.Now().Add(drl.lockTimeout).UnixNano(),
		},
	}

	msg := GossipMessage{
		SenderID:     drl.nodeID,
		VectorClock:  drl.vectorClock.Load().(map[NodeID]int64),
		Timestamp:    time.Now().UnixNano(),
		MessageID:    drl.generateMessageID(),
		LockRequests: lockRequests,
		KnownPeers:   int(drl.peerCount.Load()),
	}

	drl.encodeAndSendMessage(msg)
}

// Allow проверяет, можно ли потребить tokens с распределенной блокировкой.
func (drl *DistributedRateLimiter) Allow(ctx context.Context, userID string, tokens int64) bool {
	if drl.shutdown.Load() {
		return false
	}

	if !drl.acquireDistributedLock(ctx, userID, 10*time.Millisecond) {
		return drl.allowOptimistic(ctx, userID, tokens)
	}
	defer drl.releaseDistributedLock(userID)

	return drl.allowWithLock(ctx, userID, tokens)
}

// allowWithLock выполняет проверку лимита с уже полученной блокировкой.
// Использует nodeQuota() и nodeRate() вместо глобального bucketCapacity/ratePerSecond.
func (drl *DistributedRateLimiter) allowWithLock(ctx context.Context, userID string, tokens int64) bool {
	wrapper := drl.getOrCreateUserWrapper(userID)

	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	now := time.Now()
	wrapper.data.LastUpdateNanos = now.UnixNano()
	wrapper.data.Version++

	// ИСПРАВЛЕНИЕ: передаём квоту конкретной ноды, а не глобальный capacity
	currentTokens := drl.calculateAvailableTokensForNode(wrapper.data, now)

	if currentTokens >= tokens {
		wrapper.data.PerNodeConsumption[drl.nodeID] += tokens
		drl.updateVectorClock()
		drl.notifyUpdate()
		return true
	}

	return false
}

// allowOptimistic - оптимистичная версия без блокировки для низкой конкуренции.
// Аналогично использует per-node квоту.
func (drl *DistributedRateLimiter) allowOptimistic(ctx context.Context, userID string, tokens int64) bool {
	wrapper := drl.getOrCreateUserWrapper(userID)

	wrapper.mu.RLock()
	now := time.Now()
	oldData := *wrapper.data
	wrapper.mu.RUnlock()

	// ИСПРАВЛЕНИЕ: считаем только квоту этой ноды
	currentTokens := drl.calculateAvailableTokensForNode(&oldData, now)

	if currentTokens < tokens {
		return false
	}

	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	if wrapper.data.Version != oldData.Version {
		currentTokens = drl.calculateAvailableTokensForNode(wrapper.data, time.Now())
		if currentTokens < tokens {
			return false
		}
	}

	wrapper.data.PerNodeConsumption[drl.nodeID] += tokens
	wrapper.data.LastUpdateNanos = time.Now().UnixNano()
	wrapper.data.Version++

	drl.updateVectorClock()
	drl.notifyUpdate()
	return true
}

// calculateAvailableTokensForNode вычисляет доступное количество токенов
// только для данной ноды (её персональная квота).
//
// Это ключевое исправление бага: исходная calculateAvailableTokens суммировала
// потребление ВСЕХ нод и сравнивала с глобальным bucketCapacity. При gossip-задержке
// каждая нода думала, что чужое потребление = 0, и выдавала полный bucket.
//
// Теперь каждая нода управляет только своей долей:
//   - quota = bucketCapacity / numPeers
//   - rate  = ratePerSecond  / numPeers
//   - consumed = PerNodeConsumption[thisNode] только
//
// Gossip по-прежнему нужен для наблюдаемости и пересчёта peerCount,
// но корректность лимита больше не зависит от его своевременности.
func (drl *DistributedRateLimiter) calculateAvailableTokensForNode(uc *UserConsumption, now time.Time) int64 {
	quota := drl.nodeQuota()
	rate := drl.nodeRate()

	consumed := uc.PerNodeConsumption[drl.nodeID]

	timePassed := now.UnixNano() - uc.LastUpdateNanos
	if timePassed < 0 {
		timePassed = 0
	}

	const maxTimePassed = int64(1e18)
	if timePassed > maxTimePassed {
		timePassed = maxTimePassed
	}

	refilled := int64(float64(timePassed) / 1e9 * rate)
	available := quota - consumed + refilled

	if available > quota {
		available = quota
	}
	if available < 0 {
		available = 0
	}
	return available
}

// calculateAvailableTokens оставлен для совместимости (gossip-статистика).
// Не используется в hot path Allow().
func (drl *DistributedRateLimiter) calculateAvailableTokens(uc *UserConsumption, now time.Time) int64 {
	var totalConsumed int64
	for _, consumed := range uc.PerNodeConsumption {
		totalConsumed += consumed
	}

	timePassed := now.UnixNano() - uc.LastUpdateNanos
	if timePassed < 0 {
		timePassed = 0
	}

	const maxTimePassed = int64(1e18)
	if timePassed > maxTimePassed {
		timePassed = maxTimePassed
	}

	refilled := int64(float64(timePassed) / 1e9 * drl.ratePerSecond)
	available := drl.bucketCapacity - totalConsumed + refilled

	if available > drl.bucketCapacity {
		available = drl.bucketCapacity
	}
	if available < 0 {
		available = 0
	}
	return available
}

func (drl *DistributedRateLimiter) notifyWaitingUsersForUser(userID string) {
	value, ok := drl.waitingReqs.Load(userID)
	if !ok {
		return
	}

	channels := value.(*[]chan bool)

	drl.vectorClockMu.Lock()
	chansCopy := make([]chan bool, len(*channels))
	copy(chansCopy, *channels)
	*channels = nil
	drl.waitingReqs.Delete(userID)
	drl.vectorClockMu.Unlock()

	for _, ch := range chansCopy {
		select {
		case ch <- true:
		default:
		}
		close(ch)
	}
}

func (drl *DistributedRateLimiter) getOrCreateUserWrapper(userID string) *consumptionWrapper {
	value, _ := drl.Users.LoadOrStore(userID, &consumptionWrapper{
		data: &UserConsumption{
			PerNodeConsumption: make(map[NodeID]int64),
			LastUpdateNanos:    time.Now().UnixNano(),
			Version:            0,
		},
	})
	return value.(*consumptionWrapper)
}

func (drl *DistributedRateLimiter) updateVectorClock() {
	drl.vectorClockMu.Lock()
	defer drl.vectorClockMu.Unlock()

	oldVC := drl.vectorClock.Load().(map[NodeID]int64)
	newVC := make(map[NodeID]int64, len(oldVC)+1)

	for k, v := range oldVC {
		newVC[k] = v
	}
	newVC[drl.nodeID] = oldVC[drl.nodeID] + 1

	drl.vectorClock.Store(newVC)
}

func (drl *DistributedRateLimiter) notifyUpdate() {
	select {
	case drl.updateChan <- struct{}{}:
	default:
	}
}

// gossipReceiverLoop слушает UDP multicast.
func (drl *DistributedRateLimiter) gossipReceiverLoop() {
	defer drl.wg.Done()

	buf := make([]byte, 65535)

	for {
		select {
		case <-drl.ctx.Done():
			return
		default:
			if err := drl.gossipConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
				continue
			}

			n, _, err := drl.gossipConn.ReadFromUDP(buf)
			if err != nil {
				continue
			}

			if n < 10 {
				continue
			}

			data := make([]byte, n)
			copy(data, buf[:n])
			drl.processGossipMessage(data)
		}
	}
}

func (drl *DistributedRateLimiter) processGossipMessage(data []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in gossip decode: %v", r)
			atomic.AddUint64(&drl.droppedMessages, 1)
		}
	}()

	drl.cleanupMessageCache()

	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)

	var msg GossipMessage
	if err := dec.Decode(&msg); err != nil {
		if !drl.shutdown.Load() {
			atomic.AddUint64(&drl.droppedMessages, 1)
		}
		return
	}

	if msg.MessageID != [16]byte{} && drl.isMessageDuplicate(msg.MessageID) {
		return
	}

	if msg.SenderID == drl.nodeID {
		return
	}

	if msg.VectorClock == nil {
		return
	}

	if len(msg.LockRequests) > 0 {
		drl.mergeLockRequests(msg)
	}
	if len(msg.LockReleases) > 0 {
		drl.mergeLockReleases(msg)
	}

	drl.mergeGossipUpdate(msg)
}

func (drl *DistributedRateLimiter) mergeLockRequests(msg GossipMessage) {
	for userID, req := range msg.LockRequests {
		if req.NodeID == drl.nodeID {
			continue
		}

		drl.localLockMu.Lock()
		if existing, ok := drl.userLocks.Load(userID); ok {
			info := existing.(LockInfo)
			if req.Timestamp > info.Acquired.UnixNano() {
				drl.userLocks.Store(userID, LockInfo{
					Holder:    req.NodeID,
					Acquired:  time.Unix(0, req.Timestamp),
					ExpiresAt: time.Unix(0, req.ExpiresAt),
				})
			}
		} else {
			drl.userLocks.Store(userID, LockInfo{
				Holder:    req.NodeID,
				Acquired:  time.Unix(0, req.Timestamp),
				ExpiresAt: time.Unix(0, req.ExpiresAt),
			})
		}
		drl.localLockMu.Unlock()
	}
}

func (drl *DistributedRateLimiter) mergeLockReleases(msg GossipMessage) {
	for userID, holderID := range msg.LockReleases {
		if holderID == drl.nodeID {
			continue
		}

		drl.localLockMu.Lock()
		if existing, ok := drl.userLocks.Load(userID); ok {
			info := existing.(LockInfo)
			if info.Holder == holderID {
				drl.userLocks.Delete(userID)
				drl.notifyWaitingUsersForUser(userID)
			}
		}
		drl.localLockMu.Unlock()
	}
}

// gossipSenderLoop периодически рассылает состояние.
func (drl *DistributedRateLimiter) gossipSenderLoop() {
	defer drl.wg.Done()

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-drl.ctx.Done():
			return
		case <-ticker.C:
			if drl.shutdown.Load() {
				return
			}
			drl.SendGossip()
		case <-drl.updateChan:
			if !drl.shutdown.Load() {
				drl.SendGossip()
			}
		}
	}
}

func (drl *DistributedRateLimiter) updateHandlerLoop() {
	defer drl.wg.Done()

	for {
		select {
		case <-drl.ctx.Done():
			return
		case <-drl.updateChan:
			drl.notifyWaitingUsers()
		}
	}
}

func (drl *DistributedRateLimiter) notifyWaitingUsers() {
	drl.waitingReqs.Range(func(key, value interface{}) bool {
		userID := key.(string)
		drl.notifyWaitingUsersForUser(userID)
		return true
	})
}

// SendGossip собирает и отправляет gossip сообщение.
func (drl *DistributedRateLimiter) SendGossip() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in SendGossip: %v", r)
		}
	}()

	updates := drl.collectUpdates()
	vc := drl.vectorClock.Load().(map[NodeID]int64)

	msg := GossipMessage{
		SenderID:    drl.nodeID,
		VectorClock: vc,
		Updates:     updates,
		Timestamp:   time.Now().UnixNano(),
		MessageID:   drl.generateMessageID(),
		// Сообщаем соседям, сколько пиров мы знаем —
		// они используют max(local, remote) для пересчёта квоты.
		KnownPeers: int(drl.peerCount.Load()),
	}

	drl.encodeAndSendMessage(msg)
}

func (drl *DistributedRateLimiter) collectUpdates() map[string]map[NodeID]int64 {
	updates := make(map[string]map[NodeID]int64)

	const maxUpdates = 200
	updateCount := 0

	drl.Users.Range(func(key, value interface{}) bool {
		if updateCount >= maxUpdates {
			return false
		}

		userID, ok := key.(string)
		if !ok {
			return true
		}

		wrapper, ok := value.(*consumptionWrapper)
		if !ok {
			return true
		}

		wrapper.mu.RLock()
		consumptionCopy := make(map[NodeID]int64, len(wrapper.data.PerNodeConsumption))
		for nid, cons := range wrapper.data.PerNodeConsumption {
			consumptionCopy[nid] = cons
		}
		wrapper.mu.RUnlock()

		if len(consumptionCopy) > 0 {
			updates[userID] = consumptionCopy
			updateCount++
		}

		return true
	})

	return updates
}

func (drl *DistributedRateLimiter) encodeAndSendMessage(msg GossipMessage) {
	buf := drl.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer drl.bufferPool.Put(buf)

	enc := gob.NewEncoder(buf)
	if err := enc.Encode(msg); err != nil {
		if !drl.shutdown.Load() {
			atomic.AddUint64(&drl.droppedMessages, 1)
		}
		return
	}

	if buf.Len() > 64000 {
		if !drl.shutdown.Load() {
			atomic.AddUint64(&drl.droppedMessages, 1)
		}
		return
	}

	if err := drl.gossipConn.SetWriteDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
		return
	}

	if _, err := drl.gossipConn.WriteToUDP(buf.Bytes(), drl.gossipAddr); err != nil {
		if !drl.shutdown.Load() {
			atomic.AddUint64(&drl.droppedMessages, 1)
		}
	}
}

// mergeGossipUpdate применяет входящее gossip сообщение.
func (drl *DistributedRateLimiter) mergeGossipUpdate(msg GossipMessage) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in mergeGossipUpdate: %v", r)
		}
	}()

	if drl.isMessageStale(msg) {
		return
	}

	// Регистрируем нового пира и обновляем peerCount.
	// Используем max(local, remote.KnownPeers) для монотонного роста:
	// нода, которая видит больше пиров, передаёт эти знания другим.
	drl.peers.Store(msg.SenderID, struct{}{})
	drl.updatePeerCount(msg.KnownPeers)

	drl.applyUserUpdates(msg)
	drl.mergeVectorClock(msg.VectorClock)
	drl.notifyUpdate()
}

// updatePeerCount пересчитывает актуальное количество нод.
// Берём максимум из фактически зарегистрированных пиров и того,
// что сообщил отправитель — это даёт быстрое распространение
// информации о новых нодах через gossip.
func (drl *DistributedRateLimiter) updatePeerCount(remotePeerCount int) {
	localCount := 0
	drl.peers.Range(func(_, _ interface{}) bool {
		localCount++
		return true
	})

	newCount := localCount
	if remotePeerCount > newCount {
		newCount = remotePeerCount
	}

	if int64(newCount) != drl.peerCount.Load() {
		drl.peerCount.Store(int64(newCount))
	}
}

func (drl *DistributedRateLimiter) isMessageStale(msg GossipMessage) bool {
	localVC := drl.vectorClock.Load().(map[NodeID]int64)

	for nodeID, remoteVer := range msg.VectorClock {
		if localVer, ok := localVC[nodeID]; !ok || remoteVer > localVer {
			return false
		}
	}
	return true
}

func (drl *DistributedRateLimiter) applyUserUpdates(msg GossipMessage) {
	for userID, remoteConsumption := range msg.Updates {
		if remoteConsumption == nil {
			continue
		}

		wrapper := drl.getOrCreateUserWrapper(userID)

		wrapper.mu.Lock()

		if msg.Timestamp > wrapper.data.LastUpdateNanos {
			wrapper.data.LastUpdateNanos = msg.Timestamp
			wrapper.data.Version++
		}

		for remoteNodeID, remoteTokens := range remoteConsumption {
			if remoteNodeID == "" || remoteTokens < 0 || remoteTokens > drl.bucketCapacity*2 {
				continue
			}

			if remoteTokens > wrapper.data.PerNodeConsumption[remoteNodeID] {
				wrapper.data.PerNodeConsumption[remoteNodeID] = remoteTokens
				wrapper.data.Version++
			}
		}

		wrapper.mu.Unlock()
	}
}

// mergeVectorClock обновляет векторные часы
func (drl *DistributedRateLimiter) mergeVectorClock(remoteVC map[NodeID]int64) {
	drl.vectorClockMu.Lock()
	defer drl.vectorClockMu.Unlock()

	localVC := drl.vectorClock.Load().(map[NodeID]int64)

	needUpdate := false
	for k, v := range remoteVC {
		if v > localVC[k] {
			needUpdate = true
			break
		}
	}

	if !needUpdate {
		return
	}

	newVC := make(map[NodeID]int64, len(localVC))
	for k, v := range localVC {
		newVC[k] = v
	}
	for k, v := range remoteVC {
		if v > newVC[k] {
			newVC[k] = v
		}
	}

	drl.vectorClock.Store(newVC)
}

// cleanupLoop удаляет неактивных пользователей.
func (drl *DistributedRateLimiter) cleanupLoop() {
	defer drl.wg.Done()
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-drl.ctx.Done():
			return
		case <-ticker.C:
			if drl.shutdown.Load() {
				return
			}
			drl.cleanupStaleUsers()
			drl.cleanupMessageCache()
		}
	}
}

// cleanupStaleUsers удаляет неактивных пользователей
func (drl *DistributedRateLimiter) cleanupStaleUsers() {
	cutoff := time.Now().Add(-5 * time.Minute).UnixNano()

	drl.Users.Range(func(key, value interface{}) bool {
		wrapper := value.(*consumptionWrapper)

		wrapper.mu.RLock()
		lastUpdate := wrapper.data.LastUpdateNanos
		wrapper.mu.RUnlock()

		if lastUpdate < cutoff {
			drl.Users.Delete(key)
		}
		return true
	})
}

// GetStats возвращает статистику лимитера
func (drl *DistributedRateLimiter) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	stats["node_id"] = drl.nodeID
	stats["dropped_messages"] = atomic.LoadUint64(&drl.droppedMessages)
	stats["lock_contention"] = atomic.LoadUint64(&drl.lockContentionCount)
	stats["lock_granted"] = atomic.LoadUint64(&drl.lockGrantedCount)
	stats["peer_count"] = drl.peerCount.Load()
	stats["node_quota"] = drl.nodeQuota()
	stats["node_rate"] = drl.nodeRate()

	userCount := 0
	drl.Users.Range(func(_, _ interface{}) bool {
		userCount++
		return true
	})
	stats["user_count"] = userCount

	waitingCount := 0
	drl.waitingReqs.Range(func(_, value interface{}) bool {
		channels := value.(*[]chan bool)
		waitingCount += len(*channels)
		return true
	})
	stats["waiting_requests"] = waitingCount

	lockCount := 0
	drl.userLocks.Range(func(_, _ interface{}) bool {
		lockCount++
		return true
	})
	stats["active_locks"] = lockCount

	return stats
}

// RegisterPeers явно регистрирует известные ноды кластера.
// Вызывается при инициализации, когда состав кластера известен заранее.
// Это детерминированный путь задать квоту без ожидания gossip.
func (drl *DistributedRateLimiter) RegisterPeers(peers []NodeID) {
	for _, p := range peers {
		drl.peers.Store(p, struct{}{})
	}
	count := int64(0)
	drl.peers.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	drl.peerCount.Store(count)
	log.Printf("Node %s: registered %d peers, quota=%d, rate=%.2f",
		drl.nodeID, count, drl.nodeQuota(), drl.nodeRate())
}
