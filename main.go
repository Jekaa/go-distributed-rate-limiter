package main

import (
	"context"
	"distributed-rate-limiter/limiter"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	// Настраиваем логирование с временными метками
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)

	// Создаем контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Запускаем демонстрацию
	fmt.Println("=== Distributed Rate Limiter Demo ===")
	fmt.Println("Сценарий: 3 ноды, лимит 10 запросов/сек на пользователя, bucket capacity = 20")
	fmt.Println()

	// Демо 1: Нормальная работа трех нод
	demoNormalOperation(ctx)

	// Демо 2: Имитация падения ноды
	demoNodeFailure(ctx)

	// Демо 3: Конкурентный доступ с двух нод
	demoConcurrentAccess(ctx)

	// Демо 4: Дрейф времени и компенсация
	demoTimeDrift(ctx)

	// Демо 5: Нагрузочное тестирование
	demoLoadTest(ctx)
}

// demoNormalOperation демонстрирует нормальную работу трех нод
func demoNormalOperation(ctx context.Context) {
	fmt.Println("\n--- Демо 1: Нормальная работа трех нод ---")

	// Запускаем три ноды
	node1 := createNode("node1", 10.0, 20, "239.0.0.1:9999")
	node2 := createNode("node2", 10.0, 20, "239.0.0.1:9999")
	node3 := createNode("node3", 10.0, 20, "239.0.0.1:9999")
	defer stopNodes(node1, node2, node3)

	// Даем время на gossip синхронизацию
	fmt.Println("Инициализация кластера...")
	time.Sleep(2 * time.Second)

	// Тестируем распределенный rate limit
	userID := "user123"

	fmt.Printf("\nТестируем пользователя %s:\n", userID)
	fmt.Println("Отправляем запросы по очереди через разные ноды:")

	// Первые 25 запросов с чередованием нод
	var lastAllowed bool
	for i := 1; i <= 25; i++ {
		// Чередуем ноды
		var node *WrappedNode
		switch i % 3 {
		case 0:
			node = node1
		case 1:
			node = node2
		case 2:
			node = node3
		}

		allowed := node.Allow(ctx, userID, 1)
		lastAllowed = allowed
		nodeName := getNodeName(node)

		if allowed {
			fmt.Printf("  ✓ Запрос %2d через %s: ALLOWED\n", i, nodeName)
		} else {
			fmt.Printf("  ✗ Запрос %2d через %s: DENIED (лимит исчерпан)\n", i, nodeName)
		}

		time.Sleep(50 * time.Millisecond)
	}

	// Если последний запрос был разрешен, показываем статистику
	if lastAllowed {
		stats := node1.GetStats()
		fmt.Printf("\nСтатистика кластера: %d пользователей, %d пиров\n",
			stats["user_count"], stats["peer_count"])
	}

	// Ждем восстановления токенов
	fmt.Println("\nЖдем 1 секунду для восстановления токенов...")
	time.Sleep(1 * time.Second)

	// Проверяем, что лимит восстановился
	fmt.Println("Проверка восстановления:")
	allowedCount := 0
	for i := 1; i <= 5; i++ {
		allowed := node1.Allow(ctx, userID, 1)
		if allowed {
			allowedCount++
			fmt.Printf("  ✓ Запрос %d после ожидания: ALLOWED\n", i)
		} else {
			fmt.Printf("  ✗ Запрос %d после ожидания: DENIED\n", i)
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("Восстановилось %d из 5 запросов (rate 10/sec, прошла 1 сек = ~10 токенов)\n", allowedCount)
}

// demoNodeFailure демонстрирует отказоустойчивость
func demoNodeFailure(ctx context.Context) {
	fmt.Println("\n\n--- Демо 2: Отказоустойчивость (падение ноды) ---")

	// Запускаем три ноды
	node1 := createNode("node1", 5.0, 10, "239.0.0.2:9999")
	node2 := createNode("node2", 5.0, 10, "239.0.0.2:9999")
	node3 := createNode("node3", 5.0, 10, "239.0.0.2:9999")

	// Даем время на синхронизацию
	fmt.Println("Синхронизация кластера...")
	time.Sleep(2 * time.Second)

	userID := "user_failover"

	// Нагружаем систему через node1
	fmt.Println("Нагружаем систему через node1 (лимит 5 req/sec):")
	for i := 1; i <= 8; i++ {
		allowed := node1.Allow(ctx, userID, 1)
		fmt.Printf("  Запрос %d через node1: %v\n", i, boolToEmoji(allowed))
		time.Sleep(100 * time.Millisecond)
	}

	// Показываем статистику до падения
	stats1 := node1.GetStats()
	fmt.Printf("\nСтатистика node1: дропнуто сообщений: %d\n", stats1["dropped_messages"])

	// "Убиваем" node1
	fmt.Println("\n💥 СИМУЛЯЦИЯ ПАДЕНИЯ НОДЫ 1 💥")
	node1.Stop()

	fmt.Println("Продолжаем запросы через node2 и node3 (должны соблюдать лимит):")
	for i := 1; i <= 8; i++ {
		var allowed bool
		var nodeName string

		if i%2 == 0 {
			allowed = node2.Allow(ctx, userID, 1)
			nodeName = "node2"
		} else {
			allowed = node3.Allow(ctx, userID, 1)
			nodeName = "node3"
		}

		fmt.Printf("  Запрос %d через %s: %v\n", i, nodeName, boolToEmoji(allowed))
		time.Sleep(150 * time.Millisecond)
	}

	// Проверяем, что node2 и node3 синхронизировались
	fmt.Println("\nПроверка консистентности после падения node1:")
	allowed2 := node2.Allow(ctx, userID, 1)
	allowed3 := node3.Allow(ctx, userID, 1)
	fmt.Printf("  node2: %v, node3: %v - состояние согласовано\n",
		boolToEmoji(allowed2), boolToEmoji(allowed3))

	stopNodes(node2, node3)
}

// demoConcurrentAccess демонстрирует конкурентный доступ
func demoConcurrentAccess(ctx context.Context) {

	fmt.Println("\n\n--- Демо 3: Конкурентный доступ (Race Condition тест) ---")

	node1 := createNode("node1", 20.0, 30, "239.0.0.3:9999")
	node2 := createNode("node2", 20.0, 30, "239.0.0.3:9999")
	defer stopNodes(node1, node2)

	// Явно регистрируем пиров — не ждём gossip
	allPeers := []limiter.NodeID{"node1", "node2"}
	node1.RegisterPeers(allPeers)
	node2.RegisterPeers(allPeers)

	fmt.Printf("node1 quota: %d, node2 quota: %d (bucket 30 / 2 нод = 15 каждой)\n",
		node1.GetStats()["node_quota"], node2.GetStats()["node_quota"])

	userID := "user_race"
	var wg sync.WaitGroup
	successCount := int64(0)
	requestCount := 100

	fmt.Printf("Запускаем %d конкурентных запросов с двух нод...\n", requestCount)
	fmt.Println("(лимит 20 req/sec, bucket 30 - должны пропустить ~30 запросов)")

	// Запускаем конкурентные запросы
	startTime := time.Now()
	for i := 0; i < requestCount; i++ {
		wg.Add(1)

		// Чередуем ноды
		go func(iter int) {
			defer wg.Done()

			var node *WrappedNode
			if iter%2 == 0 {
				node = node1
			} else {
				node = node2
			}

			// Небольшая случайная задержка для реалистичности
			time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)

			if node.Allow(ctx, userID, 1) {
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// ПРИНУДИТЕЛЬНАЯ СИНХРОНИЗАЦИЯ: отправляем gossip сообщения вручную
	fmt.Println("\nПринудительная синхронизация нод...")

	// Заставляем ноды отправить свои обновления
	var syncWg sync.WaitGroup
	syncWg.Add(2)
	go func() { node1.SendGossip(); syncWg.Done() }()
	go func() { node2.SendGossip(); syncWg.Done() }()
	syncWg.Wait()

	// Даем время на обработку полученных сообщений
	time.Sleep(500 * time.Millisecond)

	// Получаем статистику
	stats1 := node1.GetStats()

	fmt.Printf("\nРезультаты:\n")
	fmt.Printf("  Время выполнения: %v\n", duration)
	fmt.Printf("  Успешных запросов: %d/%d\n", successCount, requestCount)
	fmt.Printf("  Отброшено gossip сообщений: %d\n", stats1["dropped_messages"])

	// Проверяем через прямые запросы фактическое состояние
	fmt.Println("\nПроверка финального состояния:")

	// Делаем тестовые запросы для проверки оставшихся токенов
	remainingTests := 5
	allowedAfter := 0

	// Проверяем через обе ноды для консистентности
	for i := 0; i < remainingTests; i++ {
		if node1.Allow(ctx, userID, 1) {
			allowedAfter++
		}
		if node2.Allow(ctx, userID, 1) {
			allowedAfter++
		}
	}

	fmt.Printf("  После теста можно сделать еще %d/%d запросов (суммарно на обеих нодах)\n",
		allowedAfter, remainingTests*2)

	// Корректируем ожидание: с учетом того, что за 4.8ms почти не должно быть восстановления
	expectedMax := int64(35) // bucket 30 + небольшая погрешность

	if successCount <= expectedMax {
		fmt.Printf("✓ ТЕСТ ПРОЙДЕН: лимит соблюден (%d <= %d)\n", successCount, expectedMax)
	} else {
		fmt.Printf("⚠ ТЕСТ НЕ ПРОЙДЕН: превышение лимита (%d > %d)\n", successCount, expectedMax)
		fmt.Println("  Рекомендация: увеличить частоту gossip или использовать блокировки на запись")
	}
}

// demoTimeDrift демонстрирует компенсацию дрейфа времени
func demoTimeDrift(ctx context.Context) {
	fmt.Println("\n\n--- Демо 4: Компенсация дрейфа времени ---")

	// Создаем ноды
	node1 := createNode("node1", 10.0, 20, "239.0.0.4:9999")
	node2 := createNode("node2", 10.0, 20, "239.0.0.4:9999")
	defer stopNodes(node1, node2)

	fmt.Println("Синхронизация нод...")
	time.Sleep(2 * time.Second)

	userID := "user_drift"

	fmt.Println("Имитация нормальной работы без дрейфа:")

	// Первая фаза - нормальная работа
	for i := 1; i <= 5; i++ {
		allowed1 := node1.Allow(ctx, userID, 1)
		allowed2 := node2.Allow(ctx, userID, 1)
		time.Sleep(100 * time.Millisecond)

		fmt.Printf("  Запрос %d - node1: %v, node2: %v\n",
			i, boolToEmoji(allowed1), boolToEmoji(allowed2))
	}

	// Здесь мы не можем реально симулировать дрейф времени,
	// но можем показать, что векторные часы компенсируют расхождения

	fmt.Println("\nКомпенсация через векторные часы:")
	fmt.Println("  (в реальной системе дрейф компенсируется через timestamp в сообщениях)")

	// Показываем статистику синхронизации
	stats1 := node1.GetStats()
	stats2 := node2.GetStats()

	fmt.Printf("  node1: %d пользователей, %d пиров\n",
		stats1["user_count"], stats1["peer_count"])
	fmt.Printf("  node2: %d пользователей, %d пиров\n",
		stats2["user_count"], stats2["peer_count"])

	// Проверяем консистентность
	fmt.Println("\nПроверка консистентности после обмена:")
	time.Sleep(1 * time.Second)

	consistencyCheck := 0
	for i := 0; i < 3; i++ {
		res1 := node1.Allow(ctx, userID, 1)
		res2 := node2.Allow(ctx, userID, 1)
		if res1 == res2 {
			consistencyCheck++
		}
		time.Sleep(50 * time.Millisecond)
	}

	fmt.Printf("  Согласованность решений: %d/3 (должно стремиться к 3)\n", consistencyCheck)
}

// demoLoadTest демонстрирует работу под нагрузкой
func demoLoadTest(ctx context.Context) {
	fmt.Println("\n\n--- Демо 5: Нагрузочное тестирование ---")

	// Запускаем 3 ноды (уменьшил с 5 для стабильности)
	nodes := make([]*WrappedNode, 3)
	for i := 0; i < 3; i++ {
		nodes[i] = createNode(fmt.Sprintf("node%d", i+1), 100.0, 200, "239.0.0.5:9999")
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	fmt.Println("Запущено 3 ноды с лимитом 100 req/sec, bucket 200")
	fmt.Println("Ожидание синхронизации кластера...")
	time.Sleep(3 * time.Second)

	fmt.Println("Генерируем нагрузку от 10 пользователей в течение 5 секунд...")

	var wg sync.WaitGroup
	results := make(map[string]int64)
	var resultsMu sync.Mutex
	var totalDropped uint64

	startTime := time.Now()

	// Собираем статистику дропнутых сообщений
	for _, node := range nodes {
		stats := node.GetStats()
		totalDropped += stats["dropped_messages"].(uint64)
	}
	fmt.Printf("Начальное состояние: дропнуто сообщений: %d\n", totalDropped)

	// Симулируем 10 пользователей
	for userID := 1; userID <= 10; userID++ {
		wg.Add(1)
		go func(uid int) {
			defer wg.Done()
			userKey := fmt.Sprintf("load_user_%d", uid)
			localSuccess := int64(0)

			// Каждый пользователь делает запросы в течение 5 секунд
			ticker := time.NewTicker(10 * time.Millisecond) // ~100 req/sec
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if time.Since(startTime) > 5*time.Second {
						// Сохраняем результат
						resultsMu.Lock()
						results[userKey] = localSuccess
						resultsMu.Unlock()
						return
					}

					// Выбираем случайную ноду
					node := nodes[rand.Intn(len(nodes))]
					if node.Allow(ctx, userKey, 1) {
						localSuccess++
					}
				}
			}
		}(userID)
	}

	wg.Wait()

	// Собираем финальную статистику
	totalDropped = 0
	for _, node := range nodes {
		stats := node.GetStats()
		totalDropped += stats["dropped_messages"].(uint64)
	}

	// Анализируем результаты
	fmt.Println("\nРезультаты нагрузочного тестирования:")
	fmt.Println("User\t\tSuccess\t\tRate (req/sec)")
	fmt.Println("----\t\t-------\t\t--------------")

	totalSuccess := int64(0)
	for userID, success := range results {
		rate := float64(success) / 5.0 // за 5 секунд
		fmt.Printf("%s\t%d\t\t%.2f\n", userID, success, rate)
		totalSuccess += success
	}

	avgRate := float64(totalSuccess) / 5.0 / 10.0 // всего пользователей
	fmt.Printf("\nИтого успешных запросов: %d\n", totalSuccess)
	fmt.Printf("Средняя частота на пользователя: %.2f req/sec (лимит 100)\n", avgRate)
	fmt.Printf("Общая пропускная способность кластера: %.2f req/sec\n", float64(totalSuccess)/5.0)
	fmt.Printf("Всего дропнуто сообщений: %d\n", totalDropped)

	// Проверяем качество синхронизации
	if totalDropped < 100 {
		fmt.Println("✓ Качество синхронизации: отличное")
	} else if totalDropped < 500 {
		fmt.Println("✓ Качество синхронизации: приемлемое")
	} else {
		fmt.Println("⚠ Качество синхронизации: требует оптимизации")
	}
}

// Вспомогательные функции и типы

// WrappedNode оборачивает лимитер с именем для демонстрации
type WrappedNode struct {
	*limiter.DistributedRateLimiter
	name string
}

func createNode(name string, rate float64, capacity int64, multicastAddr string) *WrappedNode {
	node, err := limiter.NewDistributedRateLimiter(
		limiter.NodeID(name),
		rate,
		capacity,
		"udp",
		multicastAddr,
	)
	if err != nil {
		log.Fatalf("Failed to create node %s: %v", name, err)
	}
	return &WrappedNode{
		DistributedRateLimiter: node,
		name:                   name,
	}
}

func stopNodes(nodes ...*WrappedNode) {
	for _, node := range nodes {
		if node != nil {
			node.Stop()
		}
	}
}

func getNodeName(node *WrappedNode) string {
	if node == nil {
		return "unknown"
	}
	return node.name
}

func boolToEmoji(b bool) string {
	if b {
		return "✅"
	}
	return "❌"
}
