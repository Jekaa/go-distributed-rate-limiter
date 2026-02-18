# Distributed Rate Limiter

Распределённый лимитер скорости на основе Gossip-протокола с поддержкой квот на ноду и распределённых блокировок.

## Архитектура

### Ключевая идея
Вместо глобального bucket'а, за синхронизацией которого нужно следить через gossip, каждая нода работает с **собственной квотой**:
- `nodeQuota = bucketCapacity / количество_нод`
- `nodeRate = ratePerSecond / количество_нод`

Это гарантирует корректность лимитов даже при задержках gossip, ценой небольшого undershoot'а при добавлении новых нод.

### Компоненты

1. **Gossip-протокол** - обмен состоянием между нодами (UDP multicast)
2. **Per-node квоты** - детерминированное разделение ресурсов
3. **Распределённые блокировки** - опциональная координация для критических секций
4. **Векторные часы** - детекция устаревших сообщений

## Установка

```bash
go get github.com/Jekaa/go-distributed-rate-limiter
```

## Использование

### Базовый пример

```go
package main

import (
    "context"
    "log"
    "time"
    "limiter"
)

func main() {
    // Создание ноды лимитера
    nodeID := limiter.NodeID("node-1")
    
    drl, err := limiter.NewDistributedRateLimiter(
        nodeID,           // ID ноды
        100.0,            // ratePerSecond - общая скорость для всех нод
        1000,             // bucketCapacity - общий размер bucket'а
        "udp",            // gossipNetwork
        "239.0.0.1:9999", // multicast адрес
    )
    if err != nil {
        log.Fatal(err)
    }
    defer drl.Stop()
    
    // Регистрация известных пиров (опционально, для быстрого старта)
    drl.RegisterPeers([]limiter.NodeID{"node-2", "node-3"})
    
    // Проверка лимитов
    ctx := context.Background()
    userID := "user-123"
    
    if drl.Allow(ctx, userID, 10) {
        log.Println("Request allowed")
    } else {
        log.Println("Rate limit exceeded")
    }
    
    // Получение статистики
    stats := drl.GetStats()
    log.Printf("Stats: %+v", stats)
}
```

### Конфигурация

```go
// Создание с кастомными параметрами
drl := &DistributedRateLimiter{
    nodeID:         "node-1",
    ratePerSecond:  1000,     // 1000 запросов в секунду суммарно
    bucketCapacity: 10000,    // bucket на 10000 токенов
    lockTimeout:    50 * time.Millisecond,
    gossipInterval: 5 * time.Millisecond,
}
```

## API

### Основные методы

#### `Allow(ctx context.Context, userID string, tokens int64) bool`
Проверяет, можно ли потребить указанное количество токенов для пользователя.
- Сначала пытается получить распределённую блокировку
- При успехе вызывает `allowWithLock`
- При неудаче вызывает `allowOptimistic`

#### `Stop()`
Корректно останавливает лимитер, закрывая все соединения и горутины.

#### `RegisterPeers(peers []NodeID)`
Явно регистрирует известные ноды кластера. Позволяет сразу установить корректную квоту без ожидания gossip.

#### `GetStats() map[string]interface{}`
Возвращает статистику работы лимитера:
- `node_id` - идентификатор ноды
- `dropped_messages` - количество потерянных gossip-сообщений
- `lock_contention` - количество конфликтов блокировок
- `lock_granted` - количество выданных блокировок
- `peer_count` - количество известных нод
- `node_quota` - текущая квота ноды
- `node_rate` - текущая скорость ноды
- `user_count` - количество отслеживаемых пользователей
- `waiting_requests` - количество ожидающих запросов
- `active_locks` - количество активных блокировок

## Механизмы работы

### Gossip-протокол
- Периодическая рассылка состояния (по умолчанию каждые 5 мс)
- Multicast UDP для эффективного распространения
- Детекция дубликатов через MessageID
- Векторные часы для определения устаревших сообщений

### Распределённые блокировки
- Временные блокировки (timeout-based)
- Автоматическое истечение через `lockTimeout`
- Широковещательное оповещение о захвате/освобождении

### Обработка состояний
- **UserConsumption** - хранение потребления по пользователям
- **consumptionWrapper** - потокобезопасная обёртка с RWMutex
- **sync.Map** для конкурентного доступа к пользователям и пирам

## Производительность

### Оптимизации
1. **Пул буферов** - переиспользование bytes.Buffer для gossip-сообщений
2. **Атомики** - для счётчиков (peerCount, droppedMessages)
3. **RWMutex** - для чтения без блокировок в оптимистичном сценарии
4. **Каналы с буфером** - асинхронная обработка событий

### Hot path
Метод `allowOptimistic` использует:
1. Read-lock для чтения текущего состояния
2. Compare-and-swap через Version
3. Write-lock только при реальном изменении

## Обработка ошибок

- Panic recovery во всех горутинах
- Graceful shutdown через context
- Счётчик dropped messages для мониторинга
- Логирование предупреждений при проблемах с сетью

## Тестирование

### Пример тестового кластера

```go
func TestCluster(t *testing.T) {
    // Запуск трёх нод
    node1 := startNode("node-1", "239.0.0.1:9999")
    node2 := startNode("node-2", "239.0.0.1:9999")
    node3 := startNode("node-3", "239.0.0.1:9999")
    
    // Регистрация пиров
    node1.RegisterPeers([]NodeID{"node-2", "node-3"})
    node2.RegisterPeers([]NodeID{"node-1", "node-3"})
    node3.RegisterPeers([]NodeID{"node-1", "node-2"})
    
    // Ждём распространения gossip
    time.Sleep(100 * time.Millisecond)
    
    // Проверка квот
    assert.Equal(t, int64(333), node1.nodeQuota()) // 1000/3 ≈ 333
}
```

## Ограничения

1. **Максимальный размер сообщения**: 64KB (ограничение UDP)
2. **Задержка распространения**: до 5-10 мс при нормальной работе
3. **Количество нод**: рекомендуется не более 100 из-за multicast
4. **Точность**: при добавлении нод возможен кратковременный undershoot

## Мониторинг

Рекомендуемые метрики для отслеживания:
- `dropped_messages` - проблемы с сетью или перегрузка
- `lock_contention` - высокая конкуренция за ресурсы
- `peer_count` - стабильность кластера
- `waiting_requests` - нагрузка на систему
