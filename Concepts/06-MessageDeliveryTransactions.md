# Message Delivery and Transactions

## Overview

Kafka provides different delivery guarantees and transactional capabilities to ensure data consistency and reliability in distributed systems. Understanding these mechanisms is crucial for building robust event-driven applications.

## Delivery Semantics

### 1. At-Most-Once Delivery

Messages may be lost but are never redelivered.

**Characteristics:**
- Fastest performance
- Lowest latency
- No duplicate processing
- Potential data loss

**Configuration:**

```csharp
var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.None,  // Don't wait for acknowledgment
    EnableIdempotence = false,
    MessageSendMaxRetries = 0  // Don't retry
};

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    EnableAutoCommit = true,
    AutoCommitIntervalMs = 1000  // Commit before processing
};
```

**Use Cases:**
- Metrics and monitoring
- Log aggregation
- Non-critical events

### 2. At-Least-Once Delivery

Messages are never lost but may be redelivered.

**Characteristics:**
- Guaranteed delivery
- Possible duplicates
- Requires idempotent processing
- Most common pattern

**Configuration:**

```csharp
var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.All,  // Wait for all replicas
    MessageSendMaxRetries = int.MaxValue,
    EnableIdempotence = false  // Can have duplicates
};

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    EnableAutoCommit = false  // Manual commit after processing
};

// Consumer implementation
while (true)
{
    var result = consumer.Consume(cancellationToken);
    
    // Process message
    await ProcessMessageAsync(result.Message);
    
    // Commit only after successful processing
    consumer.Commit(result);
}
```

**Use Cases:**
- Financial transactions (with idempotent processing)
- Order processing
- Critical business events

### 3. Exactly-Once Delivery

Each message is delivered exactly once, no duplicates or losses.

**Characteristics:**
- Strongest guarantee
- Higher latency
- Requires transactions
- Most complex to implement

**Configuration:**

```csharp
var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.All,
    EnableIdempotence = true,  // Prevents duplicates
    TransactionalId = "my-transactional-producer",  // Required for transactions
    MaxInFlight = 5,  // Automatically set with idempotence
    MessageSendMaxRetries = int.MaxValue
};

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    EnableAutoCommit = false,
    IsolationLevel = IsolationLevel.ReadCommitted  // Only read committed messages
};
```

**Use Cases:**
- Payment processing
- Account balance updates
- Critical state changes

## Idempotent Producer

An idempotent producer ensures that messages are not duplicated even if retries occur.

### How It Works

1. Producer assigns a sequence number to each message
2. Broker tracks sequence numbers per producer
3. Duplicate messages (same sequence number) are rejected
4. Guarantees exactly-once delivery to a single partition

### Implementation

```csharp
public class IdempotentProducer
{
    private readonly IProducer<string, string> _producer;
    
    public IdempotentProducer(string bootstrapServers)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            
            // Enable idempotence
            EnableIdempotence = true,
            
            // These are automatically configured when EnableIdempotence = true:
            // Acks = Acks.All
            // MaxInFlight = 5
            // Retries = int.MaxValue
        };
        
        _producer = new ProducerBuilder<string, string>(config).Build();
    }
    
    public async Task<DeliveryResult<string, string>> ProduceAsync(
        string topic, string key, string value)
    {
        var message = new Message<string, string>
        {
            Key = key,
            Value = value
        };
        
        // Even if this fails and retries, no duplicates will be created
        return await _producer.ProduceAsync(topic, message);
    }
}
```

### Benefits

- Automatic retry without duplicates
- No application-level deduplication needed
- Maintains message ordering
- Minimal performance overhead

## Transactional Producer

Transactions enable atomic writes across multiple partitions and topics.

### Use Cases

1. **Exactly-once processing**: Read from Kafka, process, write to Kafka
2. **Atomic multi-partition writes**: Write to multiple partitions atomically
3. **Atomic multi-topic writes**: Write to multiple topics atomically

### Implementation

```csharp
public class TransactionalProducer
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<TransactionalProducer> _logger;
    
    public TransactionalProducer(string bootstrapServers, string transactionalId, 
        ILogger<TransactionalProducer> logger)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            
            // Required for transactions
            TransactionalId = transactionalId,
            
            // Automatically enabled with TransactionalId
            EnableIdempotence = true,
            Acks = Acks.All
        };
        
        _producer = new ProducerBuilder<string, string>(config).Build();
        
        // Initialize transactions (must be called once)
        _producer.InitTransactions(TimeSpan.FromSeconds(30));
        
        _logger = logger;
    }
    
    public async Task ProduceTransactionallyAsync(
        List<(string topic, string key, string value)> messages)
    {
        _producer.BeginTransaction();
        
        try
        {
            // Produce all messages within transaction
            foreach (var (topic, key, value) in messages)
            {
                var message = new Message<string, string>
                {
                    Key = key,
                    Value = value
                };
                
                await _producer.ProduceAsync(topic, message);
            }
            
            // Commit transaction - all messages are visible atomically
            _producer.CommitTransaction();
            
            _logger.LogInformation("Transaction committed successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Transaction failed, aborting");
            
            // Abort transaction - no messages are visible
            _producer.AbortTransaction();
            
            throw;
        }
    }
}

// Usage
var producer = new TransactionalProducer(
    "localhost:9092", 
    "my-transaction-id",
    logger
);

var messages = new List<(string, string, string)>
{
    ("orders", "order-1", "Order data 1"),
    ("payments", "payment-1", "Payment data 1"),
    ("inventory", "item-1", "Inventory update 1")
};

// All messages are written atomically
await producer.ProduceTransactionallyAsync(messages);
```

## Exactly-Once Semantics (EOS)

### Read-Process-Write Pattern

The most common use case for transactions: read from Kafka, process, write back to Kafka.

```csharp
public class ExactlyOnceProcessor
{
    private readonly IConsumer<string, string> _consumer;
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<ExactlyOnceProcessor> _logger;
    
    public ExactlyOnceProcessor(
        string bootstrapServers,
        string groupId,
        string transactionalId,
        ILogger<ExactlyOnceProcessor> logger)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false,
            IsolationLevel = IsolationLevel.ReadCommitted  // Only read committed
        };
        
        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            TransactionalId = transactionalId,
            EnableIdempotence = true
        };
        
        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        _producer.InitTransactions(TimeSpan.FromSeconds(30));
        
        _logger = logger;
    }
    
    public async Task ProcessAsync(string inputTopic, string outputTopic, 
        CancellationToken cancellationToken)
    {
        _consumer.Subscribe(inputTopic);
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(cancellationToken);
                
                _producer.BeginTransaction();
                
                try
                {
                    // Process the message
                    var processedValue = await ProcessMessageAsync(consumeResult.Message.Value);
                    
                    // Produce result
                    await _producer.ProduceAsync(outputTopic, new Message<string, string>
                    {
                        Key = consumeResult.Message.Key,
                        Value = processedValue
                    });
                    
                    // Send consumer offsets to transaction
                    var offsets = new List<TopicPartitionOffset>
                    {
                        new TopicPartitionOffset(
                            consumeResult.TopicPartition,
                            consumeResult.Offset + 1
                        )
                    };
                    
                    _producer.SendOffsetsToTransaction(
                        offsets,
                        _consumer.ConsumerGroupMetadata,
                        TimeSpan.FromSeconds(30)
                    );
                    
                    // Commit transaction
                    _producer.CommitTransaction();
                    
                    _logger.LogInformation(
                        "Processed message at offset {Offset} exactly once",
                        consumeResult.Offset.Value
                    );
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Processing failed, aborting transaction");
                    _producer.AbortTransaction();
                }
            }
        }
        finally
        {
            _consumer.Close();
            _producer.Dispose();
        }
    }
    
    private async Task<string> ProcessMessageAsync(string input)
    {
        // Your business logic here
        await Task.Delay(100);
        return input.ToUpper();
    }
}
```

## Consumer Offset Management

### Auto Commit

```csharp
var config = new ConsumerConfig
{
    EnableAutoCommit = true,
    AutoCommitIntervalMs = 5000  // Commit every 5 seconds
};

// Offsets are committed automatically in background
var result = consumer.Consume();
// Process message
```

**Pros:**
- Simple to use
- Less code

**Cons:**
- May lose messages (if consumer crashes after commit but before processing)
- May duplicate messages (if consumer crashes after processing but before commit)

### Manual Commit (Synchronous)

```csharp
var config = new ConsumerConfig
{
    EnableAutoCommit = false
};

var result = consumer.Consume();

// Process message
await ProcessMessageAsync(result.Message);

// Commit after successful processing
consumer.Commit(result);
```

**Pros:**
- Full control over when to commit
- At-least-once guarantee

**Cons:**
- More code
- Slower (synchronous commit)

### Manual Commit (Asynchronous)

```csharp
var result = consumer.Consume();

await ProcessMessageAsync(result.Message);

// Async commit - doesn't block
consumer.Commit(result);  // Returns immediately
```

### Batch Commit

```csharp
var batch = new List<ConsumeResult<string, string>>();

for (int i = 0; i < batchSize; i++)
{
    var result = consumer.Consume(TimeSpan.FromSeconds(1));
    if (result != null)
        batch.Add(result);
}

// Process batch
await ProcessBatchAsync(batch);

// Commit last offset
if (batch.Any())
    consumer.Commit(batch.Last());
```

## Error Handling Strategies

### 1. Retry with Exponential Backoff

```csharp
public async Task<bool> ProcessWithRetryAsync(
    ConsumeResult<string, string> result,
    int maxRetries = 3)
{
    for (int attempt = 1; attempt <= maxRetries; attempt++)
    {
        try
        {
            await ProcessMessageAsync(result.Message);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Attempt {Attempt} failed", attempt);
            
            if (attempt < maxRetries)
            {
                var delay = TimeSpan.FromSeconds(Math.Pow(2, attempt));
                await Task.Delay(delay);
            }
        }
    }
    
    return false;
}
```

### 2. Dead Letter Queue (DLQ)

```csharp
public async Task ProcessWithDLQAsync(ConsumeResult<string, string> result)
{
    try
    {
        await ProcessMessageAsync(result.Message);
        _consumer.Commit(result);
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Failed to process message, sending to DLQ");
        
        // Send to dead letter queue
        await _dlqProducer.ProduceAsync("orders-dlq", new Message<string, string>
        {
            Key = result.Message.Key,
            Value = result.Message.Value,
            Headers = new Headers
            {
                { "error", Encoding.UTF8.GetBytes(ex.Message) },
                { "original-topic", Encoding.UTF8.GetBytes(result.Topic) },
                { "original-partition", BitConverter.GetBytes(result.Partition.Value) },
                { "original-offset", BitConverter.GetBytes(result.Offset.Value) }
            }
        });
        
        // Commit original message
        _consumer.Commit(result);
    }
}
```

### 3. Circuit Breaker Pattern

```csharp
public class CircuitBreakerConsumer
{
    private int _failureCount = 0;
    private DateTime _lastFailureTime = DateTime.MinValue;
    private readonly int _failureThreshold = 5;
    private readonly TimeSpan _resetTimeout = TimeSpan.FromMinutes(1);
    private bool _circuitOpen = false;
    
    public async Task ProcessAsync(ConsumeResult<string, string> result)
    {
        // Check if circuit should be reset
        if (_circuitOpen && DateTime.UtcNow - _lastFailureTime > _resetTimeout)
        {
            _circuitOpen = false;
            _failureCount = 0;
            _logger.LogInformation("Circuit breaker reset");
        }
        
        if (_circuitOpen)
        {
            _logger.LogWarning("Circuit breaker open, skipping message");
            return;
        }
        
        try
        {
            await ProcessMessageAsync(result.Message);
            _failureCount = 0;  // Reset on success
        }
        catch (Exception ex)
        {
            _failureCount++;
            _lastFailureTime = DateTime.UtcNow;
            
            if (_failureCount >= _failureThreshold)
            {
                _circuitOpen = true;
                _logger.LogError("Circuit breaker opened after {Count} failures", _failureCount);
            }
            
            throw;
        }
    }
}
```

## Performance Optimization

### Producer Batching

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    
    // Batching settings
    LingerMs = 10,  // Wait up to 10ms to batch messages
    BatchSize = 32768,  // 32KB batch size
    
    // Compression
    CompressionType = CompressionType.Snappy,
    
    // Buffer
    BufferMemory = 33554432  // 32MB
};
```

### Consumer Fetch Settings

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    
    // Fetch settings
    FetchMinBytes = 1024,  // Wait for at least 1KB
    FetchMaxBytes = 52428800,  // Max 50MB per fetch
    FetchWaitMaxMs = 500,  // Wait up to 500ms
    
    // Partition fetch
    MaxPartitionFetchBytes = 1048576  // 1MB per partition
};
```

## Monitoring and Metrics

### Producer Metrics

```csharp
var producer = new ProducerBuilder<string, string>(config)
    .SetStatisticsHandler((_, json) =>
    {
        var stats = JsonSerializer.Deserialize<Dictionary<string, object>>(json);
        
        // Extract metrics
        // - record-send-rate
        // - record-error-rate
        // - request-latency-avg
        // - outgoing-byte-rate
        
        _logger.LogInformation("Producer stats: {Stats}", json);
    })
    .Build();

// Enable statistics
config.StatisticsIntervalMs = 60000;
```

### Consumer Metrics

```csharp
var consumer = new ConsumerBuilder<string, string>(config)
    .SetStatisticsHandler((_, json) =>
    {
        var stats = JsonSerializer.Deserialize<Dictionary<string, object>>(json);
        
        // Extract metrics
        // - consumer-lag
        // - records-consumed-rate
        // - fetch-latency-avg
        
        _logger.LogInformation("Consumer stats: {Stats}", json);
    })
    .Build();
```

## Best Practices

### 1. Choose Appropriate Delivery Semantics

- **At-most-once**: Non-critical data (logs, metrics)
- **At-least-once**: Most use cases with idempotent processing
- **Exactly-once**: Critical financial/state changes

### 2. Enable Idempotence by Default

```csharp
EnableIdempotence = true  // Minimal overhead, prevents duplicates
```

### 3. Use Transactions for Multi-Partition Writes

```csharp
// Atomic write to multiple topics/partitions
_producer.BeginTransaction();
// ... produce messages
_producer.CommitTransaction();
```

### 4. Implement Idempotent Consumers

```csharp
// Track processed message IDs
private readonly HashSet<string> _processedIds = new();

if (_processedIds.Contains(message.Key))
{
    _logger.LogInformation("Skipping duplicate");
    return;
}

await ProcessAsync(message);
_processedIds.Add(message.Key);
```

### 5. Monitor Consumer Lag

```csharp
// Use consumer lag metrics to detect processing issues
// Lag = Latest Offset - Current Offset
```

## Next Steps

- [Event Streaming](./01-EventStreaming.md)
- [Microservice for Producing Kafka Messages](./02-ProducingMessages.md)
- [Microservice for Consuming Kafka Messages](./03-ConsumingMessages.md)
