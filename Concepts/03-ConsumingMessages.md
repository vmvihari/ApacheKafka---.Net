# Microservice for Consuming Kafka Messages

## Overview

A Kafka consumer is an application that subscribes to (reads and processes) events from Kafka topics. Consumers can work independently or as part of a consumer group to process messages in parallel.

## Consumer Architecture

### Key Components

1. **Consumer Instance**: The main object that reads records from Kafka
2. **Deserializer**: Converts byte arrays back to objects
3. **Consumer Group**: Logical grouping of consumers for parallel processing
4. **Offset Management**: Tracking which messages have been processed
5. **Rebalancing**: Redistributing partitions among consumers

### Consumer Workflow

```
Broker → Consumer → Deserializer → Application Logic → Commit Offset
```

## Consumer Groups

### What is a Consumer Group?

A consumer group is a set of consumers that cooperate to consume data from topics. Each partition is consumed by exactly one consumer within the group.

**Benefits:**
- **Scalability**: Add more consumers to process data faster
- **Fault Tolerance**: If a consumer fails, its partitions are reassigned
- **Load Balancing**: Partitions are distributed evenly

### Example Scenarios

```
Topic with 4 partitions:

Scenario 1: Single Consumer
Consumer-1 → [P0, P1, P2, P3]

Scenario 2: Two Consumers in Same Group
Consumer-1 → [P0, P1]
Consumer-2 → [P2, P3]

Scenario 3: Four Consumers in Same Group
Consumer-1 → [P0]
Consumer-2 → [P1]
Consumer-3 → [P2]
Consumer-4 → [P3]

Scenario 4: More Consumers than Partitions
Consumer-1 → [P0]
Consumer-2 → [P1]
Consumer-3 → [P2]
Consumer-4 → [P3]
Consumer-5 → [idle]  // No partitions assigned
```

## Setting Up a .NET Kafka Consumer

### Install NuGet Package

```bash
dotnet add package Confluent.Kafka
```

### Basic Consumer Configuration

```csharp
using Confluent.Kafka;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-consumer-group",
    
    // Offset management
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,  // Manual commit for better control
    
    // Session and heartbeat
    SessionTimeoutMs = 45000,
    HeartbeatIntervalMs = 3000,
    
    // Fetch settings
    FetchMinBytes = 1,
    FetchMaxBytes = 52428800  // 50MB
};
```

## Consumer Implementation Patterns

### 1. Simple Consumer

```csharp
using Confluent.Kafka;

public class SimpleConsumer
{
    private readonly IConsumer<string, string> _consumer;
    private readonly ILogger<SimpleConsumer> _logger;
    
    public SimpleConsumer(string bootstrapServers, string groupId, ILogger<SimpleConsumer> logger)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        
        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _logger = logger;
    }
    
    public void StartConsuming(string topic, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(cancellationToken);
                
                _logger.LogInformation(
                    "Consumed message from {Topic} [{Partition}] at offset {Offset}: Key={Key}, Value={Value}",
                    consumeResult.Topic,
                    consumeResult.Partition.Value,
                    consumeResult.Offset.Value,
                    consumeResult.Message.Key,
                    consumeResult.Message.Value
                );
                
                // Process message here
                ProcessMessage(consumeResult.Message);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer cancelled");
        }
        finally
        {
            _consumer.Close();
        }
    }
    
    private void ProcessMessage(Message<string, string> message)
    {
        // Your business logic here
        _logger.LogInformation("Processing: {Value}", message.Value);
    }
}
```

### 2. Consumer with Manual Offset Commit

```csharp
public class ManualCommitConsumer
{
    private readonly IConsumer<string, string> _consumer;
    private readonly ILogger<ManualCommitConsumer> _logger;
    
    public ManualCommitConsumer(string bootstrapServers, string groupId, ILogger<ManualCommitConsumer> logger)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false,  // Manual commit
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        
        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _logger = logger;
    }
    
    public async Task StartConsumingAsync(string topic, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(cancellationToken);
                
                try
                {
                    // Process the message
                    await ProcessMessageAsync(consumeResult.Message);
                    
                    // Commit offset only after successful processing
                    _consumer.Commit(consumeResult);
                    
                    _logger.LogInformation(
                        "Successfully processed and committed offset {Offset}",
                        consumeResult.Offset.Value
                    );
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to process message at offset {Offset}", 
                        consumeResult.Offset.Value);
                    
                    // Don't commit - message will be reprocessed
                    // Or implement dead letter queue logic
                }
            }
        }
        finally
        {
            _consumer.Close();
        }
    }
    
    private async Task ProcessMessageAsync(Message<string, string> message)
    {
        // Simulate async processing
        await Task.Delay(100);
        _logger.LogInformation("Processed: {Value}", message.Value);
    }
}
```

### 3. Consumer with Batch Processing

```csharp
public class BatchConsumer
{
    private readonly IConsumer<string, string> _consumer;
    private readonly ILogger<BatchConsumer> _logger;
    private readonly int _batchSize;
    
    public BatchConsumer(
        string bootstrapServers, 
        string groupId, 
        int batchSize,
        ILogger<BatchConsumer> logger)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            MaxPollIntervalMs = 300000  // 5 minutes for batch processing
        };
        
        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _logger = logger;
        _batchSize = batchSize;
    }
    
    public async Task StartConsumingAsync(string topic, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        var batch = new List<ConsumeResult<string, string>>();
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));
                
                if (consumeResult != null)
                {
                    batch.Add(consumeResult);
                }
                
                // Process batch when size reached or timeout
                if (batch.Count >= _batchSize || 
                    (batch.Count > 0 && consumeResult == null))
                {
                    await ProcessBatchAsync(batch);
                    
                    // Commit the last offset in the batch
                    if (batch.Count > 0)
                    {
                        _consumer.Commit(batch.Last());
                        _logger.LogInformation("Committed batch of {Count} messages", batch.Count);
                        batch.Clear();
                    }
                }
            }
        }
        finally
        {
            _consumer.Close();
        }
    }
    
    private async Task ProcessBatchAsync(List<ConsumeResult<string, string>> batch)
    {
        _logger.LogInformation("Processing batch of {Count} messages", batch.Count);
        
        // Process all messages in batch
        var tasks = batch.Select(cr => ProcessMessageAsync(cr.Message));
        await Task.WhenAll(tasks);
    }
    
    private async Task ProcessMessageAsync(Message<string, string> message)
    {
        await Task.Delay(10);
        _logger.LogDebug("Processed: {Value}", message.Value);
    }
}
```

### 4. Consumer with Error Handling and Retry

```csharp
public class ResilientConsumer
{
    private readonly IConsumer<string, string> _consumer;
    private readonly ILogger<ResilientConsumer> _logger;
    private readonly int _maxRetries;
    
    public ResilientConsumer(
        string bootstrapServers, 
        string groupId,
        int maxRetries,
        ILogger<ResilientConsumer> logger)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        
        _consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError("Consumer error: {Reason}", error.Reason);
            })
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogInformation("Partitions assigned: {Partitions}", 
                    string.Join(", ", partitions));
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                _logger.LogInformation("Partitions revoked: {Partitions}", 
                    string.Join(", ", partitions));
            })
            .Build();
        
        _logger = logger;
        _maxRetries = maxRetries;
    }
    
    public async Task StartConsumingAsync(string topic, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(cancellationToken);
                
                var success = await ProcessWithRetryAsync(consumeResult);
                
                if (success)
                {
                    _consumer.Commit(consumeResult);
                }
                else
                {
                    // Send to dead letter queue
                    await SendToDeadLetterQueueAsync(consumeResult);
                    _consumer.Commit(consumeResult);
                }
            }
        }
        finally
        {
            _consumer.Close();
        }
    }
    
    private async Task<bool> ProcessWithRetryAsync(ConsumeResult<string, string> consumeResult)
    {
        for (int attempt = 1; attempt <= _maxRetries; attempt++)
        {
            try
            {
                await ProcessMessageAsync(consumeResult.Message);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, 
                    "Failed to process message (attempt {Attempt}/{MaxRetries})",
                    attempt, _maxRetries);
                
                if (attempt < _maxRetries)
                {
                    await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt)));
                }
            }
        }
        
        _logger.LogError("Failed to process message after {MaxRetries} attempts", _maxRetries);
        return false;
    }
    
    private async Task ProcessMessageAsync(Message<string, string> message)
    {
        // Your business logic that might throw exceptions
        await Task.Delay(100);
        
        if (new Random().Next(10) == 0)
            throw new Exception("Simulated processing error");
        
        _logger.LogInformation("Processed: {Value}", message.Value);
    }
    
    private async Task SendToDeadLetterQueueAsync(ConsumeResult<string, string> consumeResult)
    {
        _logger.LogWarning("Sending message to dead letter queue: {Key}", 
            consumeResult.Message.Key);
        
        // Implement DLQ logic (e.g., produce to a DLQ topic)
        await Task.CompletedTask;
    }
}
```

## Offset Management

### Auto Commit vs Manual Commit

```csharp
// Auto Commit (easier but less control)
var autoCommitConfig = new ConsumerConfig
{
    EnableAutoCommit = true,
    AutoCommitIntervalMs = 5000  // Commit every 5 seconds
};

// Manual Commit (more control, at-least-once guarantee)
var manualCommitConfig = new ConsumerConfig
{
    EnableAutoCommit = false
};

// Commit after each message
_consumer.Commit(consumeResult);

// Commit specific offsets
var offsets = new List<TopicPartitionOffset>
{
    new TopicPartitionOffset("my-topic", 0, 100)
};
_consumer.Commit(offsets);

// Store offset without committing (for custom offset management)
_consumer.StoreOffset(consumeResult);
```

### Offset Reset Strategies

```csharp
// Start from earliest available message
AutoOffsetReset = AutoOffsetReset.Earliest

// Start from latest (skip existing messages)
AutoOffsetReset = AutoOffsetReset.Latest

// Throw error if no offset found
AutoOffsetReset = AutoOffsetReset.Error
```

## ASP.NET Core Background Service

### Hosted Service Implementation

```csharp
public class KafkaConsumerHostedService : BackgroundService
{
    private readonly ILogger<KafkaConsumerHostedService> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly ConsumerConfig _config;
    
    public KafkaConsumerHostedService(
        IConfiguration configuration,
        ILogger<KafkaConsumerHostedService> logger,
        IServiceProvider serviceProvider)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        
        _config = new ConsumerConfig();
        configuration.GetSection("Kafka:Consumer").Bind(_config);
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();  // Ensure async execution
        
        using var consumer = new ConsumerBuilder<string, string>(_config).Build();
        
        consumer.Subscribe("my-topic");
        _logger.LogInformation("Kafka consumer started");
        
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(stoppingToken);
                
                // Process with scoped services
                using var scope = _serviceProvider.CreateScope();
                var handler = scope.ServiceProvider.GetRequiredService<IMessageHandler>();
                
                await handler.HandleAsync(consumeResult.Message);
                
                consumer.Commit(consumeResult);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer cancelled");
        }
        finally
        {
            consumer.Close();
            _logger.LogInformation("Consumer closed");
        }
    }
}

// Register in Program.cs
builder.Services.AddHostedService<KafkaConsumerHostedService>();
```

### Message Handler Interface

```csharp
public interface IMessageHandler
{
    Task HandleAsync(Message<string, string> message);
}

public class OrderMessageHandler : IMessageHandler
{
    private readonly ILogger<OrderMessageHandler> _logger;
    private readonly IOrderService _orderService;
    
    public OrderMessageHandler(
        ILogger<OrderMessageHandler> logger,
        IOrderService orderService)
    {
        _logger = logger;
        _orderService = orderService;
    }
    
    public async Task HandleAsync(Message<string, string> message)
    {
        var order = System.Text.Json.JsonSerializer.Deserialize<Order>(message.Value);
        
        _logger.LogInformation("Processing order {OrderId}", order.OrderId);
        
        await _orderService.ProcessOrderAsync(order);
    }
}
```

## Consumer Configuration Deep Dive

### Session and Heartbeat

```csharp
var config = new ConsumerConfig
{
    // Maximum time between heartbeats
    SessionTimeoutMs = 45000,  // 45 seconds
    
    // How often to send heartbeats
    HeartbeatIntervalMs = 3000,  // 3 seconds (should be < 1/3 of session timeout)
    
    // Maximum time between polls
    MaxPollIntervalMs = 300000  // 5 minutes
};
```

### Fetch Settings

```csharp
var config = new ConsumerConfig
{
    // Minimum bytes to fetch
    FetchMinBytes = 1,
    
    // Maximum bytes to fetch
    FetchMaxBytes = 52428800,  // 50MB
    
    // Maximum wait time if min bytes not available
    FetchWaitMaxMs = 500,
    
    // Maximum bytes per partition
    MaxPartitionFetchBytes = 1048576  // 1MB
};
```

## Rebalancing

### Rebalance Listeners

```csharp
var consumer = new ConsumerBuilder<string, string>(config)
    .SetPartitionsAssignedHandler((c, partitions) =>
    {
        _logger.LogInformation("Partitions assigned: {Count}", partitions.Count);
        
        // Optional: Seek to specific offsets
        var offsets = partitions.Select(p => 
            new TopicPartitionOffset(p, Offset.Beginning));
        c.Assign(offsets);
    })
    .SetPartitionsRevokedHandler((c, partitions) =>
    {
        _logger.LogInformation("Partitions revoked: {Count}", partitions.Count);
        
        // Commit offsets before rebalance
        c.Commit();
    })
    .SetPartitionsLostHandler((c, partitions) =>
    {
        _logger.LogWarning("Partitions lost: {Count}", partitions.Count);
    })
    .Build();
```

## Best Practices

### 1. Always Close Consumer Gracefully

```csharp
try
{
    while (!cancellationToken.IsCancellationRequested)
    {
        var result = _consumer.Consume(cancellationToken);
        // Process...
    }
}
finally
{
    _consumer.Close();  // Triggers rebalance and commits offsets
}
```

### 2. Handle Rebalancing Properly

```csharp
// Commit offsets before partitions are revoked
.SetPartitionsRevokedHandler((c, partitions) =>
{
    c.Commit();
})
```

### 3. Use Appropriate Timeout Values

```csharp
// For long-running processing
MaxPollIntervalMs = 600000  // 10 minutes

// For quick processing
MaxPollIntervalMs = 300000  // 5 minutes (default)
```

### 4. Implement Idempotent Processing

```csharp
// Store processed message IDs to prevent duplicate processing
private readonly HashSet<string> _processedIds = new();

private async Task ProcessMessageAsync(Message<string, string> message)
{
    if (_processedIds.Contains(message.Key))
    {
        _logger.LogInformation("Skipping duplicate message: {Key}", message.Key);
        return;
    }
    
    // Process message
    await DoWorkAsync(message);
    
    _processedIds.Add(message.Key);
}
```

### 5. Monitor Consumer Lag

```csharp
var consumer = new ConsumerBuilder<string, string>(config)
    .SetStatisticsHandler((_, json) =>
    {
        // Parse statistics and monitor lag
        var stats = JsonSerializer.Deserialize<Dictionary<string, object>>(json);
        // Send to monitoring system
    })
    .Build();

// Enable statistics
config.StatisticsIntervalMs = 60000;  // Every 60 seconds
```

## Next Steps

- [Message Serialization](./04-MessageSerialization.md)
- [Working with Schemas](./05-WorkingWithSchemas.md)
- [Message Delivery and Transactions](./06-MessageDeliveryTransactions.md)
