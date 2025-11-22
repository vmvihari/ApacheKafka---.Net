# Microservice for Producing Kafka Messages

## Overview

A Kafka producer is an application that publishes (writes) events to Kafka topics. In a microservices architecture, producers are typically services that generate events based on business logic, user actions, or system events.

## Producer Architecture

### Key Components

1. **Producer Instance**: The main object that sends records to Kafka
2. **Serializer**: Converts objects to byte arrays for transmission
3. **Partitioner**: Determines which partition to send the record to
4. **Buffer**: Accumulates records before sending in batches
5. **Sender Thread**: Asynchronously sends batches to Kafka brokers

### Producer Workflow

```
Application → Producer → Serializer → Partitioner → Buffer → Broker
```

## Setting Up a .NET Kafka Producer

### Install NuGet Package

```bash
dotnet add package Confluent.Kafka
```

### Basic Producer Configuration

```csharp
using Confluent.Kafka;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    ClientId = "my-producer-client",
    
    // Acknowledgment settings
    Acks = Acks.All,  // Wait for all in-sync replicas
    
    // Retry settings
    MessageSendMaxRetries = 3,
    RetryBackoffMs = 1000,
    
    // Compression
    CompressionType = CompressionType.Snappy,
    
    // Batching for performance
    LingerMs = 10,
    BatchSize = 16384
};
```

## Producer Implementation Patterns

### 1. Simple Fire-and-Forget Producer

```csharp
using Confluent.Kafka;

public class SimpleProducer
{
    private readonly IProducer<string, string> _producer;
    
    public SimpleProducer(string bootstrapServers)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };
        
        _producer = new ProducerBuilder<string, string>(config).Build();
    }
    
    public void SendMessage(string topic, string key, string value)
    {
        var message = new Message<string, string>
        {
            Key = key,
            Value = value
        };
        
        // Fire and forget - doesn't wait for acknowledgment
        _producer.Produce(topic, message);
    }
    
    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
    }
}
```

### 2. Synchronous Producer with Error Handling

```csharp
public class SyncProducer
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<SyncProducer> _logger;
    
    public SyncProducer(string bootstrapServers, ILogger<SyncProducer> logger)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true  // Exactly-once semantics
        };
        
        _producer = new ProducerBuilder<string, string>(config).Build();
        _logger = logger;
    }
    
    public async Task<bool> SendMessageAsync(string topic, string key, string value)
    {
        try
        {
            var message = new Message<string, string>
            {
                Key = key,
                Value = value,
                Timestamp = Timestamp.Default
            };
            
            var deliveryResult = await _producer.ProduceAsync(topic, message);
            
            _logger.LogInformation(
                "Message delivered to {Topic} [{Partition}] at offset {Offset}",
                deliveryResult.Topic,
                deliveryResult.Partition.Value,
                deliveryResult.Offset.Value
            );
            
            return true;
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex, "Failed to deliver message: {Reason}", ex.Error.Reason);
            return false;
        }
    }
}
```

### 3. Asynchronous Producer with Callbacks

```csharp
public class AsyncProducer
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<AsyncProducer> _logger;
    
    public AsyncProducer(string bootstrapServers, ILogger<AsyncProducer> logger)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All
        };
        
        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError("Producer error: {Reason}", error.Reason);
            })
            .SetStatisticsHandler((_, json) =>
            {
                _logger.LogDebug("Producer statistics: {Stats}", json);
            })
            .Build();
        
        _logger = logger;
    }
    
    public void SendMessage(string topic, string key, string value, 
        Action<DeliveryReport<string, string>> deliveryHandler = null)
    {
        var message = new Message<string, string>
        {
            Key = key,
            Value = value
        };
        
        _producer.Produce(topic, message, deliveryReport =>
        {
            if (deliveryReport.Error.IsError)
            {
                _logger.LogError(
                    "Delivery failed: {Reason}",
                    deliveryReport.Error.Reason
                );
            }
            else
            {
                _logger.LogInformation(
                    "Message delivered to {Partition} at offset {Offset}",
                    deliveryReport.Partition.Value,
                    deliveryReport.Offset.Value
                );
            }
            
            deliveryHandler?.Invoke(deliveryReport);
        });
    }
}
```

### 4. Producer with Custom Serialization

```csharp
public class Order
{
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
    public DateTime CreatedAt { get; set; }
}

public class JsonProducer<TKey, TValue>
{
    private readonly IProducer<TKey, TValue> _producer;
    
    public JsonProducer(string bootstrapServers)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All
        };
        
        _producer = new ProducerBuilder<TKey, TValue>(config)
            .SetValueSerializer(new JsonSerializer<TValue>())
            .Build();
    }
    
    public async Task<DeliveryResult<TKey, TValue>> SendAsync(
        string topic, TKey key, TValue value)
    {
        var message = new Message<TKey, TValue>
        {
            Key = key,
            Value = value
        };
        
        return await _producer.ProduceAsync(topic, message);
    }
}

// Custom JSON serializer
public class JsonSerializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
    {
        if (data == null) return null;
        
        var json = System.Text.Json.JsonSerializer.Serialize(data);
        return Encoding.UTF8.GetBytes(json);
    }
}
```

## Producer Configuration Deep Dive

### Acknowledgment Settings

```csharp
// Acks.None (0): Fire and forget, no acknowledgment
Acks = Acks.None

// Acks.Leader (1): Leader acknowledges, faster but less durable
Acks = Acks.Leader

// Acks.All (-1): All in-sync replicas acknowledge, slowest but most durable
Acks = Acks.All
```

### Idempotence and Exactly-Once Semantics

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    
    // Enable idempotence to prevent duplicates
    EnableIdempotence = true,
    
    // These are automatically set when EnableIdempotence = true:
    // MaxInFlight = 5
    // Acks = Acks.All
    // Retries = int.MaxValue
};
```

### Performance Tuning

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    
    // Batching settings
    BatchSize = 32768,  // Batch size in bytes
    LingerMs = 10,      // Wait up to 10ms to batch messages
    
    // Compression
    CompressionType = CompressionType.Snappy,  // or Gzip, Lz4, Zstd
    
    // Buffer settings
    BufferMemory = 33554432,  // 32MB buffer
    
    // Network settings
    RequestTimeoutMs = 30000,
    MessageTimeoutMs = 300000
};
```

## Partitioning Strategies

### 1. Key-Based Partitioning (Default)

```csharp
// Messages with the same key go to the same partition
var message = new Message<string, string>
{
    Key = "user-123",  // All messages for user-123 go to same partition
    Value = "User action data"
};
```

### 2. Custom Partitioner

```csharp
public class CustomPartitioner : IPartitioner
{
    public int Partition(string topic, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull)
    {
        if (keyIsNull)
            return new Random().Next(partitionCount);
        
        // Custom logic: use first character of key
        var key = Encoding.UTF8.GetString(keyData);
        var firstChar = key[0];
        return firstChar % partitionCount;
    }
    
    public void Dispose() { }
}

// Use custom partitioner
var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
var producer = new ProducerBuilder<string, string>(config)
    .SetPartitioner(new CustomPartitioner())
    .Build();
```

### 3. Round-Robin (No Key)

```csharp
// When key is null, messages are distributed round-robin
var message = new Message<string, string>
{
    Key = null,
    Value = "Data without key"
};
```

## ASP.NET Core Integration

### Dependency Injection Setup

```csharp
// Program.cs or Startup.cs
public static class KafkaProducerExtensions
{
    public static IServiceCollection AddKafkaProducer(
        this IServiceCollection services, 
        IConfiguration configuration)
    {
        var producerConfig = new ProducerConfig();
        configuration.GetSection("Kafka:Producer").Bind(producerConfig);
        
        services.AddSingleton(producerConfig);
        services.AddSingleton(typeof(IProducer<,>), typeof(KafkaProducer<,>));
        
        return services;
    }
}

// appsettings.json
{
  "Kafka": {
    "Producer": {
      "BootstrapServers": "localhost:9092",
      "Acks": "All",
      "EnableIdempotence": true,
      "CompressionType": "Snappy"
    }
  }
}
```

### Producer Service

```csharp
public interface IEventProducer
{
    Task PublishAsync<T>(string topic, string key, T value);
}

public class KafkaEventProducer : IEventProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaEventProducer> _logger;
    
    public KafkaEventProducer(ProducerConfig config, ILogger<KafkaEventProducer> logger)
    {
        _producer = new ProducerBuilder<string, string>(config).Build();
        _logger = logger;
    }
    
    public async Task PublishAsync<T>(string topic, string key, T value)
    {
        var json = System.Text.Json.JsonSerializer.Serialize(value);
        var message = new Message<string, string>
        {
            Key = key,
            Value = json,
            Headers = new Headers
            {
                { "event-type", Encoding.UTF8.GetBytes(typeof(T).Name) },
                { "timestamp", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("o")) }
            }
        };
        
        try
        {
            var result = await _producer.ProduceAsync(topic, message);
            _logger.LogInformation("Published event to {Topic}", topic);
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex, "Failed to publish event");
            throw;
        }
    }
    
    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
    }
}
```

## Best Practices

### 1. Always Flush Before Shutdown

```csharp
public void Dispose()
{
    // Ensure all messages are sent before disposing
    _producer?.Flush(TimeSpan.FromSeconds(10));
    _producer?.Dispose();
}
```

### 2. Handle Errors Appropriately

```csharp
try
{
    await _producer.ProduceAsync(topic, message);
}
catch (ProduceException<string, string> ex)
{
    // Log error
    _logger.LogError(ex, "Failed to produce message");
    
    // Decide: retry, dead letter queue, or fail
    if (ex.Error.IsFatal)
        throw;
}
```

### 3. Use Idempotence for Critical Data

```csharp
var config = new ProducerConfig
{
    EnableIdempotence = true  // Prevents duplicates
};
```

### 4. Monitor Producer Metrics

```csharp
var producer = new ProducerBuilder<string, string>(config)
    .SetStatisticsHandler((_, json) =>
    {
        // Parse and send metrics to monitoring system
        var stats = JsonSerializer.Deserialize<Dictionary<string, object>>(json);
        // Send to Prometheus, DataDog, etc.
    })
    .Build();
```

## Next Steps

- [Microservice for Consuming Kafka Messages](./03-ConsumingMessages.md)
- [Message Serialization](./04-MessageSerialization.md)
- [Working with Schemas](./05-WorkingWithSchemas.md)
