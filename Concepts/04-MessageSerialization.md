# Message Serialization

## Overview

Serialization is the process of converting objects into a byte stream for transmission over the network. Deserialization is the reverse process. Kafka stores and transmits all data as byte arrays, so proper serialization is crucial for producing and consuming messages.

## Built-in Serializers

Confluent.Kafka provides several built-in serializers and deserializers:

### String Serialization

```csharp
using Confluent.Kafka;

// Producer with string serialization
var producer = new ProducerBuilder<string, string>(config).Build();

var message = new Message<string, string>
{
    Key = "user-123",
    Value = "User data as string"
};

await producer.ProduceAsync("my-topic", message);

// Consumer with string deserialization
var consumer = new ConsumerBuilder<string, string>(config).Build();
var result = consumer.Consume();
string value = result.Message.Value;
```

### Byte Array Serialization

```csharp
// Producer with byte array
var producer = new ProducerBuilder<Null, byte[]>(config).Build();

var message = new Message<Null, byte[]>
{
    Value = Encoding.UTF8.GetBytes("Raw bytes")
};

await producer.ProduceAsync("my-topic", message);
```

### Integer Serialization

```csharp
// For numeric keys or values
var producer = new ProducerBuilder<int, string>(config).Build();

var message = new Message<int, string>
{
    Key = 12345,
    Value = "Data for key 12345"
};
```

## Custom Serialization

### JSON Serialization

#### Using System.Text.Json

```csharp
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

public class JsonSerializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
    {
        if (data == null)
            return null;
        
        var json = JsonSerializer.Serialize(data, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        });
        
        return Encoding.UTF8.GetBytes(json);
    }
}

public class JsonDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
            return default;
        
        var json = Encoding.UTF8.GetString(data);
        return JsonSerializer.Deserialize<T>(json, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
    }
}

// Usage
public class Order
{
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
    public DateTime CreatedAt { get; set; }
}

var producer = new ProducerBuilder<string, Order>(config)
    .SetValueSerializer(new JsonSerializer<Order>())
    .Build();

var order = new Order
{
    OrderId = "ORD-001",
    Amount = 99.99m,
    CreatedAt = DateTime.UtcNow
};

await producer.ProduceAsync("orders", new Message<string, Order>
{
    Key = order.OrderId,
    Value = order
});
```

#### Using Newtonsoft.Json

```csharp
using Newtonsoft.Json;

public class NewtonsoftJsonSerializer<T> : ISerializer<T>
{
    private readonly JsonSerializerSettings _settings;
    
    public NewtonsoftJsonSerializer(JsonSerializerSettings settings = null)
    {
        _settings = settings ?? new JsonSerializerSettings
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
            Formatting = Formatting.None,
            NullValueHandling = NullValueHandling.Ignore
        };
    }
    
    public byte[] Serialize(T data, SerializationContext context)
    {
        if (data == null)
            return null;
        
        var json = JsonConvert.SerializeObject(data, _settings);
        return Encoding.UTF8.GetBytes(json);
    }
}

public class NewtonsoftJsonDeserializer<T> : IDeserializer<T>
{
    private readonly JsonSerializerSettings _settings;
    
    public NewtonsoftJsonDeserializer(JsonSerializerSettings settings = null)
    {
        _settings = settings ?? new JsonSerializerSettings
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver()
        };
    }
    
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
            return default;
        
        var json = Encoding.UTF8.GetString(data);
        return JsonConvert.DeserializeObject<T>(json, _settings);
    }
}
```

### Protocol Buffers (Protobuf)

Protocol Buffers is a language-neutral, platform-neutral, extensible mechanism for serializing structured data.

#### Install NuGet Packages

```bash
dotnet add package Google.Protobuf
dotnet add package Grpc.Tools
```

#### Define .proto File

```protobuf
// order.proto
syntax = "proto3";

package orders;

message Order {
    string order_id = 1;
    double amount = 2;
    int64 created_at = 3;
    string customer_id = 4;
    repeated OrderItem items = 5;
}

message OrderItem {
    string product_id = 1;
    int32 quantity = 2;
    double price = 3;
}
```

#### Protobuf Serializer

```csharp
using Google.Protobuf;
using Confluent.Kafka;

public class ProtobufSerializer<T> : ISerializer<T> where T : IMessage<T>
{
    public byte[] Serialize(T data, SerializationContext context)
    {
        if (data == null)
            return null;
        
        return data.ToByteArray();
    }
}

public class ProtobufDeserializer<T> : IDeserializer<T> where T : IMessage<T>, new()
{
    private readonly MessageParser<T> _parser;
    
    public ProtobufDeserializer()
    {
        _parser = new MessageParser<T>(() => new T());
    }
    
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
            return default;
        
        return _parser.ParseFrom(data.ToArray());
    }
}

// Usage
var producer = new ProducerBuilder<string, Order>(config)
    .SetValueSerializer(new ProtobufSerializer<Order>())
    .Build();

var order = new Order
{
    OrderId = "ORD-001",
    Amount = 99.99,
    CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
    CustomerId = "CUST-123"
};

await producer.ProduceAsync("orders", new Message<string, Order>
{
    Key = order.OrderId,
    Value = order
});
```

### Apache Avro

Avro is a row-oriented remote procedure call and data serialization framework. It's commonly used with Kafka for schema evolution.

#### Install NuGet Package

```bash
dotnet add package Confluent.SchemaRegistry.Serdes.Avro
```

#### Avro Serializer with Schema Registry

```csharp
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

// Define schema
public class Order
{
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
    public long CreatedAt { get; set; }
}

// Configure Schema Registry
var schemaRegistryConfig = new SchemaRegistryConfig
{
    Url = "http://localhost:8081"
};

var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

// Producer with Avro serialization
var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};

var producer = new ProducerBuilder<string, Order>(producerConfig)
    .SetValueSerializer(new AvroSerializer<Order>(schemaRegistry))
    .Build();

var order = new Order
{
    OrderId = "ORD-001",
    Amount = 99.99m,
    CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
};

await producer.ProduceAsync("orders", new Message<string, Order>
{
    Key = order.OrderId,
    Value = order
});

// Consumer with Avro deserialization
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "order-consumer-group"
};

var consumer = new ConsumerBuilder<string, Order>(consumerConfig)
    .SetValueDeserializer(new AvroDeserializer<Order>(schemaRegistry).AsSyncOverAsync())
    .Build();

consumer.Subscribe("orders");
var result = consumer.Consume();
Order receivedOrder = result.Message.Value;
```

## MessagePack Serialization

MessagePack is an efficient binary serialization format.

#### Install NuGet Package

```bash
dotnet add package MessagePack
```

#### MessagePack Serializer

```csharp
using MessagePack;
using Confluent.Kafka;

public class MessagePackSerializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
    {
        if (data == null)
            return null;
        
        return MessagePackSerializer.Serialize(data);
    }
}

public class MessagePackDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
            return default;
        
        return MessagePackSerializer.Deserialize<T>(data.ToArray());
    }
}

// Usage
[MessagePackObject]
public class Order
{
    [Key(0)]
    public string OrderId { get; set; }
    
    [Key(1)]
    public decimal Amount { get; set; }
    
    [Key(2)]
    public DateTime CreatedAt { get; set; }
}

var producer = new ProducerBuilder<string, Order>(config)
    .SetValueSerializer(new MessagePackSerializer<Order>())
    .Build();
```

## Serialization Comparison

| Format | Pros | Cons | Best For |
|--------|------|------|----------|
| **JSON** | Human-readable, widely supported, flexible | Larger size, slower parsing | Development, debugging, REST APIs |
| **Protobuf** | Compact, fast, schema evolution | Requires .proto files, not human-readable | High-performance systems, microservices |
| **Avro** | Compact, schema evolution, Schema Registry integration | Complex setup, not human-readable | Data lakes, schema management |
| **MessagePack** | Very compact, fast, simple | Less tooling support | High-throughput systems |
| **String** | Simple, debugging-friendly | No structure, manual parsing | Simple use cases, logging |

## Serialization Best Practices

### 1. Version Your Messages

```csharp
public class OrderV1
{
    public int Version { get; set; } = 1;
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
}

public class OrderV2
{
    public int Version { get; set; } = 2;
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
    public string CustomerId { get; set; }  // New field
}

// Deserializer that handles multiple versions
public class VersionedOrderDeserializer : IDeserializer<object>
{
    public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull) return null;
        
        var json = Encoding.UTF8.GetString(data);
        var versionCheck = JsonSerializer.Deserialize<Dictionary<string, object>>(json);
        
        var version = versionCheck.ContainsKey("version") 
            ? Convert.ToInt32(versionCheck["version"]) 
            : 1;
        
        return version switch
        {
            1 => JsonSerializer.Deserialize<OrderV1>(json),
            2 => JsonSerializer.Deserialize<OrderV2>(json),
            _ => throw new NotSupportedException($"Version {version} not supported")
        };
    }
}
```

### 2. Handle Null Values

```csharp
public class NullSafeSerializer<T> : ISerializer<T>
{
    private readonly ISerializer<T> _innerSerializer;
    
    public NullSafeSerializer(ISerializer<T> innerSerializer)
    {
        _innerSerializer = innerSerializer;
    }
    
    public byte[] Serialize(T data, SerializationContext context)
    {
        if (data == null)
        {
            // Return empty array or throw exception
            return Array.Empty<byte>();
        }
        
        return _innerSerializer.Serialize(data, context);
    }
}
```

### 3. Add Error Handling

```csharp
public class SafeDeserializer<T> : IDeserializer<T>
{
    private readonly IDeserializer<T> _innerDeserializer;
    private readonly ILogger _logger;
    
    public SafeDeserializer(IDeserializer<T> innerDeserializer, ILogger logger)
    {
        _innerDeserializer = innerDeserializer;
        _logger = logger;
    }
    
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        try
        {
            return _innerDeserializer.Deserialize(data, isNull, context);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Deserialization failed for topic {Topic}", context.Topic);
            
            // Return default or throw
            return default;
        }
    }
}
```

### 4. Use Compression

```csharp
var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    CompressionType = CompressionType.Snappy  // or Gzip, Lz4, Zstd
};
```

### 5. Include Metadata in Headers

```csharp
var message = new Message<string, Order>
{
    Key = order.OrderId,
    Value = order,
    Headers = new Headers
    {
        { "content-type", Encoding.UTF8.GetBytes("application/json") },
        { "schema-version", Encoding.UTF8.GetBytes("2") },
        { "correlation-id", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()) },
        { "timestamp", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("o")) }
    }
};

// Reading headers
var contentType = Encoding.UTF8.GetString(
    consumeResult.Message.Headers.GetLastBytes("content-type")
);
```

## Generic Serialization Factory

```csharp
public static class SerializerFactory
{
    public enum SerializationType
    {
        Json,
        Protobuf,
        Avro,
        MessagePack
    }
    
    public static ISerializer<T> CreateSerializer<T>(SerializationType type)
    {
        return type switch
        {
            SerializationType.Json => new JsonSerializer<T>(),
            SerializationType.Protobuf => new ProtobufSerializer<T>() as ISerializer<T>,
            SerializationType.MessagePack => new MessagePackSerializer<T>(),
            _ => throw new ArgumentException($"Unsupported serialization type: {type}")
        };
    }
    
    public static IDeserializer<T> CreateDeserializer<T>(SerializationType type)
    {
        return type switch
        {
            SerializationType.Json => new JsonDeserializer<T>(),
            SerializationType.Protobuf => new ProtobufDeserializer<T>() as IDeserializer<T>,
            SerializationType.MessagePack => new MessagePackDeserializer<T>(),
            _ => throw new ArgumentException($"Unsupported serialization type: {type}")
        };
    }
}

// Usage
var producer = new ProducerBuilder<string, Order>(config)
    .SetValueSerializer(SerializerFactory.CreateSerializer<Order>(
        SerializerFactory.SerializationType.Json))
    .Build();
```

## Performance Considerations

### Serialization Benchmarks (Approximate)

```
Message Size: 1KB object

JSON (System.Text.Json):     ~2-3 KB,  ~500 ns
JSON (Newtonsoft):           ~2-3 KB,  ~800 ns
Protobuf:                    ~0.5 KB,  ~200 ns
Avro:                        ~0.6 KB,  ~300 ns
MessagePack:                 ~0.4 KB,  ~150 ns
```

### Optimization Tips

1. **Reuse serializers**: Don't create new serializer instances for each message
2. **Use binary formats** for high-throughput scenarios
3. **Enable compression** for large messages
4. **Batch messages** when possible
5. **Profile your serialization** to identify bottlenecks

## Next Steps

- [Working with Schemas](./05-WorkingWithSchemas.md)
- [Message Delivery and Transactions](./06-MessageDeliveryTransactions.md)
