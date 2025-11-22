# Working with Schemas

## Overview

Schema management is critical for maintaining data quality and enabling evolution in event-driven systems. Schemas define the structure of your messages, enforce contracts between producers and consumers, and enable backward and forward compatibility.

## Why Use Schemas?

### Benefits

1. **Data Quality**: Ensure messages conform to expected structure
2. **Documentation**: Self-documenting message formats
3. **Evolution**: Safely evolve message formats over time
4. **Compatibility**: Prevent breaking changes
5. **Validation**: Catch errors at serialization time
6. **Tooling**: Enable better IDE support and code generation

### Without Schemas

```csharp
// Producer sends this
var message = new { OrderId = "123", Amount = 99.99 };

// Consumer expects this - runtime error!
var order = JsonSerializer.Deserialize<Order>(json);
// Error: Missing required field 'CustomerId'
```

### With Schemas

```csharp
// Schema enforces structure at compile/runtime
// Producer cannot send invalid data
// Consumer knows exactly what to expect
```

## Schema Registry

Confluent Schema Registry is a centralized service for managing schemas. It provides:

- **Schema storage**: Central repository for all schemas
- **Versioning**: Track schema versions over time
- **Compatibility checking**: Prevent breaking changes
- **Schema evolution**: Safely update schemas

### Architecture

```
Producer → Schema Registry (register/validate) → Kafka Broker
Consumer → Schema Registry (fetch schema) → Deserialize
```

## Setting Up Schema Registry

### Docker Compose

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
  
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  
  schema-registry:
    image: confluentinc/cp-schema-registry:latest
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
```

### Start Services

```bash
docker-compose up -d
```

## Avro Schemas

Apache Avro is the most common schema format used with Kafka.

### Install NuGet Packages

```bash
dotnet add package Confluent.SchemaRegistry
dotnet add package Confluent.SchemaRegistry.Serdes.Avro
```

### Define Avro Schema

#### Option 1: JSON Schema Definition

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.orders",
  "fields": [
    {
      "name": "orderId",
      "type": "string"
    },
    {
      "name": "customerId",
      "type": "string"
    },
    {
      "name": "amount",
      "type": "double"
    },
    {
      "name": "createdAt",
      "type": "long",
      "logicalType": "timestamp-millis"
    },
    {
      "name": "items",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "OrderItem",
          "fields": [
            {
              "name": "productId",
              "type": "string"
            },
            {
              "name": "quantity",
              "type": "int"
            },
            {
              "name": "price",
              "type": "double"
            }
          ]
        }
      }
    }
  ]
}
```

#### Option 2: C# Class with Attributes

```csharp
using Avro;
using Avro.Specific;

[AvroNamespace("com.example.orders")]
public class Order : ISpecificRecord
{
    public string OrderId { get; set; }
    public string CustomerId { get; set; }
    public double Amount { get; set; }
    public long CreatedAt { get; set; }
    public List<OrderItem> Items { get; set; }
    
    public Schema Schema => Schema.Parse(@"{
        ""type"": ""record"",
        ""name"": ""Order"",
        ""namespace"": ""com.example.orders"",
        ""fields"": [
            {""name"": ""orderId"", ""type"": ""string""},
            {""name"": ""customerId"", ""type"": ""string""},
            {""name"": ""amount"", ""type"": ""double""},
            {""name"": ""createdAt"", ""type"": ""long""},
            {""name"": ""items"", ""type"": {""type"": ""array"", ""items"": ""OrderItem""}}
        ]
    }");
    
    public object Get(int fieldPos)
    {
        return fieldPos switch
        {
            0 => OrderId,
            1 => CustomerId,
            2 => Amount,
            3 => CreatedAt,
            4 => Items,
            _ => throw new AvroRuntimeException($"Bad index {fieldPos}")
        };
    }
    
    public void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: OrderId = (string)fieldValue; break;
            case 1: CustomerId = (string)fieldValue; break;
            case 2: Amount = (double)fieldValue; break;
            case 3: CreatedAt = (long)fieldValue; break;
            case 4: Items = (List<OrderItem>)fieldValue; break;
            default: throw new AvroRuntimeException($"Bad index {fieldPos}");
        }
    }
}
```

### Producer with Avro and Schema Registry

```csharp
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

public class AvroProducer
{
    private readonly IProducer<string, Order> _producer;
    private readonly ISchemaRegistryClient _schemaRegistry;
    
    public AvroProducer(string bootstrapServers, string schemaRegistryUrl)
    {
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = schemaRegistryUrl
        };
        
        _schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All
        };
        
        _producer = new ProducerBuilder<string, Order>(producerConfig)
            .SetValueSerializer(new AvroSerializer<Order>(_schemaRegistry, new AvroSerializerConfig
            {
                // Auto-register schemas
                AutoRegisterSchemas = true,
                
                // Subject name strategy
                SubjectNameStrategy = SubjectNameStrategy.TopicRecord
            }))
            .Build();
    }
    
    public async Task ProduceAsync(string topic, Order order)
    {
        var message = new Message<string, Order>
        {
            Key = order.OrderId,
            Value = order
        };
        
        var result = await _producer.ProduceAsync(topic, message);
        
        Console.WriteLine($"Produced to {result.Topic} [{result.Partition}] @ {result.Offset}");
    }
    
    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
        _schemaRegistry?.Dispose();
    }
}
```

### Consumer with Avro and Schema Registry

```csharp
public class AvroConsumer
{
    private readonly IConsumer<string, Order> _consumer;
    private readonly ISchemaRegistryClient _schemaRegistry;
    
    public AvroConsumer(string bootstrapServers, string schemaRegistryUrl, string groupId)
    {
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = schemaRegistryUrl
        };
        
        _schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        
        _consumer = new ConsumerBuilder<string, Order>(consumerConfig)
            .SetValueDeserializer(new AvroDeserializer<Order>(_schemaRegistry).AsSyncOverAsync())
            .Build();
    }
    
    public void StartConsuming(string topic, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = _consumer.Consume(cancellationToken);
                
                var order = result.Message.Value;
                Console.WriteLine($"Consumed order: {order.OrderId}, Amount: {order.Amount}");
                
                _consumer.Commit(result);
            }
        }
        finally
        {
            _consumer.Close();
            _schemaRegistry?.Dispose();
        }
    }
}
```

## JSON Schema

JSON Schema is another popular format for defining message structure.

### Install NuGet Package

```bash
dotnet add package Confluent.SchemaRegistry.Serdes.Json
```

### Define JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Order",
  "type": "object",
  "properties": {
    "orderId": {
      "type": "string",
      "description": "Unique order identifier"
    },
    "customerId": {
      "type": "string"
    },
    "amount": {
      "type": "number",
      "minimum": 0
    },
    "createdAt": {
      "type": "string",
      "format": "date-time"
    },
    "items": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "productId": { "type": "string" },
          "quantity": { "type": "integer", "minimum": 1 },
          "price": { "type": "number", "minimum": 0 }
        },
        "required": ["productId", "quantity", "price"]
      }
    }
  },
  "required": ["orderId", "customerId", "amount", "createdAt"]
}
```

### JSON Schema Producer

```csharp
using Confluent.SchemaRegistry.Serdes;

var producer = new ProducerBuilder<string, Order>(producerConfig)
    .SetValueSerializer(new JsonSerializer<Order>(_schemaRegistry, new JsonSerializerConfig
    {
        AutoRegisterSchemas = true,
        SubjectNameStrategy = SubjectNameStrategy.TopicRecord
    }))
    .Build();
```

## Protobuf Schemas

Protocol Buffers can also be used with Schema Registry.

### Install NuGet Package

```bash
dotnet add package Confluent.SchemaRegistry.Serdes.Protobuf
```

### Define Protobuf Schema

```protobuf
syntax = "proto3";

package com.example.orders;

message Order {
  string order_id = 1;
  string customer_id = 2;
  double amount = 3;
  int64 created_at = 4;
  repeated OrderItem items = 5;
}

message OrderItem {
  string product_id = 1;
  int32 quantity = 2;
  double price = 3;
}
```

### Protobuf Producer

```csharp
using Confluent.SchemaRegistry.Serdes;

var producer = new ProducerBuilder<string, Order>(producerConfig)
    .SetValueSerializer(new ProtobufSerializer<Order>(_schemaRegistry, new ProtobufSerializerConfig
    {
        AutoRegisterSchemas = true
    }))
    .Build();
```

## Schema Evolution

### Compatibility Types

Schema Registry supports different compatibility modes:

1. **BACKWARD** (default): New schema can read old data
2. **FORWARD**: Old schema can read new data
3. **FULL**: Both backward and forward compatible
4. **NONE**: No compatibility checking

### Setting Compatibility

```bash
# Set compatibility for a subject
curl -X PUT http://localhost:8081/config/orders-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
```

### Backward Compatible Evolution

```json
// Version 1
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "amount", "type": "double"}
  ]
}

// Version 2 - Backward compatible (added optional field with default)
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "customerId", "type": ["null", "string"], "default": null}
  ]
}
```

### Forward Compatible Evolution

```json
// Version 1
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "customerId", "type": ["null", "string"], "default": null}
  ]
}

// Version 2 - Forward compatible (removed optional field)
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "amount", "type": "double"}
  ]
}
```

## Schema Registry REST API

### Register Schema

```csharp
using System.Net.Http;
using System.Text;
using System.Text.Json;

public class SchemaRegistryClient
{
    private readonly HttpClient _httpClient;
    private readonly string _baseUrl;
    
    public SchemaRegistryClient(string baseUrl)
    {
        _baseUrl = baseUrl;
        _httpClient = new HttpClient();
    }
    
    public async Task<int> RegisterSchemaAsync(string subject, string schema)
    {
        var payload = new
        {
            schema = schema,
            schemaType = "AVRO"
        };
        
        var content = new StringContent(
            JsonSerializer.Serialize(payload),
            Encoding.UTF8,
            "application/vnd.schemaregistry.v1+json"
        );
        
        var response = await _httpClient.PostAsync(
            $"{_baseUrl}/subjects/{subject}/versions",
            content
        );
        
        response.EnsureSuccessStatusCode();
        
        var result = await response.Content.ReadAsStringAsync();
        var json = JsonSerializer.Deserialize<Dictionary<string, object>>(result);
        
        return Convert.ToInt32(json["id"]);
    }
    
    public async Task<string> GetSchemaAsync(string subject, int version)
    {
        var response = await _httpClient.GetAsync(
            $"{_baseUrl}/subjects/{subject}/versions/{version}"
        );
        
        response.EnsureSuccessStatusCode();
        
        var result = await response.Content.ReadAsStringAsync();
        var json = JsonSerializer.Deserialize<Dictionary<string, object>>(result);
        
        return json["schema"].ToString();
    }
    
    public async Task<List<int>> GetVersionsAsync(string subject)
    {
        var response = await _httpClient.GetAsync(
            $"{_baseUrl}/subjects/{subject}/versions"
        );
        
        response.EnsureSuccessStatusCode();
        
        var result = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<List<int>>(result);
    }
}
```

## Best Practices

### 1. Use Meaningful Field Names

```json
// Good
{
  "name": "customerId",
  "type": "string"
}

// Bad
{
  "name": "cid",
  "type": "string"
}
```

### 2. Always Provide Defaults for Optional Fields

```json
{
  "name": "email",
  "type": ["null", "string"],
  "default": null
}
```

### 3. Use Logical Types

```json
{
  "name": "createdAt",
  "type": "long",
  "logicalType": "timestamp-millis"
}
```

### 4. Version Your Schemas

```csharp
public class OrderV1
{
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
}

public class OrderV2
{
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
    public string CustomerId { get; set; }  // New field
}
```

### 5. Test Schema Compatibility

```csharp
public async Task<bool> IsCompatibleAsync(string subject, string newSchema)
{
    var payload = new { schema = newSchema };
    
    var content = new StringContent(
        JsonSerializer.Serialize(payload),
        Encoding.UTF8,
        "application/vnd.schemaregistry.v1+json"
    );
    
    var response = await _httpClient.PostAsync(
        $"{_baseUrl}/compatibility/subjects/{subject}/versions/latest",
        content
    );
    
    var result = await response.Content.ReadAsStringAsync();
    var json = JsonSerializer.Deserialize<Dictionary<string, object>>(result);
    
    return Convert.ToBoolean(json["is_compatible"]);
}
```

### 6. Use Subject Name Strategies

```csharp
// Topic name strategy: <topic>-value
SubjectNameStrategy = SubjectNameStrategy.Topic

// Record name strategy: <namespace>.<record-name>
SubjectNameStrategy = SubjectNameStrategy.Record

// Topic-record name strategy: <topic>-<namespace>.<record-name>
SubjectNameStrategy = SubjectNameStrategy.TopicRecord
```

## Monitoring Schemas

### List All Subjects

```bash
curl http://localhost:8081/subjects
```

### Get Schema Versions

```bash
curl http://localhost:8081/subjects/orders-value/versions
```

### Get Specific Schema

```bash
curl http://localhost:8081/subjects/orders-value/versions/1
```

### Delete Schema

```bash
curl -X DELETE http://localhost:8081/subjects/orders-value/versions/1
```

## Next Steps

- [Message Delivery and Transactions](./06-MessageDeliveryTransactions.md)
- [Event Streaming](./01-EventStreaming.md)
