# Event Streaming

## What is Event Streaming?

In modern distributed systems, the traditional Request-Response model (often implemented via HTTP/REST) faces significant challenges when scaling to handle real-time data. While batch processing handles large volumes with high latency, and synchronous services handle low latency with lower throughput, **Event Streaming** bridges this gap by enabling continuous, asynchronous data processing.

Event streaming represents a paradigm shift from "data at rest" (querying a database) to "data in motion" (reacting to a continuous flow of events). It allows systems to capture, store, and process data in real-time as it is generated, decoupling the production of data from its consumption.

### The Limitations of Synchronous Architectures

To understand the necessity of event streaming, consider a synchronous microservices architecture. In this model, services communicate directly with one another, often resulting in **temporal coupling**—the sender and receiver must both be available and responsive at the exact same moment.

#### Case Study: Fitness Tracker Synchronization

Consider a fitness ecosystem where wearable devices sync activity data (steps, heart rate, SpO2) to a cloud backend.

**The Synchronous Approach (Request-Response):**
A mobile device collects data from the tracker and sends an HTTP POST request to a Metrics Service. The service processes this data, calculates aggregates, and writes to a database before returning a success response.

```mermaid
sequenceDiagram
    participant Tracker as Fitness Tracker
    participant Mobile as Mobile Device
    participant Service as Metrics Microservice
    participant DB as Database

    Note over Tracker, DB: Synchronous / Blocking Flow
    Tracker->>Mobile: Push Data (Steps, Heart Rate)
    Mobile->>Service: POST /metrics
    activate Service
    Service->>DB: INSERT Raw Data
    Service->>Service: Compute Complex Analytics
    Service->>DB: UPDATE Aggregates
    Service-->>Mobile: 200 OK (Processing Complete)
    deactivate Service
    Note right of Mobile: Client waits for entire process
```

**Architectural Bottlenecks:**

1.  **High Latency & Poor UX**: The mobile client is blocked waiting for the server to complete complex calculations. If the database is slow, the user experience degrades immediately.
2.  **Temporal Coupling**: If the Metrics Service is undergoing maintenance or crashing, the mobile app cannot sync data. It must implement complex retry logic, draining the device's battery.
3.  **Resource Contention**: During peak load (e.g., everyone syncing after a morning run), the service may become overwhelmed. Synchronous systems handle "backpressure" poorly; requests simply time out or fail, leading to data loss.
4.  **Cascading Failures**: If the database slows down, the Metrics Service threads block, eventually causing the service to stop accepting new connections.

### The Event Streaming Solution: Asynchronous & Event-Driven

To resolve the bottlenecks of synchronous coupling, we transition to an **Event-Driven Architecture (EDA)**. In this paradigm, the focus shifts from "requesting an action" to "announcing an occurrence."

#### 1. The Ingestion Layer (Smart Endpoints, Dumb Pipes)
Instead of asking the server to "calculate metrics" (a command), the mobile device simply informs the server that "steps were taken" (an event).
-   **Raw Data Ingestion**: The mobile device pushes raw, immutable facts (e.g., `StepsTaken`, `HeartRateRecorded`) to an Ingestion Service.
-   **Fast Acknowledgement**: The Ingestion Service performs minimal validation, serializes the data into an event, publishes it to a Kafka topic (e.g., `raw-telemetry`), and immediately returns a `202 Accepted` response.
-   **Client Impact**: The mobile device is released immediately. It does not wait for processing. If the backend processing is slow or down, the client is unaffected.

#### 2. The Stream Processing Pipeline
Once the event is durably stored in Kafka, a separate subsystem takes over. This is often referred to as a **Stream Processing Topology**.
-   **Decoupled Consumers**: A "Metrics Processor" microservice subscribes to the `raw-telemetry` topic. It consumes events at its own maximum throughput.
-   **Stateful Processing**: The processor might aggregate data (e.g., tumbling windows of 5 minutes) to calculate average heart rate or total steps.
-   **Deriving New Events**: The result of this computation is not just written to a database but often emitted as a *new* event to a different topic (e.g., `daily-metrics-updated`).

#### 3. Closing the Loop
Downstream applications subscribe to these processed events to trigger side effects.
-   **Notification Service**: Listens to `daily-metrics-updated` and pushes a notification to the user's device.
-   **Data Lake Sink**: Listens to the same topic to archive data for long-term analytics.

```mermaid
sequenceDiagram
    participant Mobile as Mobile Device
    participant Ingest as Ingestion Gateway
    participant Kafka as Kafka Cluster
    participant Processor as Stream Processor
    participant Notifier as Notification Service

    Note over Mobile, Notifier: Asynchronous Pipeline (Event Streaming)
    
    Mobile->>Ingest: POST /telemetry (Raw Data)
    Ingest->>Kafka: Produce: RawEvent
    Ingest-->>Mobile: 202 Accepted (Fast Ack)
    
    par Parallel Consumption
        loop Stream Processing
            Kafka->>Processor: Consume: RawEvent
            Processor->>Processor: Aggregate & Compute
            Processor->>Kafka: Produce: MetricsEvent
        end
        
        loop Reaction
            Kafka->>Notifier: Consume: MetricsEvent
            Notifier->>Mobile: Push Notification
        end
    end
```

### Technical Advantages

1.  **Temporal Decoupling**: The producer (mobile app) and consumer (metrics engine) do not need to be online at the same time. Kafka acts as a **persistent buffer**, absorbing load spikes and allowing consumers to catch up.
2.  **Eventual Consistency**: The system trades immediate consistency (ACID) for availability and partition tolerance (BASE). The mobile view will be updated "eventually" (usually milliseconds to seconds), but the system remains highly available.
3.  **Polyglot Consumption**: The `raw-telemetry` topic becomes a "source of truth." New services (e.g., a Machine Learning model for anomaly detection) can be added to subscribe to this topic without modifying the mobile app or the ingestion service.
4.  **Backpressure Management**: If the database slows down, the Stream Processor simply slows its consumption rate. The Ingestion Service continues accepting data at full speed, preventing cascading failures.

## Core Concepts

### Events
An **event** records the fact that "something happened" in the world or in your business. It is also called a record or message in the documentation.

When you read or write data to Kafka, you do this in the form of events. Conceptually, an event has:
- **Key**: Optional identifier for the event
- **Value**: The actual data payload
- **Timestamp**: When the event occurred
- **Headers**: Optional metadata

### Topics
Events are organized and durably stored in **topics**. A topic is similar to a folder in a filesystem, and the events are the files in that folder.

Key characteristics:
- Topics are **multi-producer**: Multiple applications can write events to the same topic
- Topics are **multi-subscriber**: Multiple applications can read events from the same topic
- Events are **not deleted** after consumption (unlike traditional messaging systems)
- Events are **retained** based on a configurable retention policy

### Partitions
Topics are **partitioned**, meaning a topic is spread over a number of "buckets" located on different Kafka brokers.

Benefits of partitioning:
- **Scalability**: Data is distributed across multiple brokers
- **Parallelism**: Multiple consumers can read from different partitions simultaneously
- **Ordering**: Events with the same key go to the same partition, maintaining order

### Producers and Consumers
- **Producers**: Applications that publish (write) events to topics
- **Consumers**: Applications that subscribe to (read and process) events from topics

## Event Streaming Use Cases

### Real-Time Processing
- Processing payments and financial transactions in real-time
- Tracking and monitoring cars, trucks, fleets, and shipments in real-time
- Continuously capturing and analyzing sensor data from IoT devices

### Data Integration
- Connecting and integrating data from multiple sources
- Building data pipelines between systems
- Synchronizing data across microservices

### Event-Driven Architectures
- Decoupling microservices
- Building reactive systems
- Implementing CQRS (Command Query Responsibility Segregation) patterns

## Apache Kafka Overview

Apache Kafka is a distributed event streaming platform that provides:

1. **Publish and Subscribe**: Read and write streams of events
2. **Store**: Durably and reliably store streams of events
3. **Process**: Process streams of events as they occur or retrospectively

### Kafka Architecture Components

#### Brokers
- Kafka runs as a cluster of one or more servers (brokers)
- Brokers handle read/write requests and store data
- Brokers are designed to be fault-tolerant and scalable

#### ZooKeeper (Legacy) / KRaft (Modern)
- **ZooKeeper**: Traditionally used for cluster coordination and metadata management
- **KRaft**: New consensus protocol that eliminates ZooKeeper dependency (Kafka 3.0+)

#### Topics and Partitions
- Data is organized into topics
- Each topic is divided into partitions for scalability
- Partitions are replicated across brokers for fault tolerance

#### Replication
- Each partition has one leader and zero or more followers
- Leaders handle all reads and writes
- Followers replicate the leader's data
- If a leader fails, a follower becomes the new leader

## Event Streaming Guarantees

### Delivery Semantics
1. **At-most-once**: Messages may be lost but are never redelivered
2. **At-least-once**: Messages are never lost but may be redelivered
3. **Exactly-once**: Each message is delivered exactly once (requires transactions)

### Ordering Guarantees
- Events within a single partition are strictly ordered
- Events across different partitions have no ordering guarantee
- Use the same key for related events to maintain order

## Benefits of Event Streaming with Kafka

### Scalability
- Horizontally scalable by adding more brokers
- Partitioning enables parallel processing
- Can handle millions of events per second

### Durability
- Events are persisted to disk
- Replication ensures data is not lost
- Configurable retention policies

### Performance
- High throughput for both publishing and subscribing
- Low latency (milliseconds)
- Efficient batching and compression

### Flexibility
- Decouple producers and consumers
- Multiple consumers can read the same data
- Replay events from any point in time

## Getting Started with Kafka

### Prerequisites
- Java 8+ (Kafka runs on the JVM)
- For .NET applications: Confluent.Kafka NuGet package

### Basic Workflow
1. **Set up Kafka cluster** (or use a managed service like Confluent Cloud)
2. **Create topics** to organize your events
3. **Implement producers** to publish events
4. **Implement consumers** to process events
5. **Monitor and manage** your Kafka cluster

## Next Steps

- [Microservice for Producing Kafka Messages](./02-ProducingMessages.md)
- [Microservice for Consuming Kafka Messages](./03-ConsumingMessages.md)
- [Message Serialization](./04-MessageSerialization.md)
- [Working with Schemas](./05-WorkingWithSchemas.md)
- [Message Delivery and Transactions](./06-MessageDeliveryTransactions.md)
