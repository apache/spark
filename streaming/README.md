# Spark Streaming

Spark Streaming provides scalable, high-throughput, fault-tolerant stream processing of live data streams.

## Overview

Spark Streaming supports two APIs:

1. **DStreams (Discretized Streams)** - Legacy API (Deprecated as of Spark 3.4)
2. **Structured Streaming** - Modern API built on Spark SQL (Recommended)

**Note**: DStreams are deprecated. For new applications, use **Structured Streaming** which is located in the `sql/core` module.

## DStreams (Legacy API)

### What are DStreams?

DStreams represent a continuous stream of data, internally represented as a sequence of RDDs.

**Key characteristics:**
- Micro-batch processing model
- Integration with Kafka, Flume, Kinesis, TCP sockets, and more
- Windowing operations for time-based aggregations
- Stateful transformations with updateStateByKey
- Fault tolerance through checkpointing

### Location

- Scala/Java: `src/main/scala/org/apache/spark/streaming/`
- Python: `../python/pyspark/streaming/`

### Basic Example

```scala
import org.apache.spark.streaming._
import org.apache.spark.SparkConf

val conf = new SparkConf().setAppName("NetworkWordCount")
val ssc = new StreamingContext(conf, Seconds(1))

// Create DStream from TCP source
val lines = ssc.socketTextStream("localhost", 9999)

// Process the stream
val words = lines.flatMap(_.split(" "))
val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

// Print results
wordCounts.print()

// Start the computation
ssc.start()
ssc.awaitTermination()
```

### Key Components

#### StreamingContext

The main entry point for streaming functionality.

**File**: `src/main/scala/org/apache/spark/streaming/StreamingContext.scala`

**Usage:**
```scala
val ssc = new StreamingContext(sparkContext, Seconds(batchInterval))
// or
val ssc = new StreamingContext(conf, Seconds(batchInterval))
```

#### DStream

The fundamental abstraction for a continuous data stream.

**File**: `src/main/scala/org/apache/spark/streaming/dstream/DStream.scala`

**Operations:**
- **Transformations**: map, flatMap, filter, reduce, join, window
- **Output Operations**: print, saveAsTextFiles, foreachRDD

#### Input Sources

**Built-in sources:**
- `socketTextStream`: TCP socket source
- `textFileStream`: File system monitoring
- `queueStream`: Queue-based testing source

**Advanced sources** (require external libraries):
- Kafka: `KafkaUtils.createStream`
- Flume: `FlumeUtils.createStream`
- Kinesis: `KinesisUtils.createStream`

**Location**: `src/main/scala/org/apache/spark/streaming/dstream/`

### Windowing Operations

Process data over sliding windows:

```scala
val windowedStream = lines
  .window(Seconds(30), Seconds(10))  // 30s window, 10s slide
  
val windowedWordCounts = words
  .map(x => (x, 1))
  .reduceByKeyAndWindow(_ + _, Seconds(30), Seconds(10))
```

### Stateful Operations

Maintain state across batches:

```scala
def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
  val newCount = runningCount.getOrElse(0) + newValues.sum
  Some(newCount)
}

val runningCounts = pairs.updateStateByKey(updateFunction)
```

### Checkpointing

Essential for stateful operations and fault tolerance:

```scala
ssc.checkpoint("hdfs://checkpoint/directory")
```

**What gets checkpointed:**
- Configuration
- DStream operations
- Incomplete batches
- State data (for stateful operations)

### Performance Tuning

**Batch Interval**
- Set based on processing time and latency requirements
- Too small: overhead increases
- Too large: latency increases

**Parallelism**
```scala
// Increase receiver parallelism
val numStreams = 5
val streams = (1 to numStreams).map(_ => ssc.socketTextStream(...))
val unifiedStream = ssc.union(streams)

// Repartition for processing
val repartitioned = dstream.repartition(10)
```

**Memory Management**
```scala
conf.set("spark.streaming.receiver.maxRate", "10000")
conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
```

## Structured Streaming (Recommended)

For new applications, use Structured Streaming instead of DStreams.

**Location**: `../sql/core/src/main/scala/org/apache/spark/sql/streaming/`

**Example:**
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._

val spark = SparkSession.builder()
  .appName("StructuredNetworkWordCount")
  .getOrCreate()

import spark.implicits._

// Create DataFrame from stream source
val lines = spark
  .readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 9999)
  .load()

// Process the stream
val words = lines.as[String].flatMap(_.split(" "))
val wordCounts = words.groupBy("value").count()

// Output the stream
val query = wordCounts
  .writeStream
  .outputMode("complete")
  .format("console")
  .start()

query.awaitTermination()
```

**Advantages over DStreams:**
- Unified API with batch processing
- Better performance with Catalyst optimizer
- Exactly-once semantics
- Event time processing
- Watermarking for late data
- Easier to reason about

See [Structured Streaming Guide](../docs/structured-streaming-programming-guide.md) for details.

## Building and Testing

### Build Streaming Module

```bash
# Build streaming module
./build/mvn -pl streaming -am package

# Skip tests
./build/mvn -pl streaming -am -DskipTests package
```

### Run Tests

```bash
# Run all streaming tests
./build/mvn test -pl streaming

# Run specific test suite
./build/mvn test -pl streaming -Dtest=BasicOperationsSuite
```

## Source Code Organization

```
streaming/src/main/
├── scala/org/apache/spark/streaming/
│   ├── StreamingContext.scala           # Main entry point
│   ├── Time.scala                       # Time utilities
│   ├── Checkpoint.scala                 # Checkpointing
│   ├── dstream/
│   │   ├── DStream.scala               # Base DStream
│   │   ├── InputDStream.scala          # Input sources
│   │   ├── ReceiverInputDStream.scala  # Receiver-based input
│   │   ├── WindowedDStream.scala       # Windowing operations
│   │   ├── StateDStream.scala          # Stateful operations
│   │   └── PairDStreamFunctions.scala  # Key-value operations
│   ├── receiver/
│   │   ├── Receiver.scala              # Base receiver class
│   │   ├── ReceiverSupervisor.scala    # Receiver management
│   │   └── BlockGenerator.scala        # Block generation
│   ├── scheduler/
│   │   ├── JobScheduler.scala          # Job scheduling
│   │   ├── JobGenerator.scala          # Job generation
│   │   └── ReceiverTracker.scala       # Receiver tracking
│   └── ui/
│       └── StreamingTab.scala          # Web UI
└── resources/
```

## Integration with External Systems

### Apache Kafka

**Deprecated DStreams approach:**
```scala
import org.apache.spark.streaming.kafka010._

val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "localhost:9092",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "test-group"
)

val stream = KafkaUtils.createDirectStream[String, String](
  ssc,
  PreferConsistent,
  Subscribe[String, String](topics, kafkaParams)
)
```

**Recommended Structured Streaming approach:**
```scala
val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "topic1")
  .load()
```

See [Kafka Integration Guide](../docs/streaming-kafka-integration.md).

### Amazon Kinesis

```scala
import org.apache.spark.streaming.kinesis._

val stream = KinesisInputDStream.builder
  .streamingContext(ssc)
  .endpointUrl("https://kinesis.us-east-1.amazonaws.com")
  .regionName("us-east-1")
  .streamName("myStream")
  .build()
```

See [Kinesis Integration Guide](../docs/streaming-kinesis-integration.md).

## Monitoring and Debugging

### Streaming UI

Access at: `http://<driver-node>:4040/streaming/`

**Metrics:**
- Batch processing times
- Input rates
- Scheduling delays
- Active batches

### Logs

Enable detailed logging:
```properties
log4j.logger.org.apache.spark.streaming=DEBUG
```

### Metrics

Key metrics to monitor:
- **Batch Processing Time**: Should be < batch interval
- **Scheduling Delay**: Should be minimal
- **Total Delay**: End-to-end delay
- **Input Rate**: Records per second

## Common Issues

### Batch Processing Time > Batch Interval

**Symptoms**: Scheduling delay increases over time

**Solutions:**
- Increase parallelism
- Optimize transformations
- Increase resources (executors, memory)
- Reduce batch interval data volume

### Out of Memory Errors

**Solutions:**
- Increase executor memory
- Enable compression
- Reduce window/batch size
- Persist less data

### Receiver Failures

**Solutions:**
- Enable WAL (Write-Ahead Logs)
- Increase receiver memory
- Add multiple receivers
- Use Structured Streaming with better fault tolerance

## Migration from DStreams to Structured Streaming

**Why migrate:**
- DStreams are deprecated
- Better performance and semantics
- Unified API with batch processing
- Active development and support

**Key differences:**
- DataFrame/Dataset API instead of RDDs
- Declarative operations
- Built-in support for event time
- Exactly-once semantics by default

**Migration guide**: See [Structured Streaming Migration Guide](../docs/ss-migration-guide.md)

## Examples

See [examples/src/main/scala/org/apache/spark/examples/streaming/](../examples/src/main/scala/org/apache/spark/examples/streaming/) for more examples.

**Key examples:**
- `NetworkWordCount.scala`: Basic word count
- `StatefulNetworkWordCount.scala`: Stateful processing
- `WindowedNetworkWordCount.scala`: Window operations
- `KafkaWordCount.scala`: Kafka integration

## Configuration

Key configuration parameters:

```properties
# Batch interval (set in code)
# StreamingContext(conf, Seconds(batchInterval))

# Backpressure (rate limiting)
spark.streaming.backpressure.enabled=true

# Receiver memory
spark.streaming.receiver.maxRate=10000

# Checkpoint interval
spark.streaming.checkpoint.interval=10s

# Graceful shutdown
spark.streaming.stopGracefullyOnShutdown=true
```

## Best Practices

1. **Use Structured Streaming for new applications**
2. **Set appropriate batch intervals** based on latency requirements
3. **Enable checkpointing** for stateful operations
4. **Monitor batch processing times** to ensure they're less than batch interval
5. **Use backpressure** to handle variable input rates
6. **Test failure scenarios** with checkpointing
7. **Consider using Kafka** for reliable message delivery

## Further Reading

- [Structured Streaming Programming Guide](../docs/structured-streaming-programming-guide.md) (Recommended)
- [DStreams Programming Guide](../docs/streaming-programming-guide.md) (Legacy)
- [Kafka Integration](../docs/streaming-kafka-integration.md)
- [Kinesis Integration](../docs/streaming-kinesis-integration.md)

## Contributing

For contributing to Spark Streaming, see [CONTRIBUTING.md](../CONTRIBUTING.md).

Note: New features should focus on Structured Streaming rather than DStreams.
