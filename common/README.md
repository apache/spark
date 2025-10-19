# Spark Common Modules

This directory contains common utilities and libraries shared across all Spark modules.

## Overview

The common modules provide foundational functionality used throughout Spark:

- Network communication
- Memory management utilities
- Serialization helpers
- Configuration management
- Logging infrastructure
- Testing utilities

These modules have no dependencies on Spark Core, allowing them to be used by any Spark component.

## Modules

### common/kvstore

Key-value store abstraction for metadata storage.

**Purpose:**
- Store application metadata
- Track job and stage information
- Persist UI data

**Location**: `kvstore/`

**Key classes:**
- `KVStore`: Interface for key-value storage
- `LevelDB`: LevelDB-based implementation
- `InMemoryStore`: In-memory implementation for testing

**Usage:**
```scala
val store = new LevelDB(path)
store.write(new StoreKey(id), value)
val data = store.read(classOf[ValueType], id)
```

### common/network-common

Core networking abstractions and utilities.

**Purpose:**
- RPC framework
- Block transfer protocol
- Network servers and clients

**Location**: `network-common/`

**Key components:**
- `TransportContext`: Network communication setup
- `TransportClient`: Network client
- `TransportServer`: Network server
- `MessageHandler`: Message processing
- `StreamManager`: Stream data management

**Features:**
- Netty-based implementation
- Zero-copy transfers
- SSL/TLS support
- Flow control

### common/network-shuffle

Network shuffle service for serving shuffle data.

**Purpose:**
- External shuffle service
- Serves shuffle blocks to executors
- Improves executor reliability

**Location**: `network-shuffle/`

**Key classes:**
- `ExternalShuffleService`: Standalone shuffle service
- `ExternalShuffleClient`: Client for fetching shuffle data
- `ShuffleBlockResolver`: Resolves shuffle block locations

**Benefits:**
- Executors can be killed without losing shuffle data
- Better resource utilization
- Improved fault tolerance

**Configuration:**
```properties
spark.shuffle.service.enabled=true
spark.shuffle.service.port=7337
```

### common/network-yarn

YARN-specific network integration.

**Purpose:**
- Integration with YARN shuffle service
- YARN auxiliary service implementation

**Location**: `network-yarn/`

**Usage:** Automatically used when running on YARN with shuffle service enabled.

### common/sketch

Data sketching and approximate algorithms.

**Purpose:**
- Memory-efficient approximate computations
- Probabilistic data structures

**Location**: `sketch/`

**Algorithms:**
- Count-Min Sketch: Frequency estimation
- Bloom Filter: Set membership testing
- HyperLogLog: Cardinality estimation

**Usage:**
```scala
import org.apache.spark.util.sketch._

// Create bloom filter
val bf = BloomFilter.create(expectedItems, falsePositiveRate)
bf.put("item1")
bf.mightContain("item1") // true

// Create count-min sketch
val cms = CountMinSketch.create(depth, width, seed)
cms.add("item", count)
val estimate = cms.estimateCount("item")
```

### common/tags

Test tags for categorizing tests.

**Purpose:**
- Tag tests for selective execution
- Categorize slow/flaky tests
- Enable/disable test groups

**Location**: `tags/`

**Example tags:**
- `@SlowTest`: Long-running tests
- `@ExtendedTest`: Extended test suite
- `@DockerTest`: Tests requiring Docker

### common/unsafe

Unsafe operations for performance-critical code.

**Purpose:**
- Direct memory access
- Serialization without reflection
- Performance optimizations

**Location**: `unsafe/`

**Key classes:**
- `Platform`: Platform-specific operations
- `UnsafeAlignedOffset`: Aligned memory access
- Memory utilities for sorting and hashing

**Warning:** These APIs are internal and subject to change.

## Architecture

### Layering

```
Spark Core / SQL / Streaming / MLlib
              ↓
    Common Modules (network, kvstore, etc.)
              ↓
        JVM / Netty / OS
```

### Design Principles

1. **No Spark Core dependencies**: Can be used independently
2. **Minimal external dependencies**: Reduce classpath conflicts
3. **High performance**: Optimized for throughput and latency
4. **Reusability**: Shared across all Spark components

## Networking Architecture

### Transport Layer

The network-common module provides the foundation for all network communication in Spark.

**Components:**

1. **TransportContext**: Sets up network infrastructure
2. **TransportClient**: Sends requests and receives responses
3. **TransportServer**: Accepts connections and handles requests
4. **MessageHandler**: Processes incoming messages

**Flow:**
```
Client                          Server
  |                               |
  |------ Request Message ------->|
  |                               | (Process in MessageHandler)
  |<----- Response Message -------|
  |                               |
```

### RPC Framework

Built on top of the transport layer:

```scala
// Server side
val rpcEnv = RpcEnv.create("name", host, port, conf)
val endpoint = new MyEndpoint(rpcEnv)
rpcEnv.setupEndpoint("my-endpoint", endpoint)

// Client side
val ref = rpcEnv.setupEndpointRef("spark://host:port/my-endpoint")
val response = ref.askSync[Response](request)
```

### Block Transfer

Optimized for transferring large data blocks:

```scala
val blockTransferService = new NettyBlockTransferService(conf)
blockTransferService.fetchBlocks(
  host, port, execId, blockIds,
  blockFetchingListener, tempFileManager
)
```

## Building and Testing

### Build Common Modules

```bash
# Build all common modules
./build/mvn -pl 'common/*' -am package

# Build specific module
./build/mvn -pl common/network-common -am package
```

### Run Tests

```bash
# Run all common tests
./build/mvn test -pl 'common/*'

# Run specific module tests
./build/mvn test -pl common/network-common

# Run specific test
./build/mvn test -pl common/network-common -Dtest=TransportClientSuite
```

## Module Dependencies

```
common/unsafe (no dependencies)
     ↓
common/network-common
     ↓
common/network-shuffle
     ↓
common/network-yarn
     
common/sketch (independent)
common/tags (independent)
common/kvstore (independent)
```

## Source Code Organization

```
common/
├── kvstore/              # Key-value store
│   └── src/main/java/org/apache/spark/util/kvstore/
├── network-common/       # Core networking
│   └── src/main/java/org/apache/spark/network/
│       ├── client/      # Client implementation
│       ├── server/      # Server implementation
│       ├── buffer/      # Buffer management
│       ├── crypto/      # Encryption
│       ├── protocol/    # Protocol messages
│       └── util/        # Utilities
├── network-shuffle/     # Shuffle service
│   └── src/main/java/org/apache/spark/network/shuffle/
├── network-yarn/        # YARN integration
│   └── src/main/java/org/apache/spark/network/yarn/
├── sketch/              # Sketching algorithms
│   └── src/main/java/org/apache/spark/util/sketch/
├── tags/                # Test tags
│   └── src/main/java/org/apache/spark/tags/
└── unsafe/              # Unsafe operations
    └── src/main/java/org/apache/spark/unsafe/
```

## Performance Considerations

### Zero-Copy Transfer

Network modules use zero-copy techniques:
- FileRegion for file-based transfers
- Direct buffers to avoid copying
- Netty's native transport when available

### Memory Management

```java
// Use pooled buffers
ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
ByteBuf buffer = allocator.directBuffer(size);
try {
  // Use buffer
} finally {
  buffer.release();
}
```

### Connection Pooling

Clients reuse connections:
```java
TransportClientFactory factory = context.createClientFactory();
TransportClient client = factory.createClient(host, port);
// Client is cached and reused
```

## Security

### SSL/TLS Support

Enable encryption in network communication:

```properties
spark.ssl.enabled=true
spark.ssl.protocol=TLSv1.2
spark.ssl.keyStore=/path/to/keystore
spark.ssl.keyStorePassword=password
spark.ssl.trustStore=/path/to/truststore
spark.ssl.trustStorePassword=password
```

### SASL Authentication

Support for SASL-based authentication:

```properties
spark.authenticate=true
spark.authenticate.secret=shared-secret
```

## Monitoring

### Network Metrics

Key metrics tracked:
- Active connections
- Bytes sent/received
- Request latency
- Connection failures

**Access via Spark UI**: `http://<driver>:4040/metrics/`

### Logging

Enable detailed network logging:

```properties
log4j.logger.org.apache.spark.network=DEBUG
log4j.logger.io.netty=DEBUG
```

## Configuration

### Network Settings

```properties
# Connection timeout
spark.network.timeout=120s

# I/O threads
spark.network.io.numConnectionsPerPeer=1

# Buffer sizes
spark.network.io.preferDirectBufs=true

# Maximum retries
spark.network.io.maxRetries=3

# Connection pooling
spark.rpc.numRetries=3
spark.rpc.retry.wait=3s
```

### Shuffle Service

```properties
spark.shuffle.service.enabled=true
spark.shuffle.service.port=7337
spark.shuffle.service.index.cache.size=100m
```

## Best Practices

1. **Reuse connections**: Don't create new clients unnecessarily
2. **Release buffers**: Always release ByteBuf instances
3. **Handle backpressure**: Implement flow control in handlers
4. **Enable encryption**: Use SSL for sensitive data
5. **Monitor metrics**: Track network performance
6. **Configure timeouts**: Set appropriate timeout values
7. **Use external shuffle service**: For production deployments

## Troubleshooting

### Connection Issues

**Problem**: Connection refused or timeout

**Solutions:**
- Check firewall settings
- Verify host and port
- Increase timeout values
- Check network connectivity

### Memory Leaks

**Problem**: Growing memory usage in network layer

**Solutions:**
- Ensure ByteBuf.release() is called
- Check for unclosed connections
- Monitor Netty buffer pool metrics

### Slow Performance

**Problem**: High network latency

**Solutions:**
- Enable native transport
- Increase I/O threads
- Adjust buffer sizes
- Check network bandwidth

## Internal APIs

**Note**: All classes in common modules are internal APIs and may change between versions. They are not part of the public Spark API.

## Further Reading

- [Cluster Mode Overview](../docs/cluster-overview.md)
- [Configuration Guide](../docs/configuration.md)
- [Security Guide](../docs/security.md)

## Contributing

For contributing to common modules, see [CONTRIBUTING.md](../CONTRIBUTING.md).

When adding functionality:
- Keep dependencies minimal
- Write comprehensive tests
- Document public methods
- Consider performance implications
- Maintain backward compatibility where possible
