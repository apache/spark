# JsonShuffleDataIO Design and Implementation

## Overview

This change adds a new example Spark shuffle plugin that writes shuffle map output partition data as JSON files. Each partition output is serialized into a JSON object containing metadata and Base64-encoded payload data.

## Design

### Plugin contract

The implementation follows Spark's `ShuffleDataIO` plugin API:

- `ShuffleDataIO`: the plugin entry point used by Spark to access driver and executor components.
- `ShuffleDriverComponents`: initialized on the driver, returns extra configs for executors.
- `ShuffleExecutorComponents`: initialized on executors, creates `ShuffleMapOutputWriter`.
- `ShuffleMapOutputWriter`: produces a `ShufflePartitionWriter` for each partition.
- `ShufflePartitionWriter`: collects partition bytes and writes JSON when closed.

### JSON output format

Each partition output file contains a JSON object with the following fields:

- `partitionId`: integer partition identifier
- `bytesWritten`: number of bytes written to the partition
- `timestamp`: time when the JSON file is created
- `data`: Base64-encoded partition payload

The files are written under the configured output directory as:

- `<outputDir>/shuffle-<shuffleId>/map-<mapId>/partition-<partitionId>.json`

## Implementation details

### Files added/updated

- `core/src/main/java/org/apache/spark/shuffle/example/JsonShuffleDataIO.java`
  - Plugin entry point returning driver and executor components.
- `core/src/main/java/org/apache/spark/shuffle/example/JsonShuffleDriverComponents.java`
  - Reads `spark.shuffle.sort.io.json.outputDir` and initializes application extra configs.
- `core/src/main/java/org/apache/spark/shuffle/example/JsonShuffleExecutorComponents.java`
  - Reads executor config and constructs `JsonShuffleMapOutputWriter`.
- `core/src/main/java/org/apache/spark/shuffle/example/JsonShuffleMapOutputWriter.java`
  - Creates partition writers, tracks output targets, and commits partition files.
- `core/src/main/java/org/apache/spark/shuffle/example/JsonShufflePartitionWriter.java`
  - Buffers partition bytes and writes JSON output on close.
- `core/src/test/scala/org/apache/spark/shuffle/example/JsonShuffleDataIOSuite.scala`
  - End-to-end test covering driver initialization, executor initialization, partition writing, and JSON file verification.

### Key implementation notes

- The executor reads `spark.shuffle.sort.io.json.outputDir` via extra configs supplied by `ShuffleDriverComponents.initializeApplication()`.
- Partition writers buffer all data in-memory using `ByteArrayOutputStream` and serialize JSON when the output stream is closed.
- JSON generation uses Jackson's `ObjectMapper`.
- The test verifies partition JSON files exist and validates `partitionId`, `bytesWritten`, and presence of `data`.

## Test results

### Commands run

- `build/sbt -java-home /usr/local/sdkman/candidates/java/21.0.10-ms 'core/testOnly *JsonShuffleDataIOSuite'`

### Outcome

- `JsonShuffleDataIOSuite` passed successfully.
- The new end-to-end test completed with:
  - 1 suite completed
  - 1 test succeeded
  - 0 failed

## Notes

- The implementation is intentionally simple and example-oriented. It demonstrates how to use the Spark shuffle plugin API with JSON-backed shuffle outputs.
- This design is suitable for small-to-moderate payloads because partition data is buffered in memory before serialization.
