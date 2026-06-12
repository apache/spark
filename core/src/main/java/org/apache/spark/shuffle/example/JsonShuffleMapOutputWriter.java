/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.example;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage;

public class JsonShuffleMapOutputWriter implements ShuffleMapOutputWriter {

  private final int shuffleId;
  private final long mapId;
  private final int numPartitions;
  private final Path outputDirectory;
  private final ObjectMapper objectMapper;
  private final Map<Integer, JsonShufflePartitionWriter> writers;

  public JsonShuffleMapOutputWriter(
      int shuffleId,
      long mapId,
      int numPartitions,
      String outputDirectory) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.numPartitions = numPartitions;
    this.outputDirectory = Paths.get(outputDirectory, "shuffle-" + shuffleId, "map-" + mapId);
    this.objectMapper = new ObjectMapper();
    this.writers = new HashMap<>();
  }

  @Override
  public ShufflePartitionWriter getPartitionWriter(int reducePartitionId) throws IOException {
    if (writers.containsKey(reducePartitionId)) {
      throw new IllegalArgumentException("Partition writer already created for partition " + reducePartitionId);
    }
    Path partitionFile = outputDirectory.resolve("partition-" + reducePartitionId + ".json");
    JsonShufflePartitionWriter writer = new JsonShufflePartitionWriter(
      reducePartitionId, partitionFile, objectMapper);
    writers.put(reducePartitionId, writer);
    return writer;
  }

  @Override
  public MapOutputCommitMessage commitAllPartitions(long[] checksums) throws IOException {
    long[] partitionLengths = new long[numPartitions];
    for (Map.Entry<Integer, JsonShufflePartitionWriter> entry : writers.entrySet()) {
      int partitionId = entry.getKey();
      JsonShufflePartitionWriter writer = entry.getValue();
      writer.writeJson();
      partitionLengths[partitionId] = writer.getNumBytesWritten();
    }
    return MapOutputCommitMessage.of(partitionLengths);
  }

  @Override
  public void abort(Throwable error) throws IOException {
    if (Files.exists(outputDirectory)) {
      try {
        Files.walk(outputDirectory)
          .sorted((a, b) -> b.compareTo(a))
          .forEach(path -> {
            try {
              Files.deleteIfExists(path);
            } catch (IOException ignored) {
            }
          });
      } catch (IOException ignored) {
      }
    }
  }
}
