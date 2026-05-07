/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.file.Files;

/**
 * Keeps the index information for a particular map output
 * as an in-memory LongBuffer.
 */
public class ShuffleIndexInformation {

  // The estimate of `ShuffleIndexInformation` memory footprint which is relevant in case of small
  // index files (i.e. storing only 2 offsets = 16 bytes).
  static final int INSTANCE_MEMORY_FOOTPRINT = 176;

  /** offsets as long buffer */
  private final LongBuffer offsets;

  public ShuffleIndexInformation(String indexFilePath) throws IOException {
    File indexFile = new File(indexFilePath);
    ByteBuffer buffer = ByteBuffer.allocate((int)indexFile.length());
    offsets = buffer.asLongBuffer();
    try (DataInputStream dis = new DataInputStream(Files.newInputStream(indexFile.toPath()))) {
      dis.readFully(buffer.array());
    }
  }

  public int getRetainedMemorySize() {
    // SPARK-33206: here the offsets' capacity is multiplied by 8 as offsets stores long values.
    // Integer overflow won't be an issue here as long as the number of reducers is under
    // (Integer.MAX_VALUE - INSTANCE_MEMORY_FOOTPRINT) / 8 - 1 = 268435432.
    return (offsets.capacity() << 3) + INSTANCE_MEMORY_FOOTPRINT;
  }

  /**
   * Get index offset for a particular reducer.
   */
  public ShuffleIndexRecord getIndex(int reduceId) {
    return getIndex(reduceId, reduceId + 1);
  }

  /**
   * Get index offset for the reducer range of [startReduceId, endReduceId).
   */
  public ShuffleIndexRecord getIndex(int startReduceId, int endReduceId) {
    long offset = offsets.get(startReduceId);
    long nextOffset = offsets.get(endReduceId);
    return new ShuffleIndexRecord(offset, nextOffset - offset);
  }
}
