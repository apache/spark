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
import java.util.Optional;

/**
 * Keeps the index information for a particular map output
 * as an in-memory LongBuffer.
 */
public class ShuffleIndexInformation {
  /** offsets as long buffer */
  private final LongBuffer offsets;
  private final boolean hasDigest;
  /** digests as long buffer */
  private final LongBuffer digests;
  private int size;

  public ShuffleIndexInformation(File indexFile) throws IOException {
    size = (int)indexFile.length();
    ByteBuffer offsetsBuffer = ByteBuffer.allocate(size);
    offsets = offsetsBuffer.asLongBuffer();
    try (DataInputStream dis = new DataInputStream(Files.newInputStream(indexFile.toPath()))) {
      dis.readFully(offsetsBuffer.array());
    }

    // This logic is from IndexShuffleBlockResolver, and the digest file name is from
    // ShuffleIndexDigestBlockId.
    File digestFile = new File(indexFile.getAbsolutePath() + ".digest");
    if (digestFile.exists()) {
      hasDigest = true;
      size += digestFile.length();
      ByteBuffer digestsBuffer = ByteBuffer.allocate((int)digestFile.length());
      digests = digestsBuffer.asLongBuffer();
      try (DataInputStream digIs = new DataInputStream(Files.newInputStream(digestFile.toPath()))) {
        digIs.readFully(digestsBuffer.array());
      }
    } else {
      hasDigest = false;
      digests = null;
    }
  }

  /**
   * If this indexFile has digest
   */
  public boolean isHasDigest() {
    return hasDigest;
  }

  /**
   * Size of the index file
   * @return size
   */
  public int getSize() {
    return size;
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
    if (hasDigest && endReduceId - startReduceId == 1) {
      return new ShuffleIndexRecord(offset, nextOffset - offset,
        Optional.of(digests.get(startReduceId)));
    } else if (!hasDigest) {
      return new ShuffleIndexRecord(offset, nextOffset - offset, Optional.of(-1L));
    } else {
      return new ShuffleIndexRecord(offset, nextOffset - offset, Optional.empty());
    }
  }
}
