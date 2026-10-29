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

package org.apache.spark.network.shuffle.streaming;

import io.netty.buffer.ByteBuf;
import java.util.zip.CRC32C;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Helper class for streaming shuffle checksum calculations.
 */
@NotThreadSafe
public final class ShuffleChecksum {
  private final CRC32C crc = new CRC32C();

  /**
   * Updates checksum for a specified portion of a ByteBuf message.
   *
   * @param message The ByteBuf to calculate checksum for
   * @param startIndex The index of the first byte to calculate checksum for
   * @param dataLength The length of the data to calculate checksum for
   */
  public void updateChecksum(ByteBuf message, int startIndex, int dataLength) {
    if (startIndex < 0) {
      throw new IllegalArgumentException(
        "startIndex must be non-negative: " + startIndex);
    }
    if (dataLength < 0) {
      throw new IllegalArgumentException(
        "dataLength must be non-negative: " + dataLength);
    }
    // Bound the range against writerIndex() rather than capacity(): the checksum must
    // cover actual written data only, never bytes in [writerIndex, capacity) which
    // may be uninitialized.
    if (startIndex + dataLength > message.writerIndex()) {
      throw new IllegalArgumentException(
        "startIndex + dataLength exceeds writerIndex: " +
          startIndex + " + " + dataLength + " > " + message.writerIndex());
    }
    if (message.hasArray()) {
      // heap-based ByteBuf
      crc.update(message.array(), message.arrayOffset() + startIndex, dataLength);
    } else {
      // off-heap ByteBuf
      crc.update(message.nioBuffer(startIndex, dataLength));
    }
  }

  public long getValue() {
    return crc.getValue();
  }

  public void reset() {
    crc.reset();
  }
}
