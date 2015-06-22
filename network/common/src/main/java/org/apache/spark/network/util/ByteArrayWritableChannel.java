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

package org.apache.spark.network.util;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A writable channel that stores the written data in a byte array in memory.
 */
public class ByteArrayWritableChannel implements WritableByteChannel {

  private final byte[] data;
  private int offset;

  public ByteArrayWritableChannel(int size) {
    this.data = new byte[size];
  }

  public byte[] getData() {
    return data;
  }

  public int length() {
    return offset;
  }

  /** Resets the channel so that writing to it will overwrite the existing buffer. */
  public void reset() {
    offset = 0;
  }

  /**
   * Reads from the given buffer into the internal byte array.
   */
  @Override
  public int write(ByteBuffer src) {
    int toTransfer = Math.min(src.remaining(), data.length - offset);
    src.get(data, offset, toTransfer);
    offset += toTransfer;
    return toTransfer;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean isOpen() {
    return true;
  }

}
