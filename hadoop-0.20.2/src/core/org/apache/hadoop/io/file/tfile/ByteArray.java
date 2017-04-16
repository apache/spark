/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io.file.tfile;

import org.apache.hadoop.io.BytesWritable;

/**
 * Adaptor class to wrap byte-array backed objects (including java byte array)
 * as RawComparable objects.
 */
public final class ByteArray implements RawComparable {
  private final byte[] buffer;
  private final int offset;
  private final int len;

  /**
   * Constructing a ByteArray from a {@link BytesWritable}.
   * 
   * @param other
   */
  public ByteArray(BytesWritable other) {
    this(other.get(), 0, other.getSize());
  }

  /**
   * Wrap a whole byte array as a RawComparable.
   * 
   * @param buffer
   *          the byte array buffer.
   */
  public ByteArray(byte[] buffer) {
    this(buffer, 0, buffer.length);
  }

  /**
   * Wrap a partial byte array as a RawComparable.
   * 
   * @param buffer
   *          the byte array buffer.
   * @param offset
   *          the starting offset
   * @param len
   *          the length of the consecutive bytes to be wrapped.
   */
  public ByteArray(byte[] buffer, int offset, int len) {
    if ((offset | len | (buffer.length - offset - len)) < 0) {
      throw new IndexOutOfBoundsException();
    }
    this.buffer = buffer;
    this.offset = offset;
    this.len = len;
  }

  /**
   * @return the underlying buffer.
   */
  @Override
  public byte[] buffer() {
    return buffer;
  }

  /**
   * @return the offset in the buffer.
   */
  @Override
  public int offset() {
    return offset;
  }

  /**
   * @return the size of the byte array.
   */
  @Override
  public int size() {
    return len;
  }
}
