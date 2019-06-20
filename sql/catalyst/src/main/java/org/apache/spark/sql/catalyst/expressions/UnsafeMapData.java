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

package org.apache.spark.sql.catalyst.expressions;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.unsafe.Platform;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;

/**
 * An Unsafe implementation of Map which is backed by raw memory instead of Java objects.
 *
 * Currently we just use 2 UnsafeArrayData to represent UnsafeMapData, with extra 8 bytes at head
 * to indicate the number of bytes of the unsafe key array.
 * [unsafe key array numBytes] [unsafe key array] [unsafe value array]
 *
 * Note that, user is responsible to guarantee that the key array does not have duplicated
 * elements, otherwise the behavior is undefined.
 */
// TODO: Use a more efficient format which doesn't depend on unsafe array.
public final class UnsafeMapData extends MapData implements Externalizable, KryoSerializable {

  private Object baseObject;
  private long baseOffset;

  // The size of this map's backing data, in bytes.
  // The 4-bytes header of key array `numBytes` is also included, so it's actually equal to
  // 4 + key array numBytes + value array numBytes.
  private int sizeInBytes;

  public Object getBaseObject() { return baseObject; }
  public long getBaseOffset() { return baseOffset; }
  public int getSizeInBytes() { return sizeInBytes; }

  private final UnsafeArrayData keys;
  private final UnsafeArrayData values;

  /**
   * Construct a new UnsafeMapData. The resulting UnsafeMapData won't be usable until
   * `pointTo()` has been called, since the value returned by this constructor is equivalent
   * to a null pointer.
   */
  public UnsafeMapData() {
    keys = new UnsafeArrayData();
    values = new UnsafeArrayData();
  }

  /**
   * Update this UnsafeMapData to point to different backing data.
   *
   * @param baseObject the base object
   * @param baseOffset the offset within the base object
   * @param sizeInBytes the size of this map's backing data, in bytes
   */
  public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
    // Read the numBytes of key array from the first 8 bytes.
    final long keyArraySize = Platform.getLong(baseObject, baseOffset);
    assert keyArraySize >= 0 : "keyArraySize (" + keyArraySize + ") should >= 0";
    assert keyArraySize <= Integer.MAX_VALUE :
      "keyArraySize (" + keyArraySize + ") should <= Integer.MAX_VALUE";
    final int valueArraySize = sizeInBytes - (int)keyArraySize - 8;
    assert valueArraySize >= 0 : "valueArraySize (" + valueArraySize + ") should >= 0";

    keys.pointTo(baseObject, baseOffset + 8, (int)keyArraySize);
    values.pointTo(baseObject, baseOffset + 8 + keyArraySize, valueArraySize);

    assert keys.numElements() == values.numElements();

    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.sizeInBytes = sizeInBytes;
  }

  @Override
  public int numElements() {
    return keys.numElements();
  }

  @Override
  public UnsafeArrayData keyArray() {
    return keys;
  }

  @Override
  public UnsafeArrayData valueArray() {
    return values;
  }

  public void writeToMemory(Object target, long targetOffset) {
    Platform.copyMemory(baseObject, baseOffset, target, targetOffset, sizeInBytes);
  }

  public void writeTo(ByteBuffer buffer) {
    assert(buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
    buffer.position(pos + sizeInBytes);
  }

  @Override
  public UnsafeMapData copy() {
    UnsafeMapData mapCopy = new UnsafeMapData();
    final byte[] mapDataCopy = new byte[sizeInBytes];
    Platform.copyMemory(
      baseObject, baseOffset, mapDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    mapCopy.pointTo(mapDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    return mapCopy;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    byte[] bytes = UnsafeDataUtils.getBytes(baseObject, baseOffset, sizeInBytes);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.baseOffset = BYTE_ARRAY_OFFSET;
    this.sizeInBytes = in.readInt();
    this.baseObject = new byte[sizeInBytes];
    in.readFully((byte[]) baseObject);
    pointTo(baseObject, baseOffset, sizeInBytes);
  }

  @Override
  public void write(Kryo kryo, Output output) {
    byte[] bytes = UnsafeDataUtils.getBytes(baseObject, baseOffset, sizeInBytes);
    output.writeInt(bytes.length);
    output.write(bytes);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    this.baseOffset = BYTE_ARRAY_OFFSET;
    this.sizeInBytes = input.readInt();
    this.baseObject = new byte[sizeInBytes];
    input.read((byte[]) baseObject);
    pointTo(baseObject, baseOffset, sizeInBytes);
  }
}
