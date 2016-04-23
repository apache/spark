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
package org.apache.spark.sql.execution.vectorized;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

/**
 * Implementation of OffheapColumnVector for Big Endian platforms
 */
public final class OffHeapColumnVectorBE extends OffHeapColumnVector {

  protected OffHeapColumnVectorBE(int capacity, DataType type) {
    super(capacity, type);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    long offset = data + 4 * rowId;
    for (int i = 0; i < count; ++i, offset += 4, srcOffset += 4) {
      Platform.putInt(null, offset, java.lang.Integer.reverseBytes(Platform.getInt(src, srcOffset)));
    }
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    long offset = data + 8 * rowId;
    for (int i = 0; i < count; ++i, offset += 8, srcOffset += 8) {
      Platform.putLong(null, offset, java.lang.Long.reverseBytes(Platform.getLong(src, srcOffset)));
    }
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
    long offset = data + 4 * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      Platform.putFloat(null, offset, bb.getFloat(srcIndex + (4 * i)));
    }
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
    long offset = data + 8 * rowId;
    for (int i = 0; i < count; ++i, offset += 8) {
      Platform.putDouble(null, offset, bb.getDouble(srcIndex + (8 * i)));
    }
  }

}
