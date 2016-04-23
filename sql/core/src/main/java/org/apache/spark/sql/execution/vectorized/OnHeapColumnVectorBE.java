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
import java.util.Arrays;

import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

/**
 * Implementation of OnheapColumnVector for Big Endian platforms
 */
public final class OnHeapColumnVectorBE extends OnHeapColumnVector {

  protected OnHeapColumnVectorBE(int capacity, DataType type) {
    super(capacity, type);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i, srcOffset += 4) {
      intData[i + rowId] = java.lang.Integer.reverseBytes(Platform.getInt(src, srcOffset));
    }
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    for (int i = 0; i < count; ++i, srcOffset += 8) {
      longData[i + rowId] = java.lang.Long.reverseBytes(Platform.getLong(src, srcOffset));
    }
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < count; ++i) {
      floatData[i + rowId] = bb.getFloat(srcIndex + (4 * i));
    }
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < count; ++i) {
      doubleData[i + rowId] = bb.getDouble(srcIndex + (8 * i));
    }
  }

}
