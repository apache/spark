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

import java.io.*;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

/**
 * A column backed by an in memory JVM array. But, all of data types are stored into byte[].
 * This stores the NULLs as a byte per value and a java array for the values.
 */
public final class OnHeapUnsafeColumnVector extends ColumnVector implements Serializable {

  // The data stored in these arrays need to maintain binary compatible. We can
  // directly pass this buffer to external components.

  // This is faster than a boolean array and we optimize this over memory footprint.
  private byte[] nulls;
  private byte[] compressedNulls;

  // Array for all types
  private byte[] data;
  private byte[] compressedData;

  // Only set if type is Array.
  private int[] arrayLengths;
  private int[] arrayOffsets;

  private boolean compressed;
  private transient CompressionCodec codec = null;

  OnHeapUnsafeColumnVector() { }

  protected OnHeapUnsafeColumnVector(int capacity, DataType type) {
    super(capacity, type, MemoryMode.ON_HEAP, true);
    reserveInternal(capacity);
    reset();
  }

  @Override
  public long valuesNativeAddress() {
    throw new RuntimeException("Cannot get native address for on heap column");
  }
  @Override
  public long nullsNativeAddress() {
    throw new RuntimeException("Cannot get native address for on heap column");
  }

  @Override
  public void close() {
  }

  public void compress(SparkConf conf) {
    if (compressed) return;
    if (codec == null) {
      String codecName = conf.get(SQLConf.CACHE_COMPRESSION_CODEC());
      codec = CompressionCodec$.MODULE$.createCodec(conf, codecName);
    }
    ByteArrayOutputStream bos;
    OutputStream out;

    if (data != null) {
      bos = new ByteArrayOutputStream();
      out = codec.compressedOutputStream(bos);
      try {
        try {
          out.write(data);
        } finally {
          out.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (bos.size() < data.length) {
        compressedData = bos.toByteArray();
        data = null;
      }
    }

    if (nulls != null) {
      bos = new ByteArrayOutputStream();
      out = codec.compressedOutputStream(bos);
      try {
        try {
          out.write(nulls);
        } finally {
          out.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (bos.size() < nulls.length) {
        compressedNulls = bos.toByteArray();
        nulls = null;
      }
    }
    compressed = (compressedData != null) || (compressedNulls != null);
  }

  public void decompress(SparkConf conf) {
    if (!compressed) return;
    if (codec == null) {
      String codecName = conf.get(SQLConf.CACHE_COMPRESSION_CODEC());
      codec = CompressionCodec$.MODULE$.createCodec(conf, codecName);
    }
    ByteArrayInputStream bis;
    InputStream in;

    if (compressedData != null) {
      bis = new ByteArrayInputStream(compressedData);
      in = codec.compressedInputStream(bis);
      try {
        try {
          data = IOUtils.toByteArray(in);
        } finally {
          in.close();
         }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      compressedData = null;
    }

    if (compressedNulls != null) {
      bis = new ByteArrayInputStream(compressedNulls);
      in = codec.compressedInputStream(bis);
      try {
        try {
          nulls = IOUtils.toByteArray(in);
        } finally {
          in.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      compressedNulls = null;
    }
    compressed = false;
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    Platform.putByte(nulls, Platform.BYTE_ARRAY_OFFSET + rowId, (byte)0);
  }

  @Override
  public void putNull(int rowId) {
    Platform.putByte(nulls, Platform.BYTE_ARRAY_OFFSET + rowId, (byte)1);
    ++numNulls;
    anyNullsSet = true;
  }

  @Override
  public void putNulls(int rowId, int count) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return Platform.getByte(nulls, Platform.BYTE_ARRAY_OFFSET + rowId) == 1;
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    Platform.putBoolean(data, Platform.BYTE_ARRAY_OFFSET + rowId, value);
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBoolean(int rowId) {
    return Platform.getBoolean(data, Platform.BYTE_ARRAY_OFFSET + rowId);
  }

  //

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    Platform.putByte(data, Platform.BYTE_ARRAY_OFFSET + rowId, value);
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    for (int i = 0; i < count; ++i) {
      Platform.putByte(data, Platform.BYTE_ARRAY_OFFSET + rowId + i, value);
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    System.arraycopy(src, srcIndex, data, rowId, count);
  }

  @Override
  public byte getByte(int rowId) {
    return Platform.getByte(data, Platform.BYTE_ARRAY_OFFSET + rowId);
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    Platform.putShort(data, Platform.BYTE_ARRAY_OFFSET + rowId * 2, value);
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int rowId) {
    return Platform.getShort(data, Platform.BYTE_ARRAY_OFFSET + rowId * 2);
  }


  //
  // APIs dealing with Ints
  //

  @Override
  public void putInt(int rowId, int value) {
    Platform.putInt(data, Platform.BYTE_ARRAY_OFFSET + rowId * 4, value);
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int rowId) {
    return Platform.getInt(data, Platform.BYTE_ARRAY_OFFSET + rowId * 4);
  }

  /**
   * Returns the dictionary Id for rowId.
   * This should only be called when the ColumnVector is dictionaryIds.
   * We have this separate method for dictionaryIds as per SPARK-16928.
   */
  public int getDictId(int rowId) { throw new UnsupportedOperationException(); }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    Platform.putLong(data, Platform.BYTE_ARRAY_OFFSET + rowId * 8, value);
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int rowId) {
    return Platform.getLong(data, Platform.BYTE_ARRAY_OFFSET + rowId * 8);
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    Platform.putFloat(data, Platform.BYTE_ARRAY_OFFSET + rowId * 4, value);
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int rowId) {
    return Platform.getFloat(data, Platform.BYTE_ARRAY_OFFSET + rowId * 4);
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    Platform.putDouble(data, Platform.BYTE_ARRAY_OFFSET + rowId * 8, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int rowId) {
    return Platform.getDouble(data, Platform.BYTE_ARRAY_OFFSET + rowId * 8);
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public int getArrayLength(int rowId) {
    return arrayLengths[rowId];
  }
  @Override
  public int getArrayOffset(int rowId) {
    return arrayOffsets[rowId];
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    arrayOffsets[rowId] = offset;
    arrayLengths[rowId] = length;
  }

  @Override
  public void loadBytes(ColumnVector.Array array) {
    array.byteArray = data;
    array.byteArrayOffset = array.offset;
  }

  //
  // APIs dealing with Byte Arrays
  //

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    int result = arrayData().appendBytes(length, value, offset);
    arrayOffsets[rowId] = result;
    arrayLengths[rowId] = length;
    return result;
  }

  // Spilt this function out since it is the slow path.
  @Override
  protected void reserveInternal(int newCapacity) {
    int factor = 0;
    if (this.resultArray != null || DecimalType.isByteArrayDecimalType(type)) {
      int[] newLengths = new int[newCapacity];
      int[] newOffsets = new int[newCapacity];
      if (this.arrayLengths != null) {
        System.arraycopy(this.arrayLengths, 0, newLengths, 0, elementsAppended);
        System.arraycopy(this.arrayOffsets, 0, newOffsets, 0, elementsAppended);
      }
      arrayLengths = newLengths;
      arrayOffsets = newOffsets;
      factor = -1;
    } else if (resultStruct != null || type instanceof NullType) {
      // Nothing to store.
      factor = -1;
    } else if (type instanceof BooleanType) {
      factor = 1;
    } else if (type instanceof ByteType) {
      factor = 1;
    } else if (type instanceof ShortType) {
      factor = 2;
    } else if (type instanceof IntegerType || type instanceof DateType ||
            DecimalType.is32BitDecimalType(type)) {
      factor = 4;
    } else if (type instanceof LongType || type instanceof TimestampType ||
            DecimalType.is64BitDecimalType(type)) {
      factor = 8;
    } else if (type instanceof FloatType) {
      factor = 4;
    } else if (type instanceof DoubleType) {
      factor = 8;
    }
    if (factor > 0) {
      if (data == null || capacity < newCapacity) {
        byte[] newData = new byte[newCapacity * factor];
        if (data != null)
          Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET,
            newData, Platform.BYTE_ARRAY_OFFSET, elementsAppended * factor);
        data = newData;
      }
    } else if (factor == 0) {
      throw new RuntimeException("Unhandled " + type);
    }

    byte[] newNulls = new byte[newCapacity];
    if (nulls != null) System.arraycopy(nulls, 0, newNulls, 0, elementsAppended);
    nulls = newNulls;

    capacity = newCapacity;
  }
}
