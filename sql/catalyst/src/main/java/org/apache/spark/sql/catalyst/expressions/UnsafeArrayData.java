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

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;

import java.nio.ByteBuffer;


// todo: there is a lof of duplicated code between UnsafeRow and UnsafeArrayData.
public abstract class UnsafeArrayData extends ArrayData {
  public enum Format {
    Sparse, Dense
  }

  protected Format format;

  public final Format getFormat() { return format; }

  public static final int DenseID = 1;

  public  static Format getFormat(Object baseObject, long baseOffset) {
    // Read the number of elements from the first 4 bytes.
    final int numElements = Platform.getInt(baseObject, baseOffset);
    assert numElements >= 0 : "numElements (" + numElements + ") should >= 0";
    // Read the next 4 bytes to check its format id ( != 1)
    int formatId = Platform.getInt(baseObject, baseOffset + 4);
    return (formatId == DenseID) ? Format.Dense : Format.Sparse;
  }

  protected Object baseObject;
  protected long baseOffset;

  // The number of elements in this array
  protected int numElements;

  // The size of this array's backing data, in bytes.
  // The 4-bytes header of `numElements` is also included.
  protected int sizeInBytes;

  public final Object getBaseObject() { return baseObject; }
  public final long getBaseOffset() { return baseOffset; }
  public final int getSizeInBytes() { return sizeInBytes; }

  public Object[] array() {
    throw new UnsupportedOperationException("Not supported on UnsafeArrayData.");
  }

  /**
   * Construct a new UnsafeArrayData. The resulting UnsafeArrayData won't be usable until
   * `pointTo()` has been called, since the value returned by this constructor is equivalent
   * to a null pointer.
   */
  public static final UnsafeArrayData allocate(Format format) {
    if (format == Format.Sparse) {
      return new UnsafeArrayDataSparse();
    } else {
      return new UnsafeArrayDataDense();
    }
  }

  public UnsafeArrayData() {  }

  @Override
  public final int numElements() { return numElements; }

  /**
   * Update this UnsafeArrayData to point to different backing data.
   *
   * @param baseObject the base object
   * @param baseOffset the offset within the base object
   * @param sizeInBytes the size of this array's backing data, in bytes
   */
  public abstract void pointTo(Object baseObject, long baseOffset, int sizeInBytes);

  @Override
  public final Object get(int ordinal, DataType dataType) {
    if (isNullAt(ordinal) || dataType instanceof NullType) {
      return null;
    } else if (dataType instanceof BooleanType) {
      return getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return getShort(ordinal);
    } else if (dataType instanceof IntegerType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return getLong(ordinal);
    } else if (dataType instanceof FloatType) {
      return getFloat(ordinal);
    } else if (dataType instanceof DoubleType) {
      return getDouble(ordinal);
    } else if (dataType instanceof DecimalType) {
      DecimalType dt = (DecimalType) dataType;
      return getDecimal(ordinal, dt.precision(), dt.scale());
    } else if (dataType instanceof DateType) {
      return getInt(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLong(ordinal);
    } else if (dataType instanceof BinaryType) {
      return getBinary(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    } else if (dataType instanceof CalendarIntervalType) {
      return getInterval(ordinal);
    } else if (dataType instanceof StructType) {
      return getStruct(ordinal, ((StructType) dataType).size());
    } else if (dataType instanceof ArrayType) {
      return getArray(ordinal);
    } else if (dataType instanceof MapType) {
      return getMap(ordinal);
    } else if (dataType instanceof UserDefinedType) {
      return get(ordinal, ((UserDefinedType)dataType).sqlType());
    } else {
      throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString());
    }
  }

  public abstract UnsafeRow getStruct(int ordinal, int numFields);

  public abstract UnsafeArrayData getArray(int ordinal);

  public abstract UnsafeMapData getMap(int ordinal);

  public final void writeToMemory(Object target, long targetOffset) {
    Platform.copyMemory(baseObject, baseOffset, target, targetOffset, sizeInBytes);
  }

  public final void writeTo(ByteBuffer buffer) {
    assert(buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
    buffer.position(pos + sizeInBytes);
  }

  // This `hashCode` computation could consume much processor time for large data.
  // If the computation becomes a bottleneck, we can use a light-weight logic; the first fixed bytes
  // are used to compute `hashCode` (See `Vector.hashCode`).
  // The same issue exists in `UnsafeRow.hashCode`.
  @Override
  public int hashCode() {
    return Murmur3_x86_32.hashUnsafeBytes(baseObject, baseOffset, sizeInBytes, 42);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof UnsafeArrayData) {
      UnsafeArrayData o = (UnsafeArrayData) other;
      return (sizeInBytes == o.sizeInBytes) &&
              ByteArrayMethods.arrayEquals(baseObject, baseOffset, o.baseObject, o.baseOffset,
                      sizeInBytes);
    }
    return false;
  }

  public abstract UnsafeArrayData copy();

  public static UnsafeArrayData fromPrimitiveArray(int[] arr) {
    return UnsafeArrayDataDense._fromPrimitiveArray(arr);
  }

  public static UnsafeArrayData fromPrimitiveArray(double[] arr) {
    return UnsafeArrayDataDense._fromPrimitiveArray(arr);
  }
}
