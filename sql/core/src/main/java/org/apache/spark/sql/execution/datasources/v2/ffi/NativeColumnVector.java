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

package org.apache.spark.sql.execution.datasources.v2.ffi;

import java.nio.ByteOrder;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthViewVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarCharVector;

import org.apache.spark.sql.execution.vectorized.ReusedArrowColumnVector;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * The column vector of a top-level column of a {@code NativePartitionReader}, which imports every
 * batch into the same Arrow vectors.
 *
 * <p>After each import, the reader calls {@link #refresh(long)}, and the vector then reads
 * booleans, integers, floats, dates, timestamps, decimals, and the strings of the Arrow types Utf8
 * and Utf8View at the addresses of the buffers of the batch. {@link ArrowColumnVector} reads each
 * value through Arrow Java, which by default checks on each access the bounds and the reference
 * count of the buffer, and whether the value is null.
 * Spark does not need these checks here: it reads only the rows of the batch, whose buffers Arrow
 * Java sizes by the lengths of the arrays, which the reader checks before it imports them, and
 * the buffers of a batch stay allocated while the batch may be read: until next is called again,
 * and if the reader is closed before then, until the reader is unreachable, which the vector keeps
 * it from being. Spark reads a value of a nullable column only after {@link #isNullAt(int)}, and
 * the reader checks that the columns that the schema declares not nullable have no null, so that
 * the vector of such a column does not count the nulls of each batch. The vector of a nullable
 * column takes the null_count that the library exported, and only counts the nulls when the
 * library did not.
 *
 * <p>The vector returns the decimals that fit their precision in a long, of a scale from 0 to
 * their precision, as the unscaled longs that Spark's own column vectors hold, which Spark adds as
 * longs, rather than as BigDecimals. The other decimals, large strings, binary values, structs,
 * lists and maps, and the fields of structs are read through {@link ArrowColumnVector}.
 */
class NativeColumnVector extends ReusedArrowColumnVector {

  // The reader, which the vector keeps reachable: a reader closed while its batch may still be
  // read frees the buffers of the batch only once it is unreachable.
  private Object reader;

  /**
   * The column vector of a top-level Arrow vector of {@code reader}, of a column that the schema to
   * read declares {@code nullable} or not.
   */
  static NativeColumnVector create(ValueVector vector, boolean nullable, Object reader) {
    NativeColumnVector column;
    if (vector instanceof BitVector) {
      column = new Booleans(vector, nullable);
    } else if (vector instanceof TinyIntVector) {
      column = new Bytes(vector, nullable);
    } else if (vector instanceof SmallIntVector) {
      column = new Shorts(vector, nullable);
    } else if (vector instanceof IntVector || vector instanceof DateDayVector) {
      column = new Ints(vector, nullable);
    } else if (vector instanceof BigIntVector || vector instanceof TimeStampMicroTZVector ||
        vector instanceof TimeStampMicroVector) {
      column = new Longs(vector, nullable);
    } else if (vector instanceof Float4Vector) {
      column = new Floats(vector, nullable);
    } else if (vector instanceof Float8Vector) {
      column = new Doubles(vector, nullable);
    } else if (vector instanceof DecimalVector decimalVector) {
      column = new Decimals(decimalVector, nullable);
    } else if (vector instanceof VarCharVector) {
      column = new Strings(vector, nullable);
    } else if (vector instanceof ViewVarCharVector) {
      column = new StringViews(vector, nullable);
    } else {
      column = new NativeColumnVector(vector);
    }
    column.reader = reader;
    return column;
  }

  NativeColumnVector(ValueVector vector) {
    super(vector);
  }

  /**
   * Reads the addresses of the buffers of the batch that the reader imported last, whose array of
   * this column the library exported with a null_count of {@code nullCount}.
   */
  void refresh(long nullCount) {
  }

  /** A vector that reads its values and their validity at the addresses of its buffers. */
  private abstract static class Direct extends NativeColumnVector {
    // Whether the schema to read declares the column nullable. The reader checks that the batches
    // of a column that it does not declare nullable have no null.
    private final boolean nullable;
    // The address of the validity bitmap of the batch, or 0 if the batch has no null.
    private long validity;
    // The address of the data buffer of the batch.
    long data;

    Direct(ValueVector vector, boolean nullable) {
      super(vector);
      this.nullable = nullable;
    }

    @Override
    void refresh(long nullCount) {
      ValueVector vector = getValueVector();
      // The C data interface requires a null_count other than -1 to be exact, so the nulls are
      // only counted, which Arrow Java does in the validity bitmap on each call, for a negative
      // one.
      boolean hasNulls = nullable && (nullCount < 0 ? vector.getNullCount() > 0 : nullCount > 0);
      validity = hasNulls ? vector.getValidityBuffer().memoryAddress() : 0;
      data = vector.getDataBuffer().memoryAddress();
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return validity != 0 && !bit(validity, rowId);
    }

    /** The bit of a bitmap, whose bits Arrow orders from the least significant of each byte. */
    static boolean bit(long address, int index) {
      return (Platform.getByte(null, address + (index >> 3)) & (1 << (index & 7))) != 0;
    }
  }

  private static final class Booleans extends Direct {
    Booleans(ValueVector vector, boolean nullable) {
      super(vector, nullable);
    }

    @Override
    public boolean getBoolean(int rowId) {
      return bit(data, rowId);
    }
  }

  private static final class Bytes extends Direct {
    Bytes(ValueVector vector, boolean nullable) {
      super(vector, nullable);
    }

    @Override
    public byte getByte(int rowId) {
      return Platform.getByte(null, data + rowId);
    }
  }

  private static final class Shorts extends Direct {
    Shorts(ValueVector vector, boolean nullable) {
      super(vector, nullable);
    }

    @Override
    public short getShort(int rowId) {
      return Platform.getShort(null, data + 2L * rowId);
    }
  }

  private static final class Ints extends Direct {
    Ints(ValueVector vector, boolean nullable) {
      super(vector, nullable);
    }

    @Override
    public int getInt(int rowId) {
      return Platform.getInt(null, data + 4L * rowId);
    }
  }

  private static final class Longs extends Direct {
    Longs(ValueVector vector, boolean nullable) {
      super(vector, nullable);
    }

    @Override
    public long getLong(int rowId) {
      return Platform.getLong(null, data + 8L * rowId);
    }
  }

  private static final class Floats extends Direct {
    Floats(ValueVector vector, boolean nullable) {
      super(vector, nullable);
    }

    @Override
    public float getFloat(int rowId) {
      return Platform.getFloat(null, data + 4L * rowId);
    }
  }

  private static final class Doubles extends Direct {
    Doubles(ValueVector vector, boolean nullable) {
      super(vector, nullable);
    }

    @Override
    public double getDouble(int rowId) {
      return Platform.getDouble(null, data + 8L * rowId);
    }
  }

  private static final class Decimals extends Direct {
    // Arrow stores the 128 bits of a decimal in native byte order, as ArrowColumnVector reads it.
    private static final int LOW_WORD = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 0 : 8;
    private static final int HIGH_WORD = 8 - LOW_WORD;

    // The scale of the Arrow type.
    private final int scale;

    Decimals(DecimalVector vector, boolean nullable) {
      super(vector, nullable);
      this.scale = vector.getScale();
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      // The integral conversions of a decimal that a long holds, such as Decimal.toLong, only
      // support the scales from 0 to 18, which those of Spark's own column vectors have.
      if (precision <= Decimal.MAX_LONG_DIGITS() && scale == this.scale && scale >= 0 &&
          scale <= precision) {
        long address = data + (long) DecimalVector.TYPE_WIDTH * rowId;
        long unscaled = Platform.getLong(null, address + LOW_WORD);
        if (Platform.getLong(null, address + HIGH_WORD) == unscaled >> 63) {
          // Null if the value does not fit its precision.
          Decimal decimal = new Decimal().setOrNull(unscaled, precision, scale);
          if (decimal != null) return decimal;
        }
      }
      // Wider decimals, other scales, and the values that do not fit their precision, which
      // ArrowColumnVector reads in full or rejects.
      return super.getDecimal(rowId, precision, scale);
    }
  }

  private static final class Strings extends Direct {
    // The address of the offsets of the values of the batch.
    private long offsets;

    Strings(ValueVector vector, boolean nullable) {
      super(vector, nullable);
    }

    @Override
    void refresh(long nullCount) {
      super.refresh(nullCount);
      offsets = getValueVector().getOffsetBuffer().memoryAddress();
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
      if (isNullAt(rowId)) return null;
      long offset = offsets + 4L * rowId;
      int start = Platform.getInt(null, offset);
      int length = Platform.getInt(null, offset + 4) - start;
      return UTF8String.fromAddress(null, data + start, length);
    }
  }

  private static final class StringViews extends Direct {
    // The addresses of the data buffers of the batch, which hold the values that a view does not.
    private long[] buffers = new long[0];

    StringViews(ValueVector vector, boolean nullable) {
      super(vector, nullable);
    }

    @Override
    void refresh(long nullCount) {
      super.refresh(nullCount);
      List<ArrowBuf> dataBuffers = ((ViewVarCharVector) getValueVector()).getDataBuffers();
      if (buffers.length != dataBuffers.size()) {
        buffers = new long[dataBuffers.size()];
      }
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = dataBuffers.get(i).memoryAddress();
      }
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
      if (isNullAt(rowId)) return null;
      // A view of 16 bytes: the length of the value, then the value if it fits in the other 12
      // bytes, or its first 4 bytes, the index of the data buffer that holds it, and its offset
      // in that buffer.
      long view = data + (long) BaseVariableWidthViewVector.ELEMENT_SIZE * rowId;
      int length = Platform.getInt(null, view);
      long inline = view + BaseVariableWidthViewVector.LENGTH_WIDTH;
      if (length <= BaseVariableWidthViewVector.INLINE_SIZE) {
        return UTF8String.fromAddress(null, inline, length);
      }
      long bufferIndex = inline + BaseVariableWidthViewVector.PREFIX_WIDTH;
      long buffer = buffers[Platform.getInt(null, bufferIndex)];
      int start = Platform.getInt(null, bufferIndex + BaseVariableWidthViewVector.BUF_INDEX_WIDTH);
      return UTF8String.fromAddress(null, buffer + start, length);
    }
  }
}
