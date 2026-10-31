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
 * <p>After each import, the reader calls {@link #refresh()}, and the vector then reads booleans,
 * integers, floats, dates, timestamps, decimals and strings at the addresses of the buffers of the
 * batch. {@link ArrowColumnVector} reads each value through Arrow Java, which by default checks on
 * each access the bounds and the reference count of the buffer, and whether the value is null.
 * Spark does not need these checks here: it reads only the rows of the batch, whose buffers Arrow
 * Java sizes by the lengths of the arrays, which the reader checks before it imports them, and
 * the buffers of a batch stay allocated until the thread that read it calls next again or closes
 * the reader: a close on another thread leaves the release to that thread. Spark reads a value
 * of a nullable column only after {@link #isNullAt(int)}, and the reader checks that the columns
 * that the schema declares not nullable have no null.
 *
 * <p>The vector returns the decimals that fit their precision in a long, of a scale from 0 to
 * their precision, as the unscaled longs that Spark's own column vectors hold, which Spark adds as
 * longs, rather than as BigDecimals. The other decimals, binary values, structs, lists and maps,
 * and the fields of structs are read through {@link ArrowColumnVector}.
 */
class NativeColumnVector extends ReusedArrowColumnVector {

  /** The column vector of a top-level Arrow vector of a reader. */
  static NativeColumnVector create(ValueVector vector) {
    if (vector instanceof BitVector) {
      return new Booleans(vector);
    } else if (vector instanceof TinyIntVector) {
      return new Bytes(vector);
    } else if (vector instanceof SmallIntVector) {
      return new Shorts(vector);
    } else if (vector instanceof IntVector || vector instanceof DateDayVector) {
      return new Ints(vector);
    } else if (vector instanceof BigIntVector || vector instanceof TimeStampMicroTZVector ||
        vector instanceof TimeStampMicroVector) {
      return new Longs(vector);
    } else if (vector instanceof Float4Vector) {
      return new Floats(vector);
    } else if (vector instanceof Float8Vector) {
      return new Doubles(vector);
    } else if (vector instanceof DecimalVector decimalVector) {
      return new Decimals(decimalVector);
    } else if (vector instanceof VarCharVector) {
      return new Strings(vector);
    } else if (vector instanceof ViewVarCharVector viewVector) {
      return new StringViews(viewVector);
    } else {
      return new NativeColumnVector(vector);
    }
  }

  NativeColumnVector(ValueVector vector) {
    super(vector);
  }

  /** Reads the addresses of the buffers of the batch that the reader imported last. */
  void refresh() {
  }

  /** A vector that reads its values and their validity at the addresses of its buffers. */
  private abstract static class Direct extends NativeColumnVector {
    final ValueVector vector;
    // The address of the validity bitmap of the batch, or 0 if the batch has no null.
    private long validity;
    // The address of the data buffer of the batch.
    long data;

    Direct(ValueVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    void refresh() {
      validity = vector.getNullCount() == 0 ? 0 : vector.getValidityBuffer().memoryAddress();
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
    Booleans(ValueVector vector) {
      super(vector);
    }

    @Override
    public boolean getBoolean(int rowId) {
      return bit(data, rowId);
    }
  }

  private static final class Bytes extends Direct {
    Bytes(ValueVector vector) {
      super(vector);
    }

    @Override
    public byte getByte(int rowId) {
      return Platform.getByte(null, data + rowId);
    }
  }

  private static final class Shorts extends Direct {
    Shorts(ValueVector vector) {
      super(vector);
    }

    @Override
    public short getShort(int rowId) {
      return Platform.getShort(null, data + 2L * rowId);
    }
  }

  private static final class Ints extends Direct {
    Ints(ValueVector vector) {
      super(vector);
    }

    @Override
    public int getInt(int rowId) {
      return Platform.getInt(null, data + 4L * rowId);
    }
  }

  private static final class Longs extends Direct {
    Longs(ValueVector vector) {
      super(vector);
    }

    @Override
    public long getLong(int rowId) {
      return Platform.getLong(null, data + 8L * rowId);
    }
  }

  private static final class Floats extends Direct {
    Floats(ValueVector vector) {
      super(vector);
    }

    @Override
    public float getFloat(int rowId) {
      return Platform.getFloat(null, data + 4L * rowId);
    }
  }

  private static final class Doubles extends Direct {
    Doubles(ValueVector vector) {
      super(vector);
    }

    @Override
    public double getDouble(int rowId) {
      return Platform.getDouble(null, data + 8L * rowId);
    }
  }

  private static final class Decimals extends Direct {
    // 10^i, for the precisions of the decimals that a long holds.
    private static final long[] POW_10 = new long[Decimal.MAX_LONG_DIGITS() + 1];
    // Arrow stores the 128 bits of a decimal in native byte order, as ArrowColumnVector reads it.
    private static final int LOW_WORD = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 0 : 8;
    private static final int HIGH_WORD = 8 - LOW_WORD;

    static {
      POW_10[0] = 1;
      for (int i = 1; i < POW_10.length; i++) {
        POW_10[i] = POW_10[i - 1] * 10;
      }
    }

    // The scale of the Arrow type.
    private final int scale;

    Decimals(DecimalVector vector) {
      super(vector);
      this.scale = vector.getScale();
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      // The integral conversions of a decimal that a long holds, such as Decimal.toLong, only
      // support the scales from 0 to 18, which those of Spark's own column vectors have.
      if (precision <= Decimal.MAX_LONG_DIGITS() && scale == this.scale && scale >= 0 &&
          scale <= precision) {
        long address = data + 16L * rowId;
        long unscaled = Platform.getLong(null, address + LOW_WORD);
        long bound = POW_10[precision];
        if (Platform.getLong(null, address + HIGH_WORD) == unscaled >> 63 &&
            -bound < unscaled && unscaled < bound) {
          return Decimal.createUnsafe(unscaled, precision, scale);
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

    Strings(ValueVector vector) {
      super(vector);
    }

    @Override
    void refresh() {
      super.refresh();
      offsets = vector.getOffsetBuffer().memoryAddress();
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
    private final ViewVarCharVector views;
    // The addresses of the data buffers of the batch, which hold the values that a view does not.
    private long[] buffers = new long[0];

    StringViews(ViewVarCharVector vector) {
      super(vector);
      this.views = vector;
    }

    @Override
    void refresh() {
      super.refresh();
      List<ArrowBuf> dataBuffers = views.getDataBuffers();
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
      if (length <= BaseVariableWidthViewVector.INLINE_SIZE) {
        return UTF8String.fromAddress(null, view + 4, length);
      }
      long buffer = buffers[Platform.getInt(null, view + 8)];
      return UTF8String.fromAddress(null, buffer + Platform.getInt(null, view + 12), length);
    }
  }
}
