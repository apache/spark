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

package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.*;

/**
 * Copies one key value between column vectors. Picked per key column at init time by
 * {@link #forType(DataType)} and called per surviving row. The caller handles null sources.
 *
 * <p>The types this handles are the types a storage filter's key columns may have, since
 * {@code ParquetStorageFilter.isSupportedKeyType} answers from {@link #supports}. That is narrower
 * than {@code AtomicType}:
 * <ul>
 *   <li>{@code VariantType} is a Parquet group rather than a primitive leaf, and no bloom can hash
 *       it anyway;</li>
 *   <li>{@code GeometryType} and {@code GeographyType} would work with the byte-array copier, but
 *       no bloom can hash them today;</li>
 *   <li>the nanosecond timestamp types are held in two children, which neither copier fits.</li>
 * </ul>
 */
@FunctionalInterface
interface ValueCopier {
  void copy(WritableColumnVector dst, int dstRow, WritableColumnVector src, int srcRow);

  /**
   * The copier for a value the vector holds in its byte child, which is every variable-length type
   * below. {@link #isVariableLength} answers by it.
   */
  ValueCopier BYTE_ARRAY = (dst, dRow, src, sRow) -> dst.putByteArray(dRow, src.getBinary(sRow));

  /** Whether a value of this type can be copied. */
  static boolean supports(DataType dt) {
    return forTypeOrNull(dt) != null;
  }

  /**
   * Returns a {@link ValueCopier} for the given key {@link DataType}. The planner and
   * {@code ParquetStorageFilter.create} have already asked {@link #supports}.
   */
  static ValueCopier forType(DataType dt) {
    ValueCopier copier = forTypeOrNull(dt);
    if (copier == null) {
      throw ParquetStorageFilter.internalError(
          "Splicing storage-filter pushdown does not support key type: " + dt);
    }
    return copier;
  }

  private static ValueCopier forTypeOrNull(DataType dt) {
    if (dt instanceof BooleanType) {
      return (dst, dRow, src, sRow) -> dst.putBoolean(dRow, src.getBoolean(sRow));
    }
    if (dt instanceof ByteType) {
      return (dst, dRow, src, sRow) -> dst.putByte(dRow, src.getByte(sRow));
    }
    if (dt instanceof ShortType) {
      return (dst, dRow, src, sRow) -> dst.putShort(dRow, src.getShort(sRow));
    }
    if (dt instanceof IntegerType
        || dt instanceof DateType
        || dt instanceof YearMonthIntervalType) {
      return (dst, dRow, src, sRow) -> dst.putInt(dRow, src.getInt(sRow));
    }
    if (dt instanceof LongType
        || dt instanceof TimestampType
        || dt instanceof TimestampNTZType
        || dt instanceof TimeType
        || dt instanceof DayTimeIntervalType) {
      return (dst, dRow, src, sRow) -> dst.putLong(dRow, src.getLong(sRow));
    }
    if (dt instanceof FloatType) {
      return (dst, dRow, src, sRow) -> dst.putFloat(dRow, src.getFloat(sRow));
    }
    if (dt instanceof DoubleType) {
      return (dst, dRow, src, sRow) -> dst.putDouble(dRow, src.getDouble(sRow));
    }
    if (dt instanceof DecimalType decimalType) {
      int precision = decimalType.precision();
      if (precision <= Decimal.MAX_INT_DIGITS()) {
        return (dst, dRow, src, sRow) -> dst.putInt(dRow, src.getInt(sRow));
      }
      if (precision <= Decimal.MAX_LONG_DIGITS()) {
        return (dst, dRow, src, sRow) -> dst.putLong(dRow, src.getLong(sRow));
      }
      return BYTE_ARRAY;
    }
    // StringType covers CHAR and VARCHAR: both extend it.
    if (dt instanceof StringType || dt instanceof BinaryType) {
      return BYTE_ARRAY;
    }
    return null;
  }

  /**
   * Whether a key value lives in the vector's byte child, which the survivor budget charges by
   * length. Answered by which copier the type gets, so a type added above cannot be charged as
   * fixed-width by accident.
   */
  static boolean isVariableLength(DataType dt) {
    return forTypeOrNull(dt) == BYTE_ARRAY;
  }
}
