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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.TimestampFormatter;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.catalyst.util.DateFormatter;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Option;

import static org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReader.rebaseDays;
import static org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReader.rebaseMicros;

/**
 * Convert the original data of the parquet column to the spark request schema type.
 */
public class ParquetColumnConverter {

  private static final long BYTE_MAX = Byte.MAX_VALUE;
  private static final long BYTE_MIN = Byte.MIN_VALUE;
  private static final long SHORT_MAX = Short.MAX_VALUE;
  private static final long SHORT_MIN = Short.MIN_VALUE;
  private static final long INT_MAX = Integer.MAX_VALUE;
  private static final long INT_MIN = Integer.MIN_VALUE;
  private static final long LONG_MAX = Long.MAX_VALUE;
  private static final long LONG_MIN = Long.MIN_VALUE;

  private static final String CORRECTED_MODE = "CORRECTED";
  private static final String EXCEPTION_MODE = "EXCEPTION";

  private static final ZoneId UTC = ZoneOffset.UTC;
  private static final Charset UTF8 = StandardCharsets.UTF_8;
  private static final long MICROS_PER_MILLIS = 1000L;
  private static final int BINARY_TIME_LEN = 12;

  private static final int[] POW_10_INT = new int[10];
  private static final long[] POW_10_LONG = new long[19];
  static {
    for (int i = 0; i <= 9; i++) {
      POW_10_INT[i] = (int) Math.pow(10, i);
    }
    for (int i = 0; i <= 18; i++) {
      POW_10_LONG[i] = (long) Math.pow(10, i);
    }
  }

  private static final byte[] TRUE_BYTES = ParquetRowConverter.trueUtf8String().getBytes();
  private static final byte[] FALSE_BYTES = ParquetRowConverter.falseUtf8String().getBytes();

  private final ColumnDescriptor parquetDescriptor;
  private final WritableColumnVector sparkColumn;
  private final PrimitiveType parquetType;
  private final PrimitiveType.PrimitiveTypeName parquetTypeName;
  private final OriginalType parquetTypeOriginal;
  private final boolean isFixedLenBinary;
  private final int fixedLen;
  private final int maxDefLevel;
  private final boolean convertInt96Timestamp;
  private final boolean dateTimeIsCorrected;
  private final boolean dateTimeFailIfRebase;
  private final DecimalMetadata parquetDecimal;
  private final ZoneId convertTz;
  private final DataType sparkType;

  public ParquetColumnConverter(
      ColumnDescriptor parquetDescriptor,
      WritableColumnVector sparkColumn,
      ZoneId convertTz,
      String datetimeRebaseMode,
      boolean convertInt96Timestamp) {
    this.parquetDescriptor = parquetDescriptor;
    this.parquetType = parquetDescriptor.getPrimitiveType();
    this.parquetTypeName = parquetType.getPrimitiveTypeName();
    this.parquetTypeOriginal = parquetType.getOriginalType();
    this.parquetDecimal = parquetType.getDecimalMetadata();
    this.maxDefLevel = parquetDescriptor.getMaxDefinitionLevel();
    this.isFixedLenBinary = parquetTypeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
    this.fixedLen = parquetType.getTypeLength();
    this.sparkColumn = sparkColumn;
    this.sparkType = sparkColumn.dataType();
    this.convertTz = convertTz;
    this.convertInt96Timestamp = convertInt96Timestamp && !convertTz.equals(UTC);
    this.dateTimeIsCorrected = CORRECTED_MODE.equals(datetimeRebaseMode);
    this.dateTimeFailIfRebase = EXCEPTION_MODE.equals(datetimeRebaseMode);
  }

  public Dictionary convertDictionary(Dictionary originalDictionary) {
    switch (parquetTypeName) {
      case FLOAT:
        return convertDictionaryFromFloat(originalDictionary);
      case DOUBLE:
        return convertDictionaryFromDouble(originalDictionary);
      case INT32:
        return convertDictionaryFromInt32(originalDictionary);
      case INT64:
        return convertDictionaryFromInt64(originalDictionary);
      case INT96:
        return convertDictionaryFromInt96(originalDictionary);
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        return convertDictionaryFromBinary(originalDictionary);
      default:
        throw new UnsupportedOperationException(
          "Unsupported parquet dictionary type: " + parquetTypeName);
    }
  }

  /**
   * For all the read*Batch functions, reads `num` values from this columnReader into column.
   * It is guaranteed that num is smaller than the number of values left in the current page.
   */

  public void readBooleanBatch(int rowId, int num, VectorizedRleValuesReader defColumn,
                               ValuesReader dataColumn) throws IOException {
    // Possible spark Type:
    // 1.BooleanType 2.StringType
    if (sparkType == DataTypes.BooleanType) {
      // parquet boolean -> spark boolean
      defColumn.readBooleans(
        num, sparkColumn, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (sparkType == DataTypes.StringType) {
      // parquet boolean -> spark string
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          sparkColumn.putByteArray(rowId + i,
            dataColumn.readBoolean() ? TRUE_BYTES : FALSE_BYTES);
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else {
      throw constructConvertNotSupportedException();
    }
  }

  public void readFloatBatch(int rowId, int num, VectorizedRleValuesReader defColumn,
                             ValuesReader dataColumn) throws IOException {
    // Possible spark Type:
    // 1.FloatType 2.DoubleType 3.ByteType 4.ShortType 5.IntegerType
    // 6.DecimalType 7.LongType 8.StringType
    if (sparkType == DataTypes.FloatType) {
      // parquet float -> spark float
      defColumn.readFloats(
        num, sparkColumn, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (sparkType == DataTypes.DoubleType) {
      // parquet float -> spark double
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          sparkColumn.putDouble(rowId + i, dataColumn.readFloat());
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (sparkType == DataTypes.ByteType) {
      // parquet float -> spark byte
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          float value = dataColumn.readFloat();
          checkDoubleToIntegralOverflow(value, BYTE_MAX, BYTE_MIN);
          sparkColumn.putByte(rowId + i, (byte) value);
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (sparkType == DataTypes.ShortType) {
      // parquet float -> spark short
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          float value = dataColumn.readFloat();
          checkDoubleToIntegralOverflow(value, SHORT_MAX, SHORT_MIN);
          sparkColumn.putShort(rowId + i, (short) value);
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (sparkType == DataTypes.IntegerType) {
      // parquet float -> spark int
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          float value = dataColumn.readFloat();
          checkDoubleToIntegralOverflow(value, INT_MAX, INT_MIN);
          sparkColumn.putInt(rowId + i, (int) value);
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (sparkType == DataTypes.LongType) {
      // parquet float -> spark long
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          float value = dataColumn.readFloat();
          checkDoubleToIntegralOverflow(value, LONG_MAX, LONG_MIN);
          sparkColumn.putLong(rowId + i, (long) value);
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (DecimalType.is32BitDecimalType(sparkType)) {
      // parquet float -> spark decimal(precision<=9)
      DecimalType sparkDecimal = (DecimalType) sparkType;
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          BigDecimal decimal = BigDecimal.valueOf(dataColumn.readFloat());
          BigInteger unscaled = bigDecimalToUnscaledValue(decimal, sparkDecimal);
          sparkColumn.putInt(rowId + i, unscaled.intValue());
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (DecimalType.is64BitDecimalType(sparkType)) {
      // parquet float -> spark decimal(precision<=18)
      DecimalType sparkDecimal = (DecimalType) sparkType;
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          BigDecimal decimal = BigDecimal.valueOf(dataColumn.readFloat());
          BigInteger unscaled = bigDecimalToUnscaledValue(decimal, sparkDecimal);
          sparkColumn.putLong(rowId + i, unscaled.longValue());
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (DecimalType.isByteArrayDecimalType(sparkType)) {
      // parquet float -> spark decimal(precision>18)
      DecimalType sparkDecimal = (DecimalType) sparkType;
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          BigDecimal decimal = BigDecimal.valueOf(dataColumn.readFloat());
          BigInteger unscaled = bigDecimalToUnscaledValue(decimal, sparkDecimal);
          sparkColumn.putByteArray(rowId + i, unscaled.toByteArray());
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (sparkType == DataTypes.StringType) {
      // parquet float -> spark string
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          sparkColumn.putByteArray(rowId + i,
            Float.toString(dataColumn.readFloat()).getBytes(UTF8));
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else {
      throw constructConvertNotSupportedException();
    }
  }

  public void readDoubleBatch(int rowId, int num, VectorizedRleValuesReader defColumn,
                              ValuesReader dataColumn) throws IOException {
    // Possible spark Type:
    // 1.FloatType 2.DoubleType 3.ByteType 4.ShortType 5.IntegerType
    // 6.DecimalType 7.LongType 8.StringType
    if (sparkType == DataTypes.DoubleType) {
      // parquet double -> spark double
      defColumn.readDoubles(
        num, sparkColumn, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (sparkType == DataTypes.FloatType) {
      // parquet double -> spark float
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          double value = dataColumn.readDouble();
          checkDoubleToFloatOverflow(value);
          sparkColumn.putFloat(rowId + i, (float) value);
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (sparkType == DataTypes.ByteType) {
      // parquet double -> spark byte
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          double value = dataColumn.readDouble();
          checkDoubleToIntegralOverflow(value, BYTE_MAX, BYTE_MIN);
          sparkColumn.putByte(rowId + i, (byte) value);
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (sparkType == DataTypes.ShortType) {
      // parquet double -> spark short
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          double value = dataColumn.readDouble();
          checkDoubleToIntegralOverflow(value, SHORT_MAX, SHORT_MIN);
          sparkColumn.putShort(rowId + i, (short) value);
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (sparkType == DataTypes.IntegerType) {
      // parquet double -> spark int
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          double value = dataColumn.readDouble();
          checkDoubleToIntegralOverflow(value, INT_MAX, INT_MIN);
          sparkColumn.putInt(rowId + i, (int) value);
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (sparkType == DataTypes.LongType) {
      // parquet double -> spark long
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          double value = dataColumn.readDouble();
          checkDoubleToIntegralOverflow(value, LONG_MAX, LONG_MIN);
          sparkColumn.putLong(rowId + i, (long) value);
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (DecimalType.is32BitDecimalType(sparkType)) {
      // parquet double -> spark decimal(precision<=9)
      DecimalType sparkDecimal = (DecimalType) sparkType;
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          BigDecimal decimal = BigDecimal.valueOf(dataColumn.readDouble());
          BigInteger unscaled = bigDecimalToUnscaledValue(decimal, sparkDecimal);
          sparkColumn.putInt(rowId + i, unscaled.intValue());
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (DecimalType.is64BitDecimalType(sparkType)) {
      // parquet double -> spark decimal(precision<=18)
      DecimalType sparkDecimal = (DecimalType) sparkType;
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          BigDecimal decimal = BigDecimal.valueOf(dataColumn.readDouble());
          BigInteger unscaled = bigDecimalToUnscaledValue(decimal, sparkDecimal);
          sparkColumn.putLong(rowId + i, unscaled.longValue());
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (DecimalType.isByteArrayDecimalType(sparkType)) {
      // parquet double -> spark decimal(precision>18)
      DecimalType sparkDecimal = (DecimalType) sparkType;
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          BigDecimal decimal = BigDecimal.valueOf(dataColumn.readDouble());
          BigInteger unscaled = bigDecimalToUnscaledValue(decimal, sparkDecimal);
          sparkColumn.putByteArray(rowId + i, unscaled.toByteArray());
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (sparkType == DataTypes.StringType) {
      // parquet double -> spark string
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          sparkColumn.putByteArray(rowId + i,
            Double.toString(dataColumn.readDouble()).getBytes(UTF8));
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else {
      throw constructConvertNotSupportedException();
    }
  }

  public void readInt32Batch(int rowId, int num, VectorizedRleValuesReader defColumn,
                           ValuesReader dataColumn) throws IOException {
    // Possible spark Type:
    // 1.FloatType 2.DoubleType 3.ByteType 4.ShortType 5.IntegerType
    // 6.DecimalType 7.LongType 8.StringType 9.DateType 10.TimestampType
    OriginalType originalType =
      parquetTypeOriginal == null ? OriginalType.INT_32 : parquetTypeOriginal;
    switch (originalType) {
      case DATE:
        if (sparkType == DataTypes.DateType) {
          // parquet int32-date -> spark date
          if (dateTimeIsCorrected) {
            defColumn.readIntegers(
              num, sparkColumn, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
          } else {
            defColumn.readIntegersWithRebase(num, sparkColumn, rowId, maxDefLevel,
              (VectorizedValuesReader) dataColumn, dateTimeFailIfRebase);
          }
        } else if (sparkType == DataTypes.TimestampType) {
          // parquet int32-date -> spark timestamp
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int days = dataColumn.readInteger();
              if (!dateTimeIsCorrected) {
                days = rebaseDays(days, dateTimeFailIfRebase);
              }
              sparkColumn.putLong(rowId + i, DateTimeUtils.daysToMicros(days, convertTz));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.StringType) {
          // parquet int32-date -> spark string
          DateFormatter dateFormatter = DateFormatter.apply(convertTz);
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int days = dataColumn.readInteger();
              if (!dateTimeIsCorrected) {
                days = rebaseDays(days, dateTimeFailIfRebase);
              }
              sparkColumn.putByteArray(rowId + i, dateFormatter.format(days).getBytes(UTF8));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else {
          throw constructConvertNotSupportedException();
        }
        break;
      case DECIMAL:
        int parquetScale = parquetDecimal.getScale();
        int powDiffScale = POW_10_INT[parquetScale];
        if (DecimalType.is32BitDecimalType(sparkType) &&
          isParquetSparkDecimalMatch((DecimalType) sparkType)) {
          // parquet int32-decimal -> spark decimal(precision match)
          defColumn.readIntegers(
            num, sparkColumn, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
        } else if (sparkType == DataTypes.FloatType) {
          // parquet int32-decimal -> spark float
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              BigDecimal decimal = BigDecimal.valueOf(dataColumn.readInteger(), parquetScale);
              sparkColumn.putFloat(rowId + i, decimal.floatValue());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.DoubleType) {
          // parquet int32-decimal -> spark double
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              BigDecimal decimal = BigDecimal.valueOf(dataColumn.readInteger(), parquetScale);
              sparkColumn.putDouble(rowId + i, decimal.doubleValue());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.ByteType) {
          // parquet int32-decimal -> spark byte
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int value = dataColumn.readInteger() / powDiffScale;
              checkIntToByteOverflow(value);
              sparkColumn.putByte(rowId + i, (byte) value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.ShortType) {
          // parquet int32-decimal -> spark short
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int value = dataColumn.readInteger() / powDiffScale;
              checkIntToShortOverflow(value);
              sparkColumn.putShort(rowId + i, (short) value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.IntegerType) {
          // parquet int32-decimal -> spark int
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int value = dataColumn.readInteger() / powDiffScale;
              sparkColumn.putInt(rowId + i, value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.LongType) {
          // parquet int32-decimal -> spark long
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long value = dataColumn.readInteger() / powDiffScale;
              sparkColumn.putLong(rowId + i, value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is32BitDecimalType(sparkType)) {
          // parquet int32-decimal -> spark decimal(precision<=9)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          int powDiff = POW_10_INT[Math.abs(sparkDecimal.scale() - parquetScale)];
          int upperBound = POW_10_INT[sparkDecimal.precision()];
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int unscaled = changeUnscaledIntPrecision(
                dataColumn.readInteger(), powDiff, upperBound, sparkScaleBigger);
              sparkColumn.putInt(rowId + i, unscaled);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is64BitDecimalType(sparkType)) {
          // parquet int32-decimal -> spark decimal(precision<=18)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          long powDiff = POW_10_LONG[Math.abs(sparkDecimal.scale() - parquetScale)];
          long upperBound = POW_10_LONG[sparkDecimal.precision()];
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long unscaled = changeUnscaledLongPrecision(
                dataColumn.readInteger(), powDiff, upperBound, sparkScaleBigger);
              sparkColumn.putLong(rowId + i, unscaled);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.isByteArrayDecimalType(sparkType)) {
          // parquet int32-decimal -> spark decimal(precision>18)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          BigInteger powDiff =
            BigInteger.valueOf(10).pow(Math.abs(sparkDecimal.scale() - parquetScale));
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              BigInteger unscaled = changeUnscaledBigIntegerPrecision(
                BigInteger.valueOf(dataColumn.readInteger()),
                powDiff, sparkScaleBigger, sparkDecimal);
              sparkColumn.putByteArray(rowId + i, unscaled.toByteArray());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.StringType) {
          // parquet int32-decimal -> spark string
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              BigDecimal decimal = BigDecimal.valueOf(dataColumn.readInteger(), parquetScale);
              sparkColumn.putByteArray(rowId + i, decimal.toString().getBytes(UTF8));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else {
          throw constructConvertNotSupportedException();
        }
        break;
      case INT_8:
      case INT_16:
      case INT_32:
        if (originalType == OriginalType.INT_8 && sparkType == DataTypes.ByteType) {
          // parquet int32-byte -> spark byte
          defColumn.readBytes(
            num, sparkColumn, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
        } else if (originalType == OriginalType.INT_16 && sparkType == DataTypes.ShortType) {
          // parquet int32-short -> spark short
          defColumn.readShorts(
            num, sparkColumn, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
        } else if (sparkType == DataTypes.IntegerType) {
          // parquet int32 -> spark int
          defColumn.readIntegers(
            num, sparkColumn, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
        } else if (sparkType == DataTypes.FloatType) {
          // parquet int32 -> spark float
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              sparkColumn.putFloat(rowId + i, (float) dataColumn.readInteger());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.DoubleType) {
          // parquet int32 -> spark double
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              sparkColumn.putDouble(rowId + i, dataColumn.readInteger());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.ByteType) {
          // parquet int32 -> spark byte
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int value = dataColumn.readInteger();
              checkIntToByteOverflow(value);
              sparkColumn.putByte(rowId + i, (byte) value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.ShortType) {
          // parquet int32 -> spark short
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int value = dataColumn.readInteger();
              checkIntToShortOverflow(value);
              sparkColumn.putShort(rowId + i, (short) value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.LongType) {
          // parquet int32 -> spark long
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long value = dataColumn.readInteger();
              sparkColumn.putLong(rowId + i, value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is32BitDecimalType(sparkType)) {
          // parquet int32 -> spark decimal(precision<=9)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          int powDiff = POW_10_INT[sparkDecimal.scale()];
          int upperBound = POW_10_INT[sparkDecimal.precision()];
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int unscaled = changeUnscaledIntPrecision(
                dataColumn.readInteger(), powDiff, upperBound, true);
              sparkColumn.putInt(rowId + i, unscaled);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is64BitDecimalType(sparkType)) {
          // parquet int32 -> spark decimal(precision<=18)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          long powDiff = POW_10_LONG[sparkDecimal.scale()];
          long upperBound = POW_10_LONG[sparkDecimal.precision()];
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long unscaled = changeUnscaledLongPrecision(
                dataColumn.readInteger(), powDiff, upperBound, true);
              sparkColumn.putLong(rowId + i, unscaled);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.isByteArrayDecimalType(sparkType)) {
          // parquet int32 -> spark decimal(precision>18)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          BigInteger powDiff = BigInteger.valueOf(10).pow(sparkDecimal.scale());
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              BigInteger unscaled = changeUnscaledBigIntegerPrecision(
                BigInteger.valueOf(dataColumn.readInteger()),
                powDiff, true, sparkDecimal);
              sparkColumn.putByteArray(rowId + i, unscaled.toByteArray());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.StringType) {
          // parquet int32 -> spark string
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              sparkColumn.putByteArray(rowId + i,
                Integer.toString(dataColumn.readInteger()).getBytes(UTF8));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else {
          throw constructConvertNotSupportedException();
        }
        break;
      default:
          throw constructConvertNotSupportedException();
    }
  }

  public void readInt64Batch(int rowId, int num, VectorizedRleValuesReader defColumn,
                           ValuesReader dataColumn) throws IOException {
    // Possible spark Type:
    // 1.FloatType 2.DoubleType 3.ByteType 4.ShortType 5.IntegerType
    // 6.DecimalType 7.LongType 8.StringType 9.DateType 10.TimestampType
    OriginalType originalType =
      parquetTypeOriginal == null ? OriginalType.INT_64 : parquetTypeOriginal;
    switch (originalType) {
      case TIMESTAMP_MICROS:
      case TIMESTAMP_MILLIS:
        boolean isMicros = originalType == OriginalType.TIMESTAMP_MICROS;
        if (sparkType == DataTypes.TimestampType && isMicros) {
          // parquet int64 timestamp-micros -> spark timestamp
          if (dateTimeIsCorrected) {
            defColumn.readLongs(
              num, sparkColumn, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
          } else {
            defColumn.readLongsWithRebase(
              num, sparkColumn, rowId, maxDefLevel,
              (VectorizedValuesReader) dataColumn, dateTimeFailIfRebase);
          }
        } else if (sparkType == DataTypes.TimestampType) {
          // parquet int64 timestamp-millis -> spark timestamp
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long micros = Math.multiplyExact(dataColumn.readLong(), MICROS_PER_MILLIS);
              if (!dateTimeIsCorrected) {
                micros = rebaseMicros(micros, dateTimeFailIfRebase);
              }
              sparkColumn.putLong(rowId + i, micros);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.DateType) {
          // parquet int64-timestamp -> spark date
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long micros = dataColumn.readLong();
              if (!isMicros) {
                micros = Math.multiplyExact(micros, MICROS_PER_MILLIS);
              }
              if (!dateTimeIsCorrected) {
                micros = rebaseMicros(micros, dateTimeFailIfRebase);
              }
              sparkColumn.putInt(rowId + i, DateTimeUtils.microsToDays(micros, convertTz));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.StringType) {
          // parquet int64-timestamp -> spark string
          TimestampFormatter timestampFormatter =
            TimestampFormatter.getFractionFormatter(convertTz);
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long micros = dataColumn.readLong();
              if (!isMicros) {
                micros = Math.multiplyExact(micros, MICROS_PER_MILLIS);
              }
              if (!dateTimeIsCorrected) {
                micros = rebaseMicros(micros, dateTimeFailIfRebase);
              }
              sparkColumn.putByteArray(
                rowId + i, timestampFormatter.format(micros).getBytes(UTF8));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else {
          throw constructConvertNotSupportedException();
        }
        break;
      case DECIMAL:
        int parquetScale = parquetDecimal.getScale();
        long powDiffScale = POW_10_LONG[parquetScale];
        if (DecimalType.is64BitDecimalType(sparkType) &&
          isParquetSparkDecimalMatch((DecimalType) sparkType)) {
          // parquet int64-decimal -> spark decimal(precision match)
          defColumn.readLongs(
            num, sparkColumn, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
        } else if (sparkType == DataTypes.FloatType) {
          // parquet int64-decimal -> spark float
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              BigDecimal decimal = BigDecimal.valueOf(dataColumn.readLong(), parquetScale);
              sparkColumn.putFloat(rowId + i, decimal.floatValue());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.DoubleType) {
          // parquet int64-decimal -> spark double
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              BigDecimal decimal = BigDecimal.valueOf(dataColumn.readLong(), parquetScale);
              sparkColumn.putDouble(rowId + i, decimal.doubleValue());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.ByteType) {
          // parquet int64-decimal -> spark byte
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long value = dataColumn.readLong() / powDiffScale;
              checkLongToIntOverflow(value);
              checkIntToByteOverflow((int) value);
              sparkColumn.putByte(rowId + i, (byte) value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.ShortType) {
          // parquet int64-decimal -> spark short
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long value = dataColumn.readLong() / powDiffScale;
              checkLongToIntOverflow(value);
              checkIntToShortOverflow((int) value);
              sparkColumn.putShort(rowId + i, (short) value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.IntegerType) {
          // parquet int64-decimal -> spark int
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long value = dataColumn.readLong() / powDiffScale;
              checkLongToIntOverflow(value);
              sparkColumn.putInt(rowId + i, (int) value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.LongType) {
          // parquet int64-decimal -> spark long
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long value = dataColumn.readLong() / powDiffScale;
              sparkColumn.putLong(rowId + i, value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is32BitDecimalType(sparkType)) {
          // parquet int64-decimal -> spark decimal(precision<=9)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          long powDiff = POW_10_LONG[Math.abs(sparkDecimal.scale() - parquetScale)];
          long upperBound = POW_10_LONG[sparkDecimal.precision()];
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long unscaled = changeUnscaledLongPrecision(
                dataColumn.readLong(), powDiff, upperBound, sparkScaleBigger);
              sparkColumn.putInt(rowId + i, (int) unscaled);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is64BitDecimalType(sparkType)) {
          // parquet int64-decimal -> spark decimal(precision<=18)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          long powDiff = POW_10_LONG[Math.abs(sparkDecimal.scale() - parquetScale)];
          long upperBound = POW_10_LONG[sparkDecimal.precision()];
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long unscaled = changeUnscaledLongPrecision(
                dataColumn.readLong(), powDiff, upperBound, sparkScaleBigger);
              sparkColumn.putLong(rowId + i, unscaled);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.isByteArrayDecimalType(sparkType)) {
          // parquet int64-decimal -> spark decimal(precision>18)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          BigInteger powDiff =
            BigInteger.valueOf(10).pow(Math.abs(sparkDecimal.scale() - parquetScale));
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              BigInteger unscaled = changeUnscaledBigIntegerPrecision(
                BigInteger.valueOf(dataColumn.readLong()),
                powDiff, sparkScaleBigger, sparkDecimal);
              sparkColumn.putByteArray(rowId + i, unscaled.toByteArray());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.StringType) {
          // parquet int64-decimal -> spark string
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              BigDecimal decimal = BigDecimal.valueOf(dataColumn.readLong(), parquetScale);
              sparkColumn.putByteArray(rowId + i, decimal.toString().getBytes(UTF8));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else {
          throw constructConvertNotSupportedException();
        }
        break;
      case INT_64:
        if (sparkType == DataTypes.LongType) {
          // parquet int64 -> spark long
          defColumn.readLongs(
            num, sparkColumn, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
        } else if (sparkType == DataTypes.FloatType) {
          // parquet int64 -> spark float
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              sparkColumn.putFloat(rowId + i, (float) dataColumn.readLong());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.DoubleType) {
          // parquet int64 -> spark double
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              sparkColumn.putDouble(rowId + i, (double) dataColumn.readLong());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.ByteType) {
          // parquet int64 -> spark byte
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long value = dataColumn.readLong();
              checkLongToIntOverflow(value);
              checkIntToByteOverflow((int) value);
              sparkColumn.putByte(rowId + i, (byte) value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.ShortType) {
          // parquet int64 -> spark short
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long value = dataColumn.readLong();
              checkLongToIntOverflow(value);
              checkIntToShortOverflow((int) value);
              sparkColumn.putShort(rowId + i, (short) value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.IntegerType) {
          // parquet int64 -> spark int
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long value = dataColumn.readLong();
              checkLongToIntOverflow(value);
              sparkColumn.putInt(rowId + i, (int) value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is32BitDecimalType(sparkType)) {
          // parquet int64 -> spark decimal(precision<=9)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          int powDiff = POW_10_INT[sparkDecimal.scale()];
          int upperBound = POW_10_INT[sparkDecimal.precision()];
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long value = dataColumn.readLong();
              checkLongToIntOverflow(value);
              int unscaled = changeUnscaledIntPrecision(
                (int) value, powDiff, upperBound, true);
              sparkColumn.putInt(rowId + i, unscaled);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is64BitDecimalType(sparkType)) {
          // parquet int64 -> spark decimal(precision<=18)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          long powDiff = POW_10_LONG[sparkDecimal.scale()];
          long upperBound = POW_10_LONG[sparkDecimal.precision()];
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              long unscaled = changeUnscaledLongPrecision(
                dataColumn.readLong(), powDiff, upperBound, true);
              sparkColumn.putLong(rowId + i, unscaled);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.isByteArrayDecimalType(sparkType)) {
          // parquet int64 -> spark decimal(precision>18)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          BigInteger powDiff = BigInteger.valueOf(10).pow(sparkDecimal.scale());
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              BigInteger unscaled = changeUnscaledBigIntegerPrecision(
                BigInteger.valueOf(dataColumn.readLong()),
                powDiff, true, sparkDecimal);
              sparkColumn.putByteArray(rowId + i, unscaled.toByteArray());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.StringType) {
          // parquet int64 -> spark string
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              sparkColumn.putByteArray(rowId + i,
                Long.toString(dataColumn.readLong()).getBytes(UTF8));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else {
          throw constructConvertNotSupportedException();
        }
        break;
      default:
        throw constructConvertNotSupportedException();
    }
  }

  public void readInt96Batch(int rowId, int num, VectorizedRleValuesReader defColumn,
                              ValuesReader dataColumn) throws IOException {
    // Possible spark Type:
    // 1.DateType 2.TimestampType 3.StringType
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    if (sparkType == DataTypes.TimestampType) {
      // parquet int96 -> spark timestamp
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          sparkColumn.putLong(rowId + i, int96ToMicros(data.readBinary(BINARY_TIME_LEN)));
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (sparkType == DataTypes.DateType) {
      // parquet int96 -> spark date
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          long micros = int96ToMicros(data.readBinary(BINARY_TIME_LEN));
          sparkColumn.putInt(rowId + i, DateTimeUtils.microsToDays(micros, convertTz));
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else if (sparkType == DataTypes.StringType) {
      // parquet int96 -> spark string
      TimestampFormatter timestampFormatter = TimestampFormatter.getFractionFormatter(convertTz);
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          long micros = int96ToMicros(data.readBinary(BINARY_TIME_LEN));
          sparkColumn.putByteArray(
            rowId + i, timestampFormatter.format(micros).getBytes(UTF8));
        } else {
          sparkColumn.putNull(rowId + i);
        }
      }
    } else {
      throw constructConvertNotSupportedException();
    }
  }

  public void readBinaryBatch(int rowId, int num, VectorizedRleValuesReader defColumn,
                            ValuesReader dataColumn) throws IOException {
    // Possible spark Type:
    // 1.BinaryType 2.FloatType 3.DoubleType 4.ShortType 5.IntegerType
    // 6.DateType 7.TimestampType 9.DecimalType 10.LongType 11.StringType
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    OriginalType originalType =
      parquetTypeOriginal == null ? OriginalType.UTF8 : parquetTypeOriginal;
    if (sparkType == DataTypes.BinaryType) {
      // parquet binary -> spark binary
      defColumn.readBinarys(num, sparkColumn, rowId, maxDefLevel, data);
      return;
    }
    if (isFixedLenBinary && OriginalType.DECIMAL != parquetTypeOriginal) {
      throw constructConvertNotSupportedException();
    }
    switch (originalType) {
      case UTF8:
      case ENUM:
        if (sparkType == DataTypes.StringType) {
          // parquet binary-utf8 -> spark string
          defColumn.readBinarys(num, sparkColumn, rowId, maxDefLevel, data);
        } else if (sparkType == DataTypes.FloatType) {
          // parquet binary-utf8 -> spark float
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = data.readInteger();
              sparkColumn.putFloat(rowId + i, utf8BinaryToFloat(data.readBinary(len)));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.DoubleType) {
          // parquet binary-utf8 -> spark double
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = data.readInteger();
              sparkColumn.putDouble(rowId + i, utf8BinaryToDouble(data.readBinary(len)));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.ByteType) {
          // parquet binary-utf8 -> spark byte
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = data.readInteger();
              UTF8String utf8 = ParquetRowConverter.utf8StringFromBinary(data.readBinary(len));
              sparkColumn.putByte(rowId + i, utf8.toByteExact());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.ShortType) {
          // parquet binary-utf8 -> spark short
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = data.readInteger();
              UTF8String utf8 = ParquetRowConverter.utf8StringFromBinary(data.readBinary(len));
              sparkColumn.putShort(rowId + i, utf8.toShortExact());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.IntegerType) {
          // parquet binary-utf8 -> spark int
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = data.readInteger();
              UTF8String utf8 = ParquetRowConverter.utf8StringFromBinary(data.readBinary(len));
              sparkColumn.putInt(rowId + i, utf8.toIntExact());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.LongType) {
          // parquet binary-utf8 -> spark long
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = data.readInteger();
              UTF8String utf8 = ParquetRowConverter.utf8StringFromBinary(data.readBinary(len));
              sparkColumn.putLong(rowId + i, utf8.toLongExact());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is32BitDecimalType(sparkType)) {
          // parquet binary-utf8 -> spark decimal(precision<=9)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = data.readInteger();
              String string = ParquetRowConverter.binaryToJString(data.readBinary(len)).trim();
              BigInteger unscaled = bigDecimalToUnscaledValue(new BigDecimal(string), sparkDecimal);
              sparkColumn.putInt(rowId + i, unscaled.intValue());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is64BitDecimalType(sparkType)) {
          // parquet binary-utf8 -> spark decimal(precision<=18)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = data.readInteger();
              String string = ParquetRowConverter.binaryToJString(data.readBinary(len)).trim();
              BigInteger unscaled = bigDecimalToUnscaledValue(new BigDecimal(string), sparkDecimal);
              sparkColumn.putLong(rowId + i, unscaled.longValue());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.isByteArrayDecimalType(sparkType)) {
          // parquet binary-utf8 -> spark decimal(precision>18)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = data.readInteger();
              String string = ParquetRowConverter.binaryToJString(data.readBinary(len)).trim();
              BigInteger unscaled = bigDecimalToUnscaledValue(new BigDecimal(string), sparkDecimal);
              sparkColumn.putByteArray(rowId + i, unscaled.toByteArray());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.DateType) {
          // parquet binary-utf8 -> spark date
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = data.readInteger();
              sparkColumn.putInt(rowId + i, utf8BinaryToDays(data.readBinary(len)));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.TimestampType) {
          // parquet binary-utf8 -> spark timestamp
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = data.readInteger();
              sparkColumn.putLong(rowId + i, utf8BinaryToMicros(data.readBinary(len)));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else {
          throw constructConvertNotSupportedException();
        }
        break;
      case JSON:
        if (sparkType == DataTypes.StringType) {
          defColumn.readBinarys(num, sparkColumn, rowId, maxDefLevel, data);
        } else {
          throw constructConvertNotSupportedException();
        }
        break;
      case DECIMAL:
        int parquetScale = parquetDecimal.getScale();
        BigInteger powDiffScale = BigInteger.valueOf(10).pow(parquetScale);
        if (DecimalType.is32BitDecimalType(sparkType)
          && isParquetSparkDecimalMatch((DecimalType) sparkType)) {
          // parquet binary-decimal -> spark decimal(precision match)
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = (isFixedLenBinary) ? fixedLen : data.readInteger();
              sparkColumn.putInt(rowId + i,
                (int) ParquetRowConverter.binaryToUnscaledLong(data.readBinary(len)));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is64BitDecimalType(sparkType)
          && isParquetSparkDecimalMatch((DecimalType) sparkType)) {
          // parquet binary-decimal -> spark decimal(precision match)
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = (isFixedLenBinary) ? fixedLen : data.readInteger();
              sparkColumn.putLong(rowId + i,
                ParquetRowConverter.binaryToUnscaledLong(data.readBinary(len)));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.isByteArrayDecimalType(sparkType)
          && isParquetSparkDecimalMatch((DecimalType) sparkType)) {
          if (isFixedLenBinary) {
            // parquet binary(FixedLen)-decimal -> spark decimal(precision match)
            for (int i = 0; i < num; i++) {
              if (defColumn.readInteger() == maxDefLevel) {
                sparkColumn.putByteArray(rowId + i, data.readBinary(fixedLen).getBytes());
              } else {
                sparkColumn.putNull(rowId + i);
              }
            }
          } else {
            // parquet binary-decimal -> spark decimal(precision match)
            defColumn.readBinarys(num, sparkColumn, rowId, maxDefLevel, data);
          }
        } else if (sparkType == DataTypes.FloatType) {
          // parquet binary-decimal -> spark float
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = (isFixedLenBinary) ? fixedLen : data.readInteger();
              BigDecimal decimal =
                new BigDecimal(new BigInteger(data.readBinary(len).getBytes()), parquetScale);
              float value = decimal.floatValue();
              if (parquetScale > 38) {
                checkFloatOverflow(value);
              }
              sparkColumn.putFloat(rowId + i, value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.DoubleType) {
          // parquet binary-decimal -> spark double
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = (isFixedLenBinary) ? fixedLen : data.readInteger();
              BigDecimal decimal =
                new BigDecimal(new BigInteger(data.readBinary(len).getBytes()), parquetScale);
              double value = decimal.doubleValue();
              if (parquetScale > 308) {
                checkDoubleOverflow(value);
              }
              sparkColumn.putDouble(rowId + i, value);
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.ByteType) {
          // parquet binary-decimal -> spark byte
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = (isFixedLenBinary) ? fixedLen : data.readInteger();
              BigInteger value =
                new BigInteger(data.readBinary(len).getBytes()).divide(powDiffScale);
              sparkColumn.putByte(rowId + i, value.byteValueExact());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.ShortType) {
          // parquet binary-decimal -> spark short
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = (isFixedLenBinary) ? fixedLen : data.readInteger();
              BigInteger value =
                new BigInteger(data.readBinary(len).getBytes()).divide(powDiffScale);
              sparkColumn.putShort(rowId + i, value.shortValueExact());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.IntegerType) {
          // parquet binary-decimal -> spark int
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = (isFixedLenBinary) ? fixedLen : data.readInteger();
              BigInteger value =
                new BigInteger(data.readBinary(len).getBytes()).divide(powDiffScale);
              sparkColumn.putInt(rowId + i, value.intValueExact());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.LongType) {
          // parquet binary-decimal -> spark long
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = (isFixedLenBinary) ? fixedLen : data.readInteger();
              BigInteger value =
                new BigInteger(data.readBinary(len).getBytes()).divide(powDiffScale);
              sparkColumn.putLong(rowId + i, value.longValueExact());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is32BitDecimalType(sparkType)) {
          // parquet binary-decimal -> spark decimal(precision<=9)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          BigInteger powDiff =
            BigInteger.valueOf(10).pow(Math.abs(sparkDecimal.scale() - parquetScale));
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = (isFixedLenBinary) ? fixedLen : data.readInteger();
              BigInteger unscaled = changeUnscaledBigIntegerPrecision(
                new BigInteger(data.readBinary(len).getBytes()),
                powDiff, sparkScaleBigger, sparkDecimal);
              sparkColumn.putInt(rowId + i, unscaled.intValue());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.is64BitDecimalType(sparkType)) {
          // parquet binary-decimal -> spark decimal(precision<=18)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          BigInteger powDiff =
            BigInteger.valueOf(10).pow(Math.abs(sparkDecimal.scale() - parquetScale));
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = (isFixedLenBinary) ? fixedLen : data.readInteger();
              BigInteger unscaled = changeUnscaledBigIntegerPrecision(
                new BigInteger(data.readBinary(len).getBytes()),
                powDiff, sparkScaleBigger, sparkDecimal);
              sparkColumn.putLong(rowId + i, unscaled.longValue());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (DecimalType.isByteArrayDecimalType(sparkType)) {
          // parquet binary-decimal -> spark decimal(precision>18)
          DecimalType sparkDecimal = (DecimalType) sparkType;
          BigInteger powDiff =
            BigInteger.valueOf(10).pow(Math.abs(sparkDecimal.scale() - parquetScale));
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = (isFixedLenBinary) ? fixedLen : data.readInteger();
              BigInteger unscaled = changeUnscaledBigIntegerPrecision(
                new BigInteger(data.readBinary(len).getBytes()),
                powDiff, sparkScaleBigger, sparkDecimal);
              sparkColumn.putByteArray(rowId + i, unscaled.toByteArray());
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else if (sparkType == DataTypes.StringType) {
          // parquet binary-decimal -> spark string
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              int len = (isFixedLenBinary) ? fixedLen : data.readInteger();
              BigDecimal decimal =
                new BigDecimal(new BigInteger(data.readBinary(len).getBytes()), parquetScale);
              sparkColumn.putByteArray(rowId + i, decimal.toString().getBytes(UTF8));
            } else {
              sparkColumn.putNull(rowId + i);
            }
          }
        } else {
          throw constructConvertNotSupportedException();
        }
        break;
      default:
        throw constructConvertNotSupportedException();
    }
  }

  public WritableColumnVector getSparkColumn() {
    return sparkColumn;
  }

  private Dictionary convertDictionaryFromFloat(Dictionary originalDictionary) {
    int length = originalDictionary.getMaxId() + 1;
    // Possible spark Type:
    // 1.FloatType 2.DoubleType 3.ByteType 4.ShortType 5.IntegerType
    // 6.DecimalType 7.LongType 8.StringType

    // parquet float -> spark float
    if (sparkType == DataTypes.FloatType) {
      return originalDictionary;
    }
    // parquet float -> spark double
    if (sparkType == DataTypes.DoubleType) {
      double[] arrayContent = new double[length];
      for (int i = 0; i < length; i++) {
        arrayContent[i] = originalDictionary.decodeToFloat(i);
      }
      return new ExpandedDoubleDictionary(arrayContent);
    }
    // parquet float -> spark byte
    if (sparkType == DataTypes.ByteType) {
      int[] arrayContent = new int[length];
      for (int i = 0; i < length; i++) {
        checkDoubleToIntegralOverflow(originalDictionary.decodeToFloat(i), BYTE_MAX, BYTE_MIN);
        arrayContent[i] = (int) originalDictionary.decodeToFloat(i);
      }
      return new ExpandedIntegerDictionary(arrayContent);
    }
    // parquet float -> spark short
    if (sparkType == DataTypes.ShortType) {
      int[] arrayContent = new int[length];
      for (int i = 0; i < length; i++) {
        checkDoubleToIntegralOverflow(originalDictionary.decodeToFloat(i), SHORT_MAX, SHORT_MIN);
        arrayContent[i] = (int) originalDictionary.decodeToFloat(i);
      }
      return new ExpandedIntegerDictionary(arrayContent);
    }
    // parquet float -> spark int
    if (sparkType == DataTypes.IntegerType) {
      int[] arrayContent = new int[length];
      for (int i = 0; i < length; i++) {
        checkDoubleToIntegralOverflow(originalDictionary.decodeToFloat(i), INT_MAX, INT_MIN);
        arrayContent[i] = (int) originalDictionary.decodeToFloat(i);
      }
      return new ExpandedIntegerDictionary(arrayContent);
    }
    // parquet float -> spark long
    if (sparkType == DataTypes.LongType) {
      long[] arrayContent = new long[length];
      for (int i = 0; i < length; i++) {
        checkDoubleToIntegralOverflow(originalDictionary.decodeToFloat(i), LONG_MAX, LONG_MIN);
        arrayContent[i] = (long) originalDictionary.decodeToFloat(i);
      }
      return new ExpandedLongDictionary(arrayContent);
    }
    // parquet float -> spark decimal(precision<=9)
    if (DecimalType.is32BitDecimalType(sparkType)) {
      DecimalType sparkDecimal = (DecimalType) sparkType;
      int[] arrayContent = new int[length];
      for (int i = 0; i < length; i++ ) {
        BigDecimal decimal = BigDecimal.valueOf(originalDictionary.decodeToFloat(i));
        BigInteger unscaled = bigDecimalToUnscaledValue(decimal, sparkDecimal);
        arrayContent[i] = unscaled.intValue();
      }
      return new ExpandedIntegerDictionary(arrayContent);
    }
    // parquet float -> spark decimal(precision<=18)
    if (DecimalType.is64BitDecimalType(sparkType)) {
      DecimalType sparkDecimal = (DecimalType) sparkType;
      long[] arrayContent = new long[length];
      for (int i = 0; i < length; i++ ) {
        BigDecimal decimal = BigDecimal.valueOf(originalDictionary.decodeToFloat(i));
        BigInteger unscaled = bigDecimalToUnscaledValue(decimal, sparkDecimal);
        arrayContent[i] = unscaled.longValue();
      }
      return new ExpandedLongDictionary(arrayContent);
    }
    // parquet float -> spark decimal(precision>18)
    if (DecimalType.isByteArrayDecimalType(sparkType)) {
      DecimalType sparkDecimal = (DecimalType) sparkType;
      Binary[] arrayContent = new Binary[length];
      for (int i = 0; i < length; i++ ) {
        BigDecimal decimal = BigDecimal.valueOf(originalDictionary.decodeToFloat(i));
        BigInteger unscaled = bigDecimalToUnscaledValue(decimal, sparkDecimal);
        arrayContent[i] = Binary.fromConstantByteArray(unscaled.toByteArray());
      }
      return new ExpandedBinaryDictionary(arrayContent);
    }
    // parquet float -> spark string
    if (sparkType == DataTypes.StringType) {
      Binary[] arrayContent = new Binary[length];
      for (int i = 0; i < length; i++) {
        arrayContent[i] = Binary.fromString(Float.toString(originalDictionary.decodeToFloat(i)));
      }
      return new ExpandedBinaryDictionary(arrayContent);
    }
    throw constructConvertNotSupportedException();
  }

  private Dictionary convertDictionaryFromDouble(Dictionary originalDictionary) {
    int length = originalDictionary.getMaxId() + 1;
    // Possible spark Type:
    // 1.FloatType 2.DoubleType 3.ByteType 4.ShortType 5.IntegerType
    // 6.DecimalType 7.LongType 8.StringType

    // parquet double -> spark double
    if (sparkType == DataTypes.DoubleType) {
      return originalDictionary;
    }
    // parquet double -> spark float
    if (sparkType == DataTypes.FloatType) {
      float[] arrayContent = new float[length];
      for (int i = 0; i < length; i++) {
        checkDoubleToFloatOverflow(originalDictionary.decodeToDouble(i));
        arrayContent[i] = (float) originalDictionary.decodeToDouble(i);
      }
      return new ExpandedFloatDictionary(arrayContent);
    }
    // parquet double -> spark byte
    if (sparkType == DataTypes.ByteType) {
      int[] arrayContent = new int[length];
      for (int i = 0; i < length; i++) {
        checkDoubleToIntegralOverflow(originalDictionary.decodeToDouble(i), BYTE_MAX, BYTE_MIN);
        arrayContent[i] = (int) originalDictionary.decodeToDouble(i);
      }
      return new ExpandedIntegerDictionary(arrayContent);
    }
    // parquet double -> spark short
    if (sparkType == DataTypes.ShortType) {
      int[] arrayContent = new int[length];
      for (int i = 0; i < length; i++) {
        checkDoubleToIntegralOverflow(originalDictionary.decodeToDouble(i), SHORT_MAX, SHORT_MIN);
        arrayContent[i] = (int) originalDictionary.decodeToDouble(i);
      }
      return new ExpandedIntegerDictionary(arrayContent);
    }
    // parquet double -> spark int
    if (sparkType == DataTypes.IntegerType) {
      int[] arrayContent = new int[length];
      for (int i = 0; i < length; i++) {
        checkDoubleToIntegralOverflow(originalDictionary.decodeToDouble(i), INT_MAX, INT_MIN);
        arrayContent[i] = (int) originalDictionary.decodeToDouble(i);
      }
      return new ExpandedIntegerDictionary(arrayContent);
    }
    // parquet double -> spark long
    if (sparkType == DataTypes.LongType) {
      long[] arrayContent = new long[length];
      for (int i = 0; i < length; i++) {
        checkDoubleToIntegralOverflow(originalDictionary.decodeToDouble(i), LONG_MAX, LONG_MIN);
        arrayContent[i] = (long) originalDictionary.decodeToDouble(i);
      }
      return new ExpandedLongDictionary(arrayContent);
    }
    // parquet double -> spark Decimal(precision<=9)
    if (DecimalType.is32BitDecimalType(sparkType)) {
      DecimalType sparkDecimal = (DecimalType) sparkType;
      int[] arrayContent = new int[length];
      for (int i = 0; i < length; i++ ) {
        BigDecimal decimal = BigDecimal.valueOf(originalDictionary.decodeToDouble(i));
        BigInteger unscaled = bigDecimalToUnscaledValue(decimal, sparkDecimal);
        arrayContent[i] = unscaled.intValue();
      }
      return new ExpandedIntegerDictionary(arrayContent);
    }
    // parquet double -> spark Decimal(precision<=18)
    if (DecimalType.is64BitDecimalType(sparkType)) {
      DecimalType sparkDecimal = (DecimalType) sparkType;
      long[] arrayContent = new long[length];
      for (int i = 0; i < length; i++ ) {
        BigDecimal decimal = BigDecimal.valueOf(originalDictionary.decodeToDouble(i));
        BigInteger unscaled = bigDecimalToUnscaledValue(decimal, sparkDecimal);
        arrayContent[i] = unscaled.longValue();
      }
      return new ExpandedLongDictionary(arrayContent);
    }
    // parquet double -> spark Decimal(precision>18)
    if (DecimalType.isByteArrayDecimalType(sparkType)) {
      DecimalType sparkDecimal = (DecimalType) sparkType;
      Binary[] arrayContent = new Binary[length];
      for (int i = 0; i < length; i++ ) {
        BigDecimal decimal = BigDecimal.valueOf(originalDictionary.decodeToDouble(i));
        BigInteger unscaled = bigDecimalToUnscaledValue(decimal, sparkDecimal);
        arrayContent[i] = Binary.fromConstantByteArray(unscaled.toByteArray());
      }
      return new ExpandedBinaryDictionary(arrayContent);
    }
    // parquet double -> spark string
    if (sparkType == DataTypes.StringType) {
      Binary[] arrayContent = new Binary[length];
      for (int i = 0; i < length; i++) {
        arrayContent[i] = Binary.fromString(Double.toString(originalDictionary.decodeToDouble(i)));
      }
      return new ExpandedBinaryDictionary(arrayContent);
    }
    throw constructConvertNotSupportedException();
  }

  private Dictionary convertDictionaryFromInt32(Dictionary originalDictionary) {
    int length = originalDictionary.getMaxId() + 1;
    // Possible spark Type:
    // 1.FloatType 2.DoubleType 3.ByteType 4.ShortType 5.IntegerType
    // 6.DecimalType 7.LongType 8.StringType 9.DateType 10.TimestampType
    OriginalType originalType =
      parquetTypeOriginal == null ? OriginalType.INT_32 : parquetTypeOriginal;
    switch (originalType) {
      case DATE:
        // parquet int32-date -> spark date
        if (sparkType == DataTypes.DateType) {
          if (dateTimeIsCorrected) {
            return originalDictionary;
          } else {
            int[] arrayContent = new int[length];
            for (int i = 0; i < length; i++) {
              arrayContent[i] =
                rebaseDays(originalDictionary.decodeToInt(i), dateTimeFailIfRebase);
            }
            return new ExpandedIntegerDictionary(arrayContent);
          }
        }
        // parquet int32-date -> spark timestamp
        if (sparkType == DataTypes.TimestampType) {
          long[] arrayContent = new long[length];
          for (int i = 0; i < length; i++) {
            int days = originalDictionary.decodeToInt(i);
            if (!dateTimeIsCorrected) {
              days = rebaseDays(days, dateTimeFailIfRebase);
            }
            arrayContent[i] = DateTimeUtils.daysToMicros(days, convertTz);
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet int32-date -> spark string
        if (sparkType == DataTypes.StringType) {
          DateFormatter dateFormatter = DateFormatter.apply(convertTz);
          Binary[] arrayContent = new Binary[length];
          for (int i = 0; i < length; i++) {
            int days = originalDictionary.decodeToInt(i);
            if (!dateTimeIsCorrected) {
              days = rebaseDays(days, dateTimeFailIfRebase);
            }
            arrayContent[i] = Binary.fromString(dateFormatter.format(days));
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
        break;
      case DECIMAL:
        // parquet int32-decimal -> spark decimal(precision match)
        if (DecimalType.is32BitDecimalType(sparkType) &&
          isParquetSparkDecimalMatch((DecimalType) sparkType)) {
          return originalDictionary;
        }
        // parquet int32-decimal -> spark float
        int parquetScale = parquetDecimal.getScale();
        if (sparkType == DataTypes.FloatType) {
          float[] arrayContent = new float[length];
          for (int i = 0; i < length; i++) {
            BigDecimal decimal =
              BigDecimal.valueOf(originalDictionary.decodeToInt(i), parquetScale);
            arrayContent[i] = decimal.floatValue();
          }
          return new ExpandedFloatDictionary(arrayContent);
        }
        // parquet int32-decimal -> spark double
        if (sparkType == DataTypes.DoubleType) {
          double[] arrayContent = new double[length];
          for (int i = 0; i < length; i++) {
            BigDecimal decimal =
              BigDecimal.valueOf(originalDictionary.decodeToInt(i), parquetScale);
            arrayContent[i] = decimal.doubleValue();
          }
          return new ExpandedDoubleDictionary(arrayContent);
        }
        // parquet int32-decimal -> spark byte
        int powDiffScale = POW_10_INT[parquetScale];
        if (sparkType == DataTypes.ByteType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            int value = originalDictionary.decodeToInt(i) / powDiffScale;
            checkIntToByteOverflow(value);
            arrayContent[i] = value;
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int32-decimal -> spark short
        if (sparkType == DataTypes.ShortType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            int value = originalDictionary.decodeToInt(i) / powDiffScale;
            checkIntToShortOverflow(value);
            arrayContent[i] = value;
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int32-decimal -> spark int
        if (sparkType == DataTypes.IntegerType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            int value = originalDictionary.decodeToInt(i) / powDiffScale;
            arrayContent[i] = value;
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int32-decimal -> spark long
        if (sparkType == DataTypes.LongType) {
          long[] arrayContent = new long[length];
          for (int i = 0; i < length; i++) {
            long value = originalDictionary.decodeToInt(i) / powDiffScale;
            arrayContent[i] = value;
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet int32-decimal -> spark decimal(precision<=9)
        if (DecimalType.is32BitDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          int powDiff = POW_10_INT[Math.abs(sparkDecimal.scale() - parquetScale)];
          int upperBound = POW_10_INT[sparkDecimal.precision()];
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = changeUnscaledIntPrecision(
              originalDictionary.decodeToInt(i), powDiff, upperBound, sparkScaleBigger);
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int32-decimal -> spark decimal(precision<=18)
        if (DecimalType.is64BitDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          long powDiff = POW_10_LONG[Math.abs(sparkDecimal.scale() - parquetScale)];
          long upperBound = POW_10_LONG[sparkDecimal.precision()];
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          long[] arrayContent = new long[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = changeUnscaledLongPrecision(
              originalDictionary.decodeToInt(i), powDiff, upperBound, sparkScaleBigger);
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet int32-decimal -> spark decimal(precision>18)
        if (DecimalType.isByteArrayDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          BigInteger powDiff =
            BigInteger.valueOf(10).pow(Math.abs(sparkDecimal.scale() - parquetScale));
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          Binary[] arrayContent = new Binary[length];
          for (int i = 0; i < length; i++) {
            BigInteger unscaled = changeUnscaledBigIntegerPrecision(
              BigInteger.valueOf(originalDictionary.decodeToInt(i)),
              powDiff, sparkScaleBigger, sparkDecimal);
            arrayContent[i] = Binary.fromConstantByteArray(unscaled.toByteArray());
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
        // parquet int32-decimal -> spark string
        if (sparkType == DataTypes.StringType) {
          Binary[] arrayContent = new Binary[length];
          for (int i = 0; i < length; i++) {
            BigDecimal decimal =
              BigDecimal.valueOf(originalDictionary.decodeToInt(i), parquetScale);
            arrayContent[i] = Binary.fromString(decimal.toString());
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
        break;
      case INT_8:
        // parquet int32(int8) -> spark byte
        if (sparkType == DataTypes.ByteType) {
          return originalDictionary;
        }
        // fall through
      case INT_16:
        // parquet int32(int8/int16) -> spark short
        if (sparkType == DataTypes.ShortType) {
          return originalDictionary;
        }
        // fall through
      case INT_32:
        // parquet int32 -> spark int
        if (sparkType == DataTypes.IntegerType) {
          return originalDictionary;
        }
        // parquet int32 -> spark float
        if (sparkType == DataTypes.FloatType) {
          float[] arrayContent = new float[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = (float) originalDictionary.decodeToInt(i);
          }
          return new ExpandedFloatDictionary(arrayContent);
        }
        // parquet int32 -> spark double
        if (sparkType == DataTypes.DoubleType) {
          double[] arrayContent = new double[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = originalDictionary.decodeToInt(i);
          }
          return new ExpandedDoubleDictionary(arrayContent);
        }
        // parquet int32 -> spark byte
        if (sparkType == DataTypes.ByteType) {
          for (int i = 0; i < length; i++) {
            checkIntToByteOverflow(originalDictionary.decodeToInt(i));
          }
          return originalDictionary;
        }
        // parquet int32 -> spark short
        if (sparkType == DataTypes.ShortType) {
          for (int i = 0; i < length; i++) {
            checkIntToShortOverflow(originalDictionary.decodeToInt(i));
          }
          return originalDictionary;
        }
        // parquet int32 -> spark long
        if (sparkType == DataTypes.LongType) {
          long[] arrayContent = new long[length];
          for (int i = 0; i < length; i++) {
             arrayContent[i] = originalDictionary.decodeToInt(i);
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet int32 -> spark decimal(precision<=9)
        if (DecimalType.is32BitDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          int[] arrayContent = new int[length];
          int powDiff = POW_10_INT[sparkDecimal.scale()];
          int upperBound = POW_10_INT[sparkDecimal.precision()];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = changeUnscaledIntPrecision(
              originalDictionary.decodeToInt(i), powDiff, upperBound, true);
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int32 -> spark decimal(precision<=18)
        if (DecimalType.is64BitDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          long[] arrayContent = new long[length];
          long powDiff = POW_10_LONG[sparkDecimal.scale()];
          long upperBound = POW_10_LONG[sparkDecimal.precision()];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = changeUnscaledLongPrecision(
              originalDictionary.decodeToInt(i), powDiff, upperBound, true);
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet int32 -> spark decimal(precision>18)
        if (DecimalType.isByteArrayDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          Binary[] arrayContent = new Binary[length];
          BigInteger powDiff = BigInteger.valueOf(10).pow(sparkDecimal.scale());
          for (int i = 0; i < length; i++) {
            BigInteger unscaled = changeUnscaledBigIntegerPrecision(
              BigInteger.valueOf(originalDictionary.decodeToInt(i)),
              powDiff, true, sparkDecimal);
            arrayContent[i] = Binary.fromConstantByteArray(unscaled.toByteArray());
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
        // parquet int32 -> spark string
        if (sparkType == DataTypes.StringType) {
          Binary[] arrayContent = new Binary[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] =
              Binary.fromString(Integer.toString(originalDictionary.decodeToInt(i)));
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
    }
    throw constructConvertNotSupportedException();
  }

  private Dictionary convertDictionaryFromInt64(Dictionary originalDictionary) {
    int length = originalDictionary.getMaxId() + 1;
    // Possible spark Type:
    // 1.FloatType 2.DoubleType 3.ByteType 4.ShortType 5.IntegerType
    // 6.DecimalType 7.LongType 8.StringType 9.DateType 10.TimestampType
    OriginalType originalType =
      parquetTypeOriginal == null ? OriginalType.INT_64 : parquetTypeOriginal;
    switch (originalType) {
      case TIMESTAMP_MICROS:
      case TIMESTAMP_MILLIS:
        boolean isMicros = originalType == OriginalType.TIMESTAMP_MICROS;
        // parquet int64-timestamp -> spark timestamp
        if (sparkType == DataTypes.TimestampType && dateTimeIsCorrected && isMicros) {
          return originalDictionary;
        }
        // parquet int64-timestamp -> spark timestamp
        if (sparkType == DataTypes.TimestampType) {
          long[] arrayContent = new long[length];
          for (int i = 0; i < length; i++) {
            long micros = originalDictionary.decodeToLong(i);
            if (!isMicros) {
              micros = Math.multiplyExact(micros, MICROS_PER_MILLIS);
            }
            if (!dateTimeIsCorrected) {
              micros = rebaseMicros(micros, dateTimeFailIfRebase);
            }
            arrayContent[i] = micros;
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet int64-timestamp -> spark date
        if (sparkType == DataTypes.DateType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            long micros = originalDictionary.decodeToLong(i);
            if (!isMicros) {
              micros = Math.multiplyExact(micros, MICROS_PER_MILLIS);
            }
            if (!dateTimeIsCorrected) {
              micros = rebaseMicros(micros, dateTimeFailIfRebase);
            }
            arrayContent[i] = DateTimeUtils.microsToDays(micros, convertTz);
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int64-timestamp -> spark string
        if (sparkType == DataTypes.StringType) {
          TimestampFormatter timestampFormatter =
            TimestampFormatter.getFractionFormatter(convertTz);
          Binary[] arrayContent = new Binary[length];
          for (int i = 0; i < length; i++) {
            long micros = originalDictionary.decodeToLong(i);
            if (!isMicros) {
              micros = Math.multiplyExact(micros, MICROS_PER_MILLIS);
            }
            if (!dateTimeIsCorrected) {
              micros = rebaseMicros(micros, dateTimeFailIfRebase);
            }
            arrayContent[i] = Binary.fromString(timestampFormatter.format(micros));
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
        break;
      case DECIMAL:
        // parquet int64-decimal -> spark decimal(precision match)
        if (DecimalType.is64BitDecimalType(sparkType) &&
          isParquetSparkDecimalMatch((DecimalType) sparkType)) {
          return originalDictionary;
        }
        int parquetScale = parquetDecimal.getScale();
        // parquet int64-decimal -> spark float
        if (sparkType == DataTypes.FloatType) {
          float[] arrayContent = new float[length];
          for (int i = 0; i < length; i++) {
            BigDecimal decimal =
              BigDecimal.valueOf(originalDictionary.decodeToLong(i), parquetScale);
            arrayContent[i] = decimal.floatValue();
          }
          return new ExpandedFloatDictionary(arrayContent);
        }
        // parquet int64-decimal -> spark double
        if (sparkType == DataTypes.DoubleType) {
          double[] arrayContent = new double[length];
          for (int i = 0; i < length; i++) {
            BigDecimal decimal =
              BigDecimal.valueOf(originalDictionary.decodeToLong(i), parquetScale);
            arrayContent[i] = decimal.doubleValue();
          }
          return new ExpandedDoubleDictionary(arrayContent);
        }
        // parquet int64-decimal -> spark byte
        long powDiffScale = POW_10_LONG[parquetScale];
        if (sparkType == DataTypes.ByteType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            long value = originalDictionary.decodeToLong(i) / powDiffScale;
            checkLongToIntOverflow(value);
            checkIntToByteOverflow((int) value);
            arrayContent[i] = (int) value;
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int64-decimal -> spark short
        if (sparkType == DataTypes.ShortType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            long value = originalDictionary.decodeToLong(i) / powDiffScale;
            checkLongToIntOverflow(value);
            checkIntToShortOverflow((int) value);
            arrayContent[i] = (int) value;
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int64-decimal -> spark int
        if (sparkType == DataTypes.IntegerType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            long value = originalDictionary.decodeToLong(i) / powDiffScale;
            checkLongToIntOverflow(value);
            arrayContent[i] = (int) value;
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int64-decimal -> spark long
        if (sparkType == DataTypes.LongType) {
          long[] arrayContent = new long[length];
          for (int i = 0; i < length; i++) {
            long value = originalDictionary.decodeToLong(i) / powDiffScale;
            arrayContent[i] = value;
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet int64-decimal -> spark decimal(precision<=9)
        if (DecimalType.is32BitDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          long powDiff = POW_10_LONG[Math.abs(sparkDecimal.scale() - parquetScale)];
          long upperBound = POW_10_LONG[sparkDecimal.precision()];
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            long unscaled = changeUnscaledLongPrecision(
              originalDictionary.decodeToLong(i), powDiff, upperBound, sparkScaleBigger);
            arrayContent[i] = (int) unscaled;
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int64-decimal -> spark decimal(precision<=18)
        if (DecimalType.is64BitDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          long powDiff = POW_10_LONG[Math.abs(sparkDecimal.scale() - parquetScale)];
          long upperBound = POW_10_LONG[sparkDecimal.precision()];
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          long[] arrayContent = new long[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = changeUnscaledLongPrecision(
              originalDictionary.decodeToLong(i), powDiff, upperBound, sparkScaleBigger);
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet int64-decimal -> spark decimal(precision>18)
        if (DecimalType.isByteArrayDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          BigInteger powDiff =
            BigInteger.valueOf(10).pow(Math.abs(sparkDecimal.scale() - parquetScale));
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          Binary[] arrayContent = new Binary[length];
          for (int i = 0; i < length; i++) {
            BigInteger unscaled = changeUnscaledBigIntegerPrecision(
              BigInteger.valueOf(originalDictionary.decodeToLong(i)),
              powDiff, sparkScaleBigger, sparkDecimal);
            arrayContent[i] = Binary.fromConstantByteArray(unscaled.toByteArray());
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
        // parquet int64-decimal -> spark string
        if (sparkType == DataTypes.StringType) {
          Binary[] arrayContent = new Binary[length];
          for (int i = 0; i < length; i++) {
            BigDecimal decimal =
              BigDecimal.valueOf(originalDictionary.decodeToLong(i), parquetScale);
            arrayContent[i] = Binary.fromString(decimal.toString());
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
        break;
      case INT_64:
        // parquet int64 -> spark long
        if (sparkType == DataTypes.LongType) {
          return originalDictionary;
        }
        // parquet int64 -> spark float
        if (sparkType == DataTypes.FloatType) {
          float[] arrayContent = new float[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = (float) originalDictionary.decodeToLong(i);
          }
          return new ExpandedFloatDictionary(arrayContent);
        }
        // parquet int64 -> spark double
        if (sparkType == DataTypes.DoubleType) {
          double[] arrayContent = new double[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = (double) originalDictionary.decodeToLong(i);
          }
          return new ExpandedDoubleDictionary(arrayContent);
        }
        // parquet int64 -> spark byte
        if (sparkType == DataTypes.ByteType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            long value = originalDictionary.decodeToLong(i);
            checkLongToIntOverflow(value);
            checkIntToByteOverflow((int) value);
            arrayContent[i] = (int) value;
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int64 -> spark short
        if (sparkType == DataTypes.ShortType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            long value = originalDictionary.decodeToLong(i);
            checkLongToIntOverflow(value);
            checkIntToShortOverflow((int) value);
            arrayContent[i] = (int) value;
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int64 -> spark int
        if (sparkType == DataTypes.IntegerType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            long value = originalDictionary.decodeToLong(i);
            checkLongToIntOverflow(value);
            arrayContent[i] = (int) value;
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int64 -> spark decimal(precision<=9)
        if (DecimalType.is32BitDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          int[] arrayContent = new int[length];
          int powDiff = POW_10_INT[sparkDecimal.scale()];
          int upperBound = POW_10_INT[sparkDecimal.precision()];
          for (int i = 0; i < length; i++) {
            checkLongToIntOverflow(originalDictionary.decodeToLong(i));
            arrayContent[i] = changeUnscaledIntPrecision(
              (int) originalDictionary.decodeToLong(i), powDiff, upperBound, true);
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet int64 -> spark decimal(precision<=18)
        if (DecimalType.is64BitDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          long[] arrayContent = new long[length];
          long powDiff = POW_10_LONG[sparkDecimal.scale()];
          long upperBound = POW_10_LONG[sparkDecimal.precision()];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = changeUnscaledLongPrecision(
              originalDictionary.decodeToLong(i), powDiff, upperBound, true);
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet int64 -> spark decimal(precision>18)
        if (DecimalType.isByteArrayDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          Binary[] arrayContent = new Binary[length];
          BigInteger powDiff = BigInteger.valueOf(10).pow(sparkDecimal.scale());
          for (int i = 0; i < length; i++) {
            BigInteger unscaled = changeUnscaledBigIntegerPrecision(
              BigInteger.valueOf(originalDictionary.decodeToLong(i)),
              powDiff, true, sparkDecimal);
            arrayContent[i] = Binary.fromConstantByteArray(unscaled.toByteArray());
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
        // parquet int64 -> spark string
        if (sparkType == DataTypes.StringType) {
          Binary[] arrayContent = new Binary[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = Binary.fromString(Long.toString(originalDictionary.decodeToLong(i)));
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
    }
    throw constructConvertNotSupportedException();
  }

  private Dictionary convertDictionaryFromInt96(Dictionary originalDictionary) {
    int length = originalDictionary.getMaxId() + 1;
    // Possible spark Type:
    // 1.DateType 2.TimestampType 3.StringType
    // parquet int96 -> spark timestamp
    if (sparkType == DataTypes.TimestampType) {
      long[] arrayContent = new long[length];
      for (int i = 0; i < length; i++) {
        arrayContent[i] = int96ToMicros(originalDictionary.decodeToBinary(i));
      }
      return new ExpandedLongDictionary(arrayContent);
    }
    // parquet int96 -> spark date
    if (sparkType == DataTypes.DateType) {
      int[] arrayContent = new int[length];
      for (int i = 0; i < length; i++) {
        long micros = int96ToMicros(originalDictionary.decodeToBinary(i));
        arrayContent[i] = DateTimeUtils.microsToDays(micros, convertTz);
      }
      return new ExpandedIntegerDictionary(arrayContent);
    }
    // parquet int96 -> spark string
    if (sparkType == DataTypes.StringType) {
      TimestampFormatter timestampFormatter = TimestampFormatter.getFractionFormatter(convertTz);
      Binary[] arrayContent = new Binary[length];
      for (int i = 0; i < length; i++) {
        long micros = int96ToMicros(originalDictionary.decodeToBinary(i));
        arrayContent[i] = Binary.fromString(timestampFormatter.format(micros));
      }
      return new ExpandedBinaryDictionary(arrayContent);
    }
    throw constructConvertNotSupportedException();
  }

  private Dictionary convertDictionaryFromBinary(Dictionary originalDictionary) {
    int length = originalDictionary.getMaxId() + 1;
    // Possible spark Type:
    // 1.BinaryType 2.FloatType 3.DoubleType 4.ShortType 5.IntegerType
    // 6.DateType 7.TimestampType 9.DecimalType 10.LongType 11.StringType

    // parquet binary -> spark binary
    if (sparkType == DataTypes.BinaryType) {
      return originalDictionary;
    }
    OriginalType originalType =
      parquetTypeOriginal == null ? OriginalType.UTF8 : parquetTypeOriginal;
    switch (originalType) {
      case UTF8:
      case ENUM:
        // parquet binary-utf8 -> spark string
        if (sparkType == DataTypes.StringType) {
          return originalDictionary;
        }
        // parquet binary-utf8 -> spark float
        if (sparkType == DataTypes.FloatType) {
          float[] arrayContent = new float[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = utf8BinaryToFloat(originalDictionary.decodeToBinary(i));
          }
          return new ExpandedFloatDictionary(arrayContent);
        }
        // parquet binary-utf8 -> spark double
        if (sparkType == DataTypes.DoubleType) {
          double[] arrayContent = new double[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = utf8BinaryToDouble(originalDictionary.decodeToBinary(i));
          }
          return new ExpandedDoubleDictionary(arrayContent);
        }
        // parquet binary-utf8 -> spark byte
        if (sparkType == DataTypes.ByteType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            UTF8String utf8 =
              ParquetRowConverter.utf8StringFromBinary(originalDictionary.decodeToBinary(i));
            arrayContent[i] = utf8.toByteExact();
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet binary-utf8 -> spark short
        if (sparkType == DataTypes.ShortType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            UTF8String utf8 =
              ParquetRowConverter.utf8StringFromBinary(originalDictionary.decodeToBinary(i));
            arrayContent[i] = utf8.toShortExact();
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet binary-utf8 -> spark int
        if (sparkType == DataTypes.IntegerType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            UTF8String utf8 =
              ParquetRowConverter.utf8StringFromBinary(originalDictionary.decodeToBinary(i));
            arrayContent[i] = utf8.toIntExact();
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet binary-utf8 -> spark long
        if (sparkType == DataTypes.LongType) {
          long[] arrayContent = new long[length];
          for (int i = 0; i < length; i++) {
            UTF8String utf8 =
              ParquetRowConverter.utf8StringFromBinary(originalDictionary.decodeToBinary(i));
            arrayContent[i] = utf8.toLongExact();
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet binary-utf8 -> spark decimal(precision<=9)
        if (DecimalType.is32BitDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            String string =
              ParquetRowConverter.binaryToJString(originalDictionary.decodeToBinary(i)).trim();
            BigInteger unscaled = bigDecimalToUnscaledValue(new BigDecimal(string), sparkDecimal);
            arrayContent[i] = unscaled.intValue();
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet binary-utf8 -> spark decimal(precision<=18)
        if (DecimalType.is64BitDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          long[] arrayContent = new long[length];
          for (int i = 0; i < length; i++) {
            String string =
              ParquetRowConverter.binaryToJString(originalDictionary.decodeToBinary(i)).trim();
            BigInteger unscaled = bigDecimalToUnscaledValue(new BigDecimal(string), sparkDecimal);
            arrayContent[i] = unscaled.longValue();
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet binary-utf8 -> spark decimal(precision>18)
        if (DecimalType.isByteArrayDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          Binary[] arrayContent = new Binary[length];
          for (int i = 0; i < length; i++ ) {
            String string =
              ParquetRowConverter.binaryToJString(originalDictionary.decodeToBinary(i)).trim();
            BigInteger unscaled = bigDecimalToUnscaledValue(new BigDecimal(string), sparkDecimal);
            arrayContent[i] = Binary.fromConstantByteArray(unscaled.toByteArray());
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
        // parquet binary-utf8 -> spark date
        if (sparkType == DataTypes.DateType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = utf8BinaryToDays(originalDictionary.decodeToBinary(i));
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet binary-utf8 -> spark timestamp
        if (sparkType == DataTypes.TimestampType) {
          long[] arrayContent = new long[length];
          for (int i = 0; i < length; i++) {
            arrayContent[i] = utf8BinaryToMicros(originalDictionary.decodeToBinary(i));
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        break;
      case JSON:
        // parquet binary-utf8(json) -> spark string
        if (sparkType == DataTypes.StringType) {
          return originalDictionary;
        }
        break;
      case DECIMAL:
        // parquet binary-decimal -> spark decimal(precision match)
        if (DecimalType.isByteArrayDecimalType(sparkType) &&
          isParquetSparkDecimalMatch((DecimalType) sparkType)) {
          return originalDictionary;
        }
        // parquet binary-decimal -> spark float
        int parquetScale = parquetDecimal.getScale();
        if (sparkType == DataTypes.FloatType) {
          float[] arrayContent = new float[length];
          for (int i = 0; i < length; i++) {
            BigDecimal decimal =
              new BigDecimal(
                new BigInteger(originalDictionary.decodeToBinary(i).getBytes()), parquetScale);
            float value = decimal.floatValue();
            if (parquetScale > 38) {
              checkFloatOverflow(value);
            }
            arrayContent[i] = value;
          }
          return new ExpandedFloatDictionary(arrayContent);
        }
        // parquet binary-decimal -> spark double
        if (sparkType == DataTypes.DoubleType) {
          double[] arrayContent = new double[length];
          for (int i = 0; i < length; i++) {
            BigDecimal decimal =
              new BigDecimal(
                new BigInteger(originalDictionary.decodeToBinary(i).getBytes()), parquetScale);
            double value = decimal.doubleValue();
            if (parquetScale > 308) {
              checkDoubleOverflow(value);
            }
            arrayContent[i] = value;
          }
          return new ExpandedDoubleDictionary(arrayContent);
        }
        // parquet binary-decimal -> spark byte
        BigInteger powDiffScale = BigInteger.valueOf(10).pow(parquetScale);
        if (sparkType == DataTypes.ByteType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            BigInteger value = new BigInteger(originalDictionary.decodeToBinary(i).getBytes());
            arrayContent[i] = value.divide(powDiffScale).byteValueExact();
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet binary-decimal -> spark short
        if (sparkType == DataTypes.ShortType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            BigInteger value = new BigInteger(originalDictionary.decodeToBinary(i).getBytes());
            arrayContent[i] = value.divide(powDiffScale).shortValueExact();
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet binary-decimal -> spark int
        if (sparkType == DataTypes.IntegerType) {
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            BigInteger value = new BigInteger(originalDictionary.decodeToBinary(i).getBytes());
            arrayContent[i] = value.divide(powDiffScale).intValueExact();
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet binary-decimal -> spark long
        if (sparkType == DataTypes.LongType) {
          long[] arrayContent = new long[length];
          for (int i = 0; i < length; i++) {
            BigInteger value = new BigInteger(originalDictionary.decodeToBinary(i).getBytes());
            arrayContent[i] = value.divide(powDiffScale).longValueExact();
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet binary-decimal -> spark decimal(precision<=9)
        if (DecimalType.is32BitDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          BigInteger powDiff =
            BigInteger.valueOf(10).pow(Math.abs(sparkDecimal.scale() - parquetScale));
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          int[] arrayContent = new int[length];
          for (int i = 0; i < length; i++) {
            BigInteger value = new BigInteger(originalDictionary.decodeToBinary(i).getBytes());
            BigInteger unscaled = changeUnscaledBigIntegerPrecision(
              value, powDiff, sparkScaleBigger, sparkDecimal);
            arrayContent[i] = unscaled.intValue();
          }
          return new ExpandedIntegerDictionary(arrayContent);
        }
        // parquet binary-decimal -> spark decimal(precision<=18)
        if (DecimalType.is64BitDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          BigInteger powDiff =
            BigInteger.valueOf(10).pow(Math.abs(sparkDecimal.scale() - parquetScale));
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          long[] arrayContent = new long[length];
          for (int i = 0; i < length; i++) {
            BigInteger value = new BigInteger(originalDictionary.decodeToBinary(i).getBytes());
            BigInteger unscaled = changeUnscaledBigIntegerPrecision(
              value, powDiff, sparkScaleBigger, sparkDecimal);
            arrayContent[i] = unscaled.longValue();
          }
          return new ExpandedLongDictionary(arrayContent);
        }
        // parquet binary-decimal -> spark decimal(precision>18)
        if (DecimalType.isByteArrayDecimalType(sparkType)) {
          DecimalType sparkDecimal = (DecimalType) sparkType;
          BigInteger powDiff =
            BigInteger.valueOf(10).pow(Math.abs(sparkDecimal.scale() - parquetScale));
          boolean sparkScaleBigger = sparkDecimal.scale() > parquetScale;
          Binary[] arrayContent = new Binary[length];
          for (int i = 0; i < length; i++) {
            BigInteger value = new BigInteger(originalDictionary.decodeToBinary(i).getBytes());
            BigInteger unscaled = changeUnscaledBigIntegerPrecision(
              value, powDiff, sparkScaleBigger, sparkDecimal);
            arrayContent[i] = Binary.fromConstantByteArray(unscaled.toByteArray());
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
        // parquet binary-decimal -> spark string
        if (sparkType == DataTypes.StringType) {
          Binary[] arrayContent = new Binary[length];
          for (int i = 0; i < length; i++) {
            BigDecimal decimal =
              new BigDecimal(
                new BigInteger(originalDictionary.decodeToBinary(i).getBytes()), parquetScale);
            arrayContent[i] = Binary.fromString(decimal.toString());
          }
          return new ExpandedBinaryDictionary(arrayContent);
        }
        break;
    }
    throw constructConvertNotSupportedException();
  }

  private BigInteger bigDecimalToUnscaledValue(BigDecimal value, DecimalType sparkDecimal) {
    BigDecimal decimal = value.setScale(sparkDecimal.scale(), RoundingMode.DOWN);
    if (decimal.precision() > sparkDecimal.precision()) {
      throw new ArithmeticException(
        "Decimal "+ value +" cannot be represented as " + sparkType.simpleString());
    }
    return decimal.unscaledValue();
  }

  private long int96ToMicros(Binary x) {
    long rawTime = ParquetRowConverter.binaryToSQLTimestamp(x);
    if (convertInt96Timestamp) {
      rawTime = DateTimeUtils.convertTz(rawTime, convertTz, UTC);
    }
    return rawTime;
  }

  private float utf8BinaryToFloat(Binary x) {
    float value;
    String string = ParquetRowConverter.binaryToJString(x).trim();
    // Infinity, -Infinity, NaN
    if (string.length() < 2 || string.charAt(1) <= '9') {
      value = Float.parseFloat(string);
      if (!isFiniteFloat(value)) {
        throw new ArithmeticException(
          "Casting " + string + " to float causes overflow, " + sparkType.simpleString());
      }
    } else {
      Float v = (Float) Cast.processFloatingPointSpecialLiterals(string, true);
      if (v == null) {
        throw new NumberFormatException(
          "This String " + string + " cannot be converted to float, " + sparkType.simpleString());
      }
      value = v;
    }
    return value;
  }

  private double utf8BinaryToDouble(Binary x) {
    double value;
    String string = ParquetRowConverter.binaryToJString(x).trim();
    // Infinity, -Infinity, NaN
    if (string.length() < 2 || string.charAt(1) <= '9') {
      value = Double.parseDouble(string);
      if (!isFiniteDouble(value)) {
        throw new ArithmeticException(
          "Casting " + string + " to double causes overflow, " + sparkType.simpleString());
      }
    } else {
      Double v = (Double) Cast.processFloatingPointSpecialLiterals(string, false);
      if (v == null) {
        throw new NumberFormatException(
          "This String " + string + " cannot be converted to double, " + sparkType.simpleString());
      }
      value = v;
    }
    return value;
  }

  private int utf8BinaryToDays(Binary x) {
    UTF8String utf8 = ParquetRowConverter.utf8StringFromBinary(x);
    Option<Object> days = DateTimeUtils.stringToDate(utf8, convertTz);
    if (days.isEmpty()) {
      throw new IllegalArgumentException("This String " + utf8.toString() +
        " cannot be converted to date, " + sparkType.simpleString());
    }
    return (Integer) days.get();
  }

  private long utf8BinaryToMicros(Binary x) {
    UTF8String utf8 = ParquetRowConverter.utf8StringFromBinary(x);
    Option<Object> micros = DateTimeUtils.stringToTimestamp(utf8, convertTz);
    if (micros.isEmpty()) {
      throw new IllegalArgumentException(
        "This String " + utf8.toString() +
          " cannot be converted to Timestamp, " + sparkType.simpleString());
    }
    return (Long) micros.get();
  }

  private int changeUnscaledIntPrecision(
    int x, int powDiff, int upperBound, boolean sparkScaleBigger) {
    int unscaled = sparkScaleBigger ? Math.multiplyExact(x, powDiff) : x / powDiff;
    if (unscaled >= upperBound || unscaled <= -upperBound) {
      throw new ArithmeticException("Casting Decimal causes overflow, " + sparkType.simpleString());
    }
    return unscaled;
  }

  private long changeUnscaledLongPrecision(
    long x, long powDiff, long upperBound, boolean sparkScaleBigger) {
    long unscaled = sparkScaleBigger ? Math.multiplyExact(x, powDiff) : x / powDiff;
    if (unscaled >= upperBound || unscaled <= -upperBound) {
      throw new ArithmeticException("Casting Decimal causes overflow, " + sparkType.simpleString());
    }
    return unscaled;
  }

  private BigInteger changeUnscaledBigIntegerPrecision(
    BigInteger x, BigInteger powDiff, boolean sparkScaleBigger, DecimalType decimalType) {
    BigInteger unscaled = sparkScaleBigger ? x.multiply(powDiff) : x.divide(powDiff);
    if (new BigDecimal(unscaled).precision() > decimalType.precision()) {
      throw new ArithmeticException("Casting Decimal causes overflow, " + sparkType.simpleString());
    }
    return unscaled;
  }

  private boolean isFiniteFloat(float x) {
    return Float.NEGATIVE_INFINITY < x && x < Float.POSITIVE_INFINITY;
  }

  private boolean isFiniteDouble(double x) {
    return Double.NEGATIVE_INFINITY < x && x < Double.POSITIVE_INFINITY;
  }

  private boolean isParquetSparkDecimalMatch(DecimalType decimalType) {
    return decimalType.precision() == parquetDecimal.getPrecision() &&
      decimalType.scale() == parquetDecimal.getScale();
  }

  private void checkFloatOverflow(float x) {
    if (!isFiniteFloat(x)) {
      throw new ArithmeticException(
        "Casting float " + x + " causes overflow, " + sparkType.simpleString());
    }
  }

  private void checkDoubleOverflow(double x) {
    if (!isFiniteDouble(x)) {
      throw new ArithmeticException(
        "Casting double " + x + " causes overflow, " + sparkType.simpleString());
    }
  }

  private void checkDoubleToIntegralOverflow(double x, long upperBound, long lowerBound) {
    if (Math.floor(x) > upperBound || Math.ceil(x) < lowerBound) {
      throw new ArithmeticException(
        "Casting fractional " + x + " to spark Integral causes overflow, "
          + sparkType.simpleString());
    }
  }

  private void checkDoubleToFloatOverflow(double x) {
    if ((x < Float.MIN_VALUE || x > Float.MAX_VALUE) && isFiniteDouble(x)) {
      throw new ArithmeticException(
        "Casting double " + x + " to float causes overflow, " + sparkType.simpleString());
    }
  }

  private void checkIntToByteOverflow(int x) {
    if (x != (byte) x) {
      throw new ArithmeticException(
        "Casting int " + x + " to byte causes overflow, " + sparkType.simpleString());
    }
  }

  private void checkIntToShortOverflow(int x) {
    if (x != (short) x) {
      throw new ArithmeticException(
        "Casting int " + x + " to short causes overflow, " + sparkType.simpleString());
    }
  }

  private void checkLongToIntOverflow(long x) {
    if (x != (int) x) {
      throw new ArithmeticException(
        "Casting long " + x + " to int causes overflow, " + sparkType.simpleString());
    }
  }

  /**
   * Helper function to construct exception for parquet schema mismatch.
   */
  private SchemaColumnConvertNotSupportedException constructConvertNotSupportedException() {
    return new SchemaColumnConvertNotSupportedException(
      Arrays.toString(parquetDescriptor.getPath()),
      parquetType.toString(),
      sparkType.catalogString());
  }

  private static class ExpandedIntegerDictionary extends Dictionary {
    private final int[] intDictionaryContent;

    ExpandedIntegerDictionary(int[] arrayContent) {
      super(Encoding.PLAIN);
      this.intDictionaryContent = arrayContent;
    }

    @Override
    public int decodeToInt(int id) {
      return intDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("ExpandedIntegerDictionary {\n");
      for (int i = 0; i < intDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(intDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return intDictionaryContent.length - 1;
    }
  }

  private static class ExpandedFloatDictionary extends Dictionary {
    private final float[] floatDictionaryContent;

    ExpandedFloatDictionary(float[] arrayContent) {
      super(Encoding.PLAIN);
      this.floatDictionaryContent = arrayContent;
    }

    @Override
    public float decodeToFloat(int id) {
      return floatDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("ExpandedFloatDictionary {\n");
      for (int i = 0; i < floatDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(floatDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return floatDictionaryContent.length - 1;
    }
  }

  private static class ExpandedDoubleDictionary extends Dictionary {
    private final double[] doubleDictionaryContent;

    ExpandedDoubleDictionary(double[] arrayContent) {
      super(Encoding.PLAIN);
      this.doubleDictionaryContent = arrayContent;
    }

    @Override
    public double decodeToDouble(int id) {
      return doubleDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("ExpandedDoubleDictionary {\n");
      for (int i = 0; i < doubleDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(doubleDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return doubleDictionaryContent.length - 1;
    }
  }

  private static class ExpandedLongDictionary extends Dictionary {
    private final long[] longDictionaryContent;

    ExpandedLongDictionary(long[] arrayContent) {
      super(Encoding.PLAIN);
      this.longDictionaryContent = arrayContent;
    }

    @Override
    public long decodeToLong(int id) {
      return longDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("ExpandedLongDictionary {\n");
      for (int i = 0; i < longDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(longDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return longDictionaryContent.length - 1;
    }
  }

  private static class ExpandedBinaryDictionary extends Dictionary {
    private final Binary[] binaryDictionaryContent;

    ExpandedBinaryDictionary(Binary[] arrayContent) {
      super(Encoding.PLAIN);
      this.binaryDictionaryContent = arrayContent;
    }
    @Override
    public Binary decodeToBinary(int id) {
      return binaryDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("ExpandedBinaryDictionary {\n");
      for (int i = 0; i < binaryDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(binaryDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return binaryDictionaryContent.length - 1;
    }
  }
}
