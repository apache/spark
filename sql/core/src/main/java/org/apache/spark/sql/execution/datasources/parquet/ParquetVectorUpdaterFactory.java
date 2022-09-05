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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.*;

import java.math.BigInteger;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;

public class ParquetVectorUpdaterFactory {
  private static final ZoneId UTC = ZoneOffset.UTC;

  private final LogicalTypeAnnotation logicalTypeAnnotation;
  // The timezone conversion to apply to int96 timestamps. Null if no conversion.
  private final ZoneId convertTz;
  private final String datetimeRebaseMode;
  private final String datetimeRebaseTz;
  private final String int96RebaseMode;
  private final String int96RebaseTz;

  ParquetVectorUpdaterFactory(
      LogicalTypeAnnotation logicalTypeAnnotation,
      ZoneId convertTz,
      String datetimeRebaseMode,
      String datetimeRebaseTz,
      String int96RebaseMode,
      String int96RebaseTz) {
    this.logicalTypeAnnotation = logicalTypeAnnotation;
    this.convertTz = convertTz;
    this.datetimeRebaseMode = datetimeRebaseMode;
    this.datetimeRebaseTz = datetimeRebaseTz;
    this.int96RebaseMode = int96RebaseMode;
    this.int96RebaseTz = int96RebaseTz;
  }

  public ParquetVectorUpdater getUpdater(ColumnDescriptor descriptor, DataType sparkType) {
    PrimitiveType.PrimitiveTypeName typeName = descriptor.getPrimitiveType().getPrimitiveTypeName();

    switch (typeName) {
      case BOOLEAN:
        if (sparkType == DataTypes.BooleanType) {
          return new BooleanUpdater();
        }
        break;
      case INT32:
        if (sparkType == DataTypes.IntegerType || canReadAsIntDecimal(descriptor, sparkType)) {
          return new IntegerUpdater();
        } else if (sparkType == DataTypes.LongType && isUnsignedIntTypeMatched(32)) {
          // In `ParquetToSparkSchemaConverter`, we map parquet UINT32 to our LongType.
          // For unsigned int32, it stores as plain signed int32 in Parquet when dictionary
          // fallbacks. We read them as long values.
          return new UnsignedIntegerUpdater();
        } else if (sparkType == DataTypes.ByteType) {
          return new ByteUpdater();
        } else if (sparkType == DataTypes.ShortType) {
          return new ShortUpdater();
        } else if (sparkType == DataTypes.DateType) {
          if ("CORRECTED".equals(datetimeRebaseMode)) {
            return new IntegerUpdater();
          } else {
            boolean failIfRebase = "EXCEPTION".equals(datetimeRebaseMode);
            return new IntegerWithRebaseUpdater(failIfRebase);
          }
        } else if (sparkType instanceof YearMonthIntervalType) {
          return new IntegerUpdater();
        }
        break;
      case INT64:
        // This is where we implement support for the valid type conversions.
        if (sparkType == DataTypes.LongType || canReadAsLongDecimal(descriptor, sparkType)) {
          if (DecimalType.is32BitDecimalType(sparkType)) {
            return new DowncastLongUpdater();
          } else {
            return new LongUpdater();
          }
        } else if (isLongDecimal(sparkType) && isUnsignedIntTypeMatched(64)) {
          // In `ParquetToSparkSchemaConverter`, we map parquet UINT64 to our Decimal(20, 0).
          // For unsigned int64, it stores as plain signed int64 in Parquet when dictionary
          // fallbacks. We read them as decimal values.
          return new UnsignedLongUpdater();
        } else if (isTimestampTypeMatched(LogicalTypeAnnotation.TimeUnit.MICROS)) {
          validateTimestampType(sparkType);
          if ("CORRECTED".equals(datetimeRebaseMode)) {
            return new LongUpdater();
          } else {
            boolean failIfRebase = "EXCEPTION".equals(datetimeRebaseMode);
            return new LongWithRebaseUpdater(failIfRebase, datetimeRebaseTz);
          }
        } else if (isTimestampTypeMatched(LogicalTypeAnnotation.TimeUnit.MILLIS)) {
          validateTimestampType(sparkType);
          if ("CORRECTED".equals(datetimeRebaseMode)) {
            return new LongAsMicrosUpdater();
          } else {
            final boolean failIfRebase = "EXCEPTION".equals(datetimeRebaseMode);
            return new LongAsMicrosRebaseUpdater(failIfRebase, datetimeRebaseTz);
          }
        } else if (sparkType instanceof DayTimeIntervalType) {
          return new LongUpdater();
        }
        break;
      case FLOAT:
        if (sparkType == DataTypes.FloatType) {
          return new FloatUpdater();
        }
        break;
      case DOUBLE:
        if (sparkType == DataTypes.DoubleType) {
          return new DoubleUpdater();
        }
        break;
      case INT96:
        if (sparkType == DataTypes.TimestampNTZType) {
          convertErrorForTimestampNTZ(typeName.name());
        } else if (sparkType == DataTypes.TimestampType) {
          final boolean failIfRebase = "EXCEPTION".equals(int96RebaseMode);
          if (!shouldConvertTimestamps()) {
            if ("CORRECTED".equals(int96RebaseMode)) {
              return new BinaryToSQLTimestampUpdater();
            } else {
              return new BinaryToSQLTimestampRebaseUpdater(failIfRebase, int96RebaseTz);
            }
          } else {
            if ("CORRECTED".equals(int96RebaseMode)) {
              return new BinaryToSQLTimestampConvertTzUpdater(convertTz);
            } else {
              return new BinaryToSQLTimestampConvertTzRebaseUpdater(
                failIfRebase,
                convertTz,
                int96RebaseTz);
            }
          }
        }
        break;
      case BINARY:
        if (sparkType == DataTypes.StringType || sparkType == DataTypes.BinaryType ||
          canReadAsBinaryDecimal(descriptor, sparkType)) {
          return new BinaryUpdater();
        }
        break;
      case FIXED_LEN_BYTE_ARRAY:
        int arrayLen = descriptor.getPrimitiveType().getTypeLength();
        if (canReadAsIntDecimal(descriptor, sparkType)) {
          return new FixedLenByteArrayAsIntUpdater(arrayLen);
        } else if (canReadAsLongDecimal(descriptor, sparkType)) {
          return new FixedLenByteArrayAsLongUpdater(arrayLen);
        } else if (canReadAsBinaryDecimal(descriptor, sparkType)) {
          return new FixedLenByteArrayUpdater(arrayLen);
        }
        break;
      default:
        break;
    }

    // If we get here, it means the combination of Spark and Parquet type is invalid or not
    // supported.
    throw constructConvertNotSupportedException(descriptor, sparkType);
  }

  boolean isTimestampTypeMatched(LogicalTypeAnnotation.TimeUnit unit) {
    return logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation &&
      ((TimestampLogicalTypeAnnotation) logicalTypeAnnotation).getUnit() == unit;
  }

  void validateTimestampType(DataType sparkType) {
    assert(logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation);
    // Throw an exception if the Parquet type is TimestampLTZ and the Catalyst type is TimestampNTZ.
    // This is to avoid mistakes in reading the timestamp values.
    if (((TimestampLogicalTypeAnnotation) logicalTypeAnnotation).isAdjustedToUTC() &&
      sparkType == DataTypes.TimestampNTZType) {
      convertErrorForTimestampNTZ("int64 time(" + logicalTypeAnnotation + ")");
    }
  }

  void convertErrorForTimestampNTZ(String parquetType) {
    throw new RuntimeException("Unable to create Parquet converter for data type " +
      DataTypes.TimestampNTZType.json() + " whose Parquet type is " + parquetType);
  }

  boolean isUnsignedIntTypeMatched(int bitWidth) {
    return logicalTypeAnnotation instanceof IntLogicalTypeAnnotation &&
      !((IntLogicalTypeAnnotation) logicalTypeAnnotation).isSigned() &&
      ((IntLogicalTypeAnnotation) logicalTypeAnnotation).getBitWidth() == bitWidth;
  }

  private static class BooleanUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readBooleans(total, values, offset);
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipBooleans(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      values.putBoolean(offset, valuesReader.readBoolean());
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      throw new UnsupportedOperationException("Boolean is not supported");
    }
  }

  static class IntegerUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readIntegers(total, values, offset);
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipIntegers(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      values.putInt(offset, valuesReader.readInteger());
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      values.putInt(offset, dictionary.decodeToInt(dictionaryIds.getDictId(offset)));
    }
  }

  private static class UnsignedIntegerUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readUnsignedIntegers(total, values, offset);
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipIntegers(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      values.putLong(offset, Integer.toUnsignedLong(valuesReader.readInteger()));
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      values.putLong(offset, Integer.toUnsignedLong(
          dictionary.decodeToInt(dictionaryIds.getDictId(offset))));
    }
  }

  private static class ByteUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readBytes(total, values, offset);
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipBytes(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      values.putByte(offset, valuesReader.readByte());
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      values.putByte(offset, (byte) dictionary.decodeToInt(dictionaryIds.getDictId(offset)));
    }
  }

  private static class ShortUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readShorts(total, values, offset);
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipShorts(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      values.putShort(offset, valuesReader.readShort());
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      values.putShort(offset, (short) dictionary.decodeToInt(dictionaryIds.getDictId(offset)));
    }
  }

  private static class IntegerWithRebaseUpdater implements ParquetVectorUpdater {
    private final boolean failIfRebase;

    IntegerWithRebaseUpdater(boolean failIfRebase) {
      this.failIfRebase = failIfRebase;
    }

    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readIntegersWithRebase(total, values, offset, failIfRebase);
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipIntegers(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      int julianDays = valuesReader.readInteger();
      values.putInt(offset, rebaseDays(julianDays, failIfRebase));
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      int julianDays = dictionary.decodeToInt(dictionaryIds.getDictId(offset));
      values.putInt(offset, rebaseDays(julianDays, failIfRebase));
    }
  }

  private static class LongUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readLongs(total, values, offset);
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipLongs(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      values.putLong(offset, valuesReader.readLong());
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      values.putLong(offset, dictionary.decodeToLong(dictionaryIds.getDictId(offset)));
    }
  }

  private static class DowncastLongUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      for (int i = 0; i < total; ++i) {
        values.putInt(offset + i, (int) valuesReader.readLong());
      }
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipLongs(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      values.putInt(offset, (int) valuesReader.readLong());
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      values.putLong(offset, dictionary.decodeToLong(dictionaryIds.getDictId(offset)));
    }
  }

  private static class UnsignedLongUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readUnsignedLongs(total, values, offset);
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipLongs(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      byte[] bytes = new BigInteger(Long.toUnsignedString(valuesReader.readLong())).toByteArray();
      values.putByteArray(offset, bytes);
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      long signed = dictionary.decodeToLong(dictionaryIds.getDictId(offset));
      byte[] unsigned = new BigInteger(Long.toUnsignedString(signed)).toByteArray();
      values.putByteArray(offset, unsigned);
    }
  }

  private static class LongWithRebaseUpdater implements ParquetVectorUpdater {
    private final boolean failIfRebase;
    private final String timeZone;

    LongWithRebaseUpdater(boolean failIfRebase, String timeZone) {
      this.failIfRebase = failIfRebase;
      this.timeZone = timeZone;
    }

    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readLongsWithRebase(total, values, offset, failIfRebase, timeZone);
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipLongs(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      long julianMicros = valuesReader.readLong();
      values.putLong(offset, rebaseMicros(julianMicros, failIfRebase, timeZone));
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      long julianMicros = dictionary.decodeToLong(dictionaryIds.getDictId(offset));
      values.putLong(offset, rebaseMicros(julianMicros, failIfRebase, timeZone));
    }
  }

  private static class LongAsMicrosUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      for (int i = 0; i < total; ++i) {
        readValue(offset + i, values, valuesReader);
      }
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipLongs(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      values.putLong(offset, DateTimeUtils.millisToMicros(valuesReader.readLong()));
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      long gregorianMillis = dictionary.decodeToLong(dictionaryIds.getDictId(offset));
      values.putLong(offset, DateTimeUtils.millisToMicros(gregorianMillis));
    }
  }

  private static class LongAsMicrosRebaseUpdater implements ParquetVectorUpdater {
    private final boolean failIfRebase;
    private final String timeZone;

    LongAsMicrosRebaseUpdater(boolean failIfRebase, String timeZone) {
      this.failIfRebase = failIfRebase;
      this.timeZone = timeZone;
    }

    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      for (int i = 0; i < total; ++i) {
        readValue(offset + i, values, valuesReader);
      }
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipLongs(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      long julianMicros = DateTimeUtils.millisToMicros(valuesReader.readLong());
      values.putLong(offset, rebaseMicros(julianMicros, failIfRebase, timeZone));
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      long julianMillis = dictionary.decodeToLong(dictionaryIds.getDictId(offset));
      long julianMicros = DateTimeUtils.millisToMicros(julianMillis);
      values.putLong(offset, rebaseMicros(julianMicros, failIfRebase, timeZone));
    }
  }

  private static class FloatUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readFloats(total, values, offset);
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipFloats(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      values.putFloat(offset, valuesReader.readFloat());
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      values.putFloat(offset, dictionary.decodeToFloat(dictionaryIds.getDictId(offset)));
    }
  }

  private static class DoubleUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readDoubles(total, values, offset);
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipDoubles(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      values.putDouble(offset, valuesReader.readDouble());
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      values.putDouble(offset, dictionary.decodeToDouble(dictionaryIds.getDictId(offset)));
    }
  }

  private static class BinaryUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readBinary(total, values, offset);
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipBinary(total);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      valuesReader.readBinary(1, values, offset);
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(offset));
      values.putByteArray(offset, v.getBytes());
    }
  }

  private static class BinaryToSQLTimestampUpdater implements ParquetVectorUpdater {
    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      for (int i = 0; i < total; i++) {
        readValue(offset + i, values, valuesReader);
      }
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipFixedLenByteArray(total, 12);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      // Read 12 bytes for INT96
      long gregorianMicros = ParquetRowConverter.binaryToSQLTimestamp(valuesReader.readBinary(12));
      values.putLong(offset, gregorianMicros);
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(offset));
      values.putLong(offset, ParquetRowConverter.binaryToSQLTimestamp(v));
    }
  }

  private static class BinaryToSQLTimestampConvertTzUpdater implements ParquetVectorUpdater {
    private final ZoneId convertTz;

    BinaryToSQLTimestampConvertTzUpdater(ZoneId convertTz) {
      this.convertTz = convertTz;
    }

    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      for (int i = 0; i < total; i++) {
        readValue(offset + i, values, valuesReader);
      }
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipFixedLenByteArray(total, 12);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      // Read 12 bytes for INT96
      long gregorianMicros = ParquetRowConverter.binaryToSQLTimestamp(valuesReader.readBinary(12));
      long adjTime = DateTimeUtils.convertTz(gregorianMicros, convertTz, UTC);
      values.putLong(offset, adjTime);
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(offset));
      long gregorianMicros = ParquetRowConverter.binaryToSQLTimestamp(v);
      long adjTime = DateTimeUtils.convertTz(gregorianMicros, convertTz, UTC);
      values.putLong(offset, adjTime);
    }
  }

  private static class BinaryToSQLTimestampRebaseUpdater implements ParquetVectorUpdater {
    private final boolean failIfRebase;
    private final String timeZone;

    BinaryToSQLTimestampRebaseUpdater(boolean failIfRebase, String timeZone) {
      this.failIfRebase = failIfRebase;
      this.timeZone = timeZone;
    }

    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      for (int i = 0; i < total; i++) {
        readValue(offset + i, values, valuesReader);
      }
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipFixedLenByteArray(total, 12);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      // Read 12 bytes for INT96
      long julianMicros = ParquetRowConverter.binaryToSQLTimestamp(valuesReader.readBinary(12));
      long gregorianMicros = rebaseInt96(julianMicros, failIfRebase, timeZone);
      values.putLong(offset, gregorianMicros);
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(offset));
      long julianMicros = ParquetRowConverter.binaryToSQLTimestamp(v);
      long gregorianMicros = rebaseInt96(julianMicros, failIfRebase, timeZone);
      values.putLong(offset, gregorianMicros);
    }
  }

  private static class BinaryToSQLTimestampConvertTzRebaseUpdater implements ParquetVectorUpdater {
    private final boolean failIfRebase;
    private final ZoneId convertTz;
    private final String timeZone;

    BinaryToSQLTimestampConvertTzRebaseUpdater(
        boolean failIfRebase,
        ZoneId convertTz,
        String timeZone) {
      this.failIfRebase = failIfRebase;
      this.convertTz = convertTz;
      this.timeZone = timeZone;
    }

    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      for (int i = 0; i < total; i++) {
        readValue(offset + i, values, valuesReader);
      }
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipFixedLenByteArray(total, 12);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      // Read 12 bytes for INT96
      long julianMicros = ParquetRowConverter.binaryToSQLTimestamp(valuesReader.readBinary(12));
      long gregorianMicros = rebaseInt96(julianMicros, failIfRebase, timeZone);
      long adjTime = DateTimeUtils.convertTz(gregorianMicros, convertTz, UTC);
      values.putLong(offset, adjTime);
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(offset));
      long julianMicros = ParquetRowConverter.binaryToSQLTimestamp(v);
      long gregorianMicros = rebaseInt96(julianMicros, failIfRebase, timeZone);
      long adjTime = DateTimeUtils.convertTz(gregorianMicros, convertTz, UTC);
      values.putLong(offset, adjTime);
    }
  }

  private static class FixedLenByteArrayUpdater implements ParquetVectorUpdater {
    private final int arrayLen;

    FixedLenByteArrayUpdater(int arrayLen) {
      this.arrayLen = arrayLen;
    }

    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      for (int i = 0; i < total; i++) {
        readValue(offset + i, values, valuesReader);
      }
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipFixedLenByteArray(total, arrayLen);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      values.putByteArray(offset, valuesReader.readBinary(arrayLen).getBytes());
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(offset));
      values.putByteArray(offset, v.getBytes());
    }
  }

  private static class FixedLenByteArrayAsIntUpdater implements ParquetVectorUpdater {
    private final int arrayLen;

    FixedLenByteArrayAsIntUpdater(int arrayLen) {
      this.arrayLen = arrayLen;
    }

    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      for (int i = 0; i < total; i++) {
        readValue(offset + i, values, valuesReader);
      }
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipFixedLenByteArray(total, arrayLen);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      int value = (int) ParquetRowConverter.binaryToUnscaledLong(valuesReader.readBinary(arrayLen));
      values.putInt(offset, value);
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(offset));
      values.putInt(offset, (int) ParquetRowConverter.binaryToUnscaledLong(v));
    }
  }

  private static class FixedLenByteArrayAsLongUpdater implements ParquetVectorUpdater {
    private final int arrayLen;

    FixedLenByteArrayAsLongUpdater(int arrayLen) {
      this.arrayLen = arrayLen;
    }

    @Override
    public void readValues(
        int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      for (int i = 0; i < total; i++) {
        readValue(offset + i, values, valuesReader);
      }
    }

    @Override
    public void skipValues(int total, VectorizedValuesReader valuesReader) {
      valuesReader.skipFixedLenByteArray(total, arrayLen);
    }

    @Override
    public void readValue(
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader) {
      long value = ParquetRowConverter.binaryToUnscaledLong(valuesReader.readBinary(arrayLen));
      values.putLong(offset, value);
    }

    @Override
    public void decodeSingleDictionaryId(
        int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary) {
      Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(offset));
      values.putLong(offset, ParquetRowConverter.binaryToUnscaledLong(v));
    }
  }

  private static int rebaseDays(int julianDays, final boolean failIfRebase) {
    if (failIfRebase) {
      if (julianDays < RebaseDateTime.lastSwitchJulianDay()) {
        throw DataSourceUtils.newRebaseExceptionInRead("Parquet");
      } else {
        return julianDays;
      }
    } else {
      return RebaseDateTime.rebaseJulianToGregorianDays(julianDays);
    }
  }

  private static long rebaseTimestamp(
      long julianMicros,
      final boolean failIfRebase,
      final String format,
      final String timeZone) {
    if (failIfRebase) {
      if (julianMicros < RebaseDateTime.lastSwitchJulianTs()) {
        throw DataSourceUtils.newRebaseExceptionInRead(format);
      } else {
        return julianMicros;
      }
    } else {
      return RebaseDateTime.rebaseJulianToGregorianMicros(timeZone, julianMicros);
    }
  }

  private static long rebaseMicros(
      long julianMicros,
      final boolean failIfRebase,
      final String timeZone) {
    return rebaseTimestamp(julianMicros, failIfRebase, "Parquet", timeZone);
  }

  private static long rebaseInt96(
      long julianMicros,
      final boolean failIfRebase,
      final String timeZone) {
    return rebaseTimestamp(julianMicros, failIfRebase, "Parquet INT96", timeZone);
  }

  private boolean shouldConvertTimestamps() {
    return convertTz != null && !convertTz.equals(UTC);
  }

  /**
   * Helper function to construct exception for parquet schema mismatch.
   */
  private SchemaColumnConvertNotSupportedException constructConvertNotSupportedException(
      ColumnDescriptor descriptor,
      DataType sparkType) {
    return new SchemaColumnConvertNotSupportedException(
        Arrays.toString(descriptor.getPath()),
        descriptor.getPrimitiveType().getPrimitiveTypeName().toString(),
        sparkType.catalogString());
  }

  private static boolean canReadAsIntDecimal(ColumnDescriptor descriptor, DataType dt) {
    if (!DecimalType.is32BitDecimalType(dt)) return false;
    return isDecimalTypeMatched(descriptor, dt);
  }

  private static boolean canReadAsLongDecimal(ColumnDescriptor descriptor, DataType dt) {
    if (!DecimalType.is64BitDecimalType(dt)) return false;
    return isDecimalTypeMatched(descriptor, dt);
  }

  private static boolean canReadAsBinaryDecimal(ColumnDescriptor descriptor, DataType dt) {
    if (!DecimalType.isByteArrayDecimalType(dt)) return false;
    return isDecimalTypeMatched(descriptor, dt);
  }

  private static boolean isLongDecimal(DataType dt) {
    if (dt instanceof DecimalType) {
      DecimalType d = (DecimalType) dt;
      return d.precision() == 20 && d.scale() == 0;
    }
    return false;
  }

  private static boolean isDecimalTypeMatched(ColumnDescriptor descriptor, DataType dt) {
    DecimalType d = (DecimalType) dt;
    LogicalTypeAnnotation typeAnnotation = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
    if (typeAnnotation instanceof DecimalLogicalTypeAnnotation) {
      DecimalLogicalTypeAnnotation decimalType = (DecimalLogicalTypeAnnotation) typeAnnotation;
      // It's OK if the required decimal precision is larger than or equal to the physical decimal
      // precision in the Parquet metadata, as long as the decimal scale is the same.
      return decimalType.getPrecision() <= d.precision() && decimalType.getScale() == d.scale();
    }
    return false;
  }
}

