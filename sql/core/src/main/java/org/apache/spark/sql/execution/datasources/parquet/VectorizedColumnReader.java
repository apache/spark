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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.spark.sql.execution.datasources.parquet.SpecificParquetRecordReaderBase.ValuesReaderIntIterator;
import static org.apache.spark.sql.execution.datasources.parquet.SpecificParquetRecordReaderBase.createRLEIterator;

/**
 * Decoder to return values from a single column.
 */
public class VectorizedColumnReader {
  /**
   * Total number of values read.
   */
  private long valuesRead;

  /**
   * value that indicates the end of the current page. That is,
   * if valuesRead == endOfPageValueCount, we are at the end of the page.
   */
  private long endOfPageValueCount;

  /**
   * The dictionary, if this column has dictionary encoding.
   */
  private final Dictionary dictionary;

  /**
   * If true, the current page is dictionary encoded.
   */
  private boolean isCurrentPageDictionaryEncoded;

  /**
   * Maximum definition level for this column.
   */
  private final int maxDefLevel;

  /**
   * Repetition/Definition/Value readers.
   */
  private SpecificParquetRecordReaderBase.IntIterator repetitionLevelColumn;
  private SpecificParquetRecordReaderBase.IntIterator definitionLevelColumn;
  private ValuesReader dataColumn;

  // Only set if vectorized decoding is true. This is used instead of the row by row decoding
  // with `definitionLevelColumn`.
  private VectorizedRleValuesReader defColumn;

  /**
   * Total number of values in this column (in this row group).
   */
  private final long totalValueCount;

  /**
   * Total values in the current page.
   */
  private int pageValueCount;

  private final PageReader pageReader;
  private final ColumnDescriptor descriptor;
  private final OriginalType originalType;
  // The timezone conversion to apply to int96 timestamps. Null if no conversion.
  private final ZoneId convertTz;
  private static final ZoneId UTC = ZoneOffset.UTC;
  private final String datetimeRebaseMode;
  private final String int96RebaseMode;

  private boolean isDecimalTypeMatched(DataType dt) {
    DecimalType d = (DecimalType) dt;
    DecimalMetadata dm = descriptor.getPrimitiveType().getDecimalMetadata();
    // It's OK if the required decimal precision is larger than or equal to the physical decimal
    // precision in the Parquet metadata, as long as the decimal scale is the same.
    return dm != null && dm.getPrecision() <= d.precision() && dm.getScale() == d.scale();
  }

  private boolean canReadAsIntDecimal(DataType dt) {
    if (!DecimalType.is32BitDecimalType(dt)) return false;
    return isDecimalTypeMatched(dt);
  }

  private boolean canReadAsLongDecimal(DataType dt) {
    if (!DecimalType.is64BitDecimalType(dt)) return false;
    return isDecimalTypeMatched(dt);
  }

  private boolean canReadAsBinaryDecimal(DataType dt) {
    if (!DecimalType.isByteArrayDecimalType(dt)) return false;
    return isDecimalTypeMatched(dt);
  }

  public VectorizedColumnReader(
      ColumnDescriptor descriptor,
      OriginalType originalType,
      PageReader pageReader,
      ZoneId convertTz,
      String datetimeRebaseMode,
      String int96RebaseMode) throws IOException {
    this.descriptor = descriptor;
    this.pageReader = pageReader;
    this.convertTz = convertTz;
    this.originalType = originalType;
    this.maxDefLevel = descriptor.getMaxDefinitionLevel();

    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        this.dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
        this.isCurrentPageDictionaryEncoded = true;
      } catch (IOException e) {
        throw new IOException("could not decode the dictionary for " + descriptor, e);
      }
    } else {
      this.dictionary = null;
      this.isCurrentPageDictionaryEncoded = false;
    }
    this.totalValueCount = pageReader.getTotalValueCount();
    if (totalValueCount == 0) {
      throw new IOException("totalValueCount == 0");
    }
    assert "LEGACY".equals(datetimeRebaseMode) || "EXCEPTION".equals(datetimeRebaseMode) ||
      "CORRECTED".equals(datetimeRebaseMode);
    this.datetimeRebaseMode = datetimeRebaseMode;
    assert "LEGACY".equals(int96RebaseMode) || "EXCEPTION".equals(int96RebaseMode) ||
      "CORRECTED".equals(int96RebaseMode);
    this.int96RebaseMode = int96RebaseMode;
  }

  /**
   * Advances to the next value. Returns true if the value is non-null.
   */
  private boolean next() throws IOException {
    if (valuesRead >= endOfPageValueCount) {
      if (valuesRead >= totalValueCount) {
        // How do we get here? Throw end of stream exception?
        return false;
      }
      readPage();
    }
    ++valuesRead;
    // TODO: Don't read for flat schemas
    //repetitionLevel = repetitionLevelColumn.nextInt();
    return definitionLevelColumn.nextInt() == maxDefLevel;
  }

  private boolean isLazyDecodingSupported(PrimitiveType.PrimitiveTypeName typeName) {
    boolean isSupported = false;
    switch (typeName) {
      case INT32:
        isSupported = originalType != OriginalType.DATE || "CORRECTED".equals(datetimeRebaseMode);
        break;
      case INT64:
        if (originalType == OriginalType.TIMESTAMP_MICROS) {
          isSupported = "CORRECTED".equals(datetimeRebaseMode);
        } else {
          isSupported = originalType != OriginalType.TIMESTAMP_MILLIS;
        }
        break;
      case FLOAT:
      case DOUBLE:
      case BINARY:
        isSupported = true;
        break;
    }
    return isSupported;
  }

  static int rebaseDays(int julianDays, final boolean failIfRebase) {
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
      final String format) {
    if (failIfRebase) {
      if (julianMicros < RebaseDateTime.lastSwitchJulianTs()) {
        throw DataSourceUtils.newRebaseExceptionInRead(format);
      } else {
        return julianMicros;
      }
    } else {
      return RebaseDateTime.rebaseJulianToGregorianMicros(julianMicros);
    }
  }

  static long rebaseMicros(long julianMicros, final boolean failIfRebase) {
    return rebaseTimestamp(julianMicros, failIfRebase, "Parquet");
  }

  static long rebaseInt96(long julianMicros, final boolean failIfRebase) {
    return rebaseTimestamp(julianMicros, failIfRebase, "Parquet INT96");
  }

  /**
   * Reads `total` values from this columnReader into column.
   */
  void readBatch(int total, WritableColumnVector column) throws IOException {
    int rowId = 0;
    WritableColumnVector dictionaryIds = null;
    if (dictionary != null) {
      // SPARK-16334: We only maintain a single dictionary per row batch, so that it can be used to
      // decode all previous dictionary encoded pages if we ever encounter a non-dictionary encoded
      // page.
      dictionaryIds = column.reserveDictionaryIds(total);
    }
    while (total > 0) {
      // Compute the number of values we want to read in this page.
      int leftInPage = (int) (endOfPageValueCount - valuesRead);
      if (leftInPage == 0) {
        readPage();
        leftInPage = (int) (endOfPageValueCount - valuesRead);
      }
      int num = Math.min(total, leftInPage);
      PrimitiveType.PrimitiveTypeName typeName =
        descriptor.getPrimitiveType().getPrimitiveTypeName();
      if (isCurrentPageDictionaryEncoded) {
        // Read and decode dictionary ids.
        defColumn.readIntegers(
            num, dictionaryIds, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);

        // TIMESTAMP_MILLIS encoded as INT64 can't be lazily decoded as we need to post process
        // the values to add microseconds precision.
        if (column.hasDictionary() || (rowId == 0 && isLazyDecodingSupported(typeName))) {
          // Column vector supports lazy decoding of dictionary values so just set the dictionary.
          // We can't do this if rowId != 0 AND the column doesn't have a dictionary (i.e. some
          // non-dictionary encoded values have already been added).
          column.setDictionary(new ParquetDictionary(dictionary));
        } else {
          decodeDictionaryIds(rowId, num, column, dictionaryIds);
        }
      } else {
        if (column.hasDictionary() && rowId != 0) {
          // This batch already has dictionary encoded values but this new page is not. The batch
          // does not support a mix of dictionary and not so we will decode the dictionary.
          decodeDictionaryIds(0, rowId, column, column.getDictionaryIds());
        }
        column.setDictionary(null);
        switch (typeName) {
          case BOOLEAN:
            readBooleanBatch(rowId, num, column);
            break;
          case INT32:
            readIntBatch(rowId, num, column);
            break;
          case INT64:
            readLongBatch(rowId, num, column);
            break;
          case INT96:
            readBinaryBatch(rowId, num, column);
            break;
          case FLOAT:
            readFloatBatch(rowId, num, column);
            break;
          case DOUBLE:
            readDoubleBatch(rowId, num, column);
            break;
          case BINARY:
            readBinaryBatch(rowId, num, column);
            break;
          case FIXED_LEN_BYTE_ARRAY:
            readFixedLenByteArrayBatch(
              rowId, num, column, descriptor.getPrimitiveType().getTypeLength());
            break;
          default:
            throw new IOException("Unsupported type: " + typeName);
        }
      }

      valuesRead += num;
      rowId += num;
      total -= num;
    }
  }

  private boolean shouldConvertTimestamps() {
    return convertTz != null && !convertTz.equals(UTC);
  }

  /**
   * Helper function to construct exception for parquet schema mismatch.
   */
  private SchemaColumnConvertNotSupportedException constructConvertNotSupportedException(
      ColumnDescriptor descriptor,
      WritableColumnVector column) {
    return new SchemaColumnConvertNotSupportedException(
      Arrays.toString(descriptor.getPath()),
      descriptor.getPrimitiveType().getPrimitiveTypeName().toString(),
      column.dataType().catalogString());
  }

  /**
   * Reads `num` values into column, decoding the values from `dictionaryIds` and `dictionary`.
   */
  private void decodeDictionaryIds(
      int rowId,
      int num,
      WritableColumnVector column,
      WritableColumnVector dictionaryIds) {
    switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
      case INT32:
        if (column.dataType() == DataTypes.IntegerType ||
            canReadAsIntDecimal(column.dataType()) ||
            (column.dataType() == DataTypes.DateType && "CORRECTED".equals(datetimeRebaseMode))) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putInt(i, dictionary.decodeToInt(dictionaryIds.getDictId(i)));
            }
          }
        } else if (column.dataType() == DataTypes.ByteType) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putByte(i, (byte) dictionary.decodeToInt(dictionaryIds.getDictId(i)));
            }
          }
        } else if (column.dataType() == DataTypes.ShortType) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putShort(i, (short) dictionary.decodeToInt(dictionaryIds.getDictId(i)));
            }
          }
        } else if (column.dataType() == DataTypes.DateType) {
          final boolean failIfRebase = "EXCEPTION".equals(datetimeRebaseMode);
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              int julianDays = dictionary.decodeToInt(dictionaryIds.getDictId(i));
              column.putInt(i, rebaseDays(julianDays, failIfRebase));
            }
          }
        } else {
          throw constructConvertNotSupportedException(descriptor, column);
        }
        break;

      case INT64:
        if (column.dataType() == DataTypes.LongType ||
            canReadAsLongDecimal(column.dataType()) ||
            (originalType == OriginalType.TIMESTAMP_MICROS &&
              "CORRECTED".equals(datetimeRebaseMode))) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putLong(i, dictionary.decodeToLong(dictionaryIds.getDictId(i)));
            }
          }
        } else if (originalType == OriginalType.TIMESTAMP_MILLIS) {
          if ("CORRECTED".equals(datetimeRebaseMode)) {
            for (int i = rowId; i < rowId + num; ++i) {
              if (!column.isNullAt(i)) {
                long gregorianMillis = dictionary.decodeToLong(dictionaryIds.getDictId(i));
                column.putLong(i, DateTimeUtils.millisToMicros(gregorianMillis));
              }
            }
          } else {
            final boolean failIfRebase = "EXCEPTION".equals(datetimeRebaseMode);
            for (int i = rowId; i < rowId + num; ++i) {
              if (!column.isNullAt(i)) {
                long julianMillis = dictionary.decodeToLong(dictionaryIds.getDictId(i));
                long julianMicros = DateTimeUtils.millisToMicros(julianMillis);
                column.putLong(i, rebaseMicros(julianMicros, failIfRebase));
              }
            }
          }
        } else if (originalType == OriginalType.TIMESTAMP_MICROS) {
          final boolean failIfRebase = "EXCEPTION".equals(datetimeRebaseMode);
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              long julianMicros = dictionary.decodeToLong(dictionaryIds.getDictId(i));
              column.putLong(i, rebaseMicros(julianMicros, failIfRebase));
            }
          }
        } else {
          throw constructConvertNotSupportedException(descriptor, column);
        }
        break;

      case FLOAT:
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNullAt(i)) {
            column.putFloat(i, dictionary.decodeToFloat(dictionaryIds.getDictId(i)));
          }
        }
        break;

      case DOUBLE:
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNullAt(i)) {
            column.putDouble(i, dictionary.decodeToDouble(dictionaryIds.getDictId(i)));
          }
        }
        break;
      case INT96:
        if (column.dataType() == DataTypes.TimestampType) {
          final boolean failIfRebase = "EXCEPTION".equals(int96RebaseMode);
          if (!shouldConvertTimestamps()) {
            if ("CORRECTED".equals(int96RebaseMode)) {
              for (int i = rowId; i < rowId + num; ++i) {
                if (!column.isNullAt(i)) {
                  Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
                  column.putLong(i, ParquetRowConverter.binaryToSQLTimestamp(v));
                }
              }
            } else {
              for (int i = rowId; i < rowId + num; ++i) {
                if (!column.isNullAt(i)) {
                  Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
                  long julianMicros = ParquetRowConverter.binaryToSQLTimestamp(v);
                  long gregorianMicros = rebaseInt96(julianMicros, failIfRebase);
                  column.putLong(i, gregorianMicros);
                }
              }
            }
          } else {
            if ("CORRECTED".equals(int96RebaseMode)) {
              for (int i = rowId; i < rowId + num; ++i) {
                if (!column.isNullAt(i)) {
                  Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
                  long gregorianMicros = ParquetRowConverter.binaryToSQLTimestamp(v);
                  long adjTime = DateTimeUtils.convertTz(gregorianMicros, convertTz, UTC);
                  column.putLong(i, adjTime);
                }
              }
            } else {
              for (int i = rowId; i < rowId + num; ++i) {
                if (!column.isNullAt(i)) {
                  Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
                  long julianMicros = ParquetRowConverter.binaryToSQLTimestamp(v);
                  long gregorianMicros = rebaseInt96(julianMicros, failIfRebase);
                  long adjTime = DateTimeUtils.convertTz(gregorianMicros, convertTz, UTC);
                  column.putLong(i, adjTime);
                }
              }
            }
          }
        } else {
          throw constructConvertNotSupportedException(descriptor, column);
        }
        break;
      case BINARY:
        // TODO: this is incredibly inefficient as it blows up the dictionary right here. We
        // need to do this better. We should probably add the dictionary data to the ColumnVector
        // and reuse it across batches. This should mean adding a ByteArray would just update
        // the length and offset.
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNullAt(i)) {
            Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
            column.putByteArray(i, v.getBytes());
          }
        }
        break;
      case FIXED_LEN_BYTE_ARRAY:
        // DecimalType written in the legacy mode
        if (canReadAsIntDecimal(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putInt(i, (int) ParquetRowConverter.binaryToUnscaledLong(v));
            }
          }
        } else if (canReadAsLongDecimal(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putLong(i, ParquetRowConverter.binaryToUnscaledLong(v));
            }
          }
        } else if (canReadAsBinaryDecimal(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putByteArray(i, v.getBytes());
            }
          }
        } else {
          throw constructConvertNotSupportedException(descriptor, column);
        }
        break;

      default:
        throw new UnsupportedOperationException(
          "Unsupported type: " + descriptor.getPrimitiveType().getPrimitiveTypeName());
    }
  }

  /**
   * For all the read*Batch functions, reads `num` values from this columnReader into column. It
   * is guaranteed that num is smaller than the number of values left in the current page.
   */

  private void readBooleanBatch(int rowId, int num, WritableColumnVector column)
      throws IOException {
    if (column.dataType() != DataTypes.BooleanType) {
      throw constructConvertNotSupportedException(descriptor, column);
    }
    defColumn.readBooleans(
        num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
  }

  private void readIntBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (column.dataType() == DataTypes.IntegerType ||
        canReadAsIntDecimal(column.dataType())) {
      defColumn.readIntegers(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.ByteType) {
      defColumn.readBytes(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.ShortType) {
      defColumn.readShorts(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.DateType ) {
      if ("CORRECTED".equals(datetimeRebaseMode)) {
        defColumn.readIntegers(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
      } else {
        boolean failIfRebase = "EXCEPTION".equals(datetimeRebaseMode);
        defColumn.readIntegersWithRebase(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn, failIfRebase);
      }
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readLongBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    if (column.dataType() == DataTypes.LongType ||
        canReadAsLongDecimal(column.dataType())) {
      defColumn.readLongs(
        num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (originalType == OriginalType.TIMESTAMP_MICROS) {
      if ("CORRECTED".equals(datetimeRebaseMode)) {
        defColumn.readLongs(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
      } else {
        boolean failIfRebase = "EXCEPTION".equals(datetimeRebaseMode);
        defColumn.readLongsWithRebase(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn, failIfRebase);
      }
    } else if (originalType == OriginalType.TIMESTAMP_MILLIS) {
      if ("CORRECTED".equals(datetimeRebaseMode)) {
        for (int i = 0; i < num; i++) {
          if (defColumn.readInteger() == maxDefLevel) {
            column.putLong(rowId + i, DateTimeUtils.millisToMicros(dataColumn.readLong()));
          } else {
            column.putNull(rowId + i);
          }
        }
      } else {
        final boolean failIfRebase = "EXCEPTION".equals(datetimeRebaseMode);
        for (int i = 0; i < num; i++) {
          if (defColumn.readInteger() == maxDefLevel) {
            long julianMicros = DateTimeUtils.millisToMicros(dataColumn.readLong());
            column.putLong(rowId + i, rebaseMicros(julianMicros, failIfRebase));
          } else {
            column.putNull(rowId + i);
          }
        }
      }
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readFloatBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: support implicit cast to double?
    if (column.dataType() == DataTypes.FloatType) {
      defColumn.readFloats(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readDoubleBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (column.dataType() == DataTypes.DoubleType) {
      defColumn.readDoubles(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readBinaryBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    if (column.dataType() == DataTypes.StringType || column.dataType() == DataTypes.BinaryType
            || canReadAsBinaryDecimal(column.dataType())) {
      defColumn.readBinarys(num, column, rowId, maxDefLevel, data);
    } else if (column.dataType() == DataTypes.TimestampType) {
      final boolean failIfRebase = "EXCEPTION".equals(int96RebaseMode);
      if (!shouldConvertTimestamps()) {
        if ("CORRECTED".equals(int96RebaseMode)) {
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              // Read 12 bytes for INT96
              long gregorianMicros = ParquetRowConverter.binaryToSQLTimestamp(data.readBinary(12));
              column.putLong(rowId + i, gregorianMicros);
            } else {
              column.putNull(rowId + i);
            }
          }
        } else {
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              // Read 12 bytes for INT96
              long julianMicros = ParquetRowConverter.binaryToSQLTimestamp(data.readBinary(12));
              long gregorianMicros = rebaseInt96(julianMicros, failIfRebase);
              column.putLong(rowId + i, gregorianMicros);
            } else {
              column.putNull(rowId + i);
            }
          }
        }
      } else {
        if ("CORRECTED".equals(int96RebaseMode)) {
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              // Read 12 bytes for INT96
              long gregorianMicros = ParquetRowConverter.binaryToSQLTimestamp(data.readBinary(12));
              long adjTime = DateTimeUtils.convertTz(gregorianMicros, convertTz, UTC);
              column.putLong(rowId + i, adjTime);
            } else {
              column.putNull(rowId + i);
            }
          }
        } else {
          for (int i = 0; i < num; i++) {
            if (defColumn.readInteger() == maxDefLevel) {
              // Read 12 bytes for INT96
              long julianMicros = ParquetRowConverter.binaryToSQLTimestamp(data.readBinary(12));
              long gregorianMicros = rebaseInt96(julianMicros, failIfRebase);
              long adjTime = DateTimeUtils.convertTz(gregorianMicros, convertTz, UTC);
              column.putLong(rowId + i, adjTime);
            } else {
              column.putNull(rowId + i);
            }
          }
        }
      }
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readFixedLenByteArrayBatch(
      int rowId,
      int num,
      WritableColumnVector column,
      int arrayLen) {
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (canReadAsIntDecimal(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putInt(rowId + i,
              (int) ParquetRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else if (canReadAsLongDecimal(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putLong(rowId + i,
              ParquetRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else if (canReadAsBinaryDecimal(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putByteArray(rowId + i, data.readBinary(arrayLen).getBytes());
        } else {
          column.putNull(rowId + i);
        }
      }
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readPage() {
    DataPage page = pageReader.readPage();
    // TODO: Why is this a visitor?
    page.accept(new DataPage.Visitor<Void>() {
      @Override
      public Void visit(DataPageV1 dataPageV1) {
        try {
          readPageV1(dataPageV1);
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Void visit(DataPageV2 dataPageV2) {
        try {
          readPageV2(dataPageV2);
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  private void initDataReader(Encoding dataEncoding, ByteBufferInputStream in) throws IOException {
    this.endOfPageValueCount = valuesRead + pageValueCount;
    if (dataEncoding.usesDictionary()) {
      this.dataColumn = null;
      if (dictionary == null) {
        throw new IOException(
            "could not read page in col " + descriptor +
                " as the dictionary was missing for encoding " + dataEncoding);
      }
      @SuppressWarnings("deprecation")
      Encoding plainDict = Encoding.PLAIN_DICTIONARY; // var to allow warning suppression
      if (dataEncoding != plainDict && dataEncoding != Encoding.RLE_DICTIONARY) {
        throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
      }
      this.dataColumn = new VectorizedRleValuesReader();
      this.isCurrentPageDictionaryEncoded = true;
    } else {
      if (dataEncoding != Encoding.PLAIN) {
        throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
      }
      this.dataColumn = new VectorizedPlainValuesReader();
      this.isCurrentPageDictionaryEncoded = false;
    }

    try {
      dataColumn.initFromPage(pageValueCount, in);
    } catch (IOException e) {
      throw new IOException("could not read page in col " + descriptor, e);
    }
  }

  private void readPageV1(DataPageV1 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
    ValuesReader dlReader;

    // Initialize the decoders.
    if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
      throw new UnsupportedOperationException("Unsupported encoding: " + page.getDlEncoding());
    }
    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn = new VectorizedRleValuesReader(bitWidth);
    dlReader = this.defColumn;
    this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
    this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
    try {
      BytesInput bytes = page.getBytes();
      ByteBufferInputStream in = bytes.toInputStream();
      rlReader.initFromPage(pageValueCount, in);
      dlReader.initFromPage(pageValueCount, in);
      initDataReader(page.getValueEncoding(), in);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  private void readPageV2(DataPageV2 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    this.repetitionLevelColumn = createRLEIterator(descriptor.getMaxRepetitionLevel(),
        page.getRepetitionLevels(), descriptor);

    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    // do not read the length from the stream. v2 pages handle dividing the page bytes.
    this.defColumn = new VectorizedRleValuesReader(bitWidth, false);
    this.definitionLevelColumn = new ValuesReaderIntIterator(this.defColumn);
    this.defColumn.initFromPage(
        this.pageValueCount, page.getDefinitionLevels().toInputStream());
    try {
      initDataReader(page.getDataEncoding(), page.getData().toInputStream());
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }
}
