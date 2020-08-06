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

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;
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
  private Dictionary dictionary;

  /**
   * Convert the original data of the parquet column to the spark request schema type.
   */
  private ParquetColumnConverter columnConverter;

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
  // Timezone ID of the session.
  private final ZoneId convertTz;
  // Whether to use session timezone to convert to int96 data.
  private final boolean convertInt96Timestamp;
  private final String datetimeRebaseMode;

  public VectorizedColumnReader(
      ColumnDescriptor descriptor,
      OriginalType originalType,
      PageReader pageReader,
      ZoneId convertTz,
      String datetimeRebaseMode,
      boolean convertInt96Timestamp) throws IOException {
    this.descriptor = descriptor;
    this.pageReader = pageReader;
    this.convertTz = convertTz;
    this.convertInt96Timestamp = convertInt96Timestamp;
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

  static long rebaseMicros(long julianMicros, final boolean failIfRebase) {
    if (failIfRebase) {
      if (julianMicros < RebaseDateTime.lastSwitchJulianTs()) {
        throw DataSourceUtils.newRebaseExceptionInRead("Parquet");
      } else {
        return julianMicros;
      }
    } else {
      return RebaseDateTime.rebaseJulianToGregorianMicros(julianMicros);
    }
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
    initColumnConverter(column);
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

        if (column.hasDictionary() || rowId == 0) {
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
            columnConverter.readBooleanBatch(rowId, num, defColumn, dataColumn);
            break;
          case INT32:
            columnConverter.readInt32Batch(rowId, num, defColumn, dataColumn);
            break;
          case INT64:
            columnConverter.readInt64Batch(rowId, num, defColumn, dataColumn);
            break;
          case INT96:
            columnConverter.readInt96Batch(rowId, num, defColumn, dataColumn);
            break;
          case FLOAT:
            columnConverter.readFloatBatch(rowId, num, defColumn, dataColumn);
            break;
          case DOUBLE:
            columnConverter.readDoubleBatch(rowId, num, defColumn, dataColumn);
            break;
          case BINARY:
          case FIXED_LEN_BYTE_ARRAY:
            columnConverter.readBinaryBatch(rowId, num, defColumn, dataColumn);
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

  /**
   * Reads `num` values into column, decoding the values from `dictionaryIds` and `dictionary`.
   */
  private void decodeDictionaryIds(
      int rowId,
      int num,
      WritableColumnVector column,
      WritableColumnVector dictionaryIds) {
    DataType sparkType = column.dataType();
    if (sparkType == DataTypes.FloatType) {
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNullAt(i)) {
          column.putFloat(i, dictionary.decodeToFloat(dictionaryIds.getDictId(i)));
        }
      }
    } else if (sparkType == DataTypes.DoubleType) {
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNullAt(i)) {
          column.putDouble(i, dictionary.decodeToDouble(dictionaryIds.getDictId(i)));
        }
      }
    } else if (sparkType == DataTypes.ByteType) {
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNullAt(i)) {
          column.putByte(i, (byte) dictionary.decodeToInt(dictionaryIds.getDictId(i)));
        }
      }
    } else if (sparkType == DataTypes.ShortType) {
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNullAt(i)) {
          column.putShort(i, (short) dictionary.decodeToInt(dictionaryIds.getDictId(i)));
        }
      }
    } else if (sparkType == DataTypes.IntegerType ||
      sparkType == DataTypes.DateType || DecimalType.is32BitDecimalType(sparkType)) {
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNullAt(i)) {
          column.putInt(i, dictionary.decodeToInt(dictionaryIds.getDictId(i)));
        }
      }
    } else if (sparkType == DataTypes.LongType ||
      sparkType == DataTypes.TimestampType || DecimalType.is64BitDecimalType(sparkType)) {
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNullAt(i)) {
          column.putLong(i, dictionary.decodeToLong(dictionaryIds.getDictId(i)));
        }
      }
    } else if (sparkType == DataTypes.StringType ||
      sparkType == DataTypes.BinaryType || DecimalType.isByteArrayDecimalType(sparkType)) {
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNullAt(i)) {
          Binary binary = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
          column.putByteArray(i, binary.getBytes());
        }
      }
    } else {
      throw new UnsupportedOperationException(
        "Unsupported type: " + descriptor.getPrimitiveType());
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

  private void initColumnConverter(WritableColumnVector sparkColumn) {
    if (columnConverter == null) {
      columnConverter = new ParquetColumnConverter(descriptor, sparkColumn,
        convertTz, datetimeRebaseMode, convertInt96Timestamp);
      if (dictionary != null) {
        dictionary = columnConverter.convertDictionary(dictionary);
      }
    } else {
      if (sparkColumn != columnConverter.getSparkColumn()) {
        throw new UnsupportedOperationException("Unsupported SparkColumn change");
      }
    }
  }
}
