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
import java.util.Map;

import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.spark.sql.types.DataTypes.*;

/**
 * Decoder to return values from a single column.
 */
public class VectorizedColumnReader {
  /**
   * The dictionary, if this column has dictionary encoding.
   */
  private final Dictionary dictionary;

  /**
   * If true, the current page is dictionary encoded.
   */
  private boolean isCurrentPageDictionaryEncoded;

  /**
   * Value readers.
   */
  private ValuesReader dataColumn;

  /**
   * Vectorized RLE decoder for definition levels
   */
  private VectorizedRleValuesReader defColumn;

  /**
   * Vectorized RLE decoder for repetition levels
   */
  private VectorizedRleValuesReader repColumn;

  /**
   * Factory to get type-specific vector updater.
   */
  private final ParquetVectorUpdaterFactory updaterFactory;

  /**
   * Helper struct to track intermediate states while reading Parquet pages in the column chunk.
   */
  private final ParquetReadState readState;

  /**
   * The index for the first row in the current page, among all rows across all pages in the
   * column chunk for this reader. If there is no column index, the value is 0.
   */
  private long pageFirstRowIndex;

  private final PageReader pageReader;
  private final ColumnDescriptor descriptor;
  private final LogicalTypeAnnotation logicalTypeAnnotation;
  private final String datetimeRebaseMode;
  private final ParsedVersion writerVersion;

  public VectorizedColumnReader(
      ColumnDescriptor descriptor,
      boolean isRequired,
      PageReadStore pageReadStore,
      ZoneId convertTz,
      String datetimeRebaseMode,
      String datetimeRebaseTz,
      String int96RebaseMode,
      String int96RebaseTz,
      ParsedVersion writerVersion) throws IOException {
    this.descriptor = descriptor;
    this.pageReader = pageReadStore.getPageReader(descriptor);
    this.readState = new ParquetReadState(descriptor, isRequired,
      pageReadStore.getRowIndexes().orElse(null));
    this.logicalTypeAnnotation = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
    this.updaterFactory = new ParquetVectorUpdaterFactory(
      logicalTypeAnnotation,
      convertTz,
      datetimeRebaseMode,
      datetimeRebaseTz,
      int96RebaseMode,
      int96RebaseTz);

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
    if (pageReader.getTotalValueCount() == 0) {
      throw new IOException("totalValueCount == 0");
    }
    assert "LEGACY".equals(datetimeRebaseMode) || "EXCEPTION".equals(datetimeRebaseMode) ||
      "CORRECTED".equals(datetimeRebaseMode);
    this.datetimeRebaseMode = datetimeRebaseMode;
    assert "LEGACY".equals(int96RebaseMode) || "EXCEPTION".equals(int96RebaseMode) ||
      "CORRECTED".equals(int96RebaseMode);
    this.writerVersion = writerVersion;
  }

  private boolean isLazyDecodingSupported(
      PrimitiveType.PrimitiveTypeName typeName,
      DataType sparkType) {
    boolean isSupported = false;
    // Don't use lazy dictionary decoding if the column needs extra processing: upcasting or date
    // rebasing.
    switch (typeName) {
      case INT32: {
        boolean isDecimal = sparkType instanceof DecimalType;
        boolean needsUpcast = sparkType == LongType || sparkType == DoubleType ||
          sparkType == TimestampNTZType ||
          (isDecimal && !DecimalType.is32BitDecimalType(sparkType));
        boolean needsRebase = logicalTypeAnnotation instanceof DateLogicalTypeAnnotation &&
          !"CORRECTED".equals(datetimeRebaseMode);
        isSupported = !needsUpcast && !needsRebase && !needsDecimalScaleRebase(sparkType);
        break;
      }
      case INT64: {
        boolean isDecimal = sparkType instanceof DecimalType;
        boolean needsUpcast = (isDecimal && !DecimalType.is64BitDecimalType(sparkType)) ||
          updaterFactory.isTimestampTypeMatched(TimeUnit.MILLIS);
        boolean needsRebase = updaterFactory.isTimestampTypeMatched(TimeUnit.MICROS) &&
          !"CORRECTED".equals(datetimeRebaseMode);
        isSupported = !needsUpcast && !needsRebase && !needsDecimalScaleRebase(sparkType);
        break;
      }
      case FLOAT:
        isSupported = sparkType == FloatType;
        break;
      case DOUBLE:
        isSupported = true;
        break;
      case BINARY:
        isSupported = !needsDecimalScaleRebase(sparkType);
        break;
    }
    return isSupported;
  }

  /**
   * Returns whether the Parquet type of this column and the given spark type are two decimal types
   * with different scale.
   */
  private boolean needsDecimalScaleRebase(DataType sparkType) {
      LogicalTypeAnnotation typeAnnotation =
        descriptor.getPrimitiveType().getLogicalTypeAnnotation();
      if (!(typeAnnotation instanceof DecimalLogicalTypeAnnotation)) return false;
      if (!(sparkType instanceof DecimalType)) return false;
      DecimalLogicalTypeAnnotation parquetDecimal = (DecimalLogicalTypeAnnotation) typeAnnotation;
      DecimalType sparkDecimal = (DecimalType) sparkType;
      return parquetDecimal.getScale() != sparkDecimal.scale();
  }

  /**
   * Reads `total` rows from this columnReader into column.
   */
  void readBatch(
      int total,
      WritableColumnVector column,
      WritableColumnVector repetitionLevels,
      WritableColumnVector definitionLevels) throws IOException {
    WritableColumnVector dictionaryIds = null;
    ParquetVectorUpdater updater = updaterFactory.getUpdater(descriptor, column.dataType());

    if (dictionary != null) {
      // SPARK-16334: We only maintain a single dictionary per row batch, so that it can be used to
      // decode all previous dictionary encoded pages if we ever encounter a non-dictionary encoded
      // page.
      dictionaryIds = column.reserveDictionaryIds(total);
    }
    readState.resetForNewBatch(total);
    while (readState.rowsToReadInBatch > 0 || !readState.lastListCompleted) {
      if (readState.valuesToReadInPage == 0) {
        int pageValueCount = readPage();
        if (pageValueCount < 0) {
          // we've read all the pages; this could happen when we're reading a repeated list and we
          // don't know where the list will end until we've seen all the pages.
          break;
        }
        readState.resetForNewPage(pageValueCount, pageFirstRowIndex);
      }
      PrimitiveType.PrimitiveTypeName typeName =
          descriptor.getPrimitiveType().getPrimitiveTypeName();
      if (isCurrentPageDictionaryEncoded) {
        // Save starting offset in case we need to decode dictionary IDs.
        int startOffset = readState.valueOffset;
        // Save starting row index so we can check if we need to eagerly decode dict ids later
        long startRowId = readState.rowId;

        // Read and decode dictionary ids.
        if (readState.maxRepetitionLevel == 0) {
          defColumn.readIntegers(readState, dictionaryIds, column, definitionLevels,
            (VectorizedValuesReader) dataColumn);
        } else {
          repColumn.readIntegersRepeated(readState, repetitionLevels, defColumn, definitionLevels,
            dictionaryIds, column, (VectorizedValuesReader) dataColumn);
        }

        // TIMESTAMP_MILLIS encoded as INT64 can't be lazily decoded as we need to post process
        // the values to add microseconds precision.
        if (column.hasDictionary() || (startRowId == pageFirstRowIndex &&
            isLazyDecodingSupported(typeName, column.dataType()))) {
          // Column vector supports lazy decoding of dictionary values so just set the dictionary.
          // We can't do this if startRowId is not the first row index in the page AND the column
          // doesn't have a dictionary (i.e. some non-dictionary encoded values have already been
          // added).
          PrimitiveType primitiveType = descriptor.getPrimitiveType();

          // We need to make sure that we initialize the right type for the dictionary otherwise
          // WritableColumnVector will throw an exception when trying to decode to an Int when the
          // dictionary is in fact initialized as Long
          LogicalTypeAnnotation typeAnnotation = primitiveType.getLogicalTypeAnnotation();
          boolean castLongToInt =
            typeAnnotation instanceof DecimalLogicalTypeAnnotation annotation &&
            annotation.getPrecision() <= Decimal.MAX_INT_DIGITS() &&
            primitiveType.getPrimitiveTypeName() == INT64;

          // We require a long value, but we need to use dictionary to decode the original
          // signed int first
          boolean isUnsignedInt32 = updaterFactory.isUnsignedIntTypeMatched(32);

          // We require a decimal value, but we need to use dictionary to decode the original
          // signed long first
          boolean isUnsignedInt64 = updaterFactory.isUnsignedIntTypeMatched(64);

          boolean needTransform = castLongToInt || isUnsignedInt32 || isUnsignedInt64;
          column.setDictionary(new ParquetDictionary(dictionary, needTransform));
        } else {
          updater.decodeDictionaryIds(readState.valueOffset - startOffset, startOffset, column,
            dictionaryIds, dictionary);
        }
      } else {
        if (column.hasDictionary() && readState.valueOffset != 0) {
          // This batch already has dictionary encoded values but this new page is not. The batch
          // does not support a mix of dictionary and not so we will decode the dictionary.
          updater.decodeDictionaryIds(readState.valueOffset, 0, column, dictionaryIds, dictionary);
        }
        column.setDictionary(null);
        VectorizedValuesReader valuesReader = (VectorizedValuesReader) dataColumn;
        if (readState.maxRepetitionLevel == 0) {
          defColumn.readBatch(readState, column, definitionLevels, valuesReader, updater);
        } else {
          repColumn.readBatchRepeated(readState, repetitionLevels, defColumn, definitionLevels,
            column, valuesReader, updater);
        }
      }
    }
  }

  private int readPage() {
    DataPage page = pageReader.readPage();
    if (page == null) {
      return -1;
    }
    this.pageFirstRowIndex = page.getFirstRowIndex().orElse(0L);

    return page.accept(new DataPage.Visitor<Integer>() {
      @Override
      public Integer visit(DataPageV1 dataPageV1) {
        try {
          return readPageV1(dataPageV1);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Integer visit(DataPageV2 dataPageV2) {
        try {
          return readPageV2(dataPageV2);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  private void initDataReader(
      int pageValueCount,
      Encoding dataEncoding,
      ByteBufferInputStream in) throws IOException {
    ValuesReader previousReader = this.dataColumn;
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
        throw new SparkUnsupportedOperationException(
          "_LEGACY_ERROR_TEMP_3189", Map.of("encoding", dataEncoding.toString()));
      }
      this.dataColumn = new VectorizedRleValuesReader();
      this.isCurrentPageDictionaryEncoded = true;
    } else {
      this.dataColumn = getValuesReader(dataEncoding);
      this.isCurrentPageDictionaryEncoded = false;
    }

    try {
      dataColumn.initFromPage(pageValueCount, in);
    } catch (IOException e) {
      throw new IOException("could not read page in col " + descriptor, e);
    }
    // for PARQUET-246 (See VectorizedDeltaByteArrayReader.setPreviousValues)
    if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding) &&
        previousReader instanceof RequiresPreviousReader) {
      // previousReader can only be set if reading sequentially
      ((RequiresPreviousReader) dataColumn).setPreviousReader(previousReader);
    }
  }

  private ValuesReader getValuesReader(Encoding encoding) {
    return switch (encoding) {
      case PLAIN -> new VectorizedPlainValuesReader();
      case DELTA_BYTE_ARRAY -> new VectorizedDeltaByteArrayReader();
      case DELTA_LENGTH_BYTE_ARRAY -> new VectorizedDeltaLengthByteArrayReader();
      case DELTA_BINARY_PACKED -> new VectorizedDeltaBinaryPackedReader();
      case RLE -> {
        PrimitiveType.PrimitiveTypeName typeName =
          this.descriptor.getPrimitiveType().getPrimitiveTypeName();
        // RLE encoding only supports boolean type `Values`, and  `bitwidth` is always 1.
        if (typeName == BOOLEAN) {
          yield new VectorizedRleValuesReader(1);
        } else {
          throw new SparkUnsupportedOperationException(
            "_LEGACY_ERROR_TEMP_3190", Map.of("typeName", typeName.toString()));
        }
      }
      default ->
        throw new SparkUnsupportedOperationException(
          "_LEGACY_ERROR_TEMP_3189", Map.of("encoding", encoding.toString()));
    };
  }


  private int readPageV1(DataPageV1 page) throws IOException {
    if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
      throw new SparkUnsupportedOperationException(
        "_LEGACY_ERROR_TEMP_3189", Map.of("encoding", page.getDlEncoding().toString()));
    }

    int pageValueCount = page.getValueCount();

    int rlBitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxRepetitionLevel());
    this.repColumn = new VectorizedRleValuesReader(rlBitWidth);

    int dlBitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn = new VectorizedRleValuesReader(dlBitWidth);

    try {
      BytesInput bytes = page.getBytes();
      ByteBufferInputStream in = bytes.toInputStream();

      repColumn.initFromPage(pageValueCount, in);
      defColumn.initFromPage(pageValueCount, in);
      initDataReader(pageValueCount, page.getValueEncoding(), in);
      return pageValueCount;
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  private int readPageV2(DataPageV2 page) throws IOException {
    int pageValueCount = page.getValueCount();

    // do not read the length from the stream. v2 pages handle dividing the page bytes.
    int rlBitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxRepetitionLevel());
    repColumn = new VectorizedRleValuesReader(rlBitWidth, false);
    repColumn.initFromPage(pageValueCount, page.getRepetitionLevels().toInputStream());

    int dlBitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    defColumn = new VectorizedRleValuesReader(dlBitWidth, false);
    defColumn.initFromPage(pageValueCount, page.getDefinitionLevels().toInputStream());

    try {
      initDataReader(pageValueCount, page.getDataEncoding(), page.getData().toInputStream());
      return pageValueCount;
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }
}
