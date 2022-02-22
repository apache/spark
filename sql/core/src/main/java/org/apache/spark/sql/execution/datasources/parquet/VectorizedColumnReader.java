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
import java.util.PrimitiveIterator;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.Decimal;

import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

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

  public VectorizedColumnReader(
      ColumnDescriptor descriptor,
      LogicalTypeAnnotation logicalTypeAnnotation,
      PageReader pageReader,
      PrimitiveIterator.OfLong rowIndexes,
      ZoneId convertTz,
      String datetimeRebaseMode,
      String datetimeRebaseTz,
      String int96RebaseMode,
      String int96RebaseTz) throws IOException {
    this.descriptor = descriptor;
    this.pageReader = pageReader;
    this.readState = new ParquetReadState(descriptor.getMaxDefinitionLevel(), rowIndexes);
    this.logicalTypeAnnotation = logicalTypeAnnotation;
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
  }

  private boolean isLazyDecodingSupported(PrimitiveType.PrimitiveTypeName typeName) {
    boolean isSupported = false;
    switch (typeName) {
      case INT32:
        isSupported = !(logicalTypeAnnotation instanceof DateLogicalTypeAnnotation) ||
          "CORRECTED".equals(datetimeRebaseMode);
        break;
      case INT64:
        if (updaterFactory.isTimestampTypeMatched(TimeUnit.MICROS)) {
          isSupported = "CORRECTED".equals(datetimeRebaseMode);
        } else {
          isSupported = !updaterFactory.isTimestampTypeMatched(TimeUnit.MILLIS);
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

  /**
   * Reads `total` values from this columnReader into column.
   */
  void readBatch(int total, WritableColumnVector column) throws IOException {
    WritableColumnVector dictionaryIds = null;
    ParquetVectorUpdater updater = updaterFactory.getUpdater(descriptor, column.dataType());

    if (dictionary != null) {
      // SPARK-16334: We only maintain a single dictionary per row batch, so that it can be used to
      // decode all previous dictionary encoded pages if we ever encounter a non-dictionary encoded
      // page.
      dictionaryIds = column.reserveDictionaryIds(total);
    }
    readState.resetForNewBatch(total);
    while (readState.valuesToReadInBatch > 0) {
      if (readState.valuesToReadInPage == 0) {
        int pageValueCount = readPage();
        readState.resetForNewPage(pageValueCount, pageFirstRowIndex);
      }
      PrimitiveType.PrimitiveTypeName typeName =
          descriptor.getPrimitiveType().getPrimitiveTypeName();
      if (isCurrentPageDictionaryEncoded) {
        // Save starting offset in case we need to decode dictionary IDs.
        int startOffset = readState.offset;
        // Save starting row index so we can check if we need to eagerly decode dict ids later
        long startRowId = readState.rowId;

        // Read and decode dictionary ids.
        defColumn.readIntegers(readState, dictionaryIds, column,
          (VectorizedValuesReader) dataColumn);

        // TIMESTAMP_MILLIS encoded as INT64 can't be lazily decoded as we need to post process
        // the values to add microseconds precision.
        if (column.hasDictionary() || (startRowId == pageFirstRowIndex &&
            isLazyDecodingSupported(typeName))) {
          // Column vector supports lazy decoding of dictionary values so just set the dictionary.
          // We can't do this if startRowId is not the first row index in the page AND the column
          // doesn't have a dictionary (i.e. some non-dictionary encoded values have already been
          // added).
          PrimitiveType primitiveType = descriptor.getPrimitiveType();

          // We need to make sure that we initialize the right type for the dictionary otherwise
          // WritableColumnVector will throw an exception when trying to decode to an Int when the
          // dictionary is in fact initialized as Long
          LogicalTypeAnnotation typeAnnotation = primitiveType.getLogicalTypeAnnotation();
          boolean castLongToInt = typeAnnotation instanceof DecimalLogicalTypeAnnotation &&
            ((DecimalLogicalTypeAnnotation) typeAnnotation).getPrecision() <=
            Decimal.MAX_INT_DIGITS() && primitiveType.getPrimitiveTypeName() == INT64;

          // We require a long value, but we need to use dictionary to decode the original
          // signed int first
          boolean isUnsignedInt32 = updaterFactory.isUnsignedIntTypeMatched(32);

          // We require a decimal value, but we need to use dictionary to decode the original
          // signed long first
          boolean isUnsignedInt64 = updaterFactory.isUnsignedIntTypeMatched(64);

          boolean needTransform = castLongToInt || isUnsignedInt32 || isUnsignedInt64;
          column.setDictionary(new ParquetDictionary(dictionary, needTransform));
        } else {
          updater.decodeDictionaryIds(readState.offset - startOffset, startOffset, column,
            dictionaryIds, dictionary);
        }
      } else {
        if (column.hasDictionary() && readState.offset != 0) {
          // This batch already has dictionary encoded values but this new page is not. The batch
          // does not support a mix of dictionary and not so we will decode the dictionary.
          updater.decodeDictionaryIds(readState.offset, 0, column, dictionaryIds, dictionary);
        }
        column.setDictionary(null);
        VectorizedValuesReader valuesReader = (VectorizedValuesReader) dataColumn;
        defColumn.readBatch(readState, column, valuesReader, updater);
      }
    }
  }

  private int readPage() {
    DataPage page = pageReader.readPage();
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
      this.dataColumn = getValuesReader(dataEncoding);
      this.isCurrentPageDictionaryEncoded = false;
    }

    try {
      dataColumn.initFromPage(pageValueCount, in);
    } catch (IOException e) {
      throw new IOException("could not read page in col " + descriptor, e);
    }
  }

  private ValuesReader getValuesReader(Encoding encoding) {
    switch (encoding) {
      case PLAIN:
        return new VectorizedPlainValuesReader();
      case DELTA_BYTE_ARRAY:
        return new VectorizedDeltaByteArrayReader();
      case DELTA_BINARY_PACKED:
        return new VectorizedDeltaBinaryPackedReader();
      case RLE:
        PrimitiveType.PrimitiveTypeName typeName =
          this.descriptor.getPrimitiveType().getPrimitiveTypeName();
        // RLE encoding only supports boolean type `Values`, and  `bitwidth` is always 1.
        if (typeName == BOOLEAN) {
          return new VectorizedRleValuesReader(1);
        } else {
          throw new UnsupportedOperationException(
            "RLE encoding is not supported for values of type: " + typeName);
        }
      default:
        throw new UnsupportedOperationException("Unsupported encoding: " + encoding);
    }
  }


  private int readPageV1(DataPageV1 page) throws IOException {
    if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
      throw new UnsupportedOperationException("Unsupported encoding: " + page.getDlEncoding());
    }

    int pageValueCount = page.getValueCount();
    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());

    this.defColumn = new VectorizedRleValuesReader(bitWidth);
    try {
      BytesInput bytes = page.getBytes();
      ByteBufferInputStream in = bytes.toInputStream();

      // only used now to consume the repetition level data
      page.getRlEncoding()
          .getValuesReader(descriptor, REPETITION_LEVEL)
          .initFromPage(pageValueCount, in);

      defColumn.initFromPage(pageValueCount, in);
      initDataReader(pageValueCount, page.getValueEncoding(), in);
      return pageValueCount;
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  private int readPageV2(DataPageV2 page) throws IOException {
    int pageValueCount = page.getValueCount();
    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());

    // do not read the length from the stream. v2 pages handle dividing the page bytes.
    defColumn = new VectorizedRleValuesReader(bitWidth, false);
    defColumn.initFromPage(pageValueCount, page.getDefinitionLevels().toInputStream());
    try {
      initDataReader(pageValueCount, page.getDataEncoding(), page.getData().toInputStream());
      return pageValueCount;
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }
}
