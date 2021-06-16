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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.Decimal;

import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

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
   * Value readers.
   */
  private ValuesReader dataColumn;

  /**
   * Vectorized RLE decoder for definition levels
   */
  private VectorizedRleValuesReader defColumn;

  /**
   * Total number of values in this column (in this row group).
   */
  private final long totalValueCount;

  /**
   * Total values in the current page.
   */
  private int pageValueCount;

  /**
   * Factory to get type-specific vector updater.
   */
  private final ParquetVectorUpdaterFactory updaterFactory;

  private final PageReader pageReader;
  private final ColumnDescriptor descriptor;
  private final LogicalTypeAnnotation logicalTypeAnnotation;
  private final String datetimeRebaseMode;

  public VectorizedColumnReader(
      ColumnDescriptor descriptor,
      LogicalTypeAnnotation logicalTypeAnnotation,
      PageReader pageReader,
      ZoneId convertTz,
      String datetimeRebaseMode,
      String int96RebaseMode) throws IOException {
    this.descriptor = descriptor;
    this.pageReader = pageReader;
    this.logicalTypeAnnotation = logicalTypeAnnotation;
    this.maxDefLevel = descriptor.getMaxDefinitionLevel();
    this.updaterFactory = new ParquetVectorUpdaterFactory(
        logicalTypeAnnotation, convertTz, datetimeRebaseMode, int96RebaseMode);

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
    int rowId = 0;
    WritableColumnVector dictionaryIds = null;
    ParquetVectorUpdater updater = updaterFactory.getUpdater(descriptor, column.dataType());

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
          updater.decodeDictionaryIds(num, rowId, column, dictionaryIds, dictionary);
        }
      } else {
        if (column.hasDictionary() && rowId != 0) {
          // This batch already has dictionary encoded values but this new page is not. The batch
          // does not support a mix of dictionary and not so we will decode the dictionary.
          updater.decodeDictionaryIds(rowId, 0, column, dictionaryIds, dictionary);
        }
        column.setDictionary(null);
        VectorizedValuesReader valuesReader = (VectorizedValuesReader) dataColumn;
        defColumn.readBatch(num, rowId, column, maxDefLevel, valuesReader, updater);
      }

      valuesRead += num;
      rowId += num;
      total -= num;
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

    // Initialize the decoders.
    if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
      throw new UnsupportedOperationException("Unsupported encoding: " + page.getDlEncoding());
    }

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
      initDataReader(page.getValueEncoding(), in);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  private void readPageV2(DataPageV2 page) throws IOException {
    this.pageValueCount = page.getValueCount();

    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    // do not read the length from the stream. v2 pages handle dividing the page bytes.
    defColumn = new VectorizedRleValuesReader(bitWidth, false);
    defColumn.initFromPage(this.pageValueCount, page.getDefinitionLevels().toInputStream());
    try {
      initDataReader(page.getDataEncoding(), page.getData().toInputStream());
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }
}
