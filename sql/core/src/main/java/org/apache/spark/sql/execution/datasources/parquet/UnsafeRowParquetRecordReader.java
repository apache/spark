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
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * A specialized RecordReader that reads into UnsafeRows directly using the Parquet column APIs.
 *
 * This is somewhat based on parquet-mr's ColumnReader.
 *
 * TODO: handle complex types, decimal requiring more than 8 bytes, INT96. Schema mismatch.
 * All of these can be handled efficiently and easily with codegen.
 */
public class UnsafeRowParquetRecordReader extends SpecificParquetRecordReaderBase<UnsafeRow> {
  /**
   * Batch of unsafe rows that we assemble and the current index we've returned. Everytime this
   * batch is used up (batchIdx == numBatched), we populated the batch.
   */
  private UnsafeRow[] rows = new UnsafeRow[64];
  private int batchIdx = 0;
  private int numBatched = 0;

  /**
   * Used to write variable length columns. Same length as `rows`.
   */
  private UnsafeRowWriter[] rowWriters = null;
  /**
   * True if the row contains variable length fields.
   */
  private boolean containsVarLenFields;

  /**
   * The number of bytes in the fixed length portion of the row.
   */
  private int fixedSizeBytes;

  /**
   * For each request column, the reader to read this column.
   * columnsReaders[i] populated the UnsafeRow's attribute at i.
   */
  private ColumnReader[] columnReaders;

  /**
   * The number of rows that have been returned.
   */
  private long rowsReturned;

  /**
   * The number of rows that have been reading, including the current in flight row group.
   */
  private long totalCountLoadedSoFar = 0;

  /**
   * For each column, the annotated original type.
   */
  private OriginalType[] originalTypes;

  /**
   * The default size for varlen columns. The row grows as necessary to accommodate the
   * largest column.
   */
  private static final int DEFAULT_VAR_LEN_SIZE = 32;

  /**
   * Tries to initialize the reader for this split. Returns true if this reader supports reading
   * this split and false otherwise.
   */
  public boolean tryInitialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    try {
      initialize(inputSplit, taskAttemptContext);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    super.initialize(inputSplit, taskAttemptContext);

    /**
     * Check that the requested schema is supported.
     */
    if (requestedSchema.getFieldCount() == 0) {
      // TODO: what does this mean?
      throw new IOException("Empty request schema not supported.");
    }
    int numVarLenFields = 0;
    originalTypes = new OriginalType[requestedSchema.getFieldCount()];
    for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
      Type t = requestedSchema.getFields().get(i);
      if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
        throw new IOException("Complex types not supported.");
      }
      PrimitiveType primitiveType = t.asPrimitiveType();

      originalTypes[i] = t.getOriginalType();

      // TODO: Be extremely cautious in what is supported. Expand this.
      if (originalTypes[i] != null && originalTypes[i] != OriginalType.DECIMAL &&
          originalTypes[i] != OriginalType.UTF8 && originalTypes[i] != OriginalType.DATE) {
        throw new IOException("Unsupported type: " + t);
      }
      if (originalTypes[i] == OriginalType.DECIMAL &&
          primitiveType.getDecimalMetadata().getPrecision() >
              CatalystSchemaConverter.MAX_PRECISION_FOR_INT64()) {
        throw new IOException("Decimal with high precision is not supported.");
      }
      if (primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
        throw new IOException("Int96 not supported.");
      }
      ColumnDescriptor fd = fileSchema.getColumnDescription(requestedSchema.getPaths().get(i));
      if (!fd.equals(requestedSchema.getColumns().get(i))) {
        throw new IOException("Schema evolution not supported.");
      }

      if (primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
        ++numVarLenFields;
      }
    }

    /**
     * Initialize rows and rowWriters. These objects are reused across all rows in the relation.
     */
    int rowByteSize = UnsafeRow.calculateBitSetWidthInBytes(requestedSchema.getFieldCount());
    rowByteSize += 8 * requestedSchema.getFieldCount();
    fixedSizeBytes = rowByteSize;
    rowByteSize += numVarLenFields * DEFAULT_VAR_LEN_SIZE;
    containsVarLenFields = numVarLenFields > 0;
    rowWriters = new UnsafeRowWriter[rows.length];

    for (int i = 0; i < rows.length; ++i) {
      rows[i] = new UnsafeRow();
      rowWriters[i] = new UnsafeRowWriter();
      BufferHolder holder = new BufferHolder(rowByteSize);
      rowWriters[i].initialize(rows[i], holder, requestedSchema.getFieldCount());
      rows[i].pointTo(holder.buffer, Platform.BYTE_ARRAY_OFFSET, requestedSchema.getFieldCount(),
          holder.buffer.length);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (batchIdx >= numBatched) {
      if (!loadBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  @Override
  public UnsafeRow getCurrentValue() throws IOException, InterruptedException {
    return rows[batchIdx - 1];
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float) rowsReturned / totalRowCount;
  }

  /**
   * Decodes a batch of values into `rows`. This function is the hot path.
   */
  private boolean loadBatch() throws IOException {
    // no more records left
    if (rowsReturned >= totalRowCount) { return false; }
    checkEndOfRowGroup();

    int num = (int)Math.min(rows.length, totalCountLoadedSoFar - rowsReturned);
    rowsReturned += num;

    if (containsVarLenFields) {
      for (int i = 0; i < rowWriters.length; ++i) {
        rowWriters[i].holder().resetTo(fixedSizeBytes);
      }
    }

    for (int i = 0; i < columnReaders.length; ++i) {
      switch (columnReaders[i].descriptor.getType()) {
        case BOOLEAN:
          decodeBooleanBatch(i, num);
          break;
        case INT32:
          if (originalTypes[i] == OriginalType.DECIMAL) {
            decodeIntAsDecimalBatch(i, num);
          } else {
            decodeIntBatch(i, num);
          }
          break;
        case INT64:
          Preconditions.checkState(originalTypes[i] == null
              || originalTypes[i] == OriginalType.DECIMAL,
              "Unexpected original type: " + originalTypes[i]);
          decodeLongBatch(i, num);
          break;
        case FLOAT:
          decodeFloatBatch(i, num);
          break;
        case DOUBLE:
          decodeDoubleBatch(i, num);
          break;
        case BINARY:
          decodeBinaryBatch(i, num);
          break;
        case FIXED_LEN_BYTE_ARRAY:
          Preconditions.checkState(originalTypes[i] == OriginalType.DECIMAL,
              "Unexpected original type: " + originalTypes[i]);
          decodeFixedLenArrayAsDecimalBatch(i, num);
          break;
        case INT96:
          throw new IOException("Unsupported " + columnReaders[i].descriptor.getType());
      }
      numBatched = num;
      batchIdx = 0;
    }

    // Update the total row lengths if the schema contained variable length. We did not maintain
    // this as we populated the columns.
    if (containsVarLenFields) {
      for (int i = 0; i < numBatched; ++i) {
        rows[i].setTotalSize(rowWriters[i].holder().totalSize());
      }
    }

    return true;
  }

  private void decodeBooleanBatch(int col, int num) throws IOException {
    for (int n = 0; n < num; ++n) {
      if (columnReaders[col].next()) {
        rows[n].setBoolean(col, columnReaders[col].nextBoolean());
      } else {
        rows[n].setNullAt(col);
      }
    }
  }

  private void decodeIntBatch(int col, int num) throws IOException {
    for (int n = 0; n < num; ++n) {
      if (columnReaders[col].next()) {
        rows[n].setInt(col, columnReaders[col].nextInt());
      } else {
        rows[n].setNullAt(col);
      }
    }
  }

  private void decodeIntAsDecimalBatch(int col, int num) throws IOException {
    for (int n = 0; n < num; ++n) {
      if (columnReaders[col].next()) {
        // Since this is stored as an INT, it is always a compact decimal. Just set it as a long.
        rows[n].setLong(col, columnReaders[col].nextInt());
      } else {
        rows[n].setNullAt(col);
      }
    }
  }

  private void decodeLongBatch(int col, int num) throws IOException {
    for (int n = 0; n < num; ++n) {
      if (columnReaders[col].next()) {
        rows[n].setLong(col, columnReaders[col].nextLong());
      } else {
        rows[n].setNullAt(col);
      }
    }
  }

  private void decodeFloatBatch(int col, int num) throws IOException {
    for (int n = 0; n < num; ++n) {
      if (columnReaders[col].next()) {
        rows[n].setFloat(col, columnReaders[col].nextFloat());
      } else {
        rows[n].setNullAt(col);
      }
    }
  }

  private void decodeDoubleBatch(int col, int num) throws IOException {
    for (int n = 0; n < num; ++n) {
      if (columnReaders[col].next()) {
        rows[n].setDouble(col, columnReaders[col].nextDouble());
      } else {
        rows[n].setNullAt(col);
      }
    }
  }

  private void decodeBinaryBatch(int col, int num) throws IOException {
    for (int n = 0; n < num; ++n) {
      if (columnReaders[col].next()) {
        ByteBuffer bytes = columnReaders[col].nextBinary().toByteBuffer();
        int len = bytes.limit() - bytes.position();
        if (originalTypes[col] == OriginalType.UTF8) {
          UTF8String str = UTF8String.fromBytes(bytes.array(), bytes.position(), len);
          rowWriters[n].write(col, str);
        } else {
          rowWriters[n].write(col, bytes.array(), bytes.position(), len);
        }
        rows[n].setNotNullAt(col);
      } else {
        rows[n].setNullAt(col);
      }
    }
  }

  private void decodeFixedLenArrayAsDecimalBatch(int col, int num) throws IOException {
    PrimitiveType type = requestedSchema.getFields().get(col).asPrimitiveType();
    int precision = type.getDecimalMetadata().getPrecision();
    int scale = type.getDecimalMetadata().getScale();
    Preconditions.checkState(precision <= CatalystSchemaConverter.MAX_PRECISION_FOR_INT64(),
        "Unsupported precision.");

    for (int n = 0; n < num; ++n) {
      if (columnReaders[col].next()) {
        Binary v = columnReaders[col].nextBinary();
        // Constructs a `Decimal` with an unscaled `Long` value if possible.
        long unscaled = CatalystRowConverter.binaryToUnscaledLong(v);
        rows[n].setDecimal(col, Decimal.apply(unscaled, precision, scale), precision);
      } else {
        rows[n].setNullAt(col);
      }
    }
  }

  /**
   *
   * Decoder to return values from a single column.
   */
  private static final class ColumnReader {
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
    private boolean useDictionary;

    /**
     * Maximum definition level for this column.
     */
    private final int maxDefLevel;

    /**
     * Repetition/Definition/Value readers.
     */
    private IntIterator repetitionLevelColumn;
    private IntIterator definitionLevelColumn;
    private ValuesReader dataColumn;

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

    public ColumnReader(ColumnDescriptor descriptor, PageReader pageReader)
        throws IOException {
      this.descriptor = descriptor;
      this.pageReader = pageReader;
      this.maxDefLevel = descriptor.getMaxDefinitionLevel();

      DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
      if (dictionaryPage != null) {
        try {
          this.dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
          this.useDictionary = true;
        } catch (IOException e) {
          throw new IOException("could not decode the dictionary for " + descriptor, e);
        }
      } else {
        this.dictionary = null;
        this.useDictionary = false;
      }
      this.totalValueCount = pageReader.getTotalValueCount();
      if (totalValueCount == 0) {
        throw new IOException("totalValueCount == 0");
      }
    }

    /**
     * TODO: Hoist the useDictionary branch to decode*Batch and make the batch page aligned.
     */
    public boolean nextBoolean() {
      if (!useDictionary) {
        return dataColumn.readBoolean();
      } else {
        return dictionary.decodeToBoolean(dataColumn.readValueDictionaryId());
      }
    }

    public int nextInt() {
      if (!useDictionary) {
        return dataColumn.readInteger();
      } else {
        return dictionary.decodeToInt(dataColumn.readValueDictionaryId());
      }
    }

    public long nextLong() {
      if (!useDictionary) {
        return dataColumn.readLong();
      } else {
        return dictionary.decodeToLong(dataColumn.readValueDictionaryId());
      }
    }

    public float nextFloat() {
      if (!useDictionary) {
        return dataColumn.readFloat();
      } else {
        return dictionary.decodeToFloat(dataColumn.readValueDictionaryId());
      }
    }

    public double nextDouble() {
      if (!useDictionary) {
        return dataColumn.readDouble();
      } else {
        return dictionary.decodeToDouble(dataColumn.readValueDictionaryId());
      }
    }

    public Binary nextBinary() {
      if (!useDictionary) {
        return dataColumn.readBytes();
      } else {
        return dictionary.decodeToBinary(dataColumn.readValueDictionaryId());
      }
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

    private void readPage() throws IOException {
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

    private void initDataReader(Encoding dataEncoding, byte[] bytes, int offset, int valueCount)
        throws IOException {
      this.pageValueCount = valueCount;
      this.endOfPageValueCount = valuesRead + pageValueCount;
      if (dataEncoding.usesDictionary()) {
        if (dictionary == null) {
          throw new IOException(
              "could not read page in col " + descriptor +
                  " as the dictionary was missing for encoding " + dataEncoding);
        }
        this.dataColumn = dataEncoding.getDictionaryBasedValuesReader(
            descriptor, VALUES, dictionary);
        this.useDictionary = true;
      } else {
        this.dataColumn = dataEncoding.getValuesReader(descriptor, VALUES);
        this.useDictionary = false;
      }

      try {
        dataColumn.initFromPage(pageValueCount, bytes, offset);
      } catch (IOException e) {
        throw new IOException("could not read page in col " + descriptor, e);
      }
    }

    private void readPageV1(DataPageV1 page) throws IOException {
      ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
      ValuesReader dlReader = page.getDlEncoding().getValuesReader(descriptor, DEFINITION_LEVEL);
      this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
      this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
      try {
        byte[] bytes = page.getBytes().toByteArray();
        rlReader.initFromPage(pageValueCount, bytes, 0);
        int next = rlReader.getNextOffset();
        dlReader.initFromPage(pageValueCount, bytes, next);
        next = dlReader.getNextOffset();
        initDataReader(page.getValueEncoding(), bytes, next, page.getValueCount());
      } catch (IOException e) {
        throw new IOException("could not read page " + page + " in col " + descriptor, e);
      }
    }

    private void readPageV2(DataPageV2 page) throws IOException {
      this.repetitionLevelColumn = createRLEIterator(descriptor.getMaxRepetitionLevel(),
          page.getRepetitionLevels(), descriptor);
      this.definitionLevelColumn = createRLEIterator(descriptor.getMaxDefinitionLevel(),
          page.getDefinitionLevels(), descriptor);
      try {
        initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0,
            page.getValueCount());
      } catch (IOException e) {
        throw new IOException("could not read page " + page + " in col " + descriptor, e);
      }
    }
  }

  private void checkEndOfRowGroup() throws IOException {
    if (rowsReturned != totalCountLoadedSoFar) return;
    PageReadStore pages = reader.readNextRowGroup();
    if (pages == null) {
      throw new IOException("expecting more rows but reached last block. Read "
          + rowsReturned + " out of " + totalRowCount);
    }
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    columnReaders = new ColumnReader[columns.size()];
    for (int i = 0; i < columns.size(); ++i) {
      columnReaders[i] = new ColumnReader(columns.get(i), pages.getPageReader(columns.get(i)));
    }
    totalCountLoadedSoFar += pages.getRowCount();
  }
}
