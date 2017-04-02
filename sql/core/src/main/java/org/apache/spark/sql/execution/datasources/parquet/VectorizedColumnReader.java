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

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.ArrayType;
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
   * Maximum repetition level for this column.
   */
  private final int maxRepLevel;

  /**
   * Repetition/Definition/Value readers.
   */
  private SpecificParquetRecordReaderBase.IntIterator repetitionLevelColumn;
  private SpecificParquetRecordReaderBase.IntIterator definitionLevelColumn;
  private ValuesReader dataColumn;

  // Only set if vectorized decoding is true. This is used instead of the row by row decoding
  // with `definitionLevelColumn`.
  private VectorizedRleValuesReader defColumn;

  // Only set when reading complex column.
  private VectorizedRleValuesReader defColumnCopy;

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

  public VectorizedColumnReader(ColumnDescriptor descriptor, PageReader pageReader)
      throws IOException {
    this.descriptor = descriptor;
    this.pageReader = pageReader;
    this.maxDefLevel = descriptor.getMaxDefinitionLevel();
    this.maxRepLevel = descriptor.getMaxRepetitionLevel();

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
  }
  /**
   * Whether this column is the element of a complex column.
   */
  boolean asComplexColElement;

  /**
   * The flag used in constructing nested records. When it is true, the previous status
   * will be reset.
   */
  boolean resetNestedRecord = true;

  /**
   * Reads `total` values from this columnReader into column.
   */
  public void readBatch(int total, ColumnVector column) throws IOException {
    asComplexColElement = column.getParentColumn() != null;
    boolean isRepeatedColumn = maxRepLevel > 0;
    int rowId = 0;
    int repeatedRowId = 0;
    int remaining = total;

    // The number of values to read.
    int num = 0;

    // Stores row ids and offsets during constructing nested records.
    int[] rowIds = new int[maxRepLevel + 2];
    int[] offsets = new int[maxRepLevel + 2];

    // Keeps repetition levels and corresponding repetition counts.
    int[] repetitions = new int[maxRepLevel + 2];

    ColumnVector dictionaryIds = null;
    if (dictionary != null) {
      // SPARK-16334: We only maintain a single dictionary per row batch, so that it can be used to
      // decode all previous dictionary encoded pages if we ever encounter a non-dictionary encoded
      // page.
      dictionaryIds = column.reserveDictionaryIds(total);
    }

    while (true) {
      // Compute the number of values we want to read in this page.
      int leftInPage = (int) (endOfPageValueCount - valuesRead);

      // Stop condition:
      // If we are going to read data in repeated column, the stop condition is that we
      // read `total` repeated columns. Eg., if we want to read 5 records of an array of int column.
      // we can't just read 5 integers. Instead, we have to read the integers until 5 arrays are put
      // into this array column.
      if (isRepeatedColumn) {
        if (repeatedRowId == total) break;
      } else {
        if (remaining == 0) break;
      }

      // Reaching the end of current page.
      if (leftInPage == 0) {
        boolean pageExists = readPage();
        if (!pageExists) {
          if (!resetNestedRecord) {
            insertRepeatedArray(column, rowIds, offsets, repetitions, total, 0);
            resetNestedRecord = true;
            repeatedRowId = rowIds[1];
            if (repeatedRowId == total) break;
          }
          // Should not reach here.
          throw new IOException("Failed to read page. No page exists anymore!");
        }
        leftInPage = (int) (endOfPageValueCount - valuesRead);
      }

      // Determine the number of values to read for this column in the current page.
      if (asComplexColElement) {
        // Using repetition and definition level encodings to construct nested/repeated records.
        // When constructing nested/repeated records, we returns the number of values to read in
        // this page for this column.
        num = constructComplexRecords(column, repetitions, rowIds, offsets, leftInPage, total);
        repeatedRowId = rowIds[1];
      } else {
        // If this column is not a repeated/nested column, just read minimum of remaining values
        // and all values left in the current page.
        num = Math.min(remaining, leftInPage);
      }

      if (isCurrentPageDictionaryEncoded) {
        // Read and decode dictionary ids.
        if (asComplexColElement) {
          int dictionaryCapacity = Math.max(remaining, rowId + num);
          dictionaryIds = column.reserveDictionaryIds(dictionaryCapacity);
        }
        defColumn.readIntegers(
            num, dictionaryIds, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);

        if (column.hasDictionary() || (rowId == 0 &&
            (descriptor.getType() == PrimitiveType.PrimitiveTypeName.INT32 ||
            descriptor.getType() == PrimitiveType.PrimitiveTypeName.INT64 ||
            descriptor.getType() == PrimitiveType.PrimitiveTypeName.FLOAT ||
            descriptor.getType() == PrimitiveType.PrimitiveTypeName.DOUBLE ||
            descriptor.getType() == PrimitiveType.PrimitiveTypeName.BINARY))) {
          // Column vector supports lazy decoding of dictionary values so just set the dictionary.
          // We can't do this if rowId != 0 AND the column doesn't have a dictionary (i.e. some
          // non-dictionary encoded values have already been added).
          column.setDictionary(dictionary);
        } else {
          decodeDictionaryIds(rowId, num, column, dictionaryIds);
        }
      } else {
        if (column.hasDictionary() && rowId != 0) {
          // This batch already has dictionary encoded values but this new page is not. The batch
          // does not support a mix of dictionary and not, so we will decode the dictionary.
          decodeDictionaryIds(0, rowId, column, column.getDictionaryIds());
        }
        column.setDictionary(null);
        if (asComplexColElement) {
          column.reserve(Math.max(remaining, rowId + num));
        }
        switch (descriptor.getType()) {
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
            readFixedLenByteArrayBatch(rowId, num, column, descriptor.getTypeLength());
            break;
          default:
            throw new IOException("Unsupported type: " + descriptor.getType());
        }
      }
      valuesRead += num;
      rowId += num;
      remaining -= num;
    }
  }

  /**
   * Reads `num` values into column, decoding the values from `dictionaryIds` and `dictionary`.
   */
  private void decodeDictionaryIds(int rowId, int num, ColumnVector column,
                                   ColumnVector dictionaryIds) {
    switch (descriptor.getType()) {
      case INT32:
        if (column.dataType() == DataTypes.IntegerType ||
            DecimalType.is32BitDecimalType(column.dataType())) {
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
        } else {
          throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
        }
        break;

      case INT64:
        if (column.dataType() == DataTypes.LongType ||
            DecimalType.is64BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putLong(i, dictionary.decodeToLong(dictionaryIds.getDictId(i)));
            }
          }
        } else {
          throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
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
          for (int i = rowId; i < rowId + num; ++i) {
            // TODO: Convert dictionary of Binaries to dictionary of Longs
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putLong(i, ParquetRowConverter.binaryToSQLTimestamp(v));
            }
          }
        } else {
          throw new UnsupportedOperationException();
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
        if (DecimalType.is32BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putInt(i, (int) ParquetRowConverter.binaryToUnscaledLong(v));
            }
          }
        } else if (DecimalType.is64BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putLong(i, ParquetRowConverter.binaryToUnscaledLong(v));
            }
          }
        } else if (DecimalType.isByteArrayDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putByteArray(i, v.getBytes());
            }
          }
        } else {
          throw new UnsupportedOperationException();
        }
        break;

      default:
        throw new UnsupportedOperationException("Unsupported type: " + descriptor.getType());
    }
  }

  /**
   * For all the read*Batch functions, reads `num` values from this columnReader into column. It
   * is guaranteed that num is smaller than the number of values left in the current page.
   */

  private void readBooleanBatch(int rowId, int num, ColumnVector column) throws IOException {
    assert(column.dataType() == DataTypes.BooleanType);
    defColumn.readBooleans(
        num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
  }

  private void readIntBatch(int rowId, int num, ColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (column.dataType() == DataTypes.IntegerType || column.dataType() == DataTypes.DateType ||
        DecimalType.is32BitDecimalType(column.dataType())) {
      defColumn.readIntegers(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.ByteType) {
      defColumn.readBytes(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.ShortType) {
      defColumn.readShorts(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
    }
  }

  private void readLongBatch(int rowId, int num, ColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    if (column.dataType() == DataTypes.LongType ||
        DecimalType.is64BitDecimalType(column.dataType())) {
      defColumn.readLongs(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unsupported conversion to: " + column.dataType());
    }
  }

  private void readFloatBatch(int rowId, int num, ColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: support implicit cast to double?
    if (column.dataType() == DataTypes.FloatType) {
      defColumn.readFloats(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unsupported conversion to: " + column.dataType());
    }
  }

  private void readDoubleBatch(int rowId, int num, ColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (column.dataType() == DataTypes.DoubleType) {
      defColumn.readDoubles(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
    }
  }

  private void readBinaryBatch(int rowId, int num, ColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    if (column.isArray()) {
      defColumn.readBinarys(num, column, rowId, maxDefLevel, data);
    } else if (column.dataType() == DataTypes.TimestampType) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putLong(rowId + i,
              // Read 12 bytes for INT96
              ParquetRowConverter.binaryToSQLTimestamp(data.readBinary(12)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else {
      throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
    }
  }

  private void readFixedLenByteArrayBatch(int rowId, int num,
                                          ColumnVector column, int arrayLen) throws IOException {
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (DecimalType.is32BitDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putInt(rowId + i,
              (int) ParquetRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else if (DecimalType.is64BitDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putLong(rowId + i,
              ParquetRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else if (DecimalType.isByteArrayDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putByteArray(rowId + i, data.readBinary(arrayLen).getBytes());
        } else {
          column.putNull(rowId + i);
        }
      }
    } else {
      throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
    }
  }

  private boolean readPage() throws IOException {
    DataPage page = pageReader.readPage();
    if (page == null) {
      return false;
    } else {
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
      return true;
    }
  }

  private void initDataReader(Encoding dataEncoding, byte[] bytes, int offset) throws IOException {
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
      dataColumn.initFromPage(pageValueCount, bytes, offset);
    } catch (IOException e) {
      throw new IOException("could not read page in col " + descriptor, e);
    }
  }

  /**
   * Inserts records into parent columns of a column. These parent columns are repeated columns. As
   * the real data are read into the column, we only need to insert array into its repeated columns.
   * @param column The ColumnVector which the data in the page are read into.
   * @param rowIds Mapping between repetition levels and their current row ids for constructing.
   * @param offsets The beginning offsets in columns which we use to construct nested records.
   * @param repetitions Mapping between repetition levels and their corresponding counts.
   * @param total The total number of rows to construct.
   * @param repLevel The current repetition level.
   */
  private void insertRepeatedArray(
      ColumnVector column,
      int[] rowIds,
      int[] offsets,
      int[] repetitions,
      int total,
      int repLevel) throws IOException {
    ColumnVector parentRepeatedColumn = column;
    int curRepLevel = maxRepLevel;
    while (true) {
      parentRepeatedColumn = parentRepeatedColumn.getNearestParentArrayColumn();
      if (parentRepeatedColumn != null) {
        int parentColRepLevel = parentRepeatedColumn.getRepLevel();
        // The current repetition level means the beginning level of the current value. Thus,
        // we only need to insert array into the parent columns whose repetition levels are
        // equal to or more than the given repetition level.
        if (parentColRepLevel >= repLevel) {
          parentRepeatedColumn.reserve(rowIds[curRepLevel] + 1);
          parentRepeatedColumn.putArray(rowIds[curRepLevel],
            offsets[curRepLevel], repetitions[curRepLevel]);

          offsets[curRepLevel] += repetitions[curRepLevel];
          repetitions[curRepLevel] = 0;
          rowIds[curRepLevel]++;

          // Increase the repetition count for parent repetition level as we add a new record.
          if (curRepLevel > 1) {
            repetitions[curRepLevel - 1]++;
          }

          // In vectorization, the most outside repeated element is at the repetition 1.
          if (curRepLevel == 1 && rowIds[curRepLevel] == total) {
            return;
          }
          curRepLevel--;
        } else {
          break;
        }
      } else {
        break;
      }
    }
  }

  /**
   * Finds the outside element of an inner element which is defined as Catalyst DataType,
   * with the specified definition level.
   * @param column The column as the beginning level for looking up the inner element.
   * @param defLevel The specified definition level.
   * @return the column which is the outside group element of the inner element.
   */
  private ColumnVector findInnerElementWithDefLevel(ColumnVector column, int defLevel) {
    while (true) {
      if (column == null) {
        return null;
      }
      ColumnVector parent = column.getParentColumn();
      if (parent != null && parent.getDefLevel() == defLevel) {
        ColumnVector outside = parent.getParentColumn();
        if (outside == null || outside.getDefLevel() < defLevel) {
          return column;
        }
      }
      column = parent;
    }
  }

  /**
   * Finds the outside element of the inner element which is not defined as Catalyst DataType,
   * with the specified definition level.
   * @param column The column as the beginning level for looking up the inner element.
   * @param defLevel The specified definition level.
   * @return the column which is the outside group element of the inner element.
   */
  private ColumnVector findHiddenInnerElementWithDefLevel(ColumnVector column, int defLevel) {
    while (true) {
      if (column == null) {
        return null;
      }
      ColumnVector parent = column.getParentColumn();
      if (parent != null && parent.getDefLevel() <= defLevel) {
        ColumnVector outside = parent.getParentColumn();
        if (outside == null || outside.getDefLevel() < defLevel) {
          return column;
        }
      }
      column = parent;
    }
  }

  /**
   * Checks if the given column is a legacy array in Parquet schema.
   * @param column The column we want to check if it is legacy array.
   * @return whether the given column is a legacy array in Parquet schema.
   */
  private boolean isLegacyArray(ColumnVector column) {
    ColumnVector parent = column.getNearestParentArrayColumn();
    if (parent == null) {
      return false;
    } else if (parent.getRepLevel() <= maxRepLevel && parent.getDefLevel() < maxDefLevel) {
      return true;
    }
    return false;
  }

  /**
   * Inserts a null record at specified column.
   * @param column The ColumnVector which the data in the page are read into.
   * @param rowIds Mapping between repetition levels and their current row ids for constructing.
   * @param repetitions Mapping between repetition levels and their corresponding counts.
   */
  private void insertNullRecord(
      ColumnVector column,
      int[] rowIds,
      int[] repetitions) {
    int repLevel = column.getRepLevel();

    if (repLevel == 0) {
      repLevel = 1;
    }

    rowIds[repLevel] += repetitions[repLevel];
    repetitions[repLevel] = 0;

    column.reserve(rowIds[repLevel] + 1);
    column.putNull(rowIds[repLevel]);
    rowIds[repLevel]++;
  }

  /**
   * Returns the array of repetition level values.
   */
  private int[] getRepetitionLevels() throws IOException {
    int[] repetitions = new int[this.pageValueCount];
    for (int i = 0; i < this.pageValueCount; i++) {
      repetitions[i] = this.repetitionLevelColumn.nextInt();
    }
    return repetitions;
  }

  /**
   * Iterates the values of definition and repetition levels for the values read in the page,
   * and constructs complex records accordingly.
   * @param column The ColumnVector which the data in the page are read into.
   @ @param repetitions Mapping between repetition levels and their counts.
   * @param rowIds Mapping between repetition levels and their current row ids for constructing.
   * @param offsets The beginning offsets in columns which we use to construct nested records.
   * @param leftInPage The number of values can be read in the current page.
   * @param total The total number of rows to construct.
   * @return the number of values needed to read in the current page.
   */
  private int constructComplexRecords(
      ColumnVector column,
      int[] repetitions,
      int[] rowIds,
      int[] offsets,
      int leftInPage,
      int total) throws IOException {
    for (int i = 0; i < leftInPage; i++) {
      int repLevel = repetitionLevelColumn.nextInt();
      int defLevel = definitionLevelColumn.nextInt();

      // If there are previous values and counts needed to be consider.
      if (!resetNestedRecord) {
        // When a new record begins at lower repetition level,
        // we insert array into repeated column.
        if (repLevel < maxRepLevel) {
          insertRepeatedArray(column, rowIds, offsets, repetitions, total, repLevel);
        }
      }
      resetNestedRecord = false;

      // When definition level is less than max definition level,
      // there is a null value.
      if (defLevel < maxDefLevel) {
        int offset = offsets[maxRepLevel];

        // The null value is defined at the root level.
        // Insert a null record.
        if (repLevel == 0 && defLevel == 0) {
          ColumnVector parent = column.getParentColumn();
          if (parent != null && parent.getDefLevel() == maxDefLevel
            && parent.getRepLevel() == maxRepLevel) {
            // A repeated element at root level.
            // E.g., The repeatedPrimitive at the following schema.
            // Going to insert an empty record.
            // messageType: message spark_schema {
            //   optional int32 optionalPrimitive;
            //   required int32 requiredPrimitive;
            //
            //   repeated int32 repeatedPrimitive;
            //
            //   optional group optionalMessage {
            //     optional int32 someId;
            //   }
            //   required group requiredMessage {
            //     optional int32 someId;
            //   }
            //   repeated group repeatedMessage {
            //     optional int32 someId;
            //   }
            // }
            insertRepeatedArray(column, rowIds, offsets, repetitions, total, repLevel);
          } else {
            // Obtain most outside column.
            ColumnVector topColumn = column.getParentColumn();
            while (topColumn.getParentColumn() != null) {
              topColumn = topColumn.getParentColumn();
            }

            insertNullRecord(topColumn, rowIds, repetitions);
          }
          // Move to next offset in max repetition level as we processed the current value.
          offsets[maxRepLevel]++;
          resetNestedRecord = true;
        } else if (isLegacyArray(column) &&
          column.getNearestParentArrayColumn().getDefLevel() == defLevel) {
          // For a legacy array, if a null is defined at the repeated group column, it actually
          // means an element with null value.

          repetitions[maxRepLevel]++;
        } else if (!column.getParentColumn().isArray() &&
          column.getParentColumn().getDefLevel() == defLevel) {
          // A null element defined in the wrapping non-repeated group.
          rowIds[1]++;
        } else {
          // An empty element defined in outside group.
          // E.g., the element in the following schema.
          // messageType: message spark_schema {
          //   required int32 index;
          //   optional group col {
          //     optional float f1;
          //     optional group f2 (LIST) {
          //       repeated group list {
          //         optional boolean element;
          //       }
          //     }
          //   }
          // }
          ColumnVector parent = findInnerElementWithDefLevel(column, defLevel);
          if (parent != null) {
            // Found the group with the same definition level.
            // Insert a null record at definition level.
            // E.g, R=0, D=1 for above schema.
            insertNullRecord(parent, rowIds, repetitions);
            offsets[maxRepLevel]++;
            resetNestedRecord = true;
          } else {
            // Found the group with lower definition level.
            // Insert an empty record.
            // E.g, R=0, D=2 for above schema.
            parent = findHiddenInnerElementWithDefLevel(column, defLevel);
            insertRepeatedArray(column, rowIds, offsets, repetitions, total, repLevel);
            offsets[maxRepLevel]++;
            resetNestedRecord = true;
          }
        }
      } else {
        // Determine the repetition level of non-null values.
        // A new record begins with non-null value.
        if (maxRepLevel == 0) {
          // A required record at root level.
          repetitions[1]++;
          insertRepeatedArray(column, rowIds, offsets, repetitions, total, maxRepLevel - 1);
        } else {
          // Repeated values. We increase repetition count.
          repetitions[maxRepLevel]++;
        }
      }
      // If we have constructed `total` records, return the number of values to read.
      if (rowIds[1] == total) return i + 1;
    }
    // All `leftInPage` values in the current page are needed to read.
    return leftInPage;
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
    try {
      byte[] bytes = page.getBytes().toByteArray();
      rlReader.initFromPage(pageValueCount, bytes, 0);
      int next = rlReader.getNextOffset();
      dlReader.initFromPage(pageValueCount, bytes, next);

      if (asComplexColElement) {
        ValuesReader dlReaderCopy;
        this.defColumnCopy = new VectorizedRleValuesReader(bitWidth);
        dlReaderCopy = this.defColumnCopy;
        this.definitionLevelColumn = new ValuesReaderIntIterator(dlReaderCopy);
        dlReaderCopy.initFromPage(pageValueCount, bytes, next);
      }

      next = dlReader.getNextOffset();
      initDataReader(page.getValueEncoding(), bytes, next);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  private void readPageV2(DataPageV2 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    this.repetitionLevelColumn = createRLEIterator(descriptor.getMaxRepetitionLevel(),
        page.getRepetitionLevels(), descriptor);

    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn = new VectorizedRleValuesReader(bitWidth);
    this.defColumn.initFromBuffer(
        this.pageValueCount, page.getDefinitionLevels().toByteArray());

    if (asComplexColElement) {
      this.defColumnCopy = new VectorizedRleValuesReader(bitWidth);
      this.definitionLevelColumn = new ValuesReaderIntIterator(this.defColumnCopy);
      this.defColumnCopy.initFromBuffer(
        this.pageValueCount, page.getDefinitionLevels().toByteArray());
    }

    try {
      initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }
}
