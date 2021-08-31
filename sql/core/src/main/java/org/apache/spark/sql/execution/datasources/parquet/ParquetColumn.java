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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

/**
 * Contains necessary information representing a Parquet column, either of primitive or nested type.
 */
final class ParquetColumn {
  private final ParquetType type;
  private final List<ParquetColumn> children;
  private final WritableColumnVector vector;

  /**
   * repetition & definition levels
   * these are allocated only for leaf columns; for non-leaf columns, they simply maintain
   * references to that of the former.
   */
  private WritableColumnVector repetitionLevels;
  private WritableColumnVector definitionLevels;

  /** whether this column is primitive (i.e., leaf column) */
  private final boolean isPrimitive;

  /** reader for this column - only set if 'isPrimitive' is true */
  private VectorizedColumnReader columnReader;

  ParquetColumn(
      ParquetType type,
      WritableColumnVector vector,
      int capacity,
      MemoryMode memoryMode,
      Set<ParquetType> missingColumns) {
    DataType sparkType = type.sparkType();
    if (!sparkType.sameType(vector.dataType())) {
      throw new IllegalArgumentException("Spark type: " + type.sparkType() +
        " doesn't match the type: " + vector.dataType() + " in column vector");
    }

    this.type = type;
    this.vector = vector;
    this.children = new ArrayList<>();
    this.isPrimitive = type.isPrimitive();

    if (missingColumns.contains(type)) {
      vector.setAllNull();
      return;
    }

    if (isPrimitive) {
      repetitionLevels = allocateLevelsVector(capacity, memoryMode);
      definitionLevels = allocateLevelsVector(capacity, memoryMode);
    } else {
      Preconditions.checkArgument(type.children().size() == vector.getNumChildren());
      for (int i = 0; i < type.children().size(); i++) {
        ParquetColumn childColumn = new ParquetColumn(type.children().apply(i),
          vector.getChild(i), capacity, memoryMode, missingColumns);
        children.add(childColumn);

        // only use levels from non-missing child
        if (!childColumn.vector.isAllNull()) {
          this.repetitionLevels = childColumn.repetitionLevels;
          this.definitionLevels = childColumn.definitionLevels;
        }
      }

      // this can happen if all the fields of a struct are missing, in which case we should mark
      // the struct itself as a missing column
      if (repetitionLevels == null) {
        vector.setAllNull();
      }
    }
  }

  /**
   * Returns all the children of this column.
   */
  List<ParquetColumn> getChildren() {
    return children;
  }

  /**
   * Returns all the leaf columns in depth-first order.
   */
  List<ParquetColumn> getLeaves() {
    List<ParquetColumn> result = new ArrayList<>();
    getLeavesHelper(this, result);
    return result;
  }

  /**
   * Assemble this column and calculate collection offsets recursively.
   * This is a no-op for primitive columns.
   */
  void assemble() {
    // nothing to do if the column itself is missing
    if (vector.isAllNull()) return;

    DataType type = this.type.sparkType();
    if (type instanceof ArrayType || type instanceof MapType) {
      for (ParquetColumn child : children) {
        child.assemble();
      }
      calculateCollectionOffsets();
    } else if (type instanceof StructType) {
      for (ParquetColumn child : children) {
        child.assemble();
      }
      calculateStructOffsets();
    }
  }

  void reset() {
    // nothing to do if the column itself is missing
    if (vector.isAllNull()) return;

    vector.reset();
    repetitionLevels.reset();
    definitionLevels.reset();
    for (ParquetColumn childColumn : children) {
      childColumn.reset();
    }
  }

  ParquetType getType() {
    return this.type;
  }

  WritableColumnVector getValueVector() {
    return this.vector;
  }

  WritableColumnVector getRepetitionLevelVector() {
    return this.repetitionLevels;
  }

  WritableColumnVector getDefinitionLevelVector() {
    return this.definitionLevels;
  }

  VectorizedColumnReader getColumnReader() {
    return this.columnReader;
  }

  void setColumnReader(VectorizedColumnReader reader) {
    if (!isPrimitive) {
      throw new IllegalStateException("can't set reader for non-primitive column");
    }
    this.columnReader = reader;
  }

  private static void getLeavesHelper(ParquetColumn column, List<ParquetColumn> coll) {
    if (column.isPrimitive) {
      coll.add(column);
    } else {
      for (ParquetColumn child : column.children) {
        getLeavesHelper(child, coll);
      }
    }
  }

  private void calculateCollectionOffsets() {
    int maxDefinitionLevel = type.definitionLevel();
    int maxElementRepetitionLevel = type.repetitionLevel();

    // There are 4 cases when calculating definition levels:
    //   1. definitionLevel == maxDefinitionLevel
    //     ==> value is defined and not null
    //   2. definitionLevel == maxDefinitionLevel - 1
    //     ==> value is null
    //   3. definitionLevel < maxDefinitionLevel - 1
    //     ==> value doesn't exist since one of its optional parent is null
    //   4. definitionLevel > maxDefinitionLevel
    //     ==> value is a nested element within an array or map
    //
    // `i` is the index over all leaf elements of this array, while `offset` is the index over
    // all top-level elements of this array.
    int rowId = 0;
    for (int i = 0, offset = 0; i < definitionLevels.getElementsAppended();
         i = getNextCollectionStart(maxElementRepetitionLevel, i)) {
      vector.reserve(rowId + 1);
      int definitionLevel = definitionLevels.getInt(i);
      if (definitionLevel == maxDefinitionLevel - 1) {
        // the collection is null
        vector.putNull(rowId++);
      } else if (definitionLevel == maxDefinitionLevel) {
        // collection is defined but empty
        vector.putNotNull(rowId);
        vector.putArray(rowId, offset, 0);
        rowId++;
      } else if (definitionLevel > maxDefinitionLevel) {
        // collection is defined and non-empty: find out how many top element there is till the
        // start of the next array.
        vector.putNotNull(rowId);
        int length = getCollectionSize(maxElementRepetitionLevel, i + 1);
        vector.putArray(rowId, offset, length);
        offset += length;
        rowId++;
      }
    }
    vector.addElementsAppended(rowId);
  }

  private void calculateStructOffsets() {
    int maxRepetitionLevel = type.repetitionLevel();
    int maxDefinitionLevel = type.definitionLevel();

    vector.reserve(definitionLevels.getElementsAppended());
    int rowId = 0;
    int nonnullRowId = 0;
    boolean hasRepetitionLevels = repetitionLevels.getElementsAppended() > 0;
    for (int i = 0; i < definitionLevels.getElementsAppended(); i++) {
      // if repetition level > maxRepetitionLevel, the value is a nested element (e.g., an array
      // element in struct<array<int>>), and we should skip the definition level since it doesn't
      // represent with the struct.
      if (!hasRepetitionLevels || repetitionLevels.getInt(i) <= maxRepetitionLevel) {
        if (definitionLevels.getInt(i) == maxDefinitionLevel - 1) {
          // the struct is null
          vector.putNull(rowId);
          rowId++;
        } else if (definitionLevels.getInt(i) >= maxDefinitionLevel) {
          vector.putNotNull(rowId);
          vector.putStruct(rowId, nonnullRowId);
          rowId++;
          nonnullRowId++;
        }
      }
    }
    vector.addElementsAppended(rowId);
  }

  private static WritableColumnVector allocateLevelsVector(int capacity, MemoryMode memoryMode) {
    switch (memoryMode) {
      case ON_HEAP:
        return new OnHeapColumnVector(capacity, DataTypes.IntegerType);
      case OFF_HEAP:
        return new OffHeapColumnVector(capacity, DataTypes.IntegerType);
      default:
        throw new IllegalArgumentException("Unknown memory mode: " + memoryMode);
    }
  }

  private int getNextCollectionStart(int maxRepetitionLevel, int elementIndex) {
    int idx = elementIndex + 1;
    for (; idx < repetitionLevels.getElementsAppended(); idx++) {
      if (repetitionLevels.getInt(idx) <= maxRepetitionLevel) {
        break;
      }
    }
    return idx;
  }

  private int getCollectionSize(int maxRepetitionLevel, int idx) {
    int size = 1;
    for (; idx < repetitionLevels.getElementsAppended(); idx++) {
      if (repetitionLevels.getInt(idx) <= maxRepetitionLevel) {
        break;
      } else if (repetitionLevels.getInt(idx) <= maxRepetitionLevel + 1) {
        // only count elements which belong to the current collection
        // For instance, suppose we have the following Parquet schema:
        //
        // message schema {                        max rl   max dl
        //   optional group col (LIST) {              0        1
        //     repeated group list {                  1        2
        //       optional group element (LIST) {      1        3
        //         repeated group list {              2        4
        //           required int32 element;          2        4
        //         }
        //       }
        //     }
        //   }
        // }
        //
        // For a list such as: [[[0, 1], [2, 3]], [[4, 5], [6, 7]]], the repetition & definition
        // levels would be:
        //
        // repetition levels: [0, 2, 1, 2, 0, 2, 1, 2]
        // definition levels: [2, 2, 2, 2, 2, 2, 2, 2]
        //
        // when calculating collection size for the outer array, we should only count repetition
        // levels whose value is <= 1 (which is the max repetition level for the inner array)
        size++;
      }
    }
    return size;
  }
}
