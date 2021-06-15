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

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains necessary information representing a Parquet column, either of primitive or nested type.
 */
final class ParquetColumn {
  private final ParquetTypeInfo columnInfo;
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
      ParquetTypeInfo columnInfo,
      WritableColumnVector vector,
      int capacity,
      MemoryMode memoryMode) {
    if (!columnInfo.sparkType().sameType(vector.dataType())) {
      throw new IllegalArgumentException("Spark type: " + columnInfo.sparkType() +
        " doesn't match the type: " + vector.dataType() + " in column vector");
    }
    this.columnInfo = columnInfo;
    this.vector = vector;
    this.children = new ArrayList<>();
    this.isPrimitive = columnInfo.isPrimitive();

    if (isPrimitive) {
      repetitionLevels = allocateLevelsVector(capacity, memoryMode);
      definitionLevels = allocateLevelsVector(capacity, memoryMode);
    } else {
      DataType type = columnInfo.sparkType();
      ParquetGroupTypeInfo groupInfo = (ParquetGroupTypeInfo) columnInfo;
      if (type instanceof ArrayType) {
        ParquetColumn childState = new ParquetColumn(groupInfo.children().apply(0),
          vector.getChild(0), capacity, memoryMode);
        this.repetitionLevels = childState.repetitionLevels;
        this.definitionLevels = childState.definitionLevels;
        children.add(childState);
      } else if (type instanceof MapType) {
        ParquetColumn childState = new ParquetColumn(groupInfo.children().apply(0),
          vector.getChild(0), capacity, memoryMode);
        this.repetitionLevels = childState.repetitionLevels;
        this.definitionLevels = childState.definitionLevels;
        children.add(childState);
        children.add(new ParquetColumn(groupInfo.children().apply(1), vector.getChild(1),
          capacity, memoryMode));
      } else if (type instanceof StructType) {
        for (int i = 0; i < groupInfo.children().length(); i++) {
          ParquetColumn childState = new ParquetColumn(groupInfo.children().apply(i),
            vector.getChild(i), capacity, memoryMode);
          this.repetitionLevels = childState.repetitionLevels;
          this.definitionLevels = childState.definitionLevels;
          children.add(childState);
        }
      }
    }
  }

  /**
   * Get all the leaf columns in depth-first order.
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
    DataType type = columnInfo.sparkType();
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

  ParquetTypeInfo getColumnInfo() {
    return this.columnInfo;
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
      for (ParquetColumn childCol : column.children) {
        getLeavesHelper(childCol, coll);
      }
    }
  }

  private void calculateCollectionOffsets() {
    int maxDefinitionLevel = columnInfo.definitionLevel();
    int maxElementRepetitionLevel = columnInfo.repetitionLevel();

    // `i` is the index over all leaf elements of this array, while `offset` is the index over
    // all top-level elements of this array.
    for (int i = 0, rowId = 0, offset = 0; i < definitionLevels.getElementsAppended();
         i = getNextCollectionStart(maxElementRepetitionLevel, i), rowId++) {
      vector.reserve(rowId + 1);
      int definitionLevel = definitionLevels.getInt(i);
      if (definitionLevel == maxDefinitionLevel - 1) {
        // the collection is null
        vector.putNull(rowId);
      } else if (definitionLevel == maxDefinitionLevel) {
        // collection is defined but empty
        vector.putNotNull(rowId);
        vector.putArray(rowId, offset, 0);
      } else {
        // collection is defined and non-empty: find out how many top element there is till the
        // start of the next array.
        vector.putNotNull(rowId);
        int length = getCollectionSize(maxElementRepetitionLevel, i + 1);
        vector.putArray(rowId, offset, length);
        offset += length;
      }
    }
  }

  private void calculateStructOffsets() {
    int maxDefinitionLevel = columnInfo.definitionLevel();
    vector.reserve(definitionLevels.getElementsAppended());
    for (int i = 0, rowId = 0; i < definitionLevels.getElementsAppended(); i++, rowId++) {
      if (definitionLevels.getInt(i) == maxDefinitionLevel - 1) {
        // the struct is null
        vector.putNull(rowId);
      } else {
        vector.putNotNull(rowId);
      }
    }
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
      } else {
        size++;
      }
    }
    return size;
  }
}
