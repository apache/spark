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

package org.apache.spark.sql.hive.aggregate;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Row;

/**
 * An example {@link UserDefinedAggregateFunction} to calculate the sum of a
 * {@link org.apache.spark.sql.types.DoubleType} column.
 */
public class MyDoubleSum extends UserDefinedAggregateFunction {

  private StructType _inputDataType;

  private StructType _bufferSchema;

  private DataType _returnDataType;

  public MyDoubleSum() {
    List<StructField> inputFields = new ArrayList<>();
    inputFields.add(DataTypes.createStructField("inputDouble", DataTypes.DoubleType, true));
    _inputDataType = DataTypes.createStructType(inputFields);

    List<StructField> bufferFields = new ArrayList<>();
    bufferFields.add(DataTypes.createStructField("bufferDouble", DataTypes.DoubleType, true));
    _bufferSchema = DataTypes.createStructType(bufferFields);

    _returnDataType = DataTypes.DoubleType;
  }

  @Override public StructType inputSchema() {
    return _inputDataType;
  }

  @Override public StructType bufferSchema() {
    return _bufferSchema;
  }

  @Override public DataType dataType() {
    return _returnDataType;
  }

  @Override public boolean deterministic() {
    return true;
  }

  @Override public void initialize(MutableAggregationBuffer buffer) {
    // The initial value of the sum is null.
    buffer.update(0, null);
  }

  @Override public void update(MutableAggregationBuffer buffer, Row input) {
    // This input Row only has a single column storing the input value in Double.
    // We only update the buffer when the input value is not null.
    if (!input.isNullAt(0)) {
      if (buffer.isNullAt(0)) {
        // If the buffer value (the intermediate result of the sum) is still null,
        // we set the input value to the buffer.
        buffer.update(0, input.getDouble(0));
      } else {
        // Otherwise, we add the input value to the buffer value.
        Double newValue = input.getDouble(0) + buffer.getDouble(0);
        buffer.update(0, newValue);
      }
    }
  }

  @Override public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
    // buffer1 and buffer2 have the same structure.
    // We only update the buffer1 when the input buffer2's value is not null.
    if (!buffer2.isNullAt(0)) {
      if (buffer1.isNullAt(0)) {
        // If the buffer value (intermediate result of the sum) is still null,
        // we set the it as the input buffer's value.
        buffer1.update(0, buffer2.getDouble(0));
      } else {
        // Otherwise, we add the input buffer's value (buffer1) to the mutable
        // buffer's value (buffer2).
        Double newValue = buffer2.getDouble(0) + buffer1.getDouble(0);
        buffer1.update(0, newValue);
      }
    }
  }

  @Override public Object evaluate(Row buffer) {
    if (buffer.isNullAt(0)) {
      // If the buffer value is still null, we return null.
      return null;
    } else {
      // Otherwise, the intermediate sum is the final result.
      return buffer.getDouble(0);
    }
  }
}
