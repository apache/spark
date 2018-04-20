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

package test.org.apache.spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * An example {@link UserDefinedAggregateFunction} to calculate a special average value of a
 * {@link org.apache.spark.sql.types.DoubleType} column. This special average value is the sum
 * of the average value of input values and 100.0.
 */
public class MyDoubleAvg extends UserDefinedAggregateFunction {

  private StructType _inputDataType;

  private StructType _bufferSchema;

  private DataType _returnDataType;

  public MyDoubleAvg() {
    List<StructField> inputFields = new ArrayList<>();
    inputFields.add(DataTypes.createStructField("inputDouble", DataTypes.DoubleType, true));
    _inputDataType = DataTypes.createStructType(inputFields);

    // The buffer has two values, bufferSum for storing the current sum and
    // bufferCount for storing the number of non-null input values that have been contributed
    // to the current sum.
    List<StructField> bufferFields = new ArrayList<>();
    bufferFields.add(DataTypes.createStructField("bufferSum", DataTypes.DoubleType, true));
    bufferFields.add(DataTypes.createStructField("bufferCount", DataTypes.LongType, true));
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
    // The initial value of the count is 0.
    buffer.update(1, 0L);
  }

  @Override public void update(MutableAggregationBuffer buffer, Row input) {
    // This input Row only has a single column storing the input value in Double.
    // We only update the buffer when the input value is not null.
    if (!input.isNullAt(0)) {
      // If the buffer value (the intermediate result of the sum) is still null,
      // we set the input value to the buffer and set the bufferCount to 1.
      if (buffer.isNullAt(0)) {
        buffer.update(0, input.getDouble(0));
        buffer.update(1, 1L);
      } else {
        // Otherwise, update the bufferSum and increment bufferCount.
        Double newValue = input.getDouble(0) + buffer.getDouble(0);
        buffer.update(0, newValue);
        buffer.update(1, buffer.getLong(1) + 1L);
      }
    }
  }

  @Override public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
    // buffer1 and buffer2 have the same structure.
    // We only update the buffer1 when the input buffer2's sum value is not null.
    if (!buffer2.isNullAt(0)) {
      if (buffer1.isNullAt(0)) {
        // If the buffer value (intermediate result of the sum) is still null,
        // we set the it as the input buffer's value.
        buffer1.update(0, buffer2.getDouble(0));
        buffer1.update(1, buffer2.getLong(1));
      } else {
        // Otherwise, we update the bufferSum and bufferCount.
        Double newValue = buffer2.getDouble(0) + buffer1.getDouble(0);
        buffer1.update(0, newValue);
        buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
      }
    }
  }

  @Override public Object evaluate(Row buffer) {
    if (buffer.isNullAt(0)) {
      // If the bufferSum is still null, we return null because this function has not got
      // any input row.
      return null;
    } else {
      // Otherwise, we calculate the special average value.
      return buffer.getDouble(0) / buffer.getLong(1) + 100.0;
    }
  }
}
