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

package org.apache.spark.examples.sql;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.*;

public class Spark33084 extends UserDefinedAggregateFunction {
  public StructType inputSchema() {
    return new StructType().add("inputColumn", DataTypes.LongType);
  }

  public StructType bufferSchema() {
    return new StructType()
        .add("sum", DataTypes.LongType)
        .add("count", DataTypes.LongType);
  }

  public DataType dataType() {
    return DataTypes.DoubleType;
  }

  public boolean deterministic() {
    return true;
  }

  public void initialize(MutableAggregationBuffer buffer) {
    buffer.update(0, 0L);
    buffer.update(1, 0L);
  }

  public void update(MutableAggregationBuffer buffer, Row input) {
    if (!input.isNullAt(0)) {
      buffer.update(0, buffer.getLong(0) + input.getLong(0));
      buffer.update(1, buffer.getLong(1) + 1L);
    }
  }

  public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
    buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0));
    buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
  }

  public Object evaluate(Row buffer) {
    return (double) buffer.getLong(0) / (double) buffer.getLong(1);
  }
}
