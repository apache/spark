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

package test.org.apache.spark.sql.connector.catalog.functions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.AggregateFunction;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public class JavaAverage implements UnboundFunction {
  @Override
  public String name() {
    return "avg";
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.fields().length != 1) {
      throw new UnsupportedOperationException("Expect exactly one argument");
    }
    if (inputType.fields()[0].dataType() instanceof DoubleType) {
      return new JavaDoubleAverage();
    }
    throw new UnsupportedOperationException("Unsupported non-integral type: " +
        inputType.fields()[0].dataType());
  }

  @Override
  public String description() {
    return null;
  }

  public static class JavaDoubleAverage implements AggregateFunction<State<Double>, Double> {
    @Override
    public State<Double> newAggregationState() {
      return new State<>(0.0, 0.0);
    }

    @Override
    public State<Double> update(State<Double> state, InternalRow input) {
      if (input.isNullAt(0)) {
        return state;
      }
      return new State<>(state.sum + input.getDouble(0), state.count + 1);
    }

    @Override
    public Double produceResult(State<Double> state) {
      return state.sum / state.count;
    }

    @Override
    public State<Double> merge(State<Double> leftState, State<Double> rightState) {
      return new State<>(leftState.sum + rightState.sum, leftState.count + rightState.count);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] { DataTypes.DoubleType };
    }

    @Override
    public DataType resultType() {
      return DataTypes.DoubleType;
    }

    @Override
    public String name() {
      return "davg";
    }
  }

  public static class State<T> implements Serializable {
    T sum, count;

    State(T sum, T count) {
      this.sum = sum;
      this.count = count;
    }
  }
}
