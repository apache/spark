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

package org.apache.spark.sql.connector.catalog.functions;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;

import java.io.Serializable;

/**
 * Interface for a function that produces a result value by aggregating over multiple input rows.
 * <p>
 * For each input row, Spark will call the {@link #update} method which should evaluate the row
 * and update the aggregation state. The JVM type of result values produced by
 * {@link #produceResult} must be the type used by Spark's
 * InternalRow API for the {@link DataType SQL data type} returned by {@link #resultType()}.
 * Please refer to class documentation of {@link ScalarFunction} for the mapping between
 * {@link DataType} and the JVM type.
 * <p>
 * All implementations must support partial aggregation by implementing merge so that Spark can
 * partially aggregate and shuffle intermediate results, instead of shuffling all rows for an
 * aggregate. This reduces the impact of data skew and the amount of data shuffled to produce the
 * result.
 * <p>
 * Intermediate aggregation state must be {@link Serializable} so that state produced by parallel
 * tasks can be serialized, shuffled, and then merged to produce a final result.
 *
 * @param <S> the JVM type for the aggregation's intermediate state; must be {@link Serializable}
 * @param <R> the JVM type of result values
 *
 * @since 3.2.0
 */
@Evolving
public interface AggregateFunction<S extends Serializable, R> extends BoundFunction {

  /**
   * Initialize state for an aggregation.
   * <p>
   * This method is called one or more times for every group of values to initialize intermediate
   * aggregation state. More than one intermediate aggregation state variable may be used when the
   * aggregation is run in parallel tasks.
   * <p>
   * Implementations that return null must support null state passed into all other methods.
   *
   * @return a state instance or null
   */
  S newAggregationState();

  /**
   * Update the aggregation state with a new row.
   * <p>
   * This is called for each row in a group to update an intermediate aggregation state.
   *
   * @param state intermediate aggregation state
   * @param input an input row
   * @return updated aggregation state
   */
  S update(S state, InternalRow input);

  /**
   * Merge two partial aggregation states.
   * <p>
   * This is called to merge intermediate aggregation states that were produced by parallel tasks.
   *
   * @param leftState intermediate aggregation state
   * @param rightState intermediate aggregation state
   * @return combined aggregation state
   */
  S merge(S leftState, S rightState);

  /**
   * Produce the aggregation result based on intermediate state.
   *
   * @param state intermediate aggregation state
   * @return a result value
   */
  R produceResult(S state);

}
