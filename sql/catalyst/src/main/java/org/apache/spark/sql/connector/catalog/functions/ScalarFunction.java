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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;

/**
 * Interface for a function that produces a result value for each input row.
 * <p>
 * For each input row, Spark will call a produceResult method that corresponds to the
 * {@link #inputTypes() input data types}. The expected JVM argument types must be the types used by
 * Spark's InternalRow API. If no direct method is found or when not using codegen, Spark will call
 * {@link #produceResult(InternalRow)}.
 * <p>
 * The JVM type of result values produced by this function must be the type used by Spark's
 * InternalRow API for the {@link DataType SQL data type} returned by {@link #resultType()}.
 *
 * @param <R> the JVM type of result values
 */
public interface ScalarFunction<R> extends BoundFunction {

  /**
   * Applies the function to an input row to produce a value.
   *
   * @param input an input row
   * @return a result value
   */
  default R produceResult(InternalRow input) {
    throw new UnsupportedOperationException(
        "Cannot find a compatible ScalarFunction#produceResult");
  }

}
