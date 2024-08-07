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

package org.apache.spark.ml

import org.apache.spark.annotation.Since
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

// scalastyle:off
@Since("3.0.0")
object functions {
// scalastyle:on

  /**
   * Converts a column of MLlib sparse/dense vectors into a column of dense arrays.
   * @param v: the column of MLlib sparse/dense vectors
   * @param dtype: the desired underlying data type in the returned array
   * @return an array&lt;float&gt; if dtype is float32, or array&lt;double&gt; if dtype is float64
   * @since 3.0.0
   */
  def vector_to_array(v: Column, dtype: String = "float64"): Column =
    Column.internalFn("vector_to_array", v, lit(dtype))

  /**
   * Converts a column of array of numeric type into a column of dense vectors in MLlib.
   * @param v: the column of array&lt;NumericType&gt type
   * @return a column of type `org.apache.spark.ml.linalg.Vector`
   * @since 3.1.0
   */
  def array_to_vector(v: Column): Column = Column.internalFn("array_to_vector", v)
}
