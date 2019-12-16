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
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Column

// scalastyle:off
@Since("3.0.0")
object functions {
// scalastyle:on

  private[ml] val vector_to_dense_array_udf = udf { v: Vector => v.toArray }

  /**
   * Convert MLlib sparse/dense vectors in a DataFrame into dense arrays.
   *
   * @since 3.0.0
   */
  def vector_to_dense_array(v: Column): Column = vector_to_dense_array_udf(v)
}
