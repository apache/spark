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
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.mllib.linalg.{Vector => OldVector}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf

// scalastyle:off
@Since("3.0.0")
object functions {
// scalastyle:on

  private val vectorToArrayUdf = udf { (vec: Any, dtype: String) => {
      if (dtype != "float64" && dtype != "float32") {
        throw new IllegalArgumentException(
          s"Unsupported dtype: $dtype. Valid values: float64, float32."
        )
      }

      vec match {
        case v: SparseVector =>
          if (dtype == "float32") {
            val data = new Array[Float](v.size)
            v.foreachActive { (index, value) => data(index) = value.toFloat }
            data
          } else {
            v.toArray
          }
        case v: Vector => if (dtype == "float64") v.toArray else v.toArray.map(_.toFloat)
        case v: OldVector => if (dtype == "float64") v.toArray else v.toArray.map(_.toFloat)
        case v => throw new IllegalArgumentException(
          "function vector_to_array requires a non-null input argument and input type must be " +
            "`org.apache.spark.ml.linalg.Vector` or `org.apache.spark.mllib.linalg.Vector`, " +
            s"but got ${ if (v == null) "null" else v.getClass.getName }.")
      }
    }
  }.asNonNullable()

  /**
   * Converts a column of MLlib sparse/dense vectors into a column of dense arrays.
   *
   * @since 3.0.0
   */
  def vector_to_array(v: Column, dtype: String = "float64"): Column =
    vectorToArrayUdf(v, lit(dtype))
}
