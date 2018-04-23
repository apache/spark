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

package org.apache.spark.ml.util

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.{Vectors, VectorUDT}
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType}


private[spark] object DatasetUtils {

  /**
   * preprocessing the input feature column to Vector
   * @param dataset DataFrame with columns for features
   * @param colName column name for features
   * @return Vector feature column
   */
  @Since("2.4.0")
  def columnToVector(dataset: Dataset[_], colName: String): Column = {
    val featuresDataType = dataset.schema(colName).dataType
    featuresDataType match {
      case _: VectorUDT => col(colName)
      case fdt: ArrayType =>
        val transferUDF = fdt.elementType match {
          case _: FloatType => udf(f = (vector: Seq[Float]) => {
            val featureArray = Array.fill[Double](vector.size)(0.0)
            vector.indices.foreach(idx => featureArray(idx) = vector(idx).toDouble)
            Vectors.dense(featureArray)
          })
          case _: DoubleType => udf((vector: Seq[Double]) => {
            Vectors.dense(vector.toArray)
          })
        }
        transferUDF(col(colName))
    }
  }
}
