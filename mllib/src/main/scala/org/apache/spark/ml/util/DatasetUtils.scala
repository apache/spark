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

import org.apache.spark.ml.linalg._
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType}


private[spark] object DatasetUtils {

  private[ml] def getBinaryLabelCol(labelCol: String) = {
    checkBinaryLabel(col(labelCol).cast(DoubleType))
  }

  private[ml] def getNonNegativeWeightCol(weightCol: Option[String]) = weightCol match {
    case Some(w) if w.nonEmpty => checkNonNegativeWeight(col(w).cast(DoubleType))
    case _ => lit(1.0)
  }

  private[ml] def getNonNanVectorCol(featuresCol: String) = {
    checkNonNanVector(col(featuresCol))
  }

  private def checkBinaryLabel = udf {
    label: Double =>
      require(label == 0 || label == 1,
        s"Labels MUST be in {0, 1}, but got $label")
      label
  }

  private def checkNonNegativeWeight = udf {
    weight: Double =>
      require(weight >= 0 && !weight.isInfinity,
        s"Weights MUST be non-Negative and finite, but got $weight")
      weight
  }

  private def checkNonNanVector = udf {
    vector: Vector =>
      require(vector != null, s"Vector MUST NOT be NULL")
      vector match {
        case dv: DenseVector =>
          require(dv.values.forall(v => !v.isNaN && !v.isInfinity),
            s"Vector values MUST be non-NaN and finite, but got $dv")
        case sv: SparseVector =>
          require(sv.values.forall(v => !v.isNaN && !v.isInfinity),
            s"Vector values MUST be non-NaN and finite, but got $sv")
      }
      vector
  }

  /**
   * Cast a column in a Dataset to Vector type.
   *
   * The supported data types of the input column are
   * - Vector
   * - float/double type Array.
   *
   * Note: The returned column does not have Metadata.
   *
   * @param dataset input DataFrame
   * @param colName column name.
   * @return Vector column
   */
  def columnToVector(dataset: Dataset[_], colName: String): Column = {
    val columnDataType = dataset.schema(colName).dataType
    columnDataType match {
      case _: VectorUDT => col(colName)
      case fdt: ArrayType =>
        val transferUDF = fdt.elementType match {
          case _: FloatType => udf(f = (vector: Seq[Float]) => {
            val inputArray = Array.ofDim[Double](vector.size)
            vector.indices.foreach(idx => inputArray(idx) = vector(idx).toDouble)
            Vectors.dense(inputArray)
          })
          case _: DoubleType => udf((vector: Seq[Double]) => {
            Vectors.dense(vector.toArray)
          })
          case other =>
            throw new IllegalArgumentException(s"Array[$other] column cannot be cast to Vector")
        }
        transferUDF(col(colName))
      case other =>
        throw new IllegalArgumentException(s"$other column cannot be cast to Vector")
    }
  }

  def columnToOldVector(dataset: Dataset[_], colName: String): RDD[OldVector] = {
    dataset.select(columnToVector(dataset, colName))
      .rdd.map {
      case Row(point: Vector) => OldVectors.fromML(point)
    }
  }
}
