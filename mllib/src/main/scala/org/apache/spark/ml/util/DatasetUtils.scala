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
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


private[spark] object DatasetUtils {

  private[ml] def checkNonNanValues(colName: String, displayed: String): Column = {
    val casted = col(colName).cast(DoubleType)
    when(casted.isNull || casted.isNaN, raise_error(lit(s"$displayed MUST NOT be Null or NaN")))
      .when(casted === Double.NegativeInfinity || casted === Double.PositiveInfinity,
        raise_error(concat(lit(s"$displayed MUST NOT be Infinity, but got "), casted)))
      .otherwise(casted)
  }

  private[ml] def checkRegressionLabels(labelCol: String): Column = {
    checkNonNanValues(labelCol, "Labels")
  }

  private[ml] def checkClassificationLabels(
    labelCol: String,
    numClasses: Option[Int]): Column = {
    val casted = col(labelCol).cast(DoubleType)
    numClasses match {
      case Some(2) =>
        when(casted.isNull || casted.isNaN, raise_error(lit("Labels MUST NOT be Null or NaN")))
          .when(casted =!= 0 && casted =!= 1,
            raise_error(concat(lit("Labels MUST be in {0, 1}, but got "), casted)))
          .otherwise(casted)

      case _ =>
        val n = numClasses.getOrElse(Int.MaxValue)
        require(0 < n && n <= Int.MaxValue)
        when(casted.isNull || casted.isNaN, raise_error(lit("Labels MUST NOT be Null or NaN")))
          .when(casted < 0 || casted >= n,
            raise_error(concat(lit(s"Labels MUST be in [0, $n), but got "), casted)))
          .when(casted =!= casted.cast(IntegerType),
            raise_error(concat(lit("Labels MUST be Integers, but got "), casted)))
          .otherwise(casted)
    }
  }

  private[ml] def checkNonNegativeWeights(weightCol: String): Column = {
    val casted = col(weightCol).cast(DoubleType)
    when(casted.isNull || casted.isNaN, raise_error(lit("Weights MUST NOT be Null or NaN")))
      .when(casted < 0 || casted === Double.PositiveInfinity,
        raise_error(concat(lit("Weights MUST NOT be Negative or Infinity, but got "), casted)))
      .otherwise(casted)
  }

  private[ml] def checkNonNegativeWeights(weightCol: Option[String]): Column = weightCol match {
    case Some(w) if w.nonEmpty => checkNonNegativeWeights(w)
    case _ => lit(1.0)
  }

  private[ml] def checkNonNanVectors(vectorCol: String): Column = {
    val vecCol = col(vectorCol)
    when(vecCol.isNull, raise_error(lit("Vectors MUST NOT be Null")))
      .when(!validateVector(vecCol),
        raise_error(concat(lit("Vector values MUST NOT be NaN or Infinity, but got "),
          vecCol.cast(StringType))))
      .otherwise(vecCol)
  }

  private lazy val validateVector = udf { vector: Vector =>
    vector match {
      case dv: DenseVector =>
        dv.values.forall(v => !v.isNaN && !v.isInfinity)
      case sv: SparseVector =>
        sv.values.forall(v => !v.isNaN && !v.isInfinity)
    }
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
