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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.classification.ClassifierParams
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


private[spark] object DatasetUtils extends Logging {

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

  private[ml] def checkNonNanVectors(vectorCol: Column): Column = {
    when(vectorCol.isNull, raise_error(lit("Vectors MUST NOT be Null")))
      .when(!validateVector(vectorCol),
        raise_error(concat(lit("Vector values MUST NOT be NaN or Infinity, but got "),
          vectorCol.cast(StringType))))
      .otherwise(vectorCol)
  }

  private[ml] def checkNonNanVectors(vectorCol: String): Column = {
    checkNonNanVectors(col(vectorCol))
  }

  private lazy val validateVector = udf { vector: Vector =>
    vector match {
      case dv: DenseVector =>
        dv.values.forall(v => !v.isNaN && !v.isInfinity)
      case sv: SparseVector =>
        sv.values.forall(v => !v.isNaN && !v.isInfinity)
    }
  }

  private[ml] def extractInstances(
      p: PredictorParams,
      df: Dataset[_],
      numClasses: Option[Int] = None): RDD[Instance] = {
    val labelCol = p match {
      case c: ClassifierParams =>
        checkClassificationLabels(c.getLabelCol, numClasses)
      case _ => // TODO: there is no RegressorParams, maybe add it in the future?
        checkRegressionLabels(p.getLabelCol)
    }

    val weightCol = p match {
      case w: HasWeightCol => checkNonNegativeWeights(w.get(w.weightCol))
      case _ => lit(1.0)
    }

    df.select(labelCol, weightCol, checkNonNanVectors(p.getFeaturesCol))
      .rdd.map { case Row(l: Double, w: Double, v: Vector) => Instance(l, w, v) }
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

  /**
   * Get the number of classes.  This looks in column metadata first, and if that is missing,
   * then this assumes classes are indexed 0,1,...,numClasses-1 and computes numClasses
   * by finding the maximum label value.
   *
   * Label validation (ensuring all labels are integers >= 0) needs to be handled elsewhere,
   * such as in `extractLabeledPoints()`.
   *
   * @param dataset  Dataset which contains a column [[labelCol]]
   * @param maxNumClasses  Maximum number of classes allowed when inferred from data.  If numClasses
   *                       is specified in the metadata, then maxNumClasses is ignored.
   * @return  number of classes
   * @throws IllegalArgumentException  if metadata does not specify numClasses, and the
   *                                   actual numClasses exceeds maxNumClasses
   */
  private[ml] def getNumClasses(
      dataset: Dataset[_],
      labelCol: String,
      maxNumClasses: Int = 100): Int = {
    MetadataUtils.getNumClasses(dataset.schema(labelCol)) match {
      case Some(n: Int) => n
      case None =>
        // Get number of classes from dataset itself.
        val maxLabelRow: Array[Row] = dataset
          .select(max(checkClassificationLabels(labelCol, Some(maxNumClasses))))
          .take(1)
        if (maxLabelRow.isEmpty || maxLabelRow(0).get(0) == null) {
          throw new SparkException("ML algorithm was given empty dataset.")
        }
        val maxDoubleLabel: Double = maxLabelRow.head.getDouble(0)
        require((maxDoubleLabel + 1).isValidInt, s"Classifier found max label value =" +
          s" $maxDoubleLabel but requires integers in range [0, ... ${Int.MaxValue})")
        val numClasses = maxDoubleLabel.toInt + 1
        require(numClasses <= maxNumClasses, s"Classifier inferred $numClasses from label values" +
          s" in column $labelCol, but this exceeded the max numClasses ($maxNumClasses) allowed" +
          s" to be inferred from values.  To avoid this error for labels with > $maxNumClasses" +
          s" classes, specify numClasses explicitly in the metadata; this can be done by applying" +
          s" StringIndexer to the label column.")
        logInfo(this.getClass.getCanonicalName + s" inferred $numClasses classes for" +
          s" labelCol=$labelCol since numClasses was not specified in the column metadata.")
        numClasses
    }
  }

  /**
   * Obtain the number of features in a vector column.
   * If no metadata is available, extract it from the dataset.
   */
  private[ml] def getNumFeatures(dataset: Dataset[_], vectorCol: String): Int = {
    MetadataUtils.getNumFeatures(dataset.schema(vectorCol)).getOrElse {
      dataset.select(columnToVector(dataset, vectorCol)).head.getAs[Vector](0).size
    }
  }
}
