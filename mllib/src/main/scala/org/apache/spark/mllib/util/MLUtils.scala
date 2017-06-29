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

package org.apache.spark.mllib.util

import scala.annotation.varargs
import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.{MatrixUDT => MLMatrixUDT, VectorUDT => MLVectorUDT}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.BLAS.dot
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.{PartitionwiseSampledRDD, RDD}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.BernoulliCellSampler

/**
 * Helper methods to load, save and pre-process data used in MLLib.
 */
@Since("0.8.0")
object MLUtils extends Logging {

  private[mllib] lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }

  /**
   * Loads labeled data in the LIBSVM format into an RDD[LabeledPoint].
   * The LIBSVM format is a text-based format used by LIBSVM and LIBLINEAR.
   * Each line represents a labeled sparse feature vector using the following format:
   * {{{label index1:value1 index2:value2 ...}}}
   * where the indices are one-based and in ascending order.
   * This method parses each line into a [[org.apache.spark.mllib.regression.LabeledPoint]],
   * where the feature indices are converted to zero-based.
   * @param sc Spark context
   * @param path file or directory path in any Hadoop-supported file system URI
   * @param numFeatures number of features, which will be determined from the input data if a
   *                    nonpositive value is given. This is useful when the dataset is already split
   *                    into multiple files and you want to load them separately, because some
   *                    features may not present in certain files, which leads to inconsistent
   *                    feature dimensions.
   * @param minPartitions min number of partitions
   * @return labeled data stored as an RDD[LabeledPoint]
   */
  @Since("1.0.0")
  def loadLibSVMFile(
      sc: SparkContext,
      path: String,
      numFeatures: Int,
      minPartitions: Int): RDD[LabeledPoint] = {
    val parsed = parseLibSVMFile(sc, path, minPartitions)

    // Determine number of features.
    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      parsed.persist(StorageLevel.MEMORY_ONLY)
      computeNumFeatures(parsed)
    }

    parsed.map { case (label, indices, values) =>
      LabeledPoint(label, Vectors.sparse(d, indices, values))
    }
  }

  private[spark] def computeNumFeatures(rdd: RDD[(Double, Array[Int], Array[Double])]): Int = {
    rdd.map { case (label, indices, values) =>
      indices.lastOption.getOrElse(0)
    }.reduce(math.max) + 1
  }

  private[spark] def parseLibSVMFile(
      sc: SparkContext,
      path: String,
      minPartitions: Int): RDD[(Double, Array[Int], Array[Double])] = {
    sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map(parseLibSVMRecord)
  }

  private[spark] def parseLibSVMRecord(line: String): (Double, Array[Int], Array[Double]) = {
    val items = line.split(' ')
    val label = items.head.toDouble
    val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
      val value = indexAndValue(1).toDouble
      (index, value)
    }.unzip

    // check if indices are one-based and in ascending order
    var previous = -1
    var i = 0
    val indicesLength = indices.length
    while (i < indicesLength) {
      val current = indices(i)
      require(current > previous, s"indices should be one-based and in ascending order;"
        + s""" found current=$current, previous=$previous; line="$line"""")
      previous = current
      i += 1
    }
    (label, indices.toArray, values.toArray)
  }

  /**
   * Loads labeled data in the LIBSVM format into an RDD[LabeledPoint], with the default number of
   * partitions.
   */
  @Since("1.0.0")
  def loadLibSVMFile(
      sc: SparkContext,
      path: String,
      numFeatures: Int): RDD[LabeledPoint] =
    loadLibSVMFile(sc, path, numFeatures, sc.defaultMinPartitions)

  /**
   * Loads binary labeled data in the LIBSVM format into an RDD[LabeledPoint], with number of
   * features determined automatically and the default number of partitions.
   */
  @Since("1.0.0")
  def loadLibSVMFile(sc: SparkContext, path: String): RDD[LabeledPoint] =
    loadLibSVMFile(sc, path, -1)

  /**
   * Save labeled data in LIBSVM format.
   * @param data an RDD of LabeledPoint to be saved
   * @param dir directory to save the data
   * @see `org.apache.spark.mllib.util.MLUtils.loadLibSVMFile`
   */
  @Since("1.0.0")
  def saveAsLibSVMFile(data: RDD[LabeledPoint], dir: String) {
    // TODO: allow to specify label precision and feature precision.
    val dataStr = data.map { case LabeledPoint(label, features) =>
      val sb = new StringBuilder(label.toString)
      features.foreachActive { case (i, v) =>
        sb += ' '
        sb ++= s"${i + 1}:$v"
      }
      sb.mkString
    }
    dataStr.saveAsTextFile(dir)
  }

  /**
   * Loads vectors saved using `RDD[Vector].saveAsTextFile`.
   * @param sc Spark context
   * @param path file or directory path in any Hadoop-supported file system URI
   * @param minPartitions min number of partitions
   * @return vectors stored as an RDD[Vector]
   */
  @Since("1.1.0")
  def loadVectors(sc: SparkContext, path: String, minPartitions: Int): RDD[Vector] =
    sc.textFile(path, minPartitions).map(Vectors.parse)

  /**
   * Loads vectors saved using `RDD[Vector].saveAsTextFile` with the default number of partitions.
   */
  @Since("1.1.0")
  def loadVectors(sc: SparkContext, path: String): RDD[Vector] =
    sc.textFile(path, sc.defaultMinPartitions).map(Vectors.parse)

  /**
   * Loads labeled points saved using `RDD[LabeledPoint].saveAsTextFile`.
   * @param sc Spark context
   * @param path file or directory path in any Hadoop-supported file system URI
   * @param minPartitions min number of partitions
   * @return labeled points stored as an RDD[LabeledPoint]
   */
  @Since("1.1.0")
  def loadLabeledPoints(sc: SparkContext, path: String, minPartitions: Int): RDD[LabeledPoint] =
    sc.textFile(path, minPartitions).map(LabeledPoint.parse)

  /**
   * Loads labeled points saved using `RDD[LabeledPoint].saveAsTextFile` with the default number of
   * partitions.
   */
  @Since("1.1.0")
  def loadLabeledPoints(sc: SparkContext, dir: String): RDD[LabeledPoint] =
    loadLabeledPoints(sc, dir, sc.defaultMinPartitions)

  /**
   * Return a k element array of pairs of RDDs with the first element of each pair
   * containing the training data, a complement of the validation data and the second
   * element, the validation data, containing a unique 1/kth of the data. Where k=numFolds.
   */
  @Since("1.0.0")
  def kFold[T: ClassTag](rdd: RDD[T], numFolds: Int, seed: Int): Array[(RDD[T], RDD[T])] = {
    kFold(rdd, numFolds, seed.toLong)
  }

  /**
   * Version of `kFold()` taking a Long seed.
   */
  @Since("2.0.0")
  def kFold[T: ClassTag](rdd: RDD[T], numFolds: Int, seed: Long): Array[(RDD[T], RDD[T])] = {
    val numFoldsF = numFolds.toFloat
    (1 to numFolds).map { fold =>
      val sampler = new BernoulliCellSampler[T]((fold - 1) / numFoldsF, fold / numFoldsF,
        complement = false)
      val validation = new PartitionwiseSampledRDD(rdd, sampler, true, seed)
      val training = new PartitionwiseSampledRDD(rdd, sampler.cloneComplement(), true, seed)
      (training, validation)
    }.toArray
  }

  /**
   * Returns a new vector with `1.0` (bias) appended to the input vector.
   */
  @Since("1.0.0")
  def appendBias(vector: Vector): Vector = {
    vector match {
      case dv: DenseVector =>
        val inputValues = dv.values
        val inputLength = inputValues.length
        val outputValues = Array.ofDim[Double](inputLength + 1)
        System.arraycopy(inputValues, 0, outputValues, 0, inputLength)
        outputValues(inputLength) = 1.0
        Vectors.dense(outputValues)
      case sv: SparseVector =>
        val inputValues = sv.values
        val inputIndices = sv.indices
        val inputValuesLength = inputValues.length
        val dim = sv.size
        val outputValues = Array.ofDim[Double](inputValuesLength + 1)
        val outputIndices = Array.ofDim[Int](inputValuesLength + 1)
        System.arraycopy(inputValues, 0, outputValues, 0, inputValuesLength)
        System.arraycopy(inputIndices, 0, outputIndices, 0, inputValuesLength)
        outputValues(inputValuesLength) = 1.0
        outputIndices(inputValuesLength) = dim
        Vectors.sparse(dim + 1, outputIndices, outputValues)
      case _ => throw new IllegalArgumentException(s"Do not support vector type ${vector.getClass}")
    }
  }

  /**
   * Converts vector columns in an input Dataset from the [[org.apache.spark.mllib.linalg.Vector]]
   * type to the new [[org.apache.spark.ml.linalg.Vector]] type under the `spark.ml` package.
   * @param dataset input dataset
   * @param cols a list of vector columns to be converted. New vector columns will be ignored. If
   *             unspecified, all old vector columns will be converted except nested ones.
   * @return the input `DataFrame` with old vector columns converted to the new vector type
   */
  @Since("2.0.0")
  @varargs
  def convertVectorColumnsToML(dataset: Dataset[_], cols: String*): DataFrame = {
    val schema = dataset.schema
    val colSet = if (cols.nonEmpty) {
      cols.flatMap { c =>
        val dataType = schema(c).dataType
        if (dataType.getClass == classOf[VectorUDT]) {
          Some(c)
        } else {
          // ignore new vector columns and raise an exception on other column types
          require(dataType.getClass == classOf[MLVectorUDT],
            s"Column $c must be old Vector type to be converted to new type but got $dataType.")
          None
        }
      }.toSet
    } else {
      schema.fields
        .filter(_.dataType.getClass == classOf[VectorUDT])
        .map(_.name)
        .toSet
    }

    if (colSet.isEmpty) {
      return dataset.toDF()
    }

    logWarning("Vector column conversion has serialization overhead. " +
      "Please migrate your datasets and workflows to use the spark.ml package.")

    // TODO: This implementation has performance issues due to unnecessary serialization.
    // TODO: It is better (but trickier) if we can cast the old vector type to new type directly.
    val convertToML = udf { v: Vector => v.asML }
    val exprs = schema.fields.map { field =>
      val c = field.name
      if (colSet.contains(c)) {
        convertToML(col(c)).as(c, field.metadata)
      } else {
        col(c)
      }
    }
    dataset.select(exprs: _*)
  }

  /**
   * Converts vector columns in an input Dataset to the [[org.apache.spark.mllib.linalg.Vector]]
   * type from the new [[org.apache.spark.ml.linalg.Vector]] type under the `spark.ml` package.
   * @param dataset input dataset
   * @param cols a list of vector columns to be converted. Old vector columns will be ignored. If
   *             unspecified, all new vector columns will be converted except nested ones.
   * @return the input `DataFrame` with new vector columns converted to the old vector type
   */
  @Since("2.0.0")
  @varargs
  def convertVectorColumnsFromML(dataset: Dataset[_], cols: String*): DataFrame = {
    val schema = dataset.schema
    val colSet = if (cols.nonEmpty) {
      cols.flatMap { c =>
        val dataType = schema(c).dataType
        if (dataType.getClass == classOf[MLVectorUDT]) {
          Some(c)
        } else {
          // ignore old vector columns and raise an exception on other column types
          require(dataType.getClass == classOf[VectorUDT],
            s"Column $c must be new Vector type to be converted to old type but got $dataType.")
          None
        }
      }.toSet
    } else {
      schema.fields
        .filter(_.dataType.getClass == classOf[MLVectorUDT])
        .map(_.name)
        .toSet
    }

    if (colSet.isEmpty) {
      return dataset.toDF()
    }

    logWarning("Vector column conversion has serialization overhead. " +
      "Please migrate your datasets and workflows to use the spark.ml package.")

    // TODO: This implementation has performance issues due to unnecessary serialization.
    // TODO: It is better (but trickier) if we can cast the new vector type to old type directly.
    val convertFromML = udf { Vectors.fromML _ }
    val exprs = schema.fields.map { field =>
      val c = field.name
      if (colSet.contains(c)) {
        convertFromML(col(c)).as(c, field.metadata)
      } else {
        col(c)
      }
    }
    dataset.select(exprs: _*)
  }

  /**
   * Converts Matrix columns in an input Dataset from the [[org.apache.spark.mllib.linalg.Matrix]]
   * type to the new [[org.apache.spark.ml.linalg.Matrix]] type under the `spark.ml` package.
   * @param dataset input dataset
   * @param cols a list of matrix columns to be converted. New matrix columns will be ignored. If
   *             unspecified, all old matrix columns will be converted except nested ones.
   * @return the input `DataFrame` with old matrix columns converted to the new matrix type
   */
  @Since("2.0.0")
  @varargs
  def convertMatrixColumnsToML(dataset: Dataset[_], cols: String*): DataFrame = {
    val schema = dataset.schema
    val colSet = if (cols.nonEmpty) {
      cols.flatMap { c =>
        val dataType = schema(c).dataType
        if (dataType.getClass == classOf[MatrixUDT]) {
          Some(c)
        } else {
          // ignore new matrix columns and raise an exception on other column types
          require(dataType.getClass == classOf[MLMatrixUDT],
            s"Column $c must be old Matrix type to be converted to new type but got $dataType.")
          None
        }
      }.toSet
    } else {
      schema.fields
        .filter(_.dataType.getClass == classOf[MatrixUDT])
        .map(_.name)
        .toSet
    }

    if (colSet.isEmpty) {
      return dataset.toDF()
    }

    logWarning("Matrix column conversion has serialization overhead. " +
      "Please migrate your datasets and workflows to use the spark.ml package.")

    val convertToML = udf { v: Matrix => v.asML }
    val exprs = schema.fields.map { field =>
      val c = field.name
      if (colSet.contains(c)) {
        convertToML(col(c)).as(c, field.metadata)
      } else {
        col(c)
      }
    }
    dataset.select(exprs: _*)
  }

  /**
   * Converts matrix columns in an input Dataset to the [[org.apache.spark.mllib.linalg.Matrix]]
   * type from the new [[org.apache.spark.ml.linalg.Matrix]] type under the `spark.ml` package.
   * @param dataset input dataset
   * @param cols a list of matrix columns to be converted. Old matrix columns will be ignored. If
   *             unspecified, all new matrix columns will be converted except nested ones.
   * @return the input `DataFrame` with new matrix columns converted to the old matrix type
   */
  @Since("2.0.0")
  @varargs
  def convertMatrixColumnsFromML(dataset: Dataset[_], cols: String*): DataFrame = {
    val schema = dataset.schema
    val colSet = if (cols.nonEmpty) {
      cols.flatMap { c =>
        val dataType = schema(c).dataType
        if (dataType.getClass == classOf[MLMatrixUDT]) {
          Some(c)
        } else {
          // ignore old matrix columns and raise an exception on other column types
          require(dataType.getClass == classOf[MatrixUDT],
            s"Column $c must be new Matrix type to be converted to old type but got $dataType.")
          None
        }
      }.toSet
    } else {
      schema.fields
        .filter(_.dataType.getClass == classOf[MLMatrixUDT])
        .map(_.name)
        .toSet
    }

    if (colSet.isEmpty) {
      return dataset.toDF()
    }

    logWarning("Matrix column conversion has serialization overhead. " +
      "Please migrate your datasets and workflows to use the spark.ml package.")

    val convertFromML = udf { Matrices.fromML _ }
    val exprs = schema.fields.map { field =>
      val c = field.name
      if (colSet.contains(c)) {
        convertFromML(col(c)).as(c, field.metadata)
      } else {
        col(c)
      }
    }
    dataset.select(exprs: _*)
  }


  /**
   * Returns the squared Euclidean distance between two vectors. The following formula will be used
   * if it does not introduce too much numerical error:
   * <pre>
   *   \|a - b\|_2^2 = \|a\|_2^2 + \|b\|_2^2 - 2 a^T b.
   * </pre>
   * When both vector norms are given, this is faster than computing the squared distance directly,
   * especially when one of the vectors is a sparse vector.
   * @param v1 the first vector
   * @param norm1 the norm of the first vector, non-negative
   * @param v2 the second vector
   * @param norm2 the norm of the second vector, non-negative
   * @param precision desired relative precision for the squared distance
   * @return squared distance between v1 and v2 within the specified precision
   */
  private[mllib] def fastSquaredDistance(
      v1: Vector,
      norm1: Double,
      v2: Vector,
      norm2: Double,
      precision: Double = 1e-6): Double = {
    val n = v1.size
    require(v2.size == n)
    require(norm1 >= 0.0 && norm2 >= 0.0)
    val sumSquaredNorm = norm1 * norm1 + norm2 * norm2
    val normDiff = norm1 - norm2
    var sqDist = 0.0
    /*
     * The relative error is
     * <pre>
     * EPSILON * ( \|a\|_2^2 + \|b\\_2^2 + 2 |a^T b|) / ( \|a - b\|_2^2 ),
     * </pre>
     * which is bounded by
     * <pre>
     * 2.0 * EPSILON * ( \|a\|_2^2 + \|b\|_2^2 ) / ( (\|a\|_2 - \|b\|_2)^2 ).
     * </pre>
     * The bound doesn't need the inner product, so we can use it as a sufficient condition to
     * check quickly whether the inner product approach is accurate.
     */
    val precisionBound1 = 2.0 * EPSILON * sumSquaredNorm / (normDiff * normDiff + EPSILON)
    if (precisionBound1 < precision) {
      sqDist = sumSquaredNorm - 2.0 * dot(v1, v2)
    } else if (v1.isInstanceOf[SparseVector] || v2.isInstanceOf[SparseVector]) {
      val dotValue = dot(v1, v2)
      sqDist = math.max(sumSquaredNorm - 2.0 * dotValue, 0.0)
      val precisionBound2 = EPSILON * (sumSquaredNorm + 2.0 * math.abs(dotValue)) /
        (sqDist + EPSILON)
      if (precisionBound2 > precision) {
        sqDist = Vectors.sqdist(v1, v2)
      }
    } else {
      sqDist = Vectors.sqdist(v1, v2)
    }
    sqDist
  }

  /**
   * When `x` is positive and large, computing `math.log(1 + math.exp(x))` will lead to arithmetic
   * overflow. This will happen when `x > 709.78` which is not a very large number.
   * It can be addressed by rewriting the formula into `x + math.log1p(math.exp(-x))` when `x > 0`.
   * @param x a floating-point value as input.
   * @return the result of `math.log(1 + math.exp(x))`.
   */
  private[spark] def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }
}
