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

package org.apache.spark.mllib.api.python

import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.JavaConverters._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.linalg.{Matrix, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.random.{RandomRDDs => RG}
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.configuration.{Algo, Strategy}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.stat.correlation.CorrelationNames
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * The Java stubs necessary for the Python mllib bindings.
 *
 * See python/pyspark/mllib/_common.py for the mutually agreed upon data format.
 */
@DeveloperApi
class PythonMLLibAPI extends Serializable {


  /**
   * Loads and serializes labeled points saved with `RDD#saveAsTextFile`.
   * @param jsc Java SparkContext
   * @param path file or directory path in any Hadoop-supported file system URI
   * @param minPartitions min number of partitions
   * @return serialized labeled points stored in a JavaRDD of byte array
   */
  def loadLabeledPoints(
      jsc: JavaSparkContext,
      path: String,
      minPartitions: Int): JavaRDD[Array[Byte]] =
    MLUtils.loadLabeledPoints(jsc.sc, path, minPartitions).map(SerDe.serializeLabeledPoint)

  private def trainRegressionModel(
      trainFunc: (RDD[LabeledPoint], Vector) => GeneralizedLinearModel,
      dataBytesJRDD: JavaRDD[Array[Byte]],
      initialWeightsBA: Array[Byte]): java.util.LinkedList[java.lang.Object] = {
    val data = dataBytesJRDD.rdd.map(SerDe.deserializeLabeledPoint)
    val initialWeights = SerDe.deserializeDoubleVector(initialWeightsBA)
    val model = trainFunc(data, initialWeights)
    val ret = new java.util.LinkedList[java.lang.Object]()
    ret.add(SerDe.serializeDoubleVector(model.weights))
    ret.add(model.intercept: java.lang.Double)
    ret
  }

  /**
   * Java stub for Python mllib LinearRegressionWithSGD.train()
   */
  def trainLinearRegressionModelWithSGD(
      dataBytesJRDD: JavaRDD[Array[Byte]],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double,
      initialWeightsBA: Array[Byte], 
      regParam: Double,
      regType: String,
      intercept: Boolean): java.util.List[java.lang.Object] = {
    val lrAlg = new LinearRegressionWithSGD()
    lrAlg.setIntercept(intercept)
    lrAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
    if (regType == "l2") {
      lrAlg.optimizer.setUpdater(new SquaredL2Updater)
    } else if (regType == "l1") {
      lrAlg.optimizer.setUpdater(new L1Updater)
    } else if (regType != "none") {
      throw new java.lang.IllegalArgumentException("Invalid value for 'regType' parameter."
        + " Can only be initialized using the following string values: [l1, l2, none].")
    }
    trainRegressionModel(
      (data, initialWeights) =>
        lrAlg.run(data, initialWeights),
      dataBytesJRDD,
      initialWeightsBA)
  }

  /**
   * Java stub for Python mllib LassoWithSGD.train()
   */
  def trainLassoModelWithSGD(
      dataBytesJRDD: JavaRDD[Array[Byte]],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    trainRegressionModel(
      (data, initialWeights) =>
        LassoWithSGD.train(
          data,
          numIterations,
          stepSize,
          regParam,
          miniBatchFraction,
          initialWeights),
      dataBytesJRDD,
      initialWeightsBA)
  }

  /**
   * Java stub for Python mllib RidgeRegressionWithSGD.train()
   */
  def trainRidgeModelWithSGD(
      dataBytesJRDD: JavaRDD[Array[Byte]],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    trainRegressionModel(
      (data, initialWeights) =>
        RidgeRegressionWithSGD.train(
          data,
          numIterations,
          stepSize,
          regParam,
          miniBatchFraction,
          initialWeights),
      dataBytesJRDD,
      initialWeightsBA)
  }

  /**
   * Java stub for Python mllib SVMWithSGD.train()
   */
  def trainSVMModelWithSGD(
      dataBytesJRDD: JavaRDD[Array[Byte]],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeightsBA: Array[Byte],
      regType: String,
      intercept: Boolean): java.util.List[java.lang.Object] = {
    val SVMAlg = new SVMWithSGD()
    SVMAlg.setIntercept(intercept)
    SVMAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
    if (regType == "l2") {
      SVMAlg.optimizer.setUpdater(new SquaredL2Updater)
    } else if (regType == "l1") {
      SVMAlg.optimizer.setUpdater(new L1Updater)
    } else if (regType != "none") {
      throw new java.lang.IllegalArgumentException("Invalid value for 'regType' parameter."
        + " Can only be initialized using the following string values: [l1, l2, none].")
    }
    trainRegressionModel(
      (data, initialWeights) =>
        SVMAlg.run(data, initialWeights),
      dataBytesJRDD,
      initialWeightsBA)
  }

  /**
   * Java stub for Python mllib LogisticRegressionWithSGD.train()
   */
  def trainLogisticRegressionModelWithSGD(
      dataBytesJRDD: JavaRDD[Array[Byte]],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double,
      initialWeightsBA: Array[Byte],
      regParam: Double,
      regType: String,
      intercept: Boolean): java.util.List[java.lang.Object] = {
    val LogRegAlg = new LogisticRegressionWithSGD()
    LogRegAlg.setIntercept(intercept)
    LogRegAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
    if (regType == "l2") {
      LogRegAlg.optimizer.setUpdater(new SquaredL2Updater)
    } else if (regType == "l1") {
      LogRegAlg.optimizer.setUpdater(new L1Updater)
    } else if (regType != "none") {
      throw new java.lang.IllegalArgumentException("Invalid value for 'regType' parameter."
        + " Can only be initialized using the following string values: [l1, l2, none].")
    }
    trainRegressionModel(
      (data, initialWeights) =>
        LogRegAlg.run(data, initialWeights),
      dataBytesJRDD,
      initialWeightsBA)
  }

  /**
   * Java stub for NaiveBayes.train()
   */
  def trainNaiveBayes(
      dataBytesJRDD: JavaRDD[Array[Byte]],
      lambda: Double): java.util.List[java.lang.Object] = {
    val data = dataBytesJRDD.rdd.map(SerDe.deserializeLabeledPoint)
    val model = NaiveBayes.train(data, lambda)
    val ret = new java.util.LinkedList[java.lang.Object]()
    ret.add(SerDe.serializeDoubleVector(Vectors.dense(model.labels)))
    ret.add(SerDe.serializeDoubleVector(Vectors.dense(model.pi)))
    ret.add(SerDe.serializeDoubleMatrix(model.theta))
    ret
  }

  /**
   * Java stub for Python mllib KMeans.train()
   */
  def trainKMeansModel(
      dataBytesJRDD: JavaRDD[Array[Byte]],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String): java.util.List[java.lang.Object] = {
    val data = dataBytesJRDD.rdd.map(bytes => SerDe.deserializeDoubleVector(bytes))
    val model = KMeans.train(data, k, maxIterations, runs, initializationMode)
    val ret = new java.util.LinkedList[java.lang.Object]()
    ret.add(SerDe.serializeDoubleMatrix(model.clusterCenters.map(_.toArray)))
    ret
  }

  /**
   * Java stub for Python mllib ALS.train().  This stub returns a handle
   * to the Java object instead of the content of the Java object.  Extra care
   * needs to be taken in the Python code to ensure it gets freed on exit; see
   * the Py4J documentation.
   */
  def trainALSModel(
      ratingsBytesJRDD: JavaRDD[Array[Byte]],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int): MatrixFactorizationModel = {
    val ratings = ratingsBytesJRDD.rdd.map(SerDe.unpackRating)
    ALS.train(ratings, rank, iterations, lambda, blocks)
  }

  /**
   * Java stub for Python mllib ALS.trainImplicit().  This stub returns a
   * handle to the Java object instead of the content of the Java object.
   * Extra care needs to be taken in the Python code to ensure it gets freed on
   * exit; see the Py4J documentation.
   */
  def trainImplicitALSModel(
      ratingsBytesJRDD: JavaRDD[Array[Byte]],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int,
      alpha: Double): MatrixFactorizationModel = {
    val ratings = ratingsBytesJRDD.rdd.map(SerDe.unpackRating)
    ALS.trainImplicit(ratings, rank, iterations, lambda, blocks, alpha)
  }

  /**
   * Java stub for Python mllib DecisionTree.train().
   * This stub returns a handle to the Java object instead of the content of the Java object.
   * Extra care needs to be taken in the Python code to ensure it gets freed on exit;
   * see the Py4J documentation.
   * @param dataBytesJRDD  Training data
   * @param categoricalFeaturesInfoJMap  Categorical features info, as Java map
   */
  def trainDecisionTreeModel(
      dataBytesJRDD: JavaRDD[Array[Byte]],
      algoStr: String,
      numClasses: Int,
      categoricalFeaturesInfoJMap: java.util.Map[Int, Int],
      impurityStr: String,
      maxDepth: Int,
      maxBins: Int): DecisionTreeModel = {

    val data = dataBytesJRDD.rdd.map(SerDe.deserializeLabeledPoint)

    val algo = Algo.fromString(algoStr)
    val impurity = Impurities.fromString(impurityStr)

    val strategy = new Strategy(
      algo = algo,
      impurity = impurity,
      maxDepth = maxDepth,
      numClassesForClassification = numClasses,
      maxBins = maxBins,
      categoricalFeaturesInfo = categoricalFeaturesInfoJMap.asScala.toMap)

    DecisionTree.train(data, strategy)
  }

  /**
   * Predict the label of the given data point.
   * This is a Java stub for python DecisionTreeModel.predict()
   *
   * @param featuresBytes Serialized feature vector for data point
   * @return predicted label
   */
  def predictDecisionTreeModel(
      model: DecisionTreeModel,
      featuresBytes: Array[Byte]): Double = {
    val features: Vector = SerDe.deserializeDoubleVector(featuresBytes)
    model.predict(features)
  }

  /**
   * Predict the labels of the given data points.
   * This is a Java stub for python DecisionTreeModel.predict()
   *
   * @param dataJRDD A JavaRDD with serialized feature vectors
   * @return JavaRDD of serialized predictions
   */
  def predictDecisionTreeModel(
      model: DecisionTreeModel,
      dataJRDD: JavaRDD[Array[Byte]]): JavaRDD[Array[Byte]] = {
    val data = dataJRDD.rdd.map(xBytes => SerDe.deserializeDoubleVector(xBytes))
    model.predict(data).map(SerDe.serializeDouble)
  }

  /**
   * Java stub for mllib Statistics.colStats(X: RDD[Vector]).
   * TODO figure out return type.
   */
  def colStats(X: JavaRDD[Array[Byte]]): MultivariateStatisticalSummarySerialized = {
    val cStats = Statistics.colStats(X.rdd.map(SerDe.deserializeDoubleVector(_)))
    new MultivariateStatisticalSummarySerialized(cStats)
  }

  /**
   * Java stub for mllib Statistics.corr(X: RDD[Vector], method: String).
   * Returns the correlation matrix serialized into a byte array understood by deserializers in
   * pyspark.
   */
  def corr(X: JavaRDD[Array[Byte]], method: String): Array[Byte] = {
    val inputMatrix = X.rdd.map(SerDe.deserializeDoubleVector(_))
    val result = Statistics.corr(inputMatrix, getCorrNameOrDefault(method))
    SerDe.serializeDoubleMatrix(SerDe.to2dArray(result))
  }

  /**
   * Java stub for mllib Statistics.corr(x: RDD[Double], y: RDD[Double], method: String).
   */
  def corr(x: JavaRDD[Array[Byte]], y: JavaRDD[Array[Byte]], method: String): Double = {
    val xDeser = x.rdd.map(SerDe.deserializeDouble(_))
    val yDeser = y.rdd.map(SerDe.deserializeDouble(_))
    Statistics.corr(xDeser, yDeser, getCorrNameOrDefault(method))
  }

  // used by the corr methods to retrieve the name of the correlation method passed in via pyspark
  private def getCorrNameOrDefault(method: String) = {
    if (method == null) CorrelationNames.defaultCorrName else method
  }

  // Used by the *RDD methods to get default seed if not passed in from pyspark
  private def getSeedOrDefault(seed: java.lang.Long): Long = {
    if (seed == null) Utils.random.nextLong else seed
  }

  // Used by *RDD methods to get default numPartitions if not passed in from pyspark
  private def getNumPartitionsOrDefault(numPartitions: java.lang.Integer,
      jsc: JavaSparkContext): Int = {
    if (numPartitions == null) {
      jsc.sc.defaultParallelism
    } else {
      numPartitions
    }
  }

  // Note: for the following methods, numPartitions and seed are boxed to allow nulls to be passed
  // in for either argument from pyspark

  /**
   * Java stub for Python mllib RandomRDDGenerators.uniformRDD()
   */
  def uniformRDD(jsc: JavaSparkContext,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Array[Byte]] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.uniformRDD(jsc.sc, size, parts, s).map(SerDe.serializeDouble)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.normalRDD()
   */
  def normalRDD(jsc: JavaSparkContext,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Array[Byte]] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.normalRDD(jsc.sc, size, parts, s).map(SerDe.serializeDouble)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.poissonRDD()
   */
  def poissonRDD(jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Array[Byte]] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.poissonRDD(jsc.sc, mean, size, parts, s).map(SerDe.serializeDouble)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.uniformVectorRDD()
   */
  def uniformVectorRDD(jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Array[Byte]] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.uniformVectorRDD(jsc.sc, numRows, numCols, parts, s).map(SerDe.serializeDoubleVector)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.normalVectorRDD()
   */
  def normalVectorRDD(jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Array[Byte]] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.normalVectorRDD(jsc.sc, numRows, numCols, parts, s).map(SerDe.serializeDoubleVector)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.poissonVectorRDD()
   */
  def poissonVectorRDD(jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Array[Byte]] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.poissonVectorRDD(jsc.sc, mean, numRows, numCols, parts, s).map(SerDe.serializeDoubleVector)
  }

}

/**
 * :: DeveloperApi ::
 * MultivariateStatisticalSummary with Vector fields serialized.
 */
@DeveloperApi
class MultivariateStatisticalSummarySerialized(val summary: MultivariateStatisticalSummary)
  extends Serializable {

  def mean: Array[Byte] = SerDe.serializeDoubleVector(summary.mean)

  def variance: Array[Byte] = SerDe.serializeDoubleVector(summary.variance)

  def count: Long = summary.count

  def numNonzeros: Array[Byte] = SerDe.serializeDoubleVector(summary.numNonzeros)

  def max: Array[Byte] = SerDe.serializeDoubleVector(summary.max)

  def min: Array[Byte] = SerDe.serializeDoubleVector(summary.min)
}

/**
 * SerDe utility functions for PythonMLLibAPI.
 */
private[spark] object SerDe extends Serializable {
  private val DENSE_VECTOR_MAGIC: Byte = 1
  private val SPARSE_VECTOR_MAGIC: Byte = 2
  private val DENSE_MATRIX_MAGIC: Byte = 3
  private val LABELED_POINT_MAGIC: Byte = 4

  private[python] def deserializeDoubleVector(bytes: Array[Byte], offset: Int = 0): Vector = {
    require(bytes.length - offset >= 5, "Byte array too short")
    val magic = bytes(offset)
    if (magic == DENSE_VECTOR_MAGIC) {
      deserializeDenseVector(bytes, offset)
    } else if (magic == SPARSE_VECTOR_MAGIC) {
      deserializeSparseVector(bytes, offset)
    } else {
      throw new IllegalArgumentException("Magic " + magic + " is wrong.")
    }
  }

  private[python] def deserializeDouble(bytes: Array[Byte], offset: Int = 0): Double = {
    require(bytes.length - offset == 8, "Wrong size byte array for Double")
    val bb = ByteBuffer.wrap(bytes, offset, bytes.length - offset)
    bb.order(ByteOrder.nativeOrder())
    bb.getDouble
  }

  private[python] def deserializeDenseVector(bytes: Array[Byte], offset: Int = 0): Vector = {
    val packetLength = bytes.length - offset
    require(packetLength >= 5, "Byte array too short")
    val bb = ByteBuffer.wrap(bytes, offset, bytes.length - offset)
    bb.order(ByteOrder.nativeOrder())
    val magic = bb.get()
    require(magic == DENSE_VECTOR_MAGIC, "Invalid magic: " + magic)
    val length = bb.getInt()
    require (packetLength == 5 + 8 * length, "Invalid packet length: " + packetLength)
    val db = bb.asDoubleBuffer()
    val ans = new Array[Double](length.toInt)
    db.get(ans)
    Vectors.dense(ans)
  }

  private[python] def deserializeSparseVector(bytes: Array[Byte], offset: Int = 0): Vector = {
    val packetLength = bytes.length - offset
    require(packetLength >= 9, "Byte array too short")
    val bb = ByteBuffer.wrap(bytes, offset, bytes.length - offset)
    bb.order(ByteOrder.nativeOrder())
    val magic = bb.get()
    require(magic == SPARSE_VECTOR_MAGIC, "Invalid magic: " + magic)
    val size = bb.getInt()
    val nonZeros = bb.getInt()
    require (packetLength == 9 + 12 * nonZeros, "Invalid packet length: " + packetLength)
    val ib = bb.asIntBuffer()
    val indices = new Array[Int](nonZeros)
    ib.get(indices)
    bb.position(bb.position() + 4 * nonZeros)
    val db = bb.asDoubleBuffer()
    val values = new Array[Double](nonZeros)
    db.get(values)
    Vectors.sparse(size, indices, values)
  }

  /**
   * Returns an 8-byte array for the input Double.
   *
   * Note: we currently do not use a magic byte for double for storage efficiency.
   * This should be reconsidered when we add Ser/De for other 8-byte types (e.g. Long), for safety.
   * The corresponding deserializer, deserializeDouble, needs to be modified as well if the
   * serialization scheme changes.
   */
  private[python] def serializeDouble(double: Double): Array[Byte] = {
    val bytes = new Array[Byte](8)
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.nativeOrder())
    bb.putDouble(double)
    bytes
  }

  private[python] def serializeDenseVector(doubles: Array[Double]): Array[Byte] = {
    val len = doubles.length
    val bytes = new Array[Byte](5 + 8 * len)
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.nativeOrder())
    bb.put(DENSE_VECTOR_MAGIC)
    bb.putInt(len)
    val db = bb.asDoubleBuffer()
    db.put(doubles)
    bytes
  }

  private[python] def serializeSparseVector(vector: SparseVector): Array[Byte] = {
    val nonZeros = vector.indices.length
    val bytes = new Array[Byte](9 + 12 * nonZeros)
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.nativeOrder())
    bb.put(SPARSE_VECTOR_MAGIC)
    bb.putInt(vector.size)
    bb.putInt(nonZeros)
    val ib = bb.asIntBuffer()
    ib.put(vector.indices)
    bb.position(bb.position() + 4 * nonZeros)
    val db = bb.asDoubleBuffer()
    db.put(vector.values)
    bytes
  }

  private[python] def serializeDoubleVector(vector: Vector): Array[Byte] = vector match {
    case s: SparseVector =>
      serializeSparseVector(s)
    case _ =>
      serializeDenseVector(vector.toArray)
  }

  private[python] def deserializeDoubleMatrix(bytes: Array[Byte]): Array[Array[Double]] = {
    val packetLength = bytes.length
    if (packetLength < 9) {
      throw new IllegalArgumentException("Byte array too short.")
    }
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.nativeOrder())
    val magic = bb.get()
    if (magic != DENSE_MATRIX_MAGIC) {
      throw new IllegalArgumentException("Magic " + magic + " is wrong.")
    }
    val rows = bb.getInt()
    val cols = bb.getInt()
    if (packetLength != 9 + 8 * rows * cols) {
      throw new IllegalArgumentException("Size " + rows + "x" + cols + " is wrong.")
    }
    val db = bb.asDoubleBuffer()
    val ans = new Array[Array[Double]](rows.toInt)
    for (i <- 0 until rows.toInt) {
      ans(i) = new Array[Double](cols.toInt)
      db.get(ans(i))
    }
    ans
  }

  private[python] def serializeDoubleMatrix(doubles: Array[Array[Double]]): Array[Byte] = {
    val rows = doubles.length
    var cols = 0
    if (rows > 0) {
      cols = doubles(0).length
    }
    val bytes = new Array[Byte](9 + 8 * rows * cols)
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.nativeOrder())
    bb.put(DENSE_MATRIX_MAGIC)
    bb.putInt(rows)
    bb.putInt(cols)
    val db = bb.asDoubleBuffer()
    for (i <- 0 until rows) {
      db.put(doubles(i))
    }
    bytes
  }

  private[python] def serializeLabeledPoint(p: LabeledPoint): Array[Byte] = {
    val fb = serializeDoubleVector(p.features)
    val bytes = new Array[Byte](1 + 8 + fb.length)
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.nativeOrder())
    bb.put(LABELED_POINT_MAGIC)
    bb.putDouble(p.label)
    bb.put(fb)
    bytes
  }

  private[python] def deserializeLabeledPoint(bytes: Array[Byte]): LabeledPoint = {
    require(bytes.length >= 9, "Byte array too short")
    val magic = bytes(0)
    if (magic != LABELED_POINT_MAGIC) {
      throw new IllegalArgumentException("Magic " + magic + " is wrong.")
    }
    val labelBytes = ByteBuffer.wrap(bytes, 1, 8)
    labelBytes.order(ByteOrder.nativeOrder())
    val label = labelBytes.asDoubleBuffer().get(0)
    LabeledPoint(label, deserializeDoubleVector(bytes, 9))
  }

  // Reformat a Matrix into Array[Array[Double]] for serialization
  private[python] def to2dArray(matrix: Matrix): Array[Array[Double]] = {
    val values = matrix.toArray
    Array.tabulate(matrix.numRows, matrix.numCols)((i, j) => values(i + j * matrix.numRows))
  }


  /** Unpack a Rating object from an array of bytes */
  private[python] def unpackRating(ratingBytes: Array[Byte]): Rating = {
    val bb = ByteBuffer.wrap(ratingBytes)
    bb.order(ByteOrder.nativeOrder())
    val user = bb.getInt()
    val product = bb.getInt()
    val rating = bb.getDouble()
    new Rating(user, product, rating)
  }

  /** Unpack a tuple of Ints from an array of bytes */
  def unpackTuple(tupleBytes: Array[Byte]): (Int, Int) = {
    val bb = ByteBuffer.wrap(tupleBytes)
    bb.order(ByteOrder.nativeOrder())
    val v1 = bb.getInt()
    val v2 = bb.getInt()
    (v1, v2)
  }

  /**
   * Serialize a Rating object into an array of bytes.
   * It can be deserialized using RatingDeserializer().
   *
   * @param rate the Rating object to serialize
   * @return
   */
  def serializeRating(rate: Rating): Array[Byte] = {
    val len = 3
    val bytes = new Array[Byte](4 + 8 * len)
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.nativeOrder())
    bb.putInt(len)
    val db = bb.asDoubleBuffer()
    db.put(rate.user.toDouble)
    db.put(rate.product.toDouble)
    db.put(rate.rating)
    bytes
  }
}
