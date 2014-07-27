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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.{JavaSparkContext, JavaRDD}
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * The Java stubs necessary for the Python mllib bindings.
 *
 * See python/pyspark/mllib/_common.py for the mutually agreed upon data format.
 */
@DeveloperApi
class PythonMLLibAPI extends Serializable {
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

  private def deserializeDenseVector(bytes: Array[Byte], offset: Int = 0): Vector = {
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

  private def deserializeSparseVector(bytes: Array[Byte], offset: Int = 0): Vector = {
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

  private def serializeDenseVector(doubles: Array[Double]): Array[Byte] = {
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

  private def serializeSparseVector(vector: SparseVector): Array[Byte] = {
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

  private def deserializeDoubleMatrix(bytes: Array[Byte]): Array[Array[Double]] = {
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

  private def serializeDoubleMatrix(doubles: Array[Array[Double]]): Array[Byte] = {
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
    MLUtils.loadLabeledPoints(jsc.sc, path, minPartitions).map(serializeLabeledPoint).toJavaRDD()

  private def trainRegressionModel(
      trainFunc: (RDD[LabeledPoint], Vector) => GeneralizedLinearModel,
      dataBytesJRDD: JavaRDD[Array[Byte]],
      initialWeightsBA: Array[Byte]): java.util.LinkedList[java.lang.Object] = {
    val data = dataBytesJRDD.rdd.map(deserializeLabeledPoint)
    val initialWeights = deserializeDoubleVector(initialWeightsBA)
    val model = trainFunc(data, initialWeights)
    val ret = new java.util.LinkedList[java.lang.Object]()
    ret.add(serializeDoubleVector(model.weights))
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
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    trainRegressionModel(
      (data, initialWeights) =>
        LinearRegressionWithSGD.train(
          data,
          numIterations,
          stepSize,
          miniBatchFraction,
          initialWeights),
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
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    trainRegressionModel(
      (data, initialWeights) =>
        SVMWithSGD.train(
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
   * Java stub for Python mllib LogisticRegressionWithSGD.train()
   */
  def trainLogisticRegressionModelWithSGD(
      dataBytesJRDD: JavaRDD[Array[Byte]],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    trainRegressionModel(
      (data, initialWeights) =>
        LogisticRegressionWithSGD.train(
          data,
          numIterations,
          stepSize,
          miniBatchFraction,
          initialWeights),
      dataBytesJRDD,
      initialWeightsBA)
  }

  /**
   * Java stub for NaiveBayes.train()
   */
  def trainNaiveBayes(
      dataBytesJRDD: JavaRDD[Array[Byte]],
      lambda: Double): java.util.List[java.lang.Object] = {
    val data = dataBytesJRDD.rdd.map(deserializeLabeledPoint)
    val model = NaiveBayes.train(data, lambda)
    val ret = new java.util.LinkedList[java.lang.Object]()
    ret.add(serializeDoubleVector(Vectors.dense(model.labels)))
    ret.add(serializeDoubleVector(Vectors.dense(model.pi)))
    ret.add(serializeDoubleMatrix(model.theta))
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
    val data = dataBytesJRDD.rdd.map(bytes => deserializeDoubleVector(bytes))
    val model = KMeans.train(data, k, maxIterations, runs, initializationMode)
    val ret = new java.util.LinkedList[java.lang.Object]()
    ret.add(serializeDoubleMatrix(model.clusterCenters.map(_.toArray)))
    ret
  }

  /** Unpack a Rating object from an array of bytes */
  private def unpackRating(ratingBytes: Array[Byte]): Rating = {
    val bb = ByteBuffer.wrap(ratingBytes)
    bb.order(ByteOrder.nativeOrder())
    val user = bb.getInt()
    val product = bb.getInt()
    val rating = bb.getDouble()
    new Rating(user, product, rating)
  }

  /** Unpack a tuple of Ints from an array of bytes */
  private[spark] def unpackTuple(tupleBytes: Array[Byte]): (Int, Int) = {
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
  private[spark] def serializeRating(rate: Rating): Array[Byte] = {
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
    val ratings = ratingsBytesJRDD.rdd.map(unpackRating)
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
    val ratings = ratingsBytesJRDD.rdd.map(unpackRating)
    ALS.trainImplicit(ratings, rank, iterations, lambda, blocks, alpha)
  }
}
