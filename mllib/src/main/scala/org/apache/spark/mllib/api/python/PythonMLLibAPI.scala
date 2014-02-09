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
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * The Java stubs necessary for the Python mllib bindings.
 */
class PythonMLLibAPI extends Serializable {
  private def deserializeDoubleVector(bytes: Array[Byte]): Array[Double] = {
    val packetLength = bytes.length
    if (packetLength < 16) {
      throw new IllegalArgumentException("Byte array too short.")
    }
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.nativeOrder())
    val magic = bb.getLong()
    if (magic != 1) {
      throw new IllegalArgumentException("Magic " + magic + " is wrong.")
    }
    val length = bb.getLong()
    if (packetLength != 16 + 8 * length) {
      throw new IllegalArgumentException("Length " + length + " is wrong.")
    }
    val db = bb.asDoubleBuffer()
    val ans = new Array[Double](length.toInt)
    db.get(ans)
    ans
  }

  private def serializeDoubleVector(doubles: Array[Double]): Array[Byte] = {
    val len = doubles.length
    val bytes = new Array[Byte](16 + 8 * len)
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.nativeOrder())
    bb.putLong(1)
    bb.putLong(len)
    val db = bb.asDoubleBuffer()
    db.put(doubles)
    bytes
  }

  private def deserializeDoubleMatrix(bytes: Array[Byte]): Array[Array[Double]] = {
    val packetLength = bytes.length
    if (packetLength < 24) {
      throw new IllegalArgumentException("Byte array too short.")
    }
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.nativeOrder())
    val magic = bb.getLong()
    if (magic != 2) {
      throw new IllegalArgumentException("Magic " + magic + " is wrong.")
    }
    val rows = bb.getLong()
    val cols = bb.getLong()
    if (packetLength != 24 + 8 * rows * cols) {
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
    val bytes = new Array[Byte](24 + 8 * rows * cols)
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.nativeOrder())
    bb.putLong(2)
    bb.putLong(rows)
    bb.putLong(cols)
    val db = bb.asDoubleBuffer()
    for (i <- 0 until rows) {
      db.put(doubles(i))
    }
    bytes
  }

  private def trainRegressionModel(
      trainFunc: (RDD[LabeledPoint], Array[Double]) => GeneralizedLinearModel,
      dataBytesJRDD: JavaRDD[Array[Byte]], initialWeightsBA: Array[Byte]):
      java.util.LinkedList[java.lang.Object] = {
    val data = dataBytesJRDD.rdd.map(xBytes => {
        val x = deserializeDoubleVector(xBytes)
        LabeledPoint(x(0), x.slice(1, x.length))
    })
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
  def trainLinearRegressionModelWithSGD(dataBytesJRDD: JavaRDD[Array[Byte]],
      numIterations: Int, stepSize: Double, miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    trainRegressionModel((data, initialWeights) =>
        LinearRegressionWithSGD.train(data, numIterations, stepSize,
                                      miniBatchFraction, initialWeights),
        dataBytesJRDD, initialWeightsBA)
  }

  /**
   * Java stub for Python mllib LassoWithSGD.train()
   */
  def trainLassoModelWithSGD(dataBytesJRDD: JavaRDD[Array[Byte]], numIterations: Int,
      stepSize: Double, regParam: Double, miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    trainRegressionModel((data, initialWeights) =>
        LassoWithSGD.train(data, numIterations, stepSize, regParam,
                           miniBatchFraction, initialWeights),
        dataBytesJRDD, initialWeightsBA)
  }

  /**
   * Java stub for Python mllib RidgeRegressionWithSGD.train()
   */
  def trainRidgeModelWithSGD(dataBytesJRDD: JavaRDD[Array[Byte]], numIterations: Int,
      stepSize: Double, regParam: Double, miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    trainRegressionModel((data, initialWeights) =>
        RidgeRegressionWithSGD.train(data, numIterations, stepSize, regParam,
                                     miniBatchFraction, initialWeights),
        dataBytesJRDD, initialWeightsBA)
  }

  /**
   * Java stub for Python mllib SVMWithSGD.train()
   */
  def trainSVMModelWithSGD(dataBytesJRDD: JavaRDD[Array[Byte]], numIterations: Int,
      stepSize: Double, regParam: Double, miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    trainRegressionModel((data, initialWeights) =>
        SVMWithSGD.train(data, numIterations, stepSize, regParam,
                                     miniBatchFraction, initialWeights),
        dataBytesJRDD, initialWeightsBA)
  }

  /**
   * Java stub for Python mllib LogisticRegressionWithSGD.train()
   */
  def trainLogisticRegressionModelWithSGD(dataBytesJRDD: JavaRDD[Array[Byte]],
      numIterations: Int, stepSize: Double, miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    trainRegressionModel((data, initialWeights) =>
        LogisticRegressionWithSGD.train(data, numIterations, stepSize,
                                     miniBatchFraction, initialWeights),
        dataBytesJRDD, initialWeightsBA)
  }

  /**
   * Java stub for NaiveBayes.train()
   */
  def trainNaiveBayes(dataBytesJRDD: JavaRDD[Array[Byte]], lambda: Double)
      : java.util.List[java.lang.Object] =
  {
    val data = dataBytesJRDD.rdd.map(xBytes => {
      val x = deserializeDoubleVector(xBytes)
      LabeledPoint(x(0), x.slice(1, x.length))
    })
    val model = NaiveBayes.train(data, lambda)
    val ret = new java.util.LinkedList[java.lang.Object]()
    ret.add(serializeDoubleVector(model.pi))
    ret.add(serializeDoubleMatrix(model.theta))
    ret
  }

  /**
   * Java stub for Python mllib KMeans.train()
   */
  def trainKMeansModel(dataBytesJRDD: JavaRDD[Array[Byte]], k: Int,
      maxIterations: Int, runs: Int, initializationMode: String):
      java.util.List[java.lang.Object] = {
    val data = dataBytesJRDD.rdd.map(xBytes => deserializeDoubleVector(xBytes))
    val model = KMeans.train(data, k, maxIterations, runs, initializationMode)
    val ret = new java.util.LinkedList[java.lang.Object]()
    ret.add(serializeDoubleMatrix(model.clusterCenters))
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
  def trainALSModel(ratingsBytesJRDD: JavaRDD[Array[Byte]], rank: Int,
      iterations: Int, lambda: Double, blocks: Int): MatrixFactorizationModel = {
    val ratings = ratingsBytesJRDD.rdd.map(unpackRating)
    ALS.train(ratings, rank, iterations, lambda, blocks)
  }

  /**
   * Java stub for Python mllib ALS.trainImplicit().  This stub returns a
   * handle to the Java object instead of the content of the Java object.
   * Extra care needs to be taken in the Python code to ensure it gets freed on
   * exit; see the Py4J documentation.
   */
  def trainImplicitALSModel(ratingsBytesJRDD: JavaRDD[Array[Byte]], rank: Int,
      iterations: Int, lambda: Double, blocks: Int, alpha: Double): MatrixFactorizationModel = {
    val ratings = ratingsBytesJRDD.rdd.map(unpackRating)
    ALS.trainImplicit(ratings, rank, iterations, lambda, blocks, alpha)
  }
}
