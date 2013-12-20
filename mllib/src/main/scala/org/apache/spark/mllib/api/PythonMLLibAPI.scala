import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.clustering._
import org.apache.spark.rdd.RDD
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.DoubleBuffer

class PythonMLLibAPI extends Serializable {
  def deserializeDoubleVector(bytes: Array[Byte]): Array[Double] = {
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
      throw new IllegalArgumentException("Length " + length + "is wrong.")
    }
    val db = bb.asDoubleBuffer()
    val ans = new Array[Double](length.toInt)
    db.get(ans)
    return ans
  }

  def serializeDoubleVector(doubles: Array[Double]): Array[Byte] = {
    val len = doubles.length
    val bytes = new Array[Byte](16 + 8 * len)
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.nativeOrder())
    bb.putLong(1)
    bb.putLong(len)
    val db = bb.asDoubleBuffer()
    db.put(doubles)
    return bytes
  }

  def deserializeDoubleMatrix(bytes: Array[Byte]): Array[Array[Double]] = {
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
      throw new IllegalArgumentException("Size " + rows + "x" + cols + "is wrong.")
    }
    val db = bb.asDoubleBuffer()
    val ans = new Array[Array[Double]](rows.toInt)
    var i = 0
    for (i <- 0 until rows.toInt) {
      ans(i) = new Array[Double](cols.toInt)
      db.get(ans(i))
    }
    return ans
  }

  def serializeDoubleMatrix(doubles: Array[Array[Double]]): Array[Byte] = {
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
    var i = 0
    for (i <- 0 until rows) {
      db.put(doubles(i))
    }
    return bytes
  }

  def trainRegressionModel(trainFunc: (RDD[LabeledPoint], Array[Double]) => GeneralizedLinearModel,
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
    return ret
  }

  def trainLinearRegressionModel(dataBytesJRDD: JavaRDD[Array[Byte]],
      numIterations: Int, stepSize: Double, miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    return trainRegressionModel((data, initialWeights) =>
        LinearRegressionWithSGD.train(data, numIterations, stepSize,
                                      miniBatchFraction, initialWeights),
        dataBytesJRDD, initialWeightsBA)
  }

  def trainLassoModel(dataBytesJRDD: JavaRDD[Array[Byte]], numIterations: Int,
      stepSize: Double, regParam: Double, miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    return trainRegressionModel((data, initialWeights) =>
        LassoWithSGD.train(data, numIterations, stepSize, regParam,
                           miniBatchFraction, initialWeights),
        dataBytesJRDD, initialWeightsBA)
  }

  def trainRidgeModel(dataBytesJRDD: JavaRDD[Array[Byte]], numIterations: Int,
      stepSize: Double, regParam: Double, miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    return trainRegressionModel((data, initialWeights) =>
        RidgeRegressionWithSGD.train(data, numIterations, stepSize, regParam,
                                     miniBatchFraction, initialWeights),
        dataBytesJRDD, initialWeightsBA)
  }

  def trainSVMModel(dataBytesJRDD: JavaRDD[Array[Byte]], numIterations: Int,
      stepSize: Double, regParam: Double, miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    return trainRegressionModel((data, initialWeights) =>
        SVMWithSGD.train(data, numIterations, stepSize, regParam,
                                     miniBatchFraction, initialWeights),
        dataBytesJRDD, initialWeightsBA)
  }

  def trainLogisticRegressionModel(dataBytesJRDD: JavaRDD[Array[Byte]],
      numIterations: Int, stepSize: Double, miniBatchFraction: Double,
      initialWeightsBA: Array[Byte]): java.util.List[java.lang.Object] = {
    return trainRegressionModel((data, initialWeights) =>
        LogisticRegressionWithSGD.train(data, numIterations, stepSize,
                                     miniBatchFraction, initialWeights),
        dataBytesJRDD, initialWeightsBA)
  }

  def trainKMeansModel(dataBytesJRDD: JavaRDD[Array[Byte]], k: Int,
      maxIterations: Int, runs: Int, initializationMode: String):
      java.util.List[java.lang.Object] = {
    val data = dataBytesJRDD.rdd.map(xBytes => deserializeDoubleVector(xBytes))
    val model = KMeans.train(data, k, maxIterations, runs, initializationMode)
    val ret = new java.util.LinkedList[java.lang.Object]()
    ret.add(serializeDoubleMatrix(model.clusterCenters))
    return ret
  }
}
