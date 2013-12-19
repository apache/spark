import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.regression._
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.DoubleBuffer

class PythonMLLibAPI extends Serializable {
  def deserializeDoubleVector(bytes: Array[Byte]): Array[Double] = {
    val packetLength = bytes.length;
    if (packetLength < 16) {
      throw new IllegalArgumentException("Byte array too short.");
    }
    val bb = ByteBuffer.wrap(bytes);
    bb.order(ByteOrder.nativeOrder());
    val magic = bb.getLong();
    if (magic != 1) {
      throw new IllegalArgumentException("Magic " + magic + " is wrong.");
    }
    val length = bb.getLong();
    if (packetLength != 16 + 8 * length) {
      throw new IllegalArgumentException("Length " + length + "is wrong.");
    }
    val db = bb.asDoubleBuffer();
    val ans = new Array[Double](length.toInt);
    db.get(ans);
    return ans;
  }

  def serializeDoubleVector(doubles: Array[Double]): Array[Byte] = {
    val len = doubles.length;
    val bytes = new Array[Byte](16 + 8 * len);
    val bb = ByteBuffer.wrap(bytes);
    bb.order(ByteOrder.nativeOrder());
    bb.putLong(1);
    bb.putLong(len);
    val db = bb.asDoubleBuffer();
    db.put(doubles);
    return bytes;
  }

  def trainLinearRegressionModel(dataBytesJRDD: JavaRDD[Array[Byte]]):
      java.util.List[java.lang.Object] = {
    val data = dataBytesJRDD.rdd.map(x => deserializeDoubleVector(x))
        .map(v => LabeledPoint(v(0), v.slice(1, v.length)));
    val model = LinearRegressionWithSGD.train(data, 222);
    val ret = new java.util.LinkedList[java.lang.Object]();
    ret.add(serializeDoubleVector(model.weights));
    ret.add(model.intercept: java.lang.Double);
    return ret;
  }
}
