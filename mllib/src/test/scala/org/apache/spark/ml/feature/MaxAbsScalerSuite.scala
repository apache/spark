package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row

/**
 * Created by yuhao on 1/26/16.
 */
class MaxAbsScalerSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {
  test("MaxAbsScaler fit basic case") {
    val data = Array(
      Vectors.dense(1, 0, Long.MinValue),
      Vectors.dense(2, 0, 0),
      Vectors.sparse(3, Array(0, 2), Array(3, Long.MaxValue)),
      Vectors.sparse(3, Array(0), Array(1.5)))

    val expected: Array[Vector] = Array(
      Vectors.dense(-5, 0, -5),
      Vectors.dense(0, 0, 0),
      Vectors.sparse(3, Array(0, 2), Array(5, 5)),
      Vectors.sparse(3, Array(0), Array(-2.5)))

    val df = sqlContext.createDataFrame(data.zip(expected)).toDF("features", "expected")
    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaled")

    val model = scaler.fit(df)
    model.transform(df).select("expected", "scaled").collect()
      .foreach { case Row(vector1: Vector, vector2: Vector) =>
      assert(vector1.equals(vector2), "Transformed vector is different with expected.")
    }

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)
  }

  test("MinMaxScaler read/write") {
    val t = new MaxAbsScaler()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    testDefaultReadWrite(t)
  }

  test("MinMaxScalerModel read/write") {
    val instance = new MaxAbsScalerModel(
      "myMaxAbsScalerModel", Vectors.dense(1.0, 10.0))
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.maxAbs === instance.maxAbs)
  }

}
