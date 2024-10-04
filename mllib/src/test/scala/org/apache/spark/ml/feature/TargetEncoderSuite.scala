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

package org.apache.spark.ml.feature

import scala.collection.immutable.HashMap

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class TargetEncoderSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var data: Seq[Row] = _
  @transient var schema: StructType = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // scalastyle:off
    data = Seq(
      Row(0.0, 3.0, 5.0, 0.0, 1.0/3, 0.0, 1.0/3, 10.0, 40.0, 50.0, 20.0, 42.5, 50.0, 27.5),
      Row(1.0, 4.0, 5.0, 1.0, 2.0/3, 1.0, 1.0/3, 20.0, 50.0, 50.0, 20.0, 50.0, 50.0, 27.5),
      Row(2.0, 3.0, 5.0, 0.0, 1.0/3, 0.0, 1.0/3, 30.0, 60.0, 50.0, 20.0, 57.5, 50.0, 27.5),
      Row(0.0, 4.0, 6.0, 1.0, 1.0/3, 1.0, 2.0/3, 40.0, 40.0, 50.0, 50.0, 42.5, 50.0, 50.0),
      Row(1.0, 3.0, 6.0, 0.0, 2.0/3, 0.0, 2.0/3, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0),
      Row(2.0, 4.0, 6.0, 1.0, 1.0/3, 1.0, 2.0/3, 60.0, 60.0, 50.0, 50.0, 57.5, 50.0, 50.0),
      Row(0.0, 3.0, 7.0, 0.0, 1.0/3, 0.0,   0.0, 70.0, 40.0, 50.0, 70.0, 42.5, 50.0, 60.0),
      Row(1.0, 4.0, 8.0, 1.0, 2.0/3, 1.0,   1.0, 80.0, 50.0, 50.0, 80.0, 50.0, 50.0, 65.0),
      Row(2.0, 3.0, 9.0, 0.0, 1.0/3, 0.0,   0.0, 90.0, 60.0, 50.0, 90.0, 57.5, 50.0, 70.0))
    // scalastyle:on

    schema = StructType(Array(
      StructField("input1", DoubleType),
      StructField("input2", DoubleType),
      StructField("input3", DoubleType),
      StructField("binaryLabel", DoubleType),
      StructField("binaryExpected1", DoubleType),
      StructField("binaryExpected2", DoubleType),
      StructField("binaryExpected3", DoubleType),
      StructField("continuousLabel", DoubleType),
      StructField("continuousExpected1", DoubleType),
      StructField("continuousExpected2", DoubleType),
      StructField("continuousExpected3", DoubleType),
      StructField("smoothingExpected1", DoubleType),
      StructField("smoothingExpected2", DoubleType),
      StructField("smoothingExpected3", DoubleType)))
  }

  test("params") {
    ParamsSuite.checkParams(new TargetEncoder)
  }

  test("TargetEncoder - binary target") {

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("binaryLabel")
      .setTargetType(TargetEncoder.TARGET_BINARY)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val model = encoder.fit(df)

    val expected_encodings = Map(
      "input1" -> Map(0.0 -> 1.0/3, 1.0 -> 2.0/3, 2.0 -> 1.0/3, -1.0 -> 4.0/9),
      "input2" -> Map(3.0 -> 0.0, 4.0 -> 1.0, -1.0 -> 4.0/9),
      "input3" ->
        HashMap(5.0 -> 1.0/3, 6.0 -> 2.0/3, 7.0 -> 0.0, 8.0 -> 1.0, 9.0 -> 0.0, -1.0 -> 4.0/9))

    assert(model.encodings.equals(expected_encodings))

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df.select("input1", "input2", "input3",
        "binaryExpected1", "binaryExpected2", "binaryExpected3"),
      model,
      "output1", "binaryExpected1",
      "output2", "binaryExpected2",
      "output3", "binaryExpected3") {
        case Row(output1: Double, expected1: Double,
                output2: Double, expected2: Double,
                output3: Double, expected3: Double) =>
          assert(output1 === expected1)
          assert(output2 === expected2)
          assert(output3 === expected3)
    }

  }

  test("TargetEncoder - continuous target") {

    val df = spark
      .createDataFrame(sc.parallelize(data), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("continuousLabel")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val model = encoder.fit(df)

    val expected_encodings = Map(
      "input1" -> Map(0.0 -> 40.0, 1.0 -> 50.0, 2.0 -> 60.0, -1.0 -> 50.0),
      "input2" -> Map(3.0 -> 50.0, 4.0 -> 50.0, -1.0 -> 50.0),
      "input3" ->
        HashMap(5.0 -> 20.0, 6.0 -> 50.0, 7.0 -> 70.0, 8.0 -> 80.0, 9.0 -> 90.0, -1.0 -> 50.0))

    assert(model.encodings.equals(expected_encodings))

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df.select("input1", "input2", "input3",
        "continuousExpected1", "continuousExpected2", "continuousExpected3"),
      model,
      "output1", "continuousExpected1",
      "output2", "continuousExpected2",
      "output3", "continuousExpected3") {
        case Row(output1: Double, expected1: Double,
          output2: Double, expected2: Double,
          output3: Double, expected3: Double) =>
            assert(output1 === expected1)
            assert(output2 === expected2)
            assert(output3 === expected3)
        }

  }

  test("TargetEncoder - smoothing") {

    val df = spark
      .createDataFrame(sc.parallelize(data), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("continuousLabel")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))
      .setSmoothing(1)

    val model = encoder.fit(df)

    val expected_encodings = Map(
      "input1" -> Map(0.0 -> 42.5, 1.0 -> 50.0, 2.0 -> 57.5, -1.0 -> 50.0),
      "input2" -> Map(3.0 -> 50.0, 4.0 -> 50.0, -1.0 -> 50.0),
      "input3" ->
        HashMap(5.0 -> 27.5, 6.0 -> 50.0, 7.0 -> 60.0, 8.0 -> 65.0, 9.0 -> 70.0, -1.0 -> 50.0))

    assert(model.encodings.equals(expected_encodings))

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df.select("input1", "input2", "input3",
        "smoothingExpected1", "smoothingExpected2", "smoothingExpected3"),
      model,
      "output1", "smoothingExpected1",
      "output2", "smoothingExpected2",
      "output3", "smoothingExpected3") {
      case Row(output1: Double, expected1: Double,
      output2: Double, expected2: Double,
      output3: Double, expected3: Double) =>
        assert(output1 === expected1)
        assert(output2 === expected2)
        assert(output3 === expected3)
    }

  }

  test("TargetEncoder - unseen value - keep") {

    val df = spark
      .createDataFrame(sc.parallelize(data), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("continuousLabel")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setHandleInvalid(TargetEncoder.KEEP_INVALID)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val model = encoder.fit(df)

    val data_unseen = Row(0.0, 3.0, 10.0, 0.0, 0.0, 0.0, 0.0, 0.0, 40.0, 50.0, 50.0, 0.0, 0.0, 0.0)

    val df_unseen = spark
      .createDataFrame(sc.parallelize(data :+ data_unseen), schema)

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df_unseen.select("input1", "input2", "input3",
        "continuousExpected1", "continuousExpected2", "continuousExpected3"),
      model,
      "output1", "continuousExpected1",
      "output2", "continuousExpected2",
      "output3", "continuousExpected3") {
        case Row(output1: Double, expected1: Double,
          output2: Double, expected2: Double,
          output3: Double, expected3: Double) =>
            assert(output1 === expected1)
            assert(output2 === expected2)
            assert(output3 === expected3)
       }
  }

  test("TargetEncoder - unseen value - error") {

    val df = spark
      .createDataFrame(sc.parallelize(data), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("continuousLabel")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setHandleInvalid(TargetEncoder.ERROR_INVALID)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val model = encoder.fit(df)

    val data_unseen = Row(0.0, 3.0, 10.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                    4.0/9, 4.0/9, 4.0/9, 0.0, 0.0, 0.0)

    val df_unseen = spark
      .createDataFrame(sc.parallelize(data :+ data_unseen), schema)

    val ex = intercept[SparkRuntimeException] {
      val out = model.transform(df_unseen)
      out.show(false)
    }

    assert(ex.isInstanceOf[SparkRuntimeException])
    assert(ex.getMessage.contains("Unseen value 10.0 in feature input3"))

  }

  test("TargetEncoder - missing feature") {

    val df = spark
      .createDataFrame(sc.parallelize(data), schema)
      .drop("continuousLabel")

    val encoder = new TargetEncoder()
      .setLabelCol("binaryLabel")
      .setInputCols(Array("input1", "input2", "input3"))
      .setTargetType(TargetEncoder.TARGET_BINARY)
      .setOutputCols(Array("output1", "output2", "output3"))

    val ex = intercept[SparkException] {
      val model = encoder.fit(df.drop("input3"))
      print(model.encodings)
    }

    assert(ex.isInstanceOf[SparkException])
    assert(ex.getMessage.contains("No column named input3 found on dataset"))
  }

  test("TargetEncoder - wrong data type") {

    val wrong_schema = new StructType(
      schema.map{
        field: StructField => if (field.name != "input3") field
                      else new StructField(field.name, StringType, field.nullable, field.metadata)
      }.toArray)

    val df = spark
      .createDataFrame(sc.parallelize(data), wrong_schema)
      .drop("continuousLabel")

    val encoder = new TargetEncoder()
      .setLabelCol("binaryLabel")
      .setInputCols(Array("input1", "input2", "input3"))
      .setTargetType(TargetEncoder.TARGET_BINARY)
      .setOutputCols(Array("output1", "output2", "output3"))

    val ex = intercept[SparkException] {
      val model = encoder.fit(df)
      print(model.encodings)
    }

    assert(ex.isInstanceOf[SparkException])
    assert(ex.getMessage.contains("Data type for column input3 is StringType"))
  }

  test("TargetEncoder - null value") {

    val df = spark
      .createDataFrame(sc.parallelize(data), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("continuousLabel")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setHandleInvalid(TargetEncoder.ERROR_INVALID)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))


    val data_null = Row(0.0, 3.0, null.asInstanceOf[Integer],
      0.0, 0.0, 0.0, 0.0, 0.0, 4.0/9, 4.0/9, 4.0/9, 0.0, 0.0, 0.0)

    val df_null = spark
      .createDataFrame(sc.parallelize(data :+ data_null), schema)

    val ex = intercept[SparkException] {
      val model = encoder.fit(df_null)
      print(model.encodings)
    }

    assert(ex.isInstanceOf[SparkException])
    assert(ex.getMessage.contains("Null value found in feature input3"))

  }

  test("TargetEncoder - non-indexed categories") {

    val df = spark
      .createDataFrame(sc.parallelize(data), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("binaryLabel")
      .setTargetType(TargetEncoder.TARGET_BINARY)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val data_noindex = Row(0.0, 3.0, 5.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    val df_noindex = spark
      .createDataFrame(sc.parallelize(data :+ data_noindex), schema)

    val ex = intercept[SparkException] {
      val model = encoder.fit(df_noindex)
      print(model.encodings)
    }

    assert(ex.isInstanceOf[SparkException])
    assert(ex.getMessage.contains(
      "Values from column input3 must be indices, but got 5.1"))

  }

  test("TargetEncoder - non-binary labels") {

    val df = spark
      .createDataFrame(sc.parallelize(data), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("binaryLabel")
      .setTargetType(TargetEncoder.TARGET_BINARY)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val data_non_binary = Row(0.0, 3.0, 5.0, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    val df_non_binary = spark
      .createDataFrame(sc.parallelize(data :+ data_non_binary), schema)

    val ex = intercept[SparkException] {
      val model = encoder.fit(df_non_binary)
      print(model.encodings)
    }

    assert(ex.isInstanceOf[SparkException])
    assert(ex.getMessage.contains(
      "Values from column binaryLabel must be binary (0,1) but got 0.1"))

  }

  test("TargetEncoder - R/W single-column") {

    val encoder = new TargetEncoder()
      .setLabelCol("continuousLabel")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setInputCol("input1")
      .setOutputCol("output1")
      .setHandleInvalid(TargetEncoder.ERROR_INVALID)
      .setSmoothing(2)

    testDefaultReadWrite(encoder)

  }

  test("TargetEncoder - R/W multi-column") {

    val encoder = new TargetEncoder()
      .setLabelCol("binaryLabel")
      .setTargetType(TargetEncoder.TARGET_BINARY)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))
      .setHandleInvalid(TargetEncoder.KEEP_INVALID)
      .setSmoothing(1)

    testDefaultReadWrite(encoder)

  }

}