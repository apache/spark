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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class TargetEncoderSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var data_binary: Seq[Row] = _
  @transient var data_continuous: Seq[Row] = _
  @transient var schema: StructType = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // scalastyle:off
    data_binary = Seq(
      Row(0.toShort, 3, 5.0, 0.0, 1.0/3, 0.0, 1.0/3, (3.0/4)*(1.0/3)+(1-3.0/4)*(4.0/9),           (1-5.0/6)*(4.0/9), (3.0/4)*(1.0/3)+(1-3.0/4)*(4.0/9)),
      Row(1.toShort, 4, 5.0, 1.0, 2.0/3, 1.0, 1.0/3, (3.0/4)*(2.0/3)+(1-3.0/4)*(4.0/9), (4.0/5)*1+(1-4.0/5)*(4.0/9), (3.0/4)*(1.0/3)+(1-3.0/4)*(4.0/9)),
      Row(2.toShort, 3, 5.0, 0.0, 1.0/3, 0.0, 1.0/3, (3.0/4)*(1.0/3)+(1-3.0/4)*(4.0/9),           (1-5.0/6)*(4.0/9), (3.0/4)*(1.0/3)+(1-3.0/4)*(4.0/9)),
      Row(0.toShort, 4, 6.0, 1.0, 1.0/3, 1.0, 2.0/3, (3.0/4)*(1.0/3)+(1-3.0/4)*(4.0/9), (4.0/5)*1+(1-4.0/5)*(4.0/9), (3.0/4)*(2.0/3)+(1-3.0/4)*(4.0/9)),
      Row(1.toShort, 3, 6.0, 0.0, 2.0/3, 0.0, 2.0/3, (3.0/4)*(2.0/3)+(1-3.0/4)*(4.0/9),           (1-5.0/6)*(4.0/9), (3.0/4)*(2.0/3)+(1-3.0/4)*(4.0/9)),
      Row(2.toShort, 4, 6.0, 1.0, 1.0/3, 1.0, 2.0/3, (3.0/4)*(1.0/3)+(1-3.0/4)*(4.0/9), (4.0/5)*1+(1-4.0/5)*(4.0/9), (3.0/4)*(2.0/3)+(1-3.0/4)*(4.0/9)),
      Row(0.toShort, 3, 7.0, 0.0, 1.0/3, 0.0,   0.0, (3.0/4)*(1.0/3)+(1-3.0/4)*(4.0/9),           (1-5.0/6)*(4.0/9),                 (1-1.0/2)*(4.0/9)),
      Row(1.toShort, 4, 8.0, 1.0, 2.0/3, 1.0,   1.0, (3.0/4)*(2.0/3)+(1-3.0/4)*(4.0/9), (4.0/5)*1+(1-4.0/5)*(4.0/9), (1.0/2)        +(1-1.0/2)*(4.0/9)),
      Row(2.toShort, 3, 9.0, 0.0, 1.0/3, 0.0,   0.0, (3.0/4)*(1.0/3)+(1-3.0/4)*(4.0/9),           (1-5.0/6)*(4.0/9),                 (1-1.0/2)*(4.0/9)))

    data_continuous = Seq(
      Row(0.toShort, 3, 5.0, 10.0, 40.0, 50.0, 20.0, 42.5, 50.0, 27.5),
      Row(1.toShort, 4, 5.0, 20.0, 50.0, 50.0, 20.0, 50.0, 50.0, 27.5),
      Row(2.toShort, 3, 5.0, 30.0, 60.0, 50.0, 20.0, 57.5, 50.0, 27.5),
      Row(0.toShort, 4, 6.0, 40.0, 40.0, 50.0, 50.0, 42.5, 50.0, 50.0),
      Row(1.toShort, 3, 6.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0),
      Row(2.toShort, 4, 6.0, 60.0, 60.0, 50.0, 50.0, 57.5, 50.0, 50.0),
      Row(0.toShort, 3, 7.0, 70.0, 40.0, 50.0, 70.0, 42.5, 50.0, 60.0),
      Row(1.toShort, 4, 8.0, 80.0, 50.0, 50.0, 80.0, 50.0, 50.0, 65.0),
      Row(2.toShort, 3, 9.0, 90.0, 60.0, 50.0, 90.0, 57.5, 50.0, 70.0))
    // scalastyle:on

    schema = StructType(Array(
      StructField("input1", ShortType, nullable = true),
      StructField("input2", IntegerType, nullable = true),
      StructField("input3", DoubleType, nullable = true),
      StructField("label", DoubleType),
      StructField("expected1", DoubleType),
      StructField("expected2", DoubleType),
      StructField("expected3", DoubleType),
      StructField("smoothing1", DoubleType),
      StructField("smoothing2", DoubleType),
      StructField("smoothing3", DoubleType)))
  }

  test("params") {
    ParamsSuite.checkParams(new TargetEncoder)
  }

  test("TargetEncoder - binary target") {

    val df = spark.createDataFrame(sc.parallelize(data_binary), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setTargetType(TargetEncoder.TARGET_BINARY)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val model = encoder.fit(df)

    val expected_stats = Array(
      Map(Some(0.0) -> (3.0, 1.0), Some(1.0) -> (3.0, 2.0), Some(2.0) -> (3.0, 1.0),
        Some(-1.0) -> (9.0, 4.0)),
      Map(Some(3.0) -> (5.0, 0.0), Some(4.0) -> (4.0, 4.0), Some(-1.0) -> (9.0, 4.0)),
      HashMap(Some(5.0) -> (3.0, 1.0), Some(6.0) -> (3.0, 2.0), Some(7.0) -> (1.0, 0.0),
        Some(8.0) -> (1.0, 1.0), Some(9.0) -> (1.0, 0.0), Some(-1.0) -> (9.0, 4.0)))

    model.stats.zip(expected_stats).foreach{
      case (actual, expected) => assert(actual.equals(expected))
    }

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df.select("input1", "input2", "input3",
        "expected1", "expected2", "expected3"),
      model,
      "output1", "expected1",
      "output2", "expected2",
      "output3", "expected3") {
      case Row(output1: Double, expected1: Double,
      output2: Double, expected2: Double,
      output3: Double, expected3: Double) =>
        assert(output1 === expected1)
        assert(output2 === expected2)
        assert(output3 === expected3)
    }

    val model_smooth = model.setSmoothing(1.0)

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df.select("input1", "input2", "input3",
        "smoothing1", "smoothing2", "smoothing3"),
      model_smooth,
      "output1", "smoothing1",
      "output2", "smoothing2",
      "output3", "smoothing3") {
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
      .createDataFrame(sc.parallelize(data_continuous), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val model = encoder.fit(df)

    val expected_stats = Array(
      Map(Some(0.0) -> (3.0, 40.0), Some(1.0) -> (3.0, 50.0), Some(2.0) -> (3.0, 60.0),
        Some(-1.0) -> (9.0, 50.0)),
      Map(Some(3.0) -> (5.0, 50.0), Some(4.0) -> (4.0, 50.0), Some(-1.0) -> (9.0, 50.0)),
      HashMap(Some(5.0) -> (3.0, 20.0), Some(6.0) -> (3.0, 50.0), Some(7.0) -> (1.0, 70.0),
        Some(8.0) -> (1.0, 80.0), Some(9.0) -> (1.0, 90.0), Some(-1.0) -> (9.0, 50.0)))

    model.stats.zip(expected_stats).foreach{
      case (actual, expected) => assert(actual.equals(expected))
    }

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df.select("input1", "input2", "input3",
        "expected1", "expected2", "expected3"),
      model,
      "output1", "expected1",
      "output2", "expected2",
      "output3", "expected3") {
      case Row(output1: Double, expected1: Double,
      output2: Double, expected2: Double,
      output3: Double, expected3: Double) =>
        assert(output1 === expected1)
        assert(output2 === expected2)
        assert(output3 === expected3)
    }

    val model_smooth = model.setSmoothing(1.0)

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df.select("input1", "input2", "input3",
        "smoothing1", "smoothing2", "smoothing3"),
      model_smooth,
      "output1", "smoothing1",
      "output2", "smoothing2",
      "output3", "smoothing3") {
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
      .createDataFrame(sc.parallelize(data_continuous), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setHandleInvalid(TargetEncoder.KEEP_INVALID)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val model = encoder.fit(df)

    val data_unseen = Row(0.toShort, 3, 10.0, 0.0, 40.0, 50.0, 50.0, 0.0, 0.0, 0.0)

    val df_unseen = spark
      .createDataFrame(sc.parallelize(data_continuous :+ data_unseen), schema)

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df_unseen.select("input1", "input2", "input3",
        "expected1", "expected2", "expected3"),
      model,
      "output1", "expected1",
      "output2", "expected2",
      "output3", "expected3") {
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
      .createDataFrame(sc.parallelize(data_continuous), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setHandleInvalid(TargetEncoder.ERROR_INVALID)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val model = encoder.fit(df)

    val data_unseen = Row(0.toShort, 3, 10.0, 0.0, 4.0/9, 4.0/9, 4.0/9, 0.0, 0.0, 0.0)

    val df_unseen = spark
      .createDataFrame(sc.parallelize(data_continuous :+ data_unseen), schema)

    val ex = intercept[SparkRuntimeException] {
      val out = model.transform(df_unseen)
      out.show(false)
    }

    assert(ex.isInstanceOf[SparkRuntimeException])
    assert(ex.getMessage.contains("Unseen value 10.0 in feature input3"))

  }

  test("TargetEncoder - missing feature") {

    val df = spark
      .createDataFrame(sc.parallelize(data_binary), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setInputCols(Array("input1", "input2", "input3"))
      .setTargetType(TargetEncoder.TARGET_BINARY)
      .setOutputCols(Array("output1", "output2", "output3"))

    val ex = intercept[SparkException] {
      val model = encoder.fit(df.drop("input3"))
      print(model.stats)
    }

    assert(ex.isInstanceOf[SparkException])
    assert(ex.getMessage.contains("No column named input3 found on dataset"))
  }

  test("TargetEncoder - wrong data type") {

    val wrong_schema = new StructType(
      schema.map{
        field: StructField => if (field.name != "input3") field
        else StructField(field.name, StringType, field.nullable, field.metadata)
      }.toArray)

    val df = spark
      .createDataFrame(sc.parallelize(data_binary), wrong_schema)

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setInputCols(Array("input1", "input2", "input3"))
      .setTargetType(TargetEncoder.TARGET_BINARY)
      .setOutputCols(Array("output1", "output2", "output3"))

    val ex = intercept[SparkException] {
      val model = encoder.fit(df)
      print(model.stats)
    }

    assert(ex.isInstanceOf[SparkException])
    assert(ex.getMessage.contains("Data type for column input3 is StringType"))
  }

  test("TargetEncoder - seen null category") {

    val data_null = Row(2.toShort, 3, null, 90.0, 60.0, 50.0, 90.0, 57.5, 50.0, 70.0)

    val df_null = spark
      .createDataFrame(sc.parallelize(data_continuous.dropRight(1) :+ data_null), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val model = encoder.fit(df_null)

    val expected_stats = Array(
      Map(Some(0.0) -> (3.0, 40.0), Some(1.0) -> (3.0, 50.0), Some(2.0) -> (3.0, 60.0),
        Some(-1.0) -> (9.0, 50.0)),
      Map(Some(3.0) -> (5.0, 50.0), Some(4.0) -> (4.0, 50.0), Some(-1.0) -> (9.0, 50.0)),
      HashMap(Some(5.0) -> (3.0, 20.0), Some(6.0) -> (3.0, 50.0), Some(7.0) -> (1.0, 70.0),
        Some(8.0) -> (1.0, 80.0), None -> (1.0, 90.0), Some(-1.0) -> (9.0, 50.0)))

    model.stats.zip(expected_stats).foreach{
      case (actual, expected) => assert(actual.equals(expected))
    }

    val output = model.transform(df_null)

    assert_true(
      output("output1") === output("expected1") &&
        output("output2") === output("expected2") &&
        output("output3") === output("expected3"))

  }

  test("TargetEncoder - unseen null category") {

    val df = spark
      .createDataFrame(sc.parallelize(data_continuous), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setHandleInvalid(TargetEncoder.KEEP_INVALID)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val data_null = Row(null, null, null, 90.0, 50.0, 50.0, 50.0, 57.5, 50.0, 70.0)

    val df_null = spark
      .createDataFrame(sc.parallelize(data_continuous :+ data_null), schema)

    val model = encoder.fit(df)

    val output = model.transform(df_null)

    assert_true(
      output("output1") === output("expected1") &&
        output("output2") === output("expected2") &&
        output("output3") === output("expected3"))

  }

  test("TargetEncoder - non-indexed categories") {

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setTargetType(TargetEncoder.TARGET_BINARY)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val data_noindex = Row(0.toShort, 3, 5.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    val df_noindex = spark
      .createDataFrame(sc.parallelize(data_binary :+ data_noindex), schema)

    val ex = intercept[SparkException] {
      val model = encoder.fit(df_noindex)
      print(model.stats)
    }

    assert(ex.isInstanceOf[SparkException])
    assert(ex.getMessage.contains(
      "Values from column input3 must be indices, but got 5.1"))

  }

  test("TargetEncoder - null label") {

    val data_nolabel = Row(2.toShort, 3, 5.0, null, 60.0, 50.0, 90.0, 57.5, 50.0, 70.0)

    val df_nolabel = spark
      .createDataFrame(sc.parallelize(data_continuous :+ data_nolabel), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val model = encoder.fit(df_nolabel)

    val expected_stats = Array(
      Map(Some(0.0) -> (3.0, 40.0), Some(1.0) -> (3.0, 50.0), Some(2.0) -> (3.0, 60.0),
        Some(-1.0) -> (9.0, 50.0)),
      Map(Some(3.0) -> (5.0, 50.0), Some(4.0) -> (4.0, 50.0), Some(-1.0) -> (9.0, 50.0)),
      HashMap(Some(5.0) -> (3.0, 20.0), Some(6.0) -> (3.0, 50.0), Some(7.0) -> (1.0, 70.0),
        Some(8.0) -> (1.0, 80.0), Some(9.0) -> (1.0, 90.0), Some(-1.0) -> (9.0, 50.0)))

    model.stats.zip(expected_stats).foreach{
      case (actual, expected) => assert(actual.equals(expected))
    }

  }

  test("TargetEncoder - non-binary labels") {

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setTargetType(TargetEncoder.TARGET_BINARY)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val data_non_binary = Row(0.toShort, 3, 5.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    val df_non_binary = spark
      .createDataFrame(sc.parallelize(data_binary :+ data_non_binary), schema)

    val ex = intercept[SparkException] {
      val model = encoder.fit(df_non_binary)
      print(model.stats)
    }

    assert(ex.isInstanceOf[SparkException])
    assert(ex.getMessage.contains(
      "Values from column label must be binary (0,1) but got 2.0"))

  }

  test("TargetEncoder - features renamed") {

    val df = spark
      .createDataFrame(sc.parallelize(data_continuous), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setTargetType(TargetEncoder.TARGET_CONTINUOUS)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val model = encoder.fit(df)
      .setInputCols(Array("renamed_input1", "renamed_input2", "renamed_input3"))
      .setOutputCols(Array("renamed_output1", "renamed_output2", "renamed_output3"))

    val df_renamed = df
      .withColumnsRenamed((1 to 3).map{
        f => s"input${f}" -> s"renamed_input${f}"}.toMap)

    testTransformer[(Double, Double, Double, Double, Double, Double)](
      df_renamed
        .select("renamed_input1", "renamed_input2", "renamed_input3",
          "expected1", "expected2", "expected3"),
      model,
      "renamed_output1", "expected1",
      "renamed_output2", "expected2",
      "renamed_output3", "expected3") {
      case Row(output1: Double, expected1: Double,
      output2: Double, expected2: Double,
      output3: Double, expected3: Double) =>
        assert(output1 === expected1)
        assert(output2 === expected2)
        assert(output3 === expected3)
    }

  }

  test("TargetEncoder - wrong number of features") {

    val df = spark
      .createDataFrame(sc.parallelize(data_binary), schema)

    val encoder = new TargetEncoder()
      .setLabelCol("label")
      .setTargetType(TargetEncoder.TARGET_BINARY)
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("output1", "output2", "output3"))

    val model = encoder.fit(df)

    val ex = intercept[SparkException] {
      val output = model
        .setInputCols(Array("input1", "input2"))
        .setOutputCols(Array("output1", "output2"))
        .transform(df)
      output.show()
    }

    assert(ex.isInstanceOf[SparkException])
    assert(ex.getMessage.contains(
      "does not match the number of encodings in the model (3). Found 2 features"))

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