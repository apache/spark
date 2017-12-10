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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.attribute.{AttributeGroup, BinaryAttribute, NominalAttribute}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class OneHotEncoderEstimatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  def stringIndexed(): DataFrame = stringIndexedMultipleCols().select("id", "label", "labelIndex")

  def stringIndexedMultipleCols(): DataFrame = {
    val data = Seq(
      (0, "a", "A"),
      (1, "b", "B"),
      (2, "c", "D"),
      (3, "a", "A"),
      (4, "a", "B"),
      (5, "c", "C"))
    val df = data.toDF("id", "label", "label2")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)
    val df2 = indexer.transform(df)
    val indexer2 = new StringIndexer()
      .setInputCol("label2")
      .setOutputCol("labelIndex2")
      .fit(df2)
    indexer2.transform(df2)
  }

  test("params") {
    ParamsSuite.checkParams(new OneHotEncoderEstimator)
  }

  test("OneHotEncoderEstimator dropLast = false") {
    val transformed = stringIndexed()
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("labelIndex"))
      .setOutputCols(Array("labelVec"))
    assert(encoder.getDropLast === true)
    encoder.setDropLast(false)
    assert(encoder.getDropLast === false)

    val model = encoder.fit(transformed)
    val encoded = model.transform(transformed)

    val output = encoded.select("id", "labelVec").rdd.map { r =>
      val vec = r.getAs[Vector](1)
      (r.getInt(0), vec(0), vec(1), vec(2))
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1
    val expected = Set((0, 1.0, 0.0, 0.0), (1, 0.0, 0.0, 1.0), (2, 0.0, 1.0, 0.0),
      (3, 1.0, 0.0, 0.0), (4, 1.0, 0.0, 0.0), (5, 0.0, 1.0, 0.0))
    assert(output === expected)
  }

  test("OneHotEncoderEstimator dropLast = true") {
    val transformed = stringIndexed()
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("labelIndex"))
      .setOutputCols(Array("labelVec"))

    val model = encoder.fit(transformed)
    val encoded = model.transform(transformed)

    val output = encoded.select("id", "labelVec").rdd.map { r =>
      val vec = r.getAs[Vector](1)
      (r.getInt(0), vec(0), vec(1))
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1
    val expected = Set((0, 1.0, 0.0), (1, 0.0, 0.0), (2, 0.0, 1.0),
      (3, 1.0, 0.0), (4, 1.0, 0.0), (5, 0.0, 1.0))
    assert(output === expected)
  }

  test("input column with ML attribute") {
    val attr = NominalAttribute.defaultAttr.withValues("small", "medium", "large")
    val df = Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply).toDF("size")
      .select(col("size").as("size", attr.toMetadata()))
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("size"))
      .setOutputCols(Array("encoded"))
    val model = encoder.fit(df)
    val output = model.transform(df)
    val group = AttributeGroup.fromStructField(output.schema("encoded"))
    assert(group.size === 2)
    assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("small").withIndex(0))
    assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("medium").withIndex(1))
  }

  test("input column without ML attribute") {
    val df = Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply).toDF("index")
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("index"))
      .setOutputCols(Array("encoded"))
    val model = encoder.fit(df)
    val output = model.transform(df)
    val group = AttributeGroup.fromStructField(output.schema("encoded"))
    assert(group.size === 2)
    assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("0").withIndex(0))
    assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("1").withIndex(1))
  }

  test("read/write") {
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("index"))
      .setOutputCols(Array("encoded"))
    testDefaultReadWrite(encoder)
  }

  test("OneHotEncoderModel read/write") {
    val instance = new OneHotEncoderModel("myOneHotEncoderModel", Array(1, 2, 3))
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.categorySizes === instance.categorySizes)
  }

  test("OneHotEncoderEstimator with varying types") {
    val df = stringIndexed()
    val dfWithTypes = df
      .withColumn("shortLabel", df("labelIndex").cast(ShortType))
      .withColumn("longLabel", df("labelIndex").cast(LongType))
      .withColumn("intLabel", df("labelIndex").cast(IntegerType))
      .withColumn("floatLabel", df("labelIndex").cast(FloatType))
      .withColumn("decimalLabel", df("labelIndex").cast(DecimalType(10, 0)))
    val cols = Array("labelIndex", "shortLabel", "longLabel", "intLabel",
      "floatLabel", "decimalLabel")
    for (col <- cols) {
      val encoder = new OneHotEncoderEstimator()
        .setInputCols(Array(col))
        .setOutputCols(Array("labelVec"))
        .setDropLast(false)
      val model = encoder.fit(dfWithTypes)
      val encoded = model.transform(dfWithTypes)

      val output = encoded.select("id", "labelVec").rdd.map { r =>
        val vec = r.getAs[Vector](1)
        (r.getInt(0), vec(0), vec(1), vec(2))
      }.collect().toSet
      // a -> 0, b -> 2, c -> 1
      val expected = Set((0, 1.0, 0.0, 0.0), (1, 0.0, 0.0, 1.0), (2, 0.0, 1.0, 0.0),
        (3, 1.0, 0.0, 0.0), (4, 1.0, 0.0, 0.0), (5, 0.0, 1.0, 0.0))
      assert(output === expected)
    }
  }

  test("OneHotEncoderEstimator: encoding multiple columns and dropLast = false") {
    val transformed = stringIndexedMultipleCols()
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("labelIndex", "labelIndex2"))
      .setOutputCols(Array("labelVec", "labelVec2"))
    assert(encoder.getDropLast === true)
    encoder.setDropLast(false)
    assert(encoder.getDropLast === false)

    val model = encoder.fit(transformed)
    val encoded = model.transform(transformed)

    // Verify 1st column.
    val output = encoded.select("id", "labelVec").rdd.map { r =>
      val vec = r.getAs[Vector](1)
      (r.getInt(0), vec(0), vec(1), vec(2))
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1
    val expected = Set((0, 1.0, 0.0, 0.0), (1, 0.0, 0.0, 1.0), (2, 0.0, 1.0, 0.0),
      (3, 1.0, 0.0, 0.0), (4, 1.0, 0.0, 0.0), (5, 0.0, 1.0, 0.0))
    assert(output === expected)

    // Verify 2nd column.
    val output2 = encoded.select("id", "labelVec2").rdd.map { r =>
      val vec = r.getAs[Vector](1)
      (r.getInt(0), vec(0), vec(1), vec(2), vec(3))
    }.collect().toSet
    // A -> 1, B -> 0, C -> 3, D -> 2
    val expected2 = Set((0, 0.0, 1.0, 0.0, 0.0), (1, 1.0, 0.0, 0.0, 0.0), (2, 0.0, 0.0, 1.0, 0.0),
      (3, 0.0, 1.0, 0.0, 0.0), (4, 1.0, 0.0, 0.0, 0.0), (5, 0.0, 0.0, 0.0, 1.0))
    assert(output2 === expected2)
  }

  test("OneHotEncoderEstimator: encoding multiple columns and dropLast = true") {
    val transformed = stringIndexedMultipleCols()
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("labelIndex", "labelIndex2"))
      .setOutputCols(Array("labelVec", "labelVec2"))

    val model = encoder.fit(transformed)
    val encoded = model.transform(transformed)

    // Verify 1st column.
    val output = encoded.select("id", "labelVec").rdd.map { r =>
      val vec = r.getAs[Vector](1)
      (r.getInt(0), vec(0), vec(1))
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1
    val expected = Set((0, 1.0, 0.0), (1, 0.0, 0.0), (2, 0.0, 1.0),
      (3, 1.0, 0.0), (4, 1.0, 0.0), (5, 0.0, 1.0))
    assert(output === expected)

    // Verify 2nd column.
    val output2 = encoded.select("id", "labelVec2").rdd.map { r =>
      val vec = r.getAs[Vector](1)
      (r.getInt(0), vec(0), vec(1), vec(2))
    }.collect().toSet
    // A -> 1, B -> 0, C -> 3, D -> 2
    val expected2 = Set((0, 0.0, 1.0, 0.0), (1, 1.0, 0.0, 0.0), (2, 0.0, 0.0, 1.0),
      (3, 0.0, 1.0, 0.0), (4, 1.0, 0.0, 0.0), (5, 0.0, 0.0, 0.0))
    assert(output2 === expected2)
  }

  test("Throw error on invalid values") {
    val trainingData = Seq((0, 0), (1, 1), (2, 2))
    val trainingDF = trainingData.toDF("id", "a")
    val testData = Seq((0, 0), (1, 2), (1, 3))
    val testDF = testData.toDF("id", "a")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("a"))
      .setOutputCols(Array("encoded"))

    val model = encoder.fit(trainingDF)
    val err = intercept[SparkException] {
      model.transform(testDF).show
    }
    err.getMessage.contains("Unseen value: 3.0. To handle unseen values")
  }

  test("Keep on invalid values") {
    val trainingData = Seq((0, 0), (1, 1), (2, 2))
    val trainingDF = trainingData.toDF("id", "a")
    val testData = Seq((0, 0), (1, 1), (2, 3))
    val testDF = testData.toDF("id", "a")

    val dropLasts = Seq(false, true)
    val expectedOutput = Seq(
      Set((0, Seq(1.0, 0.0, 0.0, 0.0)), (1, Seq(0.0, 1.0, 0.0, 0.0)), (2, Seq(0.0, 0.0, 0.0, 1.0))),
      Set((0, Seq(1.0, 0.0, 0.0)), (1, Seq(0.0, 1.0, 0.0)), (2, Seq(0.0, 0.0, 0.0))))

    dropLasts.zipWithIndex.foreach { case (dropLast, idx) =>
      val encoder = new OneHotEncoderEstimator()
        .setInputCols(Array("a"))
        .setOutputCols(Array("encoded"))
        .setHandleInvalid("keep")
        .setDropLast(dropLast)

      val model = encoder.fit(trainingDF)
      val encoded = model.transform(testDF)

      val output = encoded.select("id", "encoded").rdd.map { r =>
        val vec = r.getAs[Vector](1)
        (r.getInt(0), vec.toArray.toSeq)
      }.collect().toSet
      assert(output === expectedOutput(idx))
    }
  }
}
