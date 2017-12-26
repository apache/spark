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
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class OneHotEncoderEstimatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new OneHotEncoderEstimator)
  }

  test("OneHotEncoderEstimator dropLast = false") {
    val data = Seq(
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(3, Seq((1, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0)))))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected", new VectorUDT)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))
    assert(encoder.getDropLast === true)
    encoder.setDropLast(false)
    assert(encoder.getDropLast === false)

    val model = encoder.fit(df)
    val encoded = model.transform(df)
    encoded.select("output", "expected").rdd.map { r =>
      (r.getAs[Vector](0), r.getAs[Vector](1))
    }.collect().foreach { case (vec1, vec2) =>
      assert(vec1 === vec2)
    }
  }

  test("OneHotEncoderEstimator dropLast = true") {
    val data = Seq(
      Row(0.0, Vectors.sparse(2, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      Row(2.0, Vectors.sparse(2, Seq())),
      Row(0.0, Vectors.sparse(2, Seq((0, 1.0)))),
      Row(0.0, Vectors.sparse(2, Seq((0, 1.0)))),
      Row(2.0, Vectors.sparse(2, Seq())))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected", new VectorUDT)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))

    val model = encoder.fit(df)
    val encoded = model.transform(df)
    encoded.select("output", "expected").rdd.map { r =>
      (r.getAs[Vector](0), r.getAs[Vector](1))
    }.collect().foreach { case (vec1, vec2) =>
      assert(vec1 === vec2)
    }
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
    val data = Seq(
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(3, Seq((1, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0)))))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected", new VectorUDT)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val dfWithTypes = df
      .withColumn("shortInput", df("input").cast(ShortType))
      .withColumn("longInput", df("input").cast(LongType))
      .withColumn("intInput", df("input").cast(IntegerType))
      .withColumn("floatInput", df("input").cast(FloatType))
      .withColumn("decimalInput", df("input").cast(DecimalType(10, 0)))

    val cols = Array("input", "shortInput", "longInput", "intInput",
      "floatInput", "decimalInput")
    for (col <- cols) {
      val encoder = new OneHotEncoderEstimator()
        .setInputCols(Array(col))
        .setOutputCols(Array("output"))
        .setDropLast(false)

      val model = encoder.fit(dfWithTypes)
      val encoded = model.transform(dfWithTypes)

      encoded.select("output", "expected").rdd.map { r =>
        (r.getAs[Vector](0), r.getAs[Vector](1))
      }.collect().foreach { case (vec1, vec2) =>
        assert(vec1 === vec2)
      }
    }
  }

  test("OneHotEncoderEstimator: encoding multiple columns and dropLast = false") {
    val data = Seq(
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0))), 2.0, Vectors.sparse(4, Seq((2, 1.0)))),
      Row(1.0, Vectors.sparse(3, Seq((1, 1.0))), 3.0, Vectors.sparse(4, Seq((3, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0))), 0.0, Vectors.sparse(4, Seq((0, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0))), 1.0, Vectors.sparse(4, Seq((1, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0))), 0.0, Vectors.sparse(4, Seq((0, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0))), 2.0, Vectors.sparse(4, Seq((2, 1.0)))))

    val schema = StructType(Array(
        StructField("input1", DoubleType),
        StructField("expected1", new VectorUDT),
        StructField("input2", DoubleType),
        StructField("expected2", new VectorUDT)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("output1", "output2"))
    assert(encoder.getDropLast === true)
    encoder.setDropLast(false)
    assert(encoder.getDropLast === false)

    val model = encoder.fit(df)
    val encoded = model.transform(df)
    encoded.select("output1", "expected1", "output2", "expected2").rdd.map { r =>
      (r.getAs[Vector](0), r.getAs[Vector](1), r.getAs[Vector](2), r.getAs[Vector](3))
    }.collect().foreach { case (vec1, vec2, vec3, vec4) =>
      assert(vec1 === vec2)
      assert(vec3 === vec4)
    }
  }

  test("OneHotEncoderEstimator: encoding multiple columns and dropLast = true") {
    val data = Seq(
      Row(0.0, Vectors.sparse(2, Seq((0, 1.0))), 2.0, Vectors.sparse(3, Seq((2, 1.0)))),
      Row(1.0, Vectors.sparse(2, Seq((1, 1.0))), 3.0, Vectors.sparse(3, Seq())),
      Row(2.0, Vectors.sparse(2, Seq()), 0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(0.0, Vectors.sparse(2, Seq((0, 1.0))), 1.0, Vectors.sparse(3, Seq((1, 1.0)))),
      Row(0.0, Vectors.sparse(2, Seq((0, 1.0))), 0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(2.0, Vectors.sparse(2, Seq()), 2.0, Vectors.sparse(3, Seq((2, 1.0)))))

    val schema = StructType(Array(
        StructField("input1", DoubleType),
        StructField("expected1", new VectorUDT),
        StructField("input2", DoubleType),
        StructField("expected2", new VectorUDT)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("output1", "output2"))

    val model = encoder.fit(df)
    val encoded = model.transform(df)
    encoded.select("output1", "expected1", "output2", "expected2").rdd.map { r =>
      (r.getAs[Vector](0), r.getAs[Vector](1), r.getAs[Vector](2), r.getAs[Vector](3))
    }.collect().foreach { case (vec1, vec2, vec3, vec4) =>
      assert(vec1 === vec2)
      assert(vec3 === vec4)
    }
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

  test("Can't transform on negative input") {
    val trainingDF = Seq((0, 0), (1, 1), (2, 2)).toDF("a", "b")
    val testDF = Seq((0, 0), (-1, 2), (1, 3)).toDF("a", "b")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("a"))
      .setOutputCols(Array("encoded"))

    val model = encoder.fit(trainingDF)
    val err = intercept[SparkException] {
      model.transform(testDF).collect()
    }
    err.getMessage.contains("Negative value: -1.0. Input can't be negative")
  }

  test("Keep on invalid values: dropLast = false") {
    val trainingDF = Seq(Tuple1(0), Tuple1(1), Tuple1(2)).toDF("input")

    val testData = Seq(
      Row(0.0, Vectors.sparse(4, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(4, Seq((1, 1.0)))),
      Row(3.0, Vectors.sparse(4, Seq((3, 1.0)))))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected", new VectorUDT)))

    val testDF = spark.createDataFrame(sc.parallelize(testData), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))
      .setHandleInvalid("keep")
      .setDropLast(false)

    val model = encoder.fit(trainingDF)
    val encoded = model.transform(testDF)
    encoded.select("output", "expected").rdd.map { r =>
      (r.getAs[Vector](0), r.getAs[Vector](1))
    }.collect().foreach { case (vec1, vec2) =>
      assert(vec1 === vec2)
    }
  }

  test("Keep on invalid values: dropLast = true") {
    val trainingDF = Seq(Tuple1(0), Tuple1(1), Tuple1(2)).toDF("input")

    val testData = Seq(
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(3, Seq((1, 1.0)))),
      Row(3.0, Vectors.sparse(3, Seq())))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected", new VectorUDT)))

    val testDF = spark.createDataFrame(sc.parallelize(testData), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))
      .setHandleInvalid("keep")
      .setDropLast(true)

    val model = encoder.fit(trainingDF)
    val encoded = model.transform(testDF)
    encoded.select("output", "expected").rdd.map { r =>
      (r.getAs[Vector](0), r.getAs[Vector](1))
    }.collect().foreach { case (vec1, vec2) =>
      assert(vec1 === vec2)
    }
  }

  test("OneHotEncoderModel changes dropLast") {
    val data = Seq(
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0))), Vectors.sparse(2, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(3, Seq((1, 1.0))), Vectors.sparse(2, Seq((1, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0))), Vectors.sparse(2, Seq())),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0))), Vectors.sparse(2, Seq((0, 1.0)))),
      Row(0.0, Vectors.sparse(3, Seq((0, 1.0))), Vectors.sparse(2, Seq((0, 1.0)))),
      Row(2.0, Vectors.sparse(3, Seq((2, 1.0))), Vectors.sparse(2, Seq())))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected1", new VectorUDT),
        StructField("expected2", new VectorUDT)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))

    val model = encoder.fit(df)

    model.setDropLast(false)
    val encoded1 = model.transform(df)
    encoded1.select("output", "expected1").rdd.map { r =>
      (r.getAs[Vector](0), r.getAs[Vector](1))
    }.collect().foreach { case (vec1, vec2) =>
      assert(vec1 === vec2)
    }

    model.setDropLast(true)
    val encoded2 = model.transform(df)
    encoded2.select("output", "expected2").rdd.map { r =>
      (r.getAs[Vector](0), r.getAs[Vector](1))
    }.collect().foreach { case (vec1, vec2) =>
      assert(vec1 === vec2)
    }
  }

  test("OneHotEncoderModel changes handleInvalid") {
    val trainingDF = Seq(Tuple1(0), Tuple1(1), Tuple1(2)).toDF("input")

    val testData = Seq(
      Row(0.0, Vectors.sparse(4, Seq((0, 1.0)))),
      Row(1.0, Vectors.sparse(4, Seq((1, 1.0)))),
      Row(3.0, Vectors.sparse(4, Seq((3, 1.0)))))

    val schema = StructType(Array(
        StructField("input", DoubleType),
        StructField("expected", new VectorUDT)))

    val testDF = spark.createDataFrame(sc.parallelize(testData), schema)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))

    val model = encoder.fit(trainingDF)
    model.setHandleInvalid("error")

    val err = intercept[SparkException] {
      model.transform(testDF).show
    }
    err.getMessage.contains("Unseen value: 3.0. To handle unseen values")

    model.setHandleInvalid("keep")
    model.transform(testDF).collect()
  }
}
