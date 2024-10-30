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

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.{AttributeGroup, BinaryAttribute, NominalAttribute}
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types._

class OneHotEncoderSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new OneHotEncoder)
  }

  test("OneHotEncoder dropLast = false") {
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

    val encoder = new OneHotEncoder()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))
    assert(encoder.getDropLast)
    encoder.setDropLast(false)
    assert(encoder.getDropLast === false)
    val model = encoder.fit(df)
    testTransformer[(Double, Vector)](df, model, "output", "expected") {
      case Row(output: Vector, expected: Vector) =>
        assert(output === expected)
    }
  }

  test("Single Column: OneHotEncoder dropLast = false") {
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

    val encoder = new OneHotEncoder()
      .setInputCol("input")
      .setOutputCol("output")
    assert(encoder.getDropLast)
    encoder.setDropLast(false)
    assert(encoder.getDropLast === false)
    val model = encoder.fit(df)
    testTransformer[(Double, Vector)](df, model, "output", "expected") {
      case Row(output: Vector, expected: Vector) =>
        assert(output === expected)
    }
  }

  test("OneHotEncoder dropLast = true") {
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

    val encoder = new OneHotEncoder()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))

    val model = encoder.fit(df)
    testTransformer[(Double, Vector)](df, model, "output", "expected") {
      case Row(output: Vector, expected: Vector) =>
        assert(output === expected)
    }
  }

  test("input column with ML attribute") {
    val attr = NominalAttribute.defaultAttr.withValues("small", "medium", "large")
    val df = Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply).toDF("size")
      .select(col("size").as("size", attr.toMetadata()))
    val encoder = new OneHotEncoder()
      .setInputCols(Array("size"))
      .setOutputCols(Array("encoded"))
    val model = encoder.fit(df)
    testTransformerByGlobalCheckFunc[(Double)](df, model, "encoded") { rows =>
        val group = AttributeGroup.fromStructField(rows.head.schema("encoded"))
        assert(group.size === 2)
        assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("small").withIndex(0))
        assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("medium").withIndex(1))
    }
  }

  test("Single Column: input column with ML attribute") {
    val attr = NominalAttribute.defaultAttr.withValues("small", "medium", "large")
    val df = Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply).toDF("size")
      .select(col("size").as("size", attr.toMetadata()))
    val encoder = new OneHotEncoder()
      .setInputCol("size")
      .setOutputCol("encoded")
    val model = encoder.fit(df)
    testTransformerByGlobalCheckFunc[(Double)](df, model, "encoded") { rows =>
      val group = AttributeGroup.fromStructField(rows.head.schema("encoded"))
      assert(group.size === 2)
      assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("small").withIndex(0))
      assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("medium").withIndex(1))
    }
  }

  test("input column without ML attribute") {
    val df = Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply).toDF("index")
    val encoder = new OneHotEncoder()
      .setInputCols(Array("index"))
      .setOutputCols(Array("encoded"))
    val model = encoder.fit(df)
    testTransformerByGlobalCheckFunc[(Double)](df, model, "encoded") { rows =>
      val group = AttributeGroup.fromStructField(rows.head.schema("encoded"))
      assert(group.size === 2)
      assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("0").withIndex(0))
      assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("1").withIndex(1))
    }
  }

  test("read/write") {
    val encoder = new OneHotEncoder()
      .setInputCols(Array("index"))
      .setOutputCols(Array("encoded"))
    testDefaultReadWrite(encoder)
  }

  test("Single Column: read/write") {
    val encoder = new OneHotEncoder()
      .setInputCol("index")
      .setOutputCol("encoded")
    testDefaultReadWrite(encoder)
  }

  test("OneHotEncoderModel read/write") {
    val instance = new OneHotEncoderModel("myOneHotEncoderModel", Array(1, 2, 3))
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.categorySizes === instance.categorySizes)
  }

  test("OneHotEncoder with varying types") {
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

    class NumericTypeWithEncoder[A](val numericType: NumericType)
      (implicit val encoder: Encoder[(A, Vector)])

    val types = Seq(
      new NumericTypeWithEncoder[Short](ShortType),
      new NumericTypeWithEncoder[Long](LongType),
      new NumericTypeWithEncoder[Int](IntegerType),
      new NumericTypeWithEncoder[Float](FloatType),
      new NumericTypeWithEncoder[Byte](ByteType),
      new NumericTypeWithEncoder[Double](DoubleType),
      new NumericTypeWithEncoder[Decimal](DecimalType(10, 0))(ExpressionEncoder()))

    for (t <- types) {
      val dfWithTypes = df.select(col("input").cast(t.numericType), col("expected"))
      val estimator = new OneHotEncoder()
        .setInputCols(Array("input"))
        .setOutputCols(Array("output"))
        .setDropLast(false)

      val model = estimator.fit(dfWithTypes)
      testTransformer(dfWithTypes, model, "output", "expected") {
        case Row(output: Vector, expected: Vector) =>
          assert(output === expected)
      }(t.encoder)
    }
  }

  test("Single Column: OneHotEncoder with varying types") {
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

    class NumericTypeWithEncoder[A](val numericType: NumericType)
                                   (implicit val encoder: Encoder[(A, Vector)])

    val types = Seq(
      new NumericTypeWithEncoder[Short](ShortType),
      new NumericTypeWithEncoder[Long](LongType),
      new NumericTypeWithEncoder[Int](IntegerType),
      new NumericTypeWithEncoder[Float](FloatType),
      new NumericTypeWithEncoder[Byte](ByteType),
      new NumericTypeWithEncoder[Double](DoubleType),
      new NumericTypeWithEncoder[Decimal](DecimalType(10, 0))(ExpressionEncoder()))

    for (t <- types) {
      val dfWithTypes = df.select(col("input").cast(t.numericType), col("expected"))
      val estimator = new OneHotEncoder()
        .setInputCol("input")
        .setOutputCol("output")
        .setDropLast(false)

      val model = estimator.fit(dfWithTypes)
      testTransformer(dfWithTypes, model, "output", "expected") {
        case Row(output: Vector, expected: Vector) =>
          assert(output === expected)
      }(t.encoder)
    }
  }

  test("OneHotEncoder: encoding multiple columns and dropLast = false") {
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

    val encoder = new OneHotEncoder()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("output1", "output2"))
    assert(encoder.getDropLast)
    encoder.setDropLast(false)
    assert(encoder.getDropLast === false)

    val model = encoder.fit(df)
    testTransformer[(Double, Vector, Double, Vector)](
      df,
      model,
      "output1",
      "output2",
      "expected1",
      "expected2") {
      case Row(output1: Vector, output2: Vector, expected1: Vector, expected2: Vector) =>
        assert(output1 === expected1)
        assert(output2 === expected2)
    }
  }

  test("Single Column: OneHotEncoder: encoding multiple columns and dropLast = false") {
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

    val encoder1 = new OneHotEncoder()
      .setInputCol("input1")
      .setOutputCol("output1")
    assert(encoder1.getDropLast)
    encoder1.setDropLast(false)
    assert(encoder1.getDropLast === false)

    val model1 = encoder1.fit(df)
    testTransformer[(Double, Vector, Double, Vector)](
      df,
      model1,
      "output1",
      "expected1") {
      case Row(output1: Vector, expected1: Vector) =>
        assert(output1 === expected1)
    }

    val encoder2 = new OneHotEncoder()
      .setInputCol("input2")
      .setOutputCol("output2")
    assert(encoder2.getDropLast)
    encoder2.setDropLast(false)
    assert(encoder2.getDropLast === false)

    val model2 = encoder2.fit(df)
    testTransformer[(Double, Vector, Double, Vector)](
      df,
      model2,
      "output2",
      "expected2") {
      case Row(output2: Vector, expected2: Vector) =>
        assert(output2 === expected2)
    }
  }

  test("OneHotEncoder: encoding multiple columns and dropLast = true") {
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

    val encoder = new OneHotEncoder()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("output1", "output2"))

    val model = encoder.fit(df)
    testTransformer[(Double, Vector, Double, Vector)](
      df,
      model,
      "output1",
      "output2",
      "expected1",
      "expected2") {
      case Row(output1: Vector, output2: Vector, expected1: Vector, expected2: Vector) =>
        assert(output1 === expected1)
        assert(output2 === expected2)
    }
  }

  test("Throw error on invalid values") {
    val trainingData = Seq((0, 0), (1, 1), (2, 2))
    val trainingDF = trainingData.toDF("id", "a")
    val testData = Seq((0, 0), (1, 2), (1, 3))
    val testDF = testData.toDF("id", "a")

    val encoder = new OneHotEncoder()
      .setInputCols(Array("a"))
      .setOutputCols(Array("encoded"))

    val model = encoder.fit(trainingDF)
    testTransformerByInterceptingException[(Int, Int)](
      testDF,
      model,
      expectedMessagePart = "Unseen value: 3.0. To handle unseen values",
      firstResultCol = "encoded")

  }

  test("Can't transform on negative input") {
    val trainingDF = Seq((0, 0), (1, 1), (2, 2)).toDF("a", "b")
    val testDF = Seq((0, 0), (-1, 2), (1, 3)).toDF("a", "b")

    val encoder = new OneHotEncoder()
      .setInputCols(Array("a"))
      .setOutputCols(Array("encoded"))

    val model = encoder.fit(trainingDF)
    testTransformerByInterceptingException[(Int, Int)](
      testDF,
      model,
      expectedMessagePart = "Negative value: -1.0. Input can't be negative",
      firstResultCol = "encoded")
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

    val encoder = new OneHotEncoder()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))
      .setHandleInvalid("keep")
      .setDropLast(false)

    val model = encoder.fit(trainingDF)
    testTransformer[(Double, Vector)](testDF, model, "output", "expected") {
      case Row(output: Vector, expected: Vector) =>
        assert(output === expected)
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

    val encoder = new OneHotEncoder()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))
      .setHandleInvalid("keep")
      .setDropLast(true)

    val model = encoder.fit(trainingDF)
    testTransformer[(Double, Vector)](testDF, model, "output", "expected") {
      case Row(output: Vector, expected: Vector) =>
        assert(output === expected)
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

    val encoder = new OneHotEncoder()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))

    val model = encoder.fit(df)

    model.setDropLast(false)
    testTransformer[(Double, Vector, Vector)](df, model, "output", "expected1") {
      case Row(output: Vector, expected1: Vector) =>
        assert(output === expected1)
    }

    model.setDropLast(true)
    testTransformer[(Double, Vector, Vector)](df, model, "output", "expected2") {
      case Row(output: Vector, expected2: Vector) =>
        assert(output === expected2)
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

    val encoder = new OneHotEncoder()
      .setInputCols(Array("input"))
      .setOutputCols(Array("output"))

    val model = encoder.fit(trainingDF)
    model.setHandleInvalid("error")

    testTransformerByInterceptingException[(Double, Vector)](
      testDF,
      model,
      expectedMessagePart = "Unseen value: 3.0. To handle unseen values",
      firstResultCol = "output")

    model.setHandleInvalid("keep")
    testTransformerByGlobalCheckFunc[(Double, Vector)](testDF, model, "output") { _ => }
  }

  test("Transforming on mismatched attributes") {
    val attr = NominalAttribute.defaultAttr.withValues("small", "medium", "large")
    val df = Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply).toDF("size")
      .select(col("size").as("size", attr.toMetadata()))
    val encoder = new OneHotEncoder()
      .setInputCols(Array("size"))
      .setOutputCols(Array("encoded"))
    val model = encoder.fit(df)

    val testAttr = NominalAttribute.defaultAttr.withValues("tiny", "small", "medium", "large")
    val testDF = Seq(0.0, 1.0, 2.0, 3.0).map(Tuple1.apply).toDF("size")
      .select(col("size").as("size", testAttr.toMetadata()))
    testTransformerByInterceptingException[(Double)](
      testDF,
      model,
      expectedMessagePart = "OneHotEncoderModel expected 2 categorical values",
      firstResultCol = "encoded")
  }

  test("assert exception is thrown if both multi-column and single-column params are set") {
    import testImplicits._
    val df = Seq((0.5, 0.3), (0.5, -0.4)).toDF("feature1", "feature2")
    ParamsSuite.testExclusiveParams(new OneHotEncoder, df, ("inputCol", "feature1"),
      ("inputCols", Array("feature1", "feature2")))
    ParamsSuite.testExclusiveParams(new OneHotEncoder, df, ("inputCol", "feature1"),
      ("outputCol", "result1"), ("outputCols", Array("result1", "result2")))

    // this should fail because at least one of inputCol and inputCols must be set
    ParamsSuite.testExclusiveParams(new OneHotEncoder, df, ("outputCol", "feature1"))
  }

  test("Compare single/multiple column(s) OneHotEncoder in pipeline") {
    val df = Seq((0.0, 2.0), (1.0, 3.0), (2.0, 0.0), (0.0, 1.0), (0.0, 0.0), (2.0, 2.0))
      .toDF("input1", "input2")

    val multiColsEncoder = new OneHotEncoder()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("output1", "output2"))

    val plForMultiCols = new Pipeline()
      .setStages(Array(multiColsEncoder))
      .fit(df)

    val encoderForCol1 = new OneHotEncoder()
      .setInputCol("input1")
      .setOutputCol("output1")
    val encoderForCol2 = new OneHotEncoder()
      .setInputCol("input2")
      .setOutputCol("output2")

    val plForSingleCol = new Pipeline()
      .setStages(Array(encoderForCol1, encoderForCol2))
      .fit(df)

    val resultForSingleCol = plForSingleCol.transform(df)
      .select("output1", "output2")
      .collect()
    val resultForMultiCols = plForMultiCols.transform(df)
      .select("output1", "output2")
      .collect()

    resultForSingleCol.zip(resultForMultiCols).foreach {
      case (rowForSingle, rowForMultiCols) =>
        assert(rowForSingle === rowForMultiCols)
    }
  }

  test("nested input columns") {
    val attr = NominalAttribute.defaultAttr.withValues("small", "medium", "large")
    val df = Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply).toDF("size")
      .select(col("size").as("size", attr.toMetadata()))
      .select(struct("size").as("nest"))
    val encoder = new OneHotEncoder()
      .setInputCols(Array("nest.size"))
      .setOutputCols(Array("encoded"))
    val model = encoder.fit(df)
    val rows = model.transform(df).select("encoded").collect().toSeq
    val group = AttributeGroup.fromStructField(rows.head.schema("encoded"))
    assert(group.size === 2)
    assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("small").withIndex(0))
    assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("medium").withIndex(1))
  }
}
