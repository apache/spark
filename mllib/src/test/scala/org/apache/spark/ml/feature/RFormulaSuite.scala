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

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.sql.{DataFrame, Encoder, Row}
import org.apache.spark.sql.types.DoubleType

class RFormulaSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  def testRFormulaTransform[A: Encoder](
      dataframe: DataFrame,
      formulaModel: RFormulaModel,
      expected: DataFrame,
      expectedAttributes: AttributeGroup*): Unit = {
    val resultSchema = formulaModel.transformSchema(dataframe.schema)
    assert(resultSchema.json === expected.schema.json)
    assert(resultSchema === expected.schema)
    val (first +: rest) = expected.schema.fieldNames.toSeq
    val expectedRows = expected.collect()
    testTransformerByGlobalCheckFunc[A](dataframe, formulaModel, first, rest: _*) { rows =>
      assert(rows.head.schema.toString() == resultSchema.toString())
      for (expectedAttributeGroup <- expectedAttributes) {
        val attributeGroup =
          AttributeGroup.fromStructField(rows.head.schema(expectedAttributeGroup.name))
        assert(attributeGroup === expectedAttributeGroup)
      }
      assert(rows === expectedRows)
    }
  }

  test("params") {
    ParamsSuite.checkParams(new RFormula())
  }

  test("transform numeric data") {
    val formula = new RFormula().setFormula("id ~ v1 + v2")
    val original = Seq((0, 1.0, 3.0), (2, 2.0, 5.0)).toDF("id", "v1", "v2")
    val model = formula.fit(original)
    MLTestingUtils.checkCopyAndUids(formula, model)
    val expected = Seq(
      (0, 1.0, 3.0, Vectors.dense(1.0, 3.0), 0.0),
      (2, 2.0, 5.0, Vectors.dense(2.0, 5.0), 2.0)
    ).toDF("id", "v1", "v2", "features", "label")
    testRFormulaTransform[(Int, Double, Double)](original, model, expected)
  }

  test("features column already exists") {
    val formula = new RFormula().setFormula("y ~ x").setFeaturesCol("x")
    val original = Seq((0, 1.0), (2, 2.0)).toDF("x", "y")
    intercept[IllegalArgumentException] {
      formula.fit(original)
    }
  }

  test("label column already exists and forceIndexLabel was set with false") {
    val formula = new RFormula().setFormula("y ~ x").setLabelCol("y")
    val original = Seq((0, 1.0), (2, 2.0)).toDF("x", "y")
    val model = formula.fit(original)
    val expected = Seq(
      (0, 1.0, Vectors.dense(0.0)),
      (2, 2.0, Vectors.dense(2.0))
    ).toDF("x", "y", "features")
    val resultSchema = model.transformSchema(original.schema)
    assert(resultSchema.length == 3)
    testRFormulaTransform[(Int, Double)](original, model, expected)
  }

  test("label column already exists but forceIndexLabel was set with true") {
    val formula = new RFormula().setFormula("y ~ x").setLabelCol("y").setForceIndexLabel(true)
    val original = spark.createDataFrame(Seq((0, 1.0), (2, 2.0))).toDF("x", "y")
    intercept[IllegalArgumentException] {
      formula.fit(original)
    }
  }

  test("label column already exists but is not numeric type") {
    val formula = new RFormula().setFormula("y ~ x").setLabelCol("y")
    val original = Seq((0, true), (2, false)).toDF("x", "y")
    val model = formula.fit(original)
    intercept[IllegalArgumentException] {
      model.transformSchema(original.schema)
    }
    testTransformerByInterceptingException[(Int, Boolean)](
      original,
      model,
      "Label column already exists and is not of type numeric.",
      "x")
  }

  test("allow missing label column for test datasets") {
    val formula = new RFormula().setFormula("y ~ x").setLabelCol("label")
    val original = Seq((0, 1.0), (2, 2.0)).toDF("x", "_not_y")
    val model = formula.fit(original)
    val resultSchema = model.transformSchema(original.schema)
    assert(resultSchema.length == 3)
    assert(!resultSchema.exists(_.name == "label"))
    val expected = Seq(
      (0, 1.0, Vectors.dense(0.0)),
      (2, 2.0, Vectors.dense(2.0))
    ).toDF("x", "_not_y", "features")
    testRFormulaTransform[(Int, Double)](original, model, expected)
  }

  test("allow empty label") {
    val original = Seq((1, 2.0, 3.0), (4, 5.0, 6.0), (7, 8.0, 9.0)).toDF("id", "a", "b")
    val formula = new RFormula().setFormula("~ a + b")
    val model = formula.fit(original)
    val expected = Seq(
      (1, 2.0, 3.0, Vectors.dense(2.0, 3.0)),
      (4, 5.0, 6.0, Vectors.dense(5.0, 6.0)),
      (7, 8.0, 9.0, Vectors.dense(8.0, 9.0))
    ).toDF("id", "a", "b", "features")
    testRFormulaTransform[(Int, Double, Double)](original, model, expected)
  }

  test("encodes string terms") {
    val formula = new RFormula().setFormula("id ~ a + b")
    val original = Seq((1, "foo", 4), (2, "bar", 4), (3, "bar", 5), (4, "baz", 5),
      (5, "bar", 6), (6, "foo", 6))
      .toDF("id", "a", "b")
    val model = formula.fit(original)
    val expected = Seq(
        (1, "foo", 4, Vectors.dense(0.0, 1.0, 4.0), 1.0),
        (2, "bar", 4, Vectors.dense(1.0, 0.0, 4.0), 2.0),
        (3, "bar", 5, Vectors.dense(1.0, 0.0, 5.0), 3.0),
        (4, "baz", 5, Vectors.dense(0.0, 0.0, 5.0), 4.0),
        (5, "bar", 6, Vectors.dense(1.0, 0.0, 6.0), 5.0),
        (6, "foo", 6, Vectors.dense(0.0, 1.0, 6.0), 6.0)
      ).toDF("id", "a", "b", "features", "label")
    testRFormulaTransform[(Int, String, Int)](original, model, expected)
  }

  test("encodes string terms with string indexer order type") {
    val formula = new RFormula().setFormula("id ~ a + b")
    val original = Seq((1, "foo", 4), (2, "bar", 4), (3, "bar", 5), (4, "aaz", 5))
      .toDF("id", "a", "b")

    val expected = Seq(
      Seq(
        (1, "foo", 4, Vectors.dense(0.0, 0.0, 4.0), 1.0),
        (2, "bar", 4, Vectors.dense(1.0, 0.0, 4.0), 2.0),
        (3, "bar", 5, Vectors.dense(1.0, 0.0, 5.0), 3.0),
        (4, "aaz", 5, Vectors.dense(0.0, 1.0, 5.0), 4.0)
      ).toDF("id", "a", "b", "features", "label"),
      Seq(
        (1, "foo", 4, Vectors.dense(0.0, 1.0, 4.0), 1.0),
        (2, "bar", 4, Vectors.dense(0.0, 0.0, 4.0), 2.0),
        (3, "bar", 5, Vectors.dense(0.0, 0.0, 5.0), 3.0),
        (4, "aaz", 5, Vectors.dense(1.0, 0.0, 5.0), 4.0)
      ).toDF("id", "a", "b", "features", "label"),
      Seq(
        (1, "foo", 4, Vectors.dense(1.0, 0.0, 4.0), 1.0),
        (2, "bar", 4, Vectors.dense(0.0, 1.0, 4.0), 2.0),
        (3, "bar", 5, Vectors.dense(0.0, 1.0, 5.0), 3.0),
        (4, "aaz", 5, Vectors.dense(0.0, 0.0, 5.0), 4.0)
      ).toDF("id", "a", "b", "features", "label"),
      Seq(
        (1, "foo", 4, Vectors.dense(0.0, 0.0, 4.0), 1.0),
        (2, "bar", 4, Vectors.dense(0.0, 1.0, 4.0), 2.0),
        (3, "bar", 5, Vectors.dense(0.0, 1.0, 5.0), 3.0),
        (4, "aaz", 5, Vectors.dense(1.0, 0.0, 5.0), 4.0)
      ).toDF("id", "a", "b", "features", "label")
    )

    var idx = 0
    for (orderType <- StringIndexer.supportedStringOrderType) {
      val model = formula.setStringIndexerOrderType(orderType).fit(original)
      testRFormulaTransform[(Int, String, Int)](original, model, expected(idx))
      idx += 1
    }
  }

  test("test consistency with R when encoding string terms") {
    /*
     R code:

     df <- data.frame(id = c(1, 2, 3, 4),
                  a = c("foo", "bar", "bar", "aaz"),
                  b = c(4, 4, 5, 5))
     model.matrix(id ~ a + b, df)[, -1]

     abar afoo b
      0    1   4
      1    0   4
      1    0   5
      0    0   5
    */
    val original = Seq((1, "foo", 4), (2, "bar", 4), (3, "bar", 5), (4, "aaz", 5))
      .toDF("id", "a", "b")
    val formula = new RFormula().setFormula("id ~ a + b")
      .setStringIndexerOrderType(StringIndexer.alphabetDesc)

    /*
     Note that the category dropped after encoding is the same between R and Spark
     (i.e., "aaz" is treated as the reference level).
     However, the column order is still different:
     R renders the columns in ascending alphabetical order ("bar", "foo"), while
     RFormula renders the columns in descending alphabetical order ("foo", "bar").
    */
    val expected = Seq(
      (1, "foo", 4, Vectors.dense(1.0, 0.0, 4.0), 1.0),
      (2, "bar", 4, Vectors.dense(0.0, 1.0, 4.0), 2.0),
      (3, "bar", 5, Vectors.dense(0.0, 1.0, 5.0), 3.0),
      (4, "aaz", 5, Vectors.dense(0.0, 0.0, 5.0), 4.0)
    ).toDF("id", "a", "b", "features", "label")

    val model = formula.fit(original)
    testRFormulaTransform[(Int, String, Int)](original, model, expected)
  }

  test("formula w/o intercept, we should output reference category when encoding string terms") {
    /*
     R code:

     df <- data.frame(id = c(1, 2, 3, 4),
                  a = c("foo", "bar", "bar", "baz"),
                  b = c("zq", "zz", "zz", "zz"),
                  c = c(4, 4, 5, 5))
     model.matrix(id ~ a + b + c - 1, df)

       abar abaz afoo bzz c
     1    0    0    1   0 4
     2    1    0    0   1 4
     3    1    0    0   1 5
     4    0    1    0   1 5

     model.matrix(id ~ a:b + c - 1, df)

       c abar:bzq abaz:bzq afoo:bzq abar:bzz abaz:bzz afoo:bzz
     1 4        0        0        1        0        0        0
     2 4        0        0        0        1        0        0
     3 5        0        0        0        1        0        0
     4 5        0        0        0        0        1        0
    */
    val original = Seq((1, "foo", "zq", 4), (2, "bar", "zz", 4), (3, "bar", "zz", 5),
      (4, "baz", "zz", 5)).toDF("id", "a", "b", "c")

    val formula1 = new RFormula().setFormula("id ~ a + b + c - 1")
      .setStringIndexerOrderType(StringIndexer.alphabetDesc)
    val model1 = formula1.fit(original)
    val expectedAttrs1 = new AttributeGroup(
      "features",
      Array[Attribute](
        new BinaryAttribute(Some("a_foo"), Some(1)),
        new BinaryAttribute(Some("a_baz"), Some(2)),
        new BinaryAttribute(Some("a_bar"), Some(3)),
        new BinaryAttribute(Some("b_zz"), Some(4)),
        new NumericAttribute(Some("c"), Some(5))))
    // Note the column order is different between R and Spark.
    val expected1 = Seq(
      (1, "foo", "zq", 4, Vectors.sparse(5, Array(0, 4), Array(1.0, 4.0)), 1.0),
      (2, "bar", "zz", 4, Vectors.dense(0.0, 0.0, 1.0, 1.0, 4.0), 2.0),
      (3, "bar", "zz", 5, Vectors.dense(0.0, 0.0, 1.0, 1.0, 5.0), 3.0),
      (4, "baz", "zz", 5, Vectors.dense(0.0, 1.0, 0.0, 1.0, 5.0), 4.0)
    ).toDF("id", "a", "b", "c", "features", "label")

    testRFormulaTransform[(Int, String, String, Int)](original, model1, expected1, expectedAttrs1)

    // There is no impact for string terms interaction.
    val formula2 = new RFormula().setFormula("id ~ a:b + c - 1")
      .setStringIndexerOrderType(StringIndexer.alphabetDesc)
    val model2 = formula2.fit(original)
    // Note the column order is different between R and Spark.
    val expected2 = Seq(
      (1, "foo", "zq", 4, Vectors.sparse(7, Array(1, 6), Array(1.0, 4.0)), 1.0),
      (2, "bar", "zz", 4, Vectors.sparse(7, Array(4, 6), Array(1.0, 4.0)), 2.0),
      (3, "bar", "zz", 5, Vectors.sparse(7, Array(4, 6), Array(1.0, 5.0)), 3.0),
      (4, "baz", "zz", 5, Vectors.sparse(7, Array(2, 6), Array(1.0, 5.0)), 4.0)
    ).toDF("id", "a", "b", "c", "features", "label")
    val expectedAttrs2 = new AttributeGroup(
      "features",
      Array[Attribute](
        new NumericAttribute(Some("a_foo:b_zz"), Some(1)),
        new NumericAttribute(Some("a_foo:b_zq"), Some(2)),
        new NumericAttribute(Some("a_baz:b_zz"), Some(3)),
        new NumericAttribute(Some("a_baz:b_zq"), Some(4)),
        new NumericAttribute(Some("a_bar:b_zz"), Some(5)),
        new NumericAttribute(Some("a_bar:b_zq"), Some(6)),
        new NumericAttribute(Some("c"), Some(7))))

    testRFormulaTransform[(Int, String, String, Int)](original, model2, expected2, expectedAttrs2)
  }

  test("index string label") {
    val formula = new RFormula().setFormula("id ~ a + b")
    val original =
      Seq(("male", "foo", 4), ("female", "bar", 4), ("female", "bar", 5), ("male", "baz", 5),
        ("female", "bar", 6), ("female", "foo", 6))
        .toDF("id", "a", "b")
    val model = formula.fit(original)
    val attr = NominalAttribute.defaultAttr
    val expected = Seq(
        ("male", "foo", 4, Vectors.dense(0.0, 1.0, 4.0), 1.0),
        ("female", "bar", 4, Vectors.dense(1.0, 0.0, 4.0), 0.0),
        ("female", "bar", 5, Vectors.dense(1.0, 0.0, 5.0), 0.0),
        ("male", "baz", 5, Vectors.dense(0.0, 0.0, 5.0), 1.0),
        ("female", "bar", 6, Vectors.dense(1.0, 0.0, 6.0), 0.0),
        ("female", "foo", 6, Vectors.dense(0.0, 1.0, 6.0), 0.0)
    ).toDF("id", "a", "b", "features", "label")
      .select($"id", $"a", $"b", $"features", $"label".as("label", attr.toMetadata()))
    testRFormulaTransform[(String, String, Int)](original, model, expected)
  }

  test("force to index label even it is numeric type") {
    val formula = new RFormula().setFormula("id ~ a + b").setForceIndexLabel(true)
    val original = spark.createDataFrame(
      Seq((1.0, "foo", 4), (1.0, "bar", 4), (0.0, "bar", 5), (1.0, "baz", 5),
      (1.0, "bar", 6), (0.0, "foo", 6))
    ).toDF("id", "a", "b")
    val model = formula.fit(original)
    val attr = NominalAttribute.defaultAttr
    val expected = Seq(
        (1.0, "foo", 4, Vectors.dense(0.0, 1.0, 4.0), 0.0),
        (1.0, "bar", 4, Vectors.dense(1.0, 0.0, 4.0), 0.0),
        (0.0, "bar", 5, Vectors.dense(1.0, 0.0, 5.0), 1.0),
        (1.0, "baz", 5, Vectors.dense(0.0, 0.0, 5.0), 0.0),
        (1.0, "bar", 6, Vectors.dense(1.0, 0.0, 6.0), 0.0),
        (0.0, "foo", 6, Vectors.dense(0.0, 1.0, 6.0), 1.0))
      .toDF("id", "a", "b", "features", "label")
      .select($"id", $"a", $"b", $"features", $"label".as("label", attr.toMetadata()))
    testRFormulaTransform[(Double, String, Int)](original, model, expected)
  }

  test("attribute generation") {
    val formula = new RFormula().setFormula("id ~ a + b")
    val original = Seq((1, "foo", 4), (2, "bar", 4), (3, "bar", 5), (4, "baz", 5),
      (1, "bar", 6), (0, "foo", 6))
      .toDF("id", "a", "b")
    val model = formula.fit(original)
    val expected = Seq(
      (1, "foo", 4, Vectors.dense(0.0, 1.0, 4.0), 1.0),
      (2, "bar", 4, Vectors.dense(1.0, 0.0, 4.0), 2.0),
      (3, "bar", 5, Vectors.dense(1.0, 0.0, 5.0), 3.0),
      (4, "baz", 5, Vectors.dense(0.0, 0.0, 5.0), 4.0),
      (1, "bar", 6, Vectors.dense(1.0, 0.0, 6.0), 1.0),
      (0, "foo", 6, Vectors.dense(0.0, 1.0, 6.0), 0.0))
      .toDF("id", "a", "b", "features", "label")
    val expectedAttrs = new AttributeGroup(
      "features",
      Array(
        new BinaryAttribute(Some("a_bar"), Some(1)),
        new BinaryAttribute(Some("a_foo"), Some(2)),
        new NumericAttribute(Some("b"), Some(3))))
    testRFormulaTransform[(Int, String, Int)](original, model, expected, expectedAttrs)

  }

  test("vector attribute generation") {
    val formula = new RFormula().setFormula("id ~ vec")
    val original = Seq((1, Vectors.dense(0.0, 1.0)), (2, Vectors.dense(1.0, 2.0)))
      .toDF("id", "vec")
    val model = formula.fit(original)
    val attrs = new AttributeGroup("vec", 2)
    val expected = Seq(
      (1, Vectors.dense(0.0, 1.0), Vectors.dense(0.0, 1.0), 1.0),
      (2, Vectors.dense(1.0, 2.0), Vectors.dense(1.0, 2.0), 2.0))
      .toDF("id", "vec", "features", "label")
      .select($"id", $"vec".as("vec", attrs.toMetadata()), $"features", $"label")
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](
        new NumericAttribute(Some("vec_0"), Some(1)),
        new NumericAttribute(Some("vec_1"), Some(2))))

    testRFormulaTransform[(Int, Vector)](original, model, expected, expectedAttrs)
  }

  test("vector attribute generation with unnamed input attrs") {
    val formula = new RFormula().setFormula("id ~ vec2")
    val base = Seq((1, Vectors.dense(0.0, 1.0)), (2, Vectors.dense(1.0, 2.0)))
      .toDF("id", "vec")
    val metadata = new AttributeGroup(
      "vec2",
      Array[Attribute](
        NumericAttribute.defaultAttr,
        NumericAttribute.defaultAttr)).toMetadata()
    val original = base.select(base.col("id"), base.col("vec").as("vec2", metadata))
    val model = formula.fit(original)
    val expected = Seq(
      (1, Vectors.dense(0.0, 1.0), Vectors.dense(0.0, 1.0), 1.0),
      (2, Vectors.dense(1.0, 2.0), Vectors.dense(1.0, 2.0), 2.0)
    ).toDF("id", "vec2", "features", "label")
      .select($"id", $"vec2".as("vec2", metadata), $"features", $"label")
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](
        new NumericAttribute(Some("vec2_0"), Some(1)),
        new NumericAttribute(Some("vec2_1"), Some(2))))
    testRFormulaTransform[(Int, Vector)](original, model, expected, expectedAttrs)
  }

  test("numeric interaction") {
    val formula = new RFormula().setFormula("a ~ b:c:d")
    val original = Seq((1, 2, 4, 2), (2, 3, 4, 1)).toDF("a", "b", "c", "d")
    val model = formula.fit(original)
    val expected = Seq(
      (1, 2, 4, 2, Vectors.dense(16.0), 1.0),
      (2, 3, 4, 1, Vectors.dense(12.0), 2.0)
    ).toDF("a", "b", "c", "d", "features", "label")
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](new NumericAttribute(Some("b:c:d"), Some(1))))
    testRFormulaTransform[(Int, Int, Int, Int)](original, model, expected, expectedAttrs)
  }

  test("factor numeric interaction") {
    val formula = new RFormula().setFormula("id ~ a:b")
    val original =
      Seq((1, "foo", 4), (2, "bar", 4), (3, "bar", 5), (4, "baz", 5), (4, "baz", 5), (4, "baz", 5))
        .toDF("id", "a", "b")
    val model = formula.fit(original)
    val expected = Seq(
      (1, "foo", 4, Vectors.dense(0.0, 0.0, 4.0), 1.0),
      (2, "bar", 4, Vectors.dense(0.0, 4.0, 0.0), 2.0),
      (3, "bar", 5, Vectors.dense(0.0, 5.0, 0.0), 3.0),
      (4, "baz", 5, Vectors.dense(5.0, 0.0, 0.0), 4.0),
      (4, "baz", 5, Vectors.dense(5.0, 0.0, 0.0), 4.0),
      (4, "baz", 5, Vectors.dense(5.0, 0.0, 0.0), 4.0)
    ).toDF("id", "a", "b", "features", "label")
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](
        new NumericAttribute(Some("a_baz:b"), Some(1)),
        new NumericAttribute(Some("a_bar:b"), Some(2)),
        new NumericAttribute(Some("a_foo:b"), Some(3))))
    testRFormulaTransform[(Int, String, Int)](original, model, expected, expectedAttrs)
  }

  test("factor factor interaction") {
    val formula = new RFormula().setFormula("id ~ a:b")
    val original =
      Seq((1, "foo", "zq"), (2, "bar", "zq"), (3, "bar", "zz")).toDF("id", "a", "b")
    val model = formula.fit(original)
    val expected = Seq(
      (1, "foo", "zq", Vectors.dense(0.0, 0.0, 1.0, 0.0), 1.0),
      (2, "bar", "zq", Vectors.dense(1.0, 0.0, 0.0, 0.0), 2.0),
      (3, "bar", "zz", Vectors.dense(0.0, 1.0, 0.0, 0.0), 3.0)
    ).toDF("id", "a", "b", "features", "label")
    testRFormulaTransform[(Int, String, String)](original, model, expected)
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](
        new NumericAttribute(Some("a_bar:b_zq"), Some(1)),
        new NumericAttribute(Some("a_bar:b_zz"), Some(2)),
        new NumericAttribute(Some("a_foo:b_zq"), Some(3)),
        new NumericAttribute(Some("a_foo:b_zz"), Some(4))))
    testRFormulaTransform[(Int, String, String)](original, model, expected, expectedAttrs)
  }

  test("read/write: RFormula") {
    val rFormula = new RFormula()
      .setFormula("id ~ a:b")
      .setFeaturesCol("myFeatures")
      .setLabelCol("myLabels")

    testDefaultReadWrite(rFormula)
  }

  test("read/write: RFormulaModel") {
    def checkModelData(model: RFormulaModel, model2: RFormulaModel): Unit = {
      assert(model.uid === model2.uid)

      assert(model.resolvedFormula.label === model2.resolvedFormula.label)
      assert(model.resolvedFormula.terms === model2.resolvedFormula.terms)
      assert(model.resolvedFormula.hasIntercept === model2.resolvedFormula.hasIntercept)

      assert(model.pipelineModel.uid === model2.pipelineModel.uid)

      model.pipelineModel.stages.zip(model2.pipelineModel.stages).foreach {
        case (transformer1, transformer2) =>
          assert(transformer1.uid === transformer2.uid)
          assert(transformer1.params === transformer2.params)
      }
    }

    val dataset = Seq((1, "foo", "zq"), (2, "bar", "zq"), (3, "bar", "zz")).toDF("id", "a", "b")

    val rFormula = new RFormula().setFormula("id ~ a:b")

    val model = rFormula.fit(dataset)
    val newModel = testDefaultReadWrite(model)
    checkModelData(model, newModel)
  }

  test("should support all NumericType labels") {
    val formula = new RFormula().setFormula("label ~ features")
      .setLabelCol("x")
      .setFeaturesCol("y")
    val dfs = MLTestingUtils.genRegressionDFWithNumericLabelCol(spark)
    val expected = formula.fit(dfs(DoubleType))
    val actuals = dfs.keys.filter(_ != DoubleType).map(t => formula.fit(dfs(t)))
    actuals.foreach { actual =>
      assert(expected.pipelineModel.stages.length === actual.pipelineModel.stages.length)
      expected.pipelineModel.stages.zip(actual.pipelineModel.stages).foreach {
        case (exTransformer, acTransformer) =>
          assert(exTransformer.params === acTransformer.params)
      }
      assert(expected.resolvedFormula.label === actual.resolvedFormula.label)
      assert(expected.resolvedFormula.terms === actual.resolvedFormula.terms)
      assert(expected.resolvedFormula.hasIntercept === actual.resolvedFormula.hasIntercept)
    }
  }

  test("handle unseen features or labels") {
    val df1 = Seq((1, "foo", "zq"), (2, "bar", "zq"), (3, "bar", "zz")).toDF("id", "a", "b")
    val df2 = Seq((1, "foo", "zq"), (2, "bar", "zq"), (3, "bar", "zy")).toDF("id", "a", "b")

    // Handle unseen features.
    val formula1 = new RFormula().setFormula("id ~ a + b")
    testTransformerByInterceptingException[(Int, String, String)](
      df2,
      formula1.fit(df1),
      "Unseen label:",
      "features")
    val model1 = formula1.setHandleInvalid("skip").fit(df1)
    val model2 = formula1.setHandleInvalid("keep").fit(df1)

    val expected1 = Seq(
      (1, "foo", "zq", Vectors.dense(0.0, 1.0), 1.0),
      (2, "bar", "zq", Vectors.dense(1.0, 1.0), 2.0)
    ).toDF("id", "a", "b", "features", "label")
    val expected2 = Seq(
      (1, "foo", "zq", Vectors.dense(0.0, 1.0, 1.0, 0.0), 1.0),
      (2, "bar", "zq", Vectors.dense(1.0, 0.0, 1.0, 0.0), 2.0),
      (3, "bar", "zy", Vectors.dense(1.0, 0.0, 0.0, 0.0), 3.0)
    ).toDF("id", "a", "b", "features", "label")

    testRFormulaTransform[(Int, String, String)](df2, model1, expected1)
    testRFormulaTransform[(Int, String, String)](df2, model2, expected2)

    // Handle unseen labels.
    val formula2 = new RFormula().setFormula("b ~ a + id")
    testTransformerByInterceptingException[(Int, String, String)](
      df2,
      formula2.fit(df1),
      "Unseen label:",
      "label")

    val model3 = formula2.setHandleInvalid("skip").fit(df1)
    val model4 = formula2.setHandleInvalid("keep").fit(df1)

    val attr = NominalAttribute.defaultAttr
    val expected3 = Seq(
      (1, "foo", "zq", Vectors.dense(0.0, 1.0), 0.0),
      (2, "bar", "zq", Vectors.dense(1.0, 2.0), 0.0)
    ).toDF("id", "a", "b", "features", "label")
      .select($"id", $"a", $"b", $"features", $"label".as("label", attr.toMetadata()))

    val expected4 = Seq(
      (1, "foo", "zq", Vectors.dense(0.0, 1.0, 1.0), 0.0),
      (2, "bar", "zq", Vectors.dense(1.0, 0.0, 2.0), 0.0),
      (3, "bar", "zy", Vectors.dense(1.0, 0.0, 3.0), 2.0)
    ).toDF("id", "a", "b", "features", "label")
      .select($"id", $"a", $"b", $"features", $"label".as("label", attr.toMetadata()))

    testRFormulaTransform[(Int, String, String)](df2, model3, expected3)
    testRFormulaTransform[(Int, String, String)](df2, model4, expected4)
  }

  test("Use Vectors as inputs to formula.") {
    val original = Seq(
      (1, 4, Vectors.dense(0.0, 0.0, 4.0)),
      (2, 4, Vectors.dense(1.0, 0.0, 4.0)),
      (3, 5, Vectors.dense(1.0, 0.0, 5.0)),
      (4, 5, Vectors.dense(0.0, 1.0, 5.0))
    ).toDF("id", "a", "b")
    val formula = new RFormula().setFormula("id ~ a + b")
    val (first +: rest) = Seq("id", "a", "b", "features", "label")
    testTransformer[(Int, Int, Vector)](original, formula.fit(original), first, rest: _*) {
      case Row(id: Int, a: Int, b: Vector, features: Vector, label: Double) =>
        assert(label === id)
        assert(features.toArray === a +: b.toArray)
    }

    val group = new AttributeGroup("b", 3)
    val vectorColWithMetadata = original("b").as("b", group.toMetadata())
    val dfWithMetadata = original.withColumn("b", vectorColWithMetadata)
    val model = formula.fit(dfWithMetadata)
    // model should work even when applied to dataframe without metadata.
    testTransformer[(Int, Int, Vector)](original, model, first, rest: _*) {
      case Row(id: Int, a: Int, b: Vector, features: Vector, label: Double) =>
        assert(label === id)
        assert(features.toArray === a +: b.toArray)
    }
  }

  test("SPARK-23562 RFormula handleInvalid should handle invalid values in non-string columns.") {
    val d1 = Seq(
      (1001L, "a"),
      (1002L, "b")).toDF("id1", "c1")
    val d2 = Seq[(java.lang.Long, String)](
      (20001L, "x"),
      (20002L, "y"),
      (null, null)).toDF("id2", "c2")
    val dataset = d1.crossJoin(d2)

    def get_output(mode: String): DataFrame = {
      val formula = new RFormula().setFormula("c1 ~ id2").setHandleInvalid(mode)
      formula.fit(dataset).transform(dataset).select("features", "label")
    }

    assert(intercept[SparkException](get_output("error").collect())
      .getMessage.contains("Encountered null while assembling a row"))
    assert(get_output("skip").count() == 4)
    assert(get_output("keep").count() == 6)
  }

  test("SPARK-37026: Ensure the element type of ResolvedRFormula.terms is " +
    "scala.Seq for Scala 2.13") {
    withTempPath { path =>
      val dataset = Seq((1, "foo", "zq"), (2, "bar", "zq"), (3, "bar", "zz")).toDF("id", "a", "b")
      val rFormula = new RFormula().setFormula("id ~ a:b")
      val model = rFormula.fit(dataset)
      model.save(path.getCanonicalPath)
      val newModel = RFormulaModel.load(path.getCanonicalPath)
      newModel.resolvedFormula.toString
    }
  }
}
