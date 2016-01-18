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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext

class RFormulaSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("params") {
    ParamsSuite.checkParams(new RFormula())
  }

  test("transform numeric data") {
    val formula = new RFormula().setFormula("id ~ v1 + v2")
    val original = sqlContext.createDataFrame(
      Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")
    val model = formula.fit(original)
    val result = model.transform(original)
    val resultSchema = model.transformSchema(original.schema)
    val expected = sqlContext.createDataFrame(
      Seq(
        (0, 1.0, 3.0, Vectors.dense(1.0, 3.0), 0.0),
        (2, 2.0, 5.0, Vectors.dense(2.0, 5.0), 2.0))
      ).toDF("id", "v1", "v2", "features", "label")
    // TODO(ekl) make schema comparisons ignore metadata, to avoid .toString
    assert(result.schema.toString == resultSchema.toString)
    assert(resultSchema == expected.schema)
    assert(result.collect() === expected.collect())
  }

  test("features column already exists") {
    val formula = new RFormula().setFormula("y ~ x").setFeaturesCol("x")
    val original = sqlContext.createDataFrame(Seq((0, 1.0), (2, 2.0))).toDF("x", "y")
    intercept[IllegalArgumentException] {
      formula.fit(original)
    }
    intercept[IllegalArgumentException] {
      formula.fit(original)
    }
  }

  test("label column already exists") {
    val formula = new RFormula().setFormula("y ~ x").setLabelCol("y")
    val original = sqlContext.createDataFrame(Seq((0, 1.0), (2, 2.0))).toDF("x", "y")
    val model = formula.fit(original)
    val resultSchema = model.transformSchema(original.schema)
    assert(resultSchema.length == 3)
    assert(resultSchema.toString == model.transform(original).schema.toString)
  }

  test("label column already exists but is not double type") {
    val formula = new RFormula().setFormula("y ~ x").setLabelCol("y")
    val original = sqlContext.createDataFrame(Seq((0, 1), (2, 2))).toDF("x", "y")
    val model = formula.fit(original)
    intercept[IllegalArgumentException] {
      model.transformSchema(original.schema)
    }
    intercept[IllegalArgumentException] {
      model.transform(original)
    }
  }

  test("allow missing label column for test datasets") {
    val formula = new RFormula().setFormula("y ~ x").setLabelCol("label")
    val original = sqlContext.createDataFrame(Seq((0, 1.0), (2, 2.0))).toDF("x", "_not_y")
    val model = formula.fit(original)
    val resultSchema = model.transformSchema(original.schema)
    assert(resultSchema.length == 3)
    assert(!resultSchema.exists(_.name == "label"))
    assert(resultSchema.toString == model.transform(original).schema.toString)
  }

  test("encodes string terms") {
    val formula = new RFormula().setFormula("id ~ a + b")
    val original = sqlContext.createDataFrame(
      Seq((1, "foo", 4), (2, "bar", 4), (3, "bar", 5), (4, "baz", 5))
    ).toDF("id", "a", "b")
    val model = formula.fit(original)
    val result = model.transform(original)
    val resultSchema = model.transformSchema(original.schema)
    val expected = sqlContext.createDataFrame(
      Seq(
        (1, "foo", 4, Vectors.dense(0.0, 1.0, 4.0), 1.0),
        (2, "bar", 4, Vectors.dense(1.0, 0.0, 4.0), 2.0),
        (3, "bar", 5, Vectors.dense(1.0, 0.0, 5.0), 3.0),
        (4, "baz", 5, Vectors.dense(0.0, 0.0, 5.0), 4.0))
      ).toDF("id", "a", "b", "features", "label")
    assert(result.schema.toString == resultSchema.toString)
    assert(result.collect() === expected.collect())
  }

  test("index string label") {
    val formula = new RFormula().setFormula("id ~ a + b")
    val original = sqlContext.createDataFrame(
      Seq(("male", "foo", 4), ("female", "bar", 4), ("female", "bar", 5), ("male", "baz", 5))
    ).toDF("id", "a", "b")
    val model = formula.fit(original)
    val result = model.transform(original)
    val resultSchema = model.transformSchema(original.schema)
    val expected = sqlContext.createDataFrame(
      Seq(
        ("male", "foo", 4, Vectors.dense(0.0, 1.0, 4.0), 1.0),
        ("female", "bar", 4, Vectors.dense(1.0, 0.0, 4.0), 0.0),
        ("female", "bar", 5, Vectors.dense(1.0, 0.0, 5.0), 0.0),
        ("male", "baz", 5, Vectors.dense(0.0, 0.0, 5.0), 1.0))
    ).toDF("id", "a", "b", "features", "label")
    // assert(result.schema.toString == resultSchema.toString)
    assert(result.collect() === expected.collect())
  }

  test("attribute generation") {
    val formula = new RFormula().setFormula("id ~ a + b")
    val original = sqlContext.createDataFrame(
      Seq((1, "foo", 4), (2, "bar", 4), (3, "bar", 5), (4, "baz", 5))
    ).toDF("id", "a", "b")
    val model = formula.fit(original)
    val result = model.transform(original)
    val attrs = AttributeGroup.fromStructField(result.schema("features"))
    val expectedAttrs = new AttributeGroup(
      "features",
      Array(
        new BinaryAttribute(Some("a_bar"), Some(1)),
        new BinaryAttribute(Some("a_foo"), Some(2)),
        new NumericAttribute(Some("b"), Some(3))))
    assert(attrs === expectedAttrs)
  }

  test("vector attribute generation") {
    val formula = new RFormula().setFormula("id ~ vec")
    val original = sqlContext.createDataFrame(
      Seq((1, Vectors.dense(0.0, 1.0)), (2, Vectors.dense(1.0, 2.0)))
    ).toDF("id", "vec")
    val model = formula.fit(original)
    val result = model.transform(original)
    val attrs = AttributeGroup.fromStructField(result.schema("features"))
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](
        new NumericAttribute(Some("vec_0"), Some(1)),
        new NumericAttribute(Some("vec_1"), Some(2))))
    assert(attrs === expectedAttrs)
  }

  test("vector attribute generation with unnamed input attrs") {
    val formula = new RFormula().setFormula("id ~ vec2")
    val base = sqlContext.createDataFrame(
      Seq((1, Vectors.dense(0.0, 1.0)), (2, Vectors.dense(1.0, 2.0)))
    ).toDF("id", "vec")
    val metadata = new AttributeGroup(
      "vec2",
      Array[Attribute](
        NumericAttribute.defaultAttr,
        NumericAttribute.defaultAttr)).toMetadata
    val original = base.select(base.col("id"), base.col("vec").as("vec2", metadata))
    val model = formula.fit(original)
    val result = model.transform(original)
    val attrs = AttributeGroup.fromStructField(result.schema("features"))
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](
        new NumericAttribute(Some("vec2_0"), Some(1)),
        new NumericAttribute(Some("vec2_1"), Some(2))))
    assert(attrs === expectedAttrs)
  }

  test("numeric interaction") {
    val formula = new RFormula().setFormula("a ~ b:c:d")
    val original = sqlContext.createDataFrame(
      Seq((1, 2, 4, 2), (2, 3, 4, 1))
    ).toDF("a", "b", "c", "d")
    val model = formula.fit(original)
    val result = model.transform(original)
    val expected = sqlContext.createDataFrame(
      Seq(
        (1, 2, 4, 2, Vectors.dense(16.0), 1.0),
        (2, 3, 4, 1, Vectors.dense(12.0), 2.0))
      ).toDF("a", "b", "c", "d", "features", "label")
    assert(result.collect() === expected.collect())
    val attrs = AttributeGroup.fromStructField(result.schema("features"))
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](new NumericAttribute(Some("b:c:d"), Some(1))))
    assert(attrs === expectedAttrs)
  }

  test("factor numeric interaction") {
    val formula = new RFormula().setFormula("id ~ a:b")
    val original = sqlContext.createDataFrame(
      Seq((1, "foo", 4), (2, "bar", 4), (3, "bar", 5), (4, "baz", 5), (4, "baz", 5), (4, "baz", 5))
    ).toDF("id", "a", "b")
    val model = formula.fit(original)
    val result = model.transform(original)
    val expected = sqlContext.createDataFrame(
      Seq(
        (1, "foo", 4, Vectors.dense(0.0, 0.0, 4.0), 1.0),
        (2, "bar", 4, Vectors.dense(0.0, 4.0, 0.0), 2.0),
        (3, "bar", 5, Vectors.dense(0.0, 5.0, 0.0), 3.0),
        (4, "baz", 5, Vectors.dense(5.0, 0.0, 0.0), 4.0),
        (4, "baz", 5, Vectors.dense(5.0, 0.0, 0.0), 4.0),
        (4, "baz", 5, Vectors.dense(5.0, 0.0, 0.0), 4.0))
      ).toDF("id", "a", "b", "features", "label")
    assert(result.collect() === expected.collect())
    val attrs = AttributeGroup.fromStructField(result.schema("features"))
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](
        new NumericAttribute(Some("a_baz:b"), Some(1)),
        new NumericAttribute(Some("a_bar:b"), Some(2)),
        new NumericAttribute(Some("a_foo:b"), Some(3))))
    assert(attrs === expectedAttrs)
  }

  test("factor factor interaction") {
    val formula = new RFormula().setFormula("id ~ a:b")
    val original = sqlContext.createDataFrame(
      Seq((1, "foo", "zq"), (2, "bar", "zq"), (3, "bar", "zz"))
    ).toDF("id", "a", "b")
    val model = formula.fit(original)
    val result = model.transform(original)
    val expected = sqlContext.createDataFrame(
      Seq(
        (1, "foo", "zq", Vectors.dense(0.0, 0.0, 1.0, 0.0), 1.0),
        (2, "bar", "zq", Vectors.dense(1.0, 0.0, 0.0, 0.0), 2.0),
        (3, "bar", "zz", Vectors.dense(0.0, 1.0, 0.0, 0.0), 3.0))
      ).toDF("id", "a", "b", "features", "label")
    assert(result.collect() === expected.collect())
    val attrs = AttributeGroup.fromStructField(result.schema("features"))
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](
        new NumericAttribute(Some("a_bar:b_zq"), Some(1)),
        new NumericAttribute(Some("a_bar:b_zz"), Some(2)),
        new NumericAttribute(Some("a_foo:b_zq"), Some(3)),
        new NumericAttribute(Some("a_foo:b_zz"), Some(4))))
    assert(attrs === expectedAttrs)
  }
}
