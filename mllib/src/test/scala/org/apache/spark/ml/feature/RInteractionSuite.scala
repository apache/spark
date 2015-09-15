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

class RInteractionSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("params") {
    ParamsSuite.checkParams(new RInteraction())
  }

  test("parameter validation") {
    val data = sqlContext.createDataFrame(
      Seq((1, "foo", "zq"), (2, "bar", "zq"), (3, "bar", "zz"))
    ).toDF("id", "a", "b")
    def check(inputCols: Array[String], outputCol: String, expectOk: Boolean): Unit = {
      val interaction = new RInteraction()
      if (inputCols != null) {
        interaction.setInputCols(inputCols)
      }
      if (outputCol != null) {
        interaction.setOutputCol(outputCol)
      }
      if (expectOk) {
        interaction.transformSchema(data.schema)
        interaction.fit(data).transform(data).collect()
      } else {
        intercept[IllegalArgumentException] {
          interaction.fit(data)
        }
        intercept[IllegalArgumentException] {
          interaction.transformSchema(data.schema)
        }
      }
    }
    check(Array("a", "b"), "test", true)
    check(Array("id"), "test", true)
    check(Array("b"), "test", true)
    check(Array("b"), "test", true)
    check(Array(), "test", false)
    check(Array("a", "b", "b"), "id", false)
    check(Array("a", "b"), null, false)
    check(null, "test", false)
  }

  test("numeric interaction") {
    val interaction = new RInteraction()
      .setInputCols(Array("b", "c", "d"))
      .setOutputCol("test")
    val original = sqlContext.createDataFrame(
      Seq((1, 2, 4, 2), (2, 3, 4, 1))
    ).toDF("a", "b", "c", "d")
    val model = interaction.fit(original)
    val result = model.transform(original)
    val expected = sqlContext.createDataFrame(
      Seq(
        (1, 2, 4, 2, 16.0),
        (2, 3, 4, 1, 12.0))
      ).toDF("a", "b", "c", "d", "test")
    assert(result.collect() === expected.collect())
    val attr = Attribute.decodeStructField(result.schema("test"), preserveName = true)
    val expectedAttr = new NumericAttribute(Some("b:c:d"), None)
    assert(attr === expectedAttr)
  }

  test("factor interaction") {
    val interaction = new RInteraction()
      .setInputCols(Array("a", "b"))
      .setOutputCol("test")
    val original = sqlContext.createDataFrame(
      Seq((1, "foo", "zq"), (2, "bar", "zq"), (3, "bar", "zz"))
    ).toDF("id", "a", "b")
    val model = interaction.fit(original)
    val result = model.transform(original)
    val expected = sqlContext.createDataFrame(
      Seq(
        (1, "foo", "zq", Vectors.dense(0.0, 1.0, 0.0, 0.0)),
        (2, "bar", "zq", Vectors.dense(1.0, 0.0, 0.0, 0.0)),
        (3, "bar", "zz", Vectors.dense(0.0, 0.0, 1.0, 0.0)))
      ).toDF("id", "a", "b", "test")
    assert(result.collect() === expected.collect())
    val attrs = AttributeGroup.fromStructField(result.schema("test"))
    val expectedAttrs = new AttributeGroup(
      "test",
      Array[Attribute](
        new BinaryAttribute(Some("a_bar:b_zq"), Some(1)),
        new BinaryAttribute(Some("a_foo:b_zq"), Some(2)),
        new BinaryAttribute(Some("a_bar:b_zz"), Some(3)),
        new BinaryAttribute(Some("a_foo:b_zz"), Some(4))))
    assert(attrs === expectedAttrs)
  }

  test("factor numeric interaction") {
    val interaction = new RInteraction()
      .setInputCols(Array("a", "b"))
      .setOutputCol("test")
    val original = sqlContext.createDataFrame(
      Seq((1, "foo", 4), (2, "bar", 4), (3, "bar", 5), (4, "baz", 5), (4, "baz", 5), (4, "baz", 5))
    ).toDF("id", "a", "b")
    val model = interaction.fit(original)
    val result = model.transform(original)
    val expected = sqlContext.createDataFrame(
      Seq(
        (1, "foo", 4, Vectors.dense(0.0, 0.0, 4.0)),
        (2, "bar", 4, Vectors.dense(0.0, 4.0, 0.0)),
        (3, "bar", 5, Vectors.dense(0.0, 5.0, 0.0)),
        (4, "baz", 5, Vectors.dense(5.0, 0.0, 0.0)),
        (4, "baz", 5, Vectors.dense(5.0, 0.0, 0.0)),
        (4, "baz", 5, Vectors.dense(5.0, 0.0, 0.0)))
      ).toDF("id", "a", "b", "test")
    assert(result.collect() === expected.collect())
    val attrs = AttributeGroup.fromStructField(result.schema("test"))
    val expectedAttrs = new AttributeGroup(
      "test",
      Array[Attribute](
        new BinaryAttribute(Some("a_baz:b"), Some(1)),
        new BinaryAttribute(Some("a_bar:b"), Some(2)),
        new BinaryAttribute(Some("a_foo:b"), Some(3))))
    assert(attrs === expectedAttrs)
  }
}
