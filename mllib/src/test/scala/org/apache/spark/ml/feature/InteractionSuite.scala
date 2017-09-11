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

import scala.collection.mutable.ArrayBuilder

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.functions.col

class InteractionSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new Interaction())
  }

  test("feature encoder") {
    def encode(cardinalities: Array[Int], value: Any): Vector = {
      var indices = ArrayBuilder.make[Int]
      var values = ArrayBuilder.make[Double]
      val encoder = new FeatureEncoder(cardinalities)
      encoder.foreachNonzeroOutput(value, (i, v) => {
        indices += i
        values += v
      })
      Vectors.sparse(encoder.outputSize, indices.result(), values.result()).compressed
    }
    assert(encode(Array(1), 2.2) === Vectors.dense(2.2))
    assert(encode(Array(3), Vectors.dense(1)) === Vectors.dense(0, 1, 0))
    assert(encode(Array(1, 1), Vectors.dense(1.1, 2.2)) === Vectors.dense(1.1, 2.2))
    assert(encode(Array(3, 1), Vectors.dense(1, 2.2)) === Vectors.dense(0, 1, 0, 2.2))
    assert(encode(Array(2, 1), Vectors.dense(1, 2.2)) === Vectors.dense(0, 1, 2.2))
    assert(encode(Array(2, 1, 1), Vectors.dense(0, 2.2, 0)) === Vectors.dense(1, 0, 2.2, 0))
    intercept[SparkException] { encode(Array(1), "foo") }
    intercept[SparkException] { encode(Array(1), null) }
    intercept[AssertionError] { encode(Array(2), 2.2) }
    intercept[AssertionError] { encode(Array(3), Vectors.dense(2.2)) }
    intercept[AssertionError] { encode(Array(1), Vectors.dense(1.0, 2.0, 3.0)) }
    intercept[AssertionError] { encode(Array(3), Vectors.dense(-1)) }
    intercept[AssertionError] { encode(Array(3), Vectors.dense(3)) }
  }

  test("numeric interaction") {
    val data = Seq(
      (2, Vectors.dense(3.0, 4.0)),
      (1, Vectors.dense(1.0, 5.0))
    ).toDF("a", "b")
    val groupAttr = new AttributeGroup(
      "b",
      Array[Attribute](
        NumericAttribute.defaultAttr.withName("foo"),
        NumericAttribute.defaultAttr.withName("bar")))
    val df = data.select(
      col("a").as("a", NumericAttribute.defaultAttr.toMetadata()),
      col("b").as("b", groupAttr.toMetadata()))
    val trans = new Interaction().setInputCols(Array("a", "b")).setOutputCol("features")
    val res = trans.transform(df)
    val expected = Seq(
      (2, Vectors.dense(3.0, 4.0), Vectors.dense(6.0, 8.0)),
      (1, Vectors.dense(1.0, 5.0), Vectors.dense(1.0, 5.0))
    ).toDF("a", "b", "features")
    assert(res.collect() === expected.collect())
    val attrs = AttributeGroup.fromStructField(res.schema("features"))
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](
        new NumericAttribute(Some("a:b_foo"), Some(1)),
        new NumericAttribute(Some("a:b_bar"), Some(2))))
    assert(attrs === expectedAttrs)
  }

  test("nominal interaction") {
    val data = Seq(
      (2, Vectors.dense(3.0, 4.0)),
      (1, Vectors.dense(1.0, 5.0))
    ).toDF("a", "b")
    val groupAttr = new AttributeGroup(
      "b",
      Array[Attribute](
        NumericAttribute.defaultAttr.withName("foo"),
        NumericAttribute.defaultAttr.withName("bar")))
    val df = data.select(
      col("a").as(
        "a", NominalAttribute.defaultAttr.withValues(Array("up", "down", "left")).toMetadata()),
      col("b").as("b", groupAttr.toMetadata()))
    val trans = new Interaction().setInputCols(Array("a", "b")).setOutputCol("features")
    val res = trans.transform(df)
    val expected = Seq(
      (2, Vectors.dense(3.0, 4.0), Vectors.dense(0, 0, 0, 0, 3, 4)),
      (1, Vectors.dense(1.0, 5.0), Vectors.dense(0, 0, 1, 5, 0, 0))
    ).toDF("a", "b", "features")
    assert(res.collect() === expected.collect())
    val attrs = AttributeGroup.fromStructField(res.schema("features"))
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](
        new NumericAttribute(Some("a_up:b_foo"), Some(1)),
        new NumericAttribute(Some("a_up:b_bar"), Some(2)),
        new NumericAttribute(Some("a_down:b_foo"), Some(3)),
        new NumericAttribute(Some("a_down:b_bar"), Some(4)),
        new NumericAttribute(Some("a_left:b_foo"), Some(5)),
        new NumericAttribute(Some("a_left:b_bar"), Some(6))))
    assert(attrs === expectedAttrs)
  }

  test("default attr names") {
    val data = Seq(
        (2, Vectors.dense(0.0, 4.0), 1.0),
        (1, Vectors.dense(1.0, 5.0), 10.0)
      ).toDF("a", "b", "c")
    val groupAttr = new AttributeGroup(
      "b",
      Array[Attribute](
        NominalAttribute.defaultAttr.withNumValues(2),
        NumericAttribute.defaultAttr))
    val df = data.select(
      col("a").as("a", NominalAttribute.defaultAttr.withNumValues(3).toMetadata()),
      col("b").as("b", groupAttr.toMetadata()),
      col("c").as("c", NumericAttribute.defaultAttr.toMetadata()))
    val trans = new Interaction().setInputCols(Array("a", "b", "c")).setOutputCol("features")
    val res = trans.transform(df)
    val expected = Seq(
      (2, Vectors.dense(0.0, 4.0), 1.0, Vectors.dense(0, 0, 0, 0, 0, 0, 1, 0, 4)),
      (1, Vectors.dense(1.0, 5.0), 10.0, Vectors.dense(0, 0, 0, 0, 10, 50, 0, 0, 0))
    ).toDF("a", "b", "c", "features")
    assert(res.collect() === expected.collect())
    val attrs = AttributeGroup.fromStructField(res.schema("features"))
    val expectedAttrs = new AttributeGroup(
      "features",
      Array[Attribute](
        new NumericAttribute(Some("a_0:b_0_0:c"), Some(1)),
        new NumericAttribute(Some("a_0:b_0_1:c"), Some(2)),
        new NumericAttribute(Some("a_0:b_1:c"), Some(3)),
        new NumericAttribute(Some("a_1:b_0_0:c"), Some(4)),
        new NumericAttribute(Some("a_1:b_0_1:c"), Some(5)),
        new NumericAttribute(Some("a_1:b_1:c"), Some(6)),
        new NumericAttribute(Some("a_2:b_0_0:c"), Some(7)),
        new NumericAttribute(Some("a_2:b_0_1:c"), Some(8)),
        new NumericAttribute(Some("a_2:b_1:c"), Some(9))))
    assert(attrs === expectedAttrs)
  }

  test("read/write") {
    val t = new Interaction()
      .setInputCols(Array("myInputCol", "myInputCol2"))
      .setOutputCol("myOutputCol")
    testDefaultReadWrite(t)
  }
}
