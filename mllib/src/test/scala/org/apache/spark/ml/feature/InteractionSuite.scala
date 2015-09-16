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
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.functions.col

class RInteractionSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("params") {
    ParamsSuite.checkParams(new Interaction())
  }

  test("encoding iterator") {
    def encode(cardinalities: Array[Int], value: Any): Vector = {
      var indices = ArrayBuilder.make[Int]
      var values = ArrayBuilder.make[Double]
      val iter = new EncodingIterator(cardinalities)
      iter.foreachActive(value, (i, v) => {
        indices += i
        values += v
      })
      Vectors.sparse(iter.size, indices.result(), values.result()).compressed
    }
    assert(encode(Array(0), 2.2) === Vectors.dense(2.2))
    assert(encode(Array(3), Vectors.dense(1)) === Vectors.dense(0, 1, 0))
    assert(encode(Array(0, 0), Vectors.dense(1.1, 2.2)) === Vectors.dense(1.1, 2.2))
    assert(encode(Array(3, 0), Vectors.dense(1, 2.2)) === Vectors.dense(0, 1, 0, 2.2))
    assert(encode(Array(2, 0), Vectors.dense(1, 2.2)) === Vectors.dense(0, 1, 2.2))
    assert(encode(Array(2, 0, 1), Vectors.dense(0, 2.2, 0)) === Vectors.dense(1, 0, 2.2, 1))
    intercept[SparkException] { encode(Array(0), "foo") }
    intercept[SparkException] { encode(Array(0), null) }
    intercept[AssertionError] { encode(Array(1), 2.2) }
    intercept[AssertionError] { encode(Array(3), Vectors.dense(2.2)) }
    intercept[AssertionError] { encode(Array(1), Vectors.dense(1.0, 2.0, 3.0)) }
  }

  test("numeric interaction") {
    val data = sqlContext.createDataFrame(
      Seq(
        (2, Vectors.dense(3.0, 4.0)),
        (1, Vectors.dense(1.0, 5.0)))
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
    val expected = sqlContext.createDataFrame(
      Seq(
        (2, Vectors.dense(3.0, 4.0), Vectors.dense(6.0, 8.0)),
        (1, Vectors.dense(1.0, 5.0), Vectors.dense(1.0, 5.0)))
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
    val data = sqlContext.createDataFrame(
      Seq(
        (2, Vectors.dense(3.0, 4.0)),
        (1, Vectors.dense(1.0, 5.0)))
      ).toDF("a", "b")
    val groupAttr = new AttributeGroup(
      "b",
      Array[Attribute](
        NumericAttribute.defaultAttr.withName("foo"),
        NumericAttribute.defaultAttr.withName("bar")))
    val df = data.select(
      col("a").as("a",
        NominalAttribute.defaultAttr.withValues(Array("up", "down", "left")).toMetadata()),
      col("b").as("b", groupAttr.toMetadata()))
    val trans = new Interaction().setInputCols(Array("a", "b")).setOutputCol("features")
    val res = trans.transform(df)
    val expected = sqlContext.createDataFrame(
      Seq(
        (2, Vectors.dense(3.0, 4.0), Vectors.dense(0, 0, 0, 0, 3, 4)),
        (1, Vectors.dense(1.0, 5.0), Vectors.dense(0, 0, 1, 5, 0, 0)))
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

//  test("new interaction") {
//    val data = sqlContext.createDataFrame(
//      Seq(
//        (0, "foo", true, 4, Vectors.dense(0.0, 0.0, 1.0), Vectors.dense(5.0, 3.0)),
//        (0, "bar", true, 4, Vectors.dense(1.0, 4.0, 2.0), Vectors.dense(4.0, 3.0)),
//        (0, "bar", true, 5, Vectors.dense(2.0, 5.0, 3.0), Vectors.dense(5.0, 3.0)),
//        (0, "baz", true, 5, Vectors.dense(3.0, 8.0, 4.0), Vectors.dense(5.0, 2.0)),
//        (0, "baz", false, 5, Vectors.dense(4.0, 9.0, 8.0), Vectors.dense(7.0, 1.0)),
//        (1, "baz", false, 5, Vectors.dense(5.0, 2.0, 9.0), Vectors.dense(2.0, 0.0)))
//      ).toDF("id", "a", "bin", "b", "test", "test2")
//    val attrs = new AttributeGroup(
//      "test",
//      Array[Attribute](
//        NominalAttribute.defaultAttr.withValues(Array("a", "b", "c", "d", "e", "f")),
//        NumericAttribute.defaultAttr.withName("magnitude"),
//        NominalAttribute.defaultAttr.withName("colors").withValues(
//          Array("green", "blue", "red", "violet", "yellow",
//            "orange", "black", "white", "azure", "gray"))))
//
//    val idAttr = NominalAttribute.defaultAttr.withValues(Array("red", "blue"))
//    val attrs2 = new AttributeGroup(
//      "test2",
//      Array[Attribute](
//        NumericAttribute.defaultAttr,
//        NominalAttribute.defaultAttr.withValues(Array("one", "two", "three", "four"))))
//    val df = data.select(
//      col("id").as("id", idAttr.toMetadata()), col("b"), col("bin"),
//      col("test").as("test", attrs.toMetadata()),
//      col("test2").as("test2", attrs2.toMetadata()))
//    df.collect.foreach(println)
//    println(df.schema)
//    df.schema.foreach { field =>
//      println(field.metadata)
//    }
////    val trans = new Interaction().setInputCols(Array("id", "test2", "test")).setOutputCol("feature")
//    val trans = new Interaction().setInputCols(Array("id", "test2")).setOutputCol("feature")
//    val res = trans.transform(df)
//    res.collect.foreach(println)
//    println(res.schema)
//    res.schema.foreach { field =>
//      println(field.metadata)
//    }
//  }
//
//  test("parameter validation") {
//    val data = sqlContext.createDataFrame(
//      Seq((1, "foo", "zq"), (2, "bar", "zq"), (3, "bar", "zz"))
//    ).toDF("id", "a", "b")
//    def check(inputCols: Array[String], outputCol: String, expectOk: Boolean): Unit = {
//      val interaction = new RInteraction()
//      if (inputCols != null) {
//        interaction.setInputCols(inputCols)
//      }
//      if (outputCol != null) {
//        interaction.setOutputCol(outputCol)
//      }
//      if (expectOk) {
//        interaction.transformSchema(data.schema)
//        interaction.fit(data).transform(data).collect()
//      } else {
//        intercept[IllegalArgumentException] {
//          interaction.fit(data)
//        }
//        intercept[IllegalArgumentException] {
//          interaction.transformSchema(data.schema)
//        }
//      }
//    }
//    check(Array("a", "b"), "test", true)
//    check(Array("id"), "test", true)
//    check(Array("b"), "test", true)
//    check(Array("b"), "test", true)
//    check(Array(), "test", false)
//    check(Array("a", "b", "b"), "id", false)
//    check(Array("a", "b"), null, false)
//    check(null, "test", false)
//  }
//
//  test("numeric interaction") {
//    val interaction = new RInteraction()
//      .setInputCols(Array("b", "c", "d"))
//      .setOutputCol("test")
//    val original = sqlContext.createDataFrame(
//      Seq((1, 2, 4, 2), (2, 3, 4, 1))
//    ).toDF("a", "b", "c", "d")
//    val model = interaction.fit(original)
//    val result = model.transform(original)
//    val expected = sqlContext.createDataFrame(
//      Seq(
//        (1, 2, 4, 2, 16.0),
//        (2, 3, 4, 1, 12.0))
//      ).toDF("a", "b", "c", "d", "test")
//    assert(result.collect() === expected.collect())
//    val attr = Attribute.decodeStructField(result.schema("test"), preserveName = true)
//    val expectedAttr = new NumericAttribute(Some("b:c:d"), None)
//    assert(attr === expectedAttr)
//  }
//
//  test("factor interaction") {
//    val interaction = new RInteraction()
//      .setInputCols(Array("a", "b"))
//      .setOutputCol("test")
//    val original = sqlContext.createDataFrame(
//      Seq((1, "foo", "zq"), (2, "bar", "zq"), (3, "bar", "zz"))
//    ).toDF("id", "a", "b")
//    val model = interaction.fit(original)
//    val result = model.transform(original)
//    val expected = sqlContext.createDataFrame(
//      Seq(
//        (1, "foo", "zq", Vectors.dense(0.0, 1.0, 0.0, 0.0)),
//        (2, "bar", "zq", Vectors.dense(1.0, 0.0, 0.0, 0.0)),
//        (3, "bar", "zz", Vectors.dense(0.0, 0.0, 1.0, 0.0)))
//      ).toDF("id", "a", "b", "test")
//    assert(result.collect() === expected.collect())
//    val attrs = AttributeGroup.fromStructField(result.schema("test"))
//    val expectedAttrs = new AttributeGroup(
//      "test",
//      Array[Attribute](
//        new BinaryAttribute(Some("a_bar:b_zq"), Some(1)),
//        new BinaryAttribute(Some("a_foo:b_zq"), Some(2)),
//        new BinaryAttribute(Some("a_bar:b_zz"), Some(3)),
//        new BinaryAttribute(Some("a_foo:b_zz"), Some(4))))
//    assert(attrs === expectedAttrs)
//  }
//
//  test("factor numeric interaction") {
//    val interaction = new RInteraction()
//      .setInputCols(Array("a", "b"))
//      .setOutputCol("test")
//    val original = sqlContext.createDataFrame(
//      Seq((1, "foo", 4), (2, "bar", 4), (3, "bar", 5), (4, "baz", 5), (4, "baz", 5), (4, "baz", 5))
//    ).toDF("id", "a", "b")
//    val model = interaction.fit(original)
//    val result = model.transform(original)
//    val expected = sqlContext.createDataFrame(
//      Seq(
//        (1, "foo", 4, Vectors.dense(0.0, 0.0, 4.0)),
//        (2, "bar", 4, Vectors.dense(0.0, 4.0, 0.0)),
//        (3, "bar", 5, Vectors.dense(0.0, 5.0, 0.0)),
//        (4, "baz", 5, Vectors.dense(5.0, 0.0, 0.0)),
//        (4, "baz", 5, Vectors.dense(5.0, 0.0, 0.0)),
//        (4, "baz", 5, Vectors.dense(5.0, 0.0, 0.0)))
//      ).toDF("id", "a", "b", "test")
//    assert(result.collect() === expected.collect())
//    val attrs = AttributeGroup.fromStructField(result.schema("test"))
//    val expectedAttrs = new AttributeGroup(
//      "test",
//      Array[Attribute](
//        new BinaryAttribute(Some("a_baz:b"), Some(1)),
//        new BinaryAttribute(Some("a_bar:b"), Some(2)),
//        new BinaryAttribute(Some("a_foo:b"), Some(3))))
//    assert(attrs === expectedAttrs)
//  }
}
