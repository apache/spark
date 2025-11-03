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

import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class FeatureHasherSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  import FeatureHasherSuite.murmur3FeatureIdx

  implicit private val vectorEncoder: ExpressionEncoder[Vector] = ExpressionEncoder[Vector]()

  test("params") {
    ParamsSuite.checkParams(new FeatureHasher)
  }

  test("specify input cols using varargs or array") {
    val featureHasher1 = new FeatureHasher()
      .setInputCols("int", "double", "float", "stringNum", "string")
    val featureHasher2 = new FeatureHasher()
      .setInputCols(Array("int", "double", "float", "stringNum", "string"))
    assert(featureHasher1.getInputCols === featureHasher2.getInputCols)
  }

  test("feature hashing") {
    val numFeatures = 100
    // Assume perfect hash on field names in computing expected results
    def idx: Any => Int = murmur3FeatureIdx(numFeatures)

    val df = Seq(
      (2.0, true, "1", "foo",
        Vectors.sparse(numFeatures, Seq((idx("real"), 2.0), (idx("bool=true"), 1.0),
          (idx("stringNum=1"), 1.0), (idx("string=foo"), 1.0)))),
      (3.0, false, "2", "bar",
        Vectors.sparse(numFeatures, Seq((idx("real"), 3.0), (idx("bool=false"), 1.0),
          (idx("stringNum=2"), 1.0), (idx("string=bar"), 1.0))))
    ).toDF("real", "bool", "stringNum", "string", "expected")

    val hasher = new FeatureHasher()
      .setInputCols("real", "bool", "stringNum", "string")
      .setOutputCol("features")
      .setNumFeatures(numFeatures)
    val output = hasher.transform(df)
    val attrGroup = AttributeGroup.fromStructField(output.schema("features"))
    assert(attrGroup.numAttributes === Some(numFeatures))

    testTransformer[(Double, Boolean, String, String, Vector)](df, hasher, "features", "expected") {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14 )
    }
  }

  test("setting explicit numerical columns to treat as categorical") {
    val df = Seq(
      (2.0, 1, "foo"),
      (3.0, 2, "bar")
    ).toDF("real", "int", "string")

    val n = 100
    val hasher = new FeatureHasher()
      .setInputCols("real", "int", "string")
      .setCategoricalCols(Array("real"))
      .setOutputCol("features")
      .setNumFeatures(n)
    val output = hasher.transform(df)

    val features = output.select("features").as[Vector].collect()
    // Assume perfect hash on field names
    def idx: Any => Int = murmur3FeatureIdx(n)
    // check expected indices
    val expected = Seq(
      Vectors.sparse(n, Seq((idx("real=2.0"), 1.0), (idx("int"), 1.0), (idx("string=foo"), 1.0))),
      Vectors.sparse(n, Seq((idx("real=3.0"), 1.0), (idx("int"), 2.0), (idx("string=bar"), 1.0)))
    )
    assert(features.zip(expected).forall { case (e, a) => e ~== a absTol 1e-14 })
  }

  test("hashing works for all numeric types") {
    val df = Seq(5.0, 10.0, 15.0).toDF("real")

    val hasher = new FeatureHasher()
      .setInputCols("real")
      .setOutputCol("features")

    val expectedResult = hasher.transform(df).select("features").as[Vector].collect()
    // check all numeric types work as expected. String & boolean types are tested in default case
    val types =
      Seq(ShortType, LongType, IntegerType, FloatType, ByteType, DoubleType, DecimalType(10, 0))
    types.foreach { t =>
      val castDF = df.select(col("real").cast(t))
      val castResult = hasher.transform(castDF).select("features").as[Vector].collect()
      withClue(s"FeatureHasher works for all numeric types (testing $t): ") {
        assert(castResult.zip(expectedResult).forall { case (actual, expected) =>
          actual ~== expected absTol 1e-14
        })
      }
    }
  }

  test("invalid input type should fail") {
    val df = Seq(
      Vectors.dense(1),
      Vectors.dense(2)
    ).toDF("vec")

    intercept[IllegalArgumentException] {
      new FeatureHasher().setInputCols("vec").transform(df)
    }
  }

  test("hash collisions sum feature values") {
    val df = Seq(
      (1.0, "foo", "foo"),
      (2.0, "bar", "baz")
    ).toDF("real", "string1", "string2")

    val n = 1
    val hasher = new FeatureHasher()
      .setInputCols("real", "string1", "string2")
      .setOutputCol("features")
      .setNumFeatures(n)

    val features = hasher.transform(df).select("features").as[Vector].collect()
    def idx: Any => Int = murmur3FeatureIdx(n)
    // everything should hash into one field
    assert(idx("real") === idx("string1=foo"))
    assert(idx("string1=foo") === idx("string2=foo"))
    assert(idx("string2=foo") === idx("string1=bar"))
    assert(idx("string1=bar") === idx("string2=baz"))
    val expected = Seq(
      Vectors.sparse(n, Seq((idx("string1=foo"), 3.0))),
      Vectors.sparse(n, Seq((idx("string2=bar"), 4.0)))
    )
    assert(features.zip(expected).forall { case (e, a) => e ~== a absTol 1e-14 })
  }

  test("ignores null values in feature hashing") {
    import org.apache.spark.sql.functions._

    val df = Seq(
      (2.0, "foo", null),
      (3.0, "bar", "baz")
    ).toDF("real", "string1", "string2").select(
      when(col("real") === 3.0, null).otherwise(col("real")).alias("real"),
      col("string1"),
      col("string2")
    )

    val n = 100
    val hasher = new FeatureHasher()
      .setInputCols("real", "string1", "string2")
      .setOutputCol("features")
      .setNumFeatures(n)

    val features = hasher.transform(df).select("features").as[Vector].collect()
    def idx: Any => Int = murmur3FeatureIdx(n)
    val expected = Seq(
      Vectors.sparse(n, Seq((idx("real"), 2.0), (idx("string1=foo"), 1.0))),
      Vectors.sparse(n, Seq((idx("string1=bar"), 1.0), (idx("string2=baz"), 1.0)))
    )
    assert(features.zip(expected).forall { case (e, a) => e ~== a absTol 1e-14 })
  }

  test("unicode column names and values") {
    // scalastyle:off nonascii
    val df = Seq((2.0, "中文")).toDF("中文", "unicode")

    val n = 100
    val hasher = new FeatureHasher()
      .setInputCols("中文", "unicode")
      .setOutputCol("features")
      .setNumFeatures(n)

    val features = hasher.transform(df).select("features").as[Vector].collect()
    def idx: Any => Int = murmur3FeatureIdx(n)
    val expected = Seq(
      Vectors.sparse(n, Seq((idx("中文"), 2.0), (idx("unicode=中文"), 1.0)))
    )
    assert(features.zip(expected).forall { case (e, a) => e ~== a absTol 1e-14 })
    // scalastyle:on nonascii
  }

  test("read/write") {
    val t = new FeatureHasher()
      .setInputCols(Array("myCol1", "myCol2", "myCol3"))
      .setOutputCol("myOutputCol")
      .setNumFeatures(10)
    testDefaultReadWrite(t)
  }

  test("FeatureHasher with nested input columns") {
    val df = Seq(5.0, 10.0, 15.0).toDF("real")
    val nestDf = df.select(struct(col("real")).as("nest"))

    val hasher = new FeatureHasher()
      .setInputCols("nest.real")
      .setOutputCol("features")

    val expectedResult = hasher.transform(nestDf).select("features").as[Vector].collect()
    // check all numeric types work as expected. String & boolean types are tested in default case
    val types =
      Seq(ShortType, LongType, IntegerType, FloatType, ByteType, DoubleType, DecimalType(10, 0))
    types.foreach { t =>
      val castDF = df.select(col("real").cast(t))
        .select(struct(col("real")).as("nest"))
      val castResult = hasher.transform(castDF).select("features").as[Vector].collect()
      withClue(s"FeatureHasher works for all numeric types (testing $t): ") {
        assert(castResult.zip(expectedResult).forall { case (actual, expected) =>
          actual ~== expected absTol 1e-14
        })
      }
    }
  }
}

object FeatureHasherSuite {

  private[feature] def murmur3FeatureIdx(numFeatures: Int)(term: Any): Int = {
    Utils.nonNegativeMod(FeatureHasher.murmur3Hash(term), numFeatures)
  }

}
