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

import org.apache.spark.ml.attribute.{AttributeGroup, BinaryAttribute, NominalAttribute}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.{DataFrame, Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class OneHotEncoderSuite
  extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  def stringIndexed(): DataFrame = {
    val data = Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    val df = data.toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)
    indexer.transform(df)
  }

  test("params") {
    ParamsSuite.checkParams(new OneHotEncoder)
  }

  test("OneHotEncoder dropLast = false") {
    val transformed = stringIndexed()
    val encoder = new OneHotEncoder()
      .setInputCol("labelIndex")
      .setOutputCol("labelVec")
    assert(encoder.getDropLast === true)
    encoder.setDropLast(false)
    assert(encoder.getDropLast === false)
    val expected = Seq(
      (0, Vectors.sparse(3, Seq((0, 1.0)))),
      (1, Vectors.sparse(3, Seq((2, 1.0)))),
      (2, Vectors.sparse(3, Seq((1, 1.0)))),
      (3, Vectors.sparse(3, Seq((0, 1.0)))),
      (4, Vectors.sparse(3, Seq((0, 1.0)))),
      (5, Vectors.sparse(3, Seq((1, 1.0))))).toDF("id", "expected")

    val withExpected = transformed.join(expected, "id")
    testTransformer[(Int, String, Double, Vector)](withExpected, encoder, "labelVec", "expected") {
      case Row(output: Vector, expected: Vector) =>
        assert(output === expected)
    }
  }

  test("OneHotEncoder dropLast = true") {
    val transformed = stringIndexed()
    val encoder = new OneHotEncoder()
      .setInputCol("labelIndex")
      .setOutputCol("labelVec")
    val expected = Seq(
      (0, Vectors.sparse(2, Seq((0, 1.0)))),
      (1, Vectors.sparse(2, Seq())),
      (2, Vectors.sparse(2, Seq((1, 1.0)))),
      (3, Vectors.sparse(2, Seq((0, 1.0)))),
      (4, Vectors.sparse(2, Seq((0, 1.0)))),
      (5, Vectors.sparse(2, Seq((1, 1.0))))).toDF("id", "expected")

    val withExpected = transformed.join(expected, "id")
    testTransformer[(Int, String, Double, Vector)](withExpected, encoder, "labelVec", "expected") {
      case Row(output: Vector, expected: Vector) =>
        assert(output === expected)
    }
  }

  test("input column with ML attribute") {
    val attr = NominalAttribute.defaultAttr.withValues("small", "medium", "large")
    val df = Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply).toDF("size")
      .select(col("size").as("size", attr.toMetadata()))
    val encoder = new OneHotEncoder()
      .setInputCol("size")
      .setOutputCol("encoded")
    testTransformerByGlobalCheckFunc[(Double)](df, encoder, "encoded") { rows =>
      val group = AttributeGroup.fromStructField(rows.head.schema("encoded"))
      assert(group.size === 2)
      assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("small").withIndex(0))
      assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("medium").withIndex(1))
    }
  }


  test("input column without ML attribute") {
    val df = Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply).toDF("index")
    val encoder = new OneHotEncoder()
      .setInputCol("index")
      .setOutputCol("encoded")
    val rows = encoder.transform(df).select("encoded").collect()
    val group = AttributeGroup.fromStructField(rows.head.schema("encoded"))
    assert(group.size === 2)
    assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("0").withIndex(0))
    assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("1").withIndex(1))
  }

  test("read/write") {
    val t = new OneHotEncoder()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setDropLast(false)
    testDefaultReadWrite(t)
  }

  test("OneHotEncoder with varying types") {
    val df = stringIndexed()
    val attr = NominalAttribute.defaultAttr.withValues("small", "medium", "large")
    val expected = Seq(
      (0, Vectors.sparse(3, Seq((0, 1.0)))),
      (1, Vectors.sparse(3, Seq((2, 1.0)))),
      (2, Vectors.sparse(3, Seq((1, 1.0)))),
      (3, Vectors.sparse(3, Seq((0, 1.0)))),
      (4, Vectors.sparse(3, Seq((0, 1.0)))),
      (5, Vectors.sparse(3, Seq((1, 1.0))))).toDF("id", "expected")

    val withExpected = df.join(expected, "id")

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
      val dfWithTypes = withExpected.select(col("labelIndex")
        .cast(t.numericType).as("labelIndex", attr.toMetadata()), col("expected"))
      val encoder = new OneHotEncoder()
        .setInputCol("labelIndex")
        .setOutputCol("labelVec")
        .setDropLast(false)

      testTransformer(dfWithTypes, encoder, "labelVec", "expected") {
        case Row(output: Vector, expected: Vector) =>
          assert(output === expected)
      }(t.encoder)
    }
  }
}
