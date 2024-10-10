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

package org.apache.spark.sql

import java.sql.{Date, Timestamp}

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.test.SharedSparkSession

case class ReflectData(
    stringField: String,
    intField: Int,
    longField: Long,
    floatField: Float,
    doubleField: Double,
    shortField: Short,
    byteField: Byte,
    booleanField: Boolean,
    decimalField: java.math.BigDecimal,
    date: Date,
    timestampField: Timestamp,
    seqInt: Seq[Int],
    javaBigInt: java.math.BigInteger,
    scalaBigInt: scala.math.BigInt)

case class NullReflectData(
    intField: java.lang.Integer,
    longField: java.lang.Long,
    floatField: java.lang.Float,
    doubleField: java.lang.Double,
    shortField: java.lang.Short,
    byteField: java.lang.Byte,
    booleanField: java.lang.Boolean)

case class OptionalReflectData(
    intField: Option[Int],
    longField: Option[Long],
    floatField: Option[Float],
    doubleField: Option[Double],
    shortField: Option[Short],
    byteField: Option[Byte],
    booleanField: Option[Boolean])

case class ReflectBinary(data: Array[Byte])

case class Nested(i: Option[Int], s: String)

case class Data(
    array: Seq[Int],
    arrayContainsNull: Seq[Option[Int]],
    map: Map[Int, Long],
    mapContainsNul: Map[Int, Option[Long]],
    nested: Nested)

case class ComplexReflectData(
    arrayField: Seq[Int],
    arrayFieldContainsNull: Seq[Option[Int]],
    mapField: Map[Int, Long],
    mapFieldContainsNull: Map[Int, Option[Long]],
    dataField: Data)

case class InvalidInJava(`abstract`: Int)

class ScalaReflectionRelationSuite extends SparkFunSuite with SharedSparkSession {
  import testImplicits._

  // To avoid syntax error thrown by genjavadoc, make this case class non-top level and private.
  private case class InvalidInJava2(`0`: Int)

  test("query case class RDD") {
    withTempView("reflectData") {
      val data = ReflectData("a", 1, 1L, 1.toFloat, 1.toDouble, 1.toShort, 1.toByte, true,
        new java.math.BigDecimal(1), Date.valueOf("1970-01-01"), new Timestamp(12345), Seq(1, 2, 3),
        new java.math.BigInteger("1"), scala.math.BigInt(1))
      Seq(data).toDF().createOrReplaceTempView("reflectData")

      assert(sql("SELECT * FROM reflectData").collect().head ===
        Row("a", 1, 1L, 1.toFloat, 1.toDouble, 1.toShort, 1.toByte, true,
          new java.math.BigDecimal(1), Date.valueOf("1970-01-01"),
          new Timestamp(12345), Seq(1, 2, 3), new java.math.BigDecimal(1),
          new java.math.BigDecimal(1)))
    }
  }

  test("query case class RDD with nulls") {
    withTempView("reflectNullData") {
      val data = NullReflectData(null, null, null, null, null, null, null)
      Seq(data).toDF().createOrReplaceTempView("reflectNullData")

      assert(sql("SELECT * FROM reflectNullData").collect().head ===
        Row.fromSeq(Seq.fill(7)(null)))
    }
  }

  test("query case class RDD with Nones") {
    withTempView("reflectOptionalData") {
      val data = OptionalReflectData(None, None, None, None, None, None, None)
      Seq(data).toDF().createOrReplaceTempView("reflectOptionalData")

      assert(sql("SELECT * FROM reflectOptionalData").collect().head ===
        Row.fromSeq(Seq.fill(7)(null)))
    }
  }

  // Equality is broken for Arrays, so we test that separately.
  test("query binary data") {
    withTempView("reflectBinary") {
      Seq(ReflectBinary(Array[Byte](1))).toDF().createOrReplaceTempView("reflectBinary")

      val result = sql("SELECT data FROM reflectBinary")
        .collect().head(0).asInstanceOf[Array[Byte]]
      assert(result.toSeq === Seq[Byte](1))
    }
  }

  test("query complex data") {
    withTempView("reflectComplexData") {
      val data = ComplexReflectData(
        Seq(1, 2, 3),
        Seq(Some(1), Some(2), None),
        Map(1 -> 10L, 2 -> 20L),
        Map(1 -> Some(10L), 2 -> Some(20L), 3 -> None),
        Data(
          Seq(10, 20, 30),
          Seq(Some(10), Some(20), None),
          Map(10 -> 100L, 20 -> 200L),
          Map(10 -> Some(100L), 20 -> Some(200L), 30 -> None),
          Nested(None, "abc")))

      Seq(data).toDF().createOrReplaceTempView("reflectComplexData")
      assert(sql("SELECT * FROM reflectComplexData").collect().head ===
        Row(
          Seq(1, 2, 3),
          Seq(1, 2, null),
          Map(1 -> 10L, 2 -> 20L),
          Map(1 -> 10L, 2 -> 20L, 3 -> null),
          Row(
            Seq(10, 20, 30),
            Seq(10, 20, null),
            Map(10 -> 100L, 20 -> 200L),
            Map(10 -> 100L, 20 -> 200L, 30 -> null),
            Row(null, "abc"))))
    }
  }

  test("better error message when use java reserved keyword as field name") {
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        Seq(InvalidInJava(1)).toDS()
      },
      condition = "INVALID_JAVA_IDENTIFIER_AS_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "abstract",
        "walkedTypePath" -> "- root class: \"org.apache.spark.sql.InvalidInJava\""))
  }

  test("better error message when use invalid java identifier as field name") {
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        Seq(InvalidInJava2(1)).toDS()
      },
      condition = "INVALID_JAVA_IDENTIFIER_AS_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "0",
        "walkedTypePath" ->
          "- root class: \"org.apache.spark.sql.ScalaReflectionRelationSuite.InvalidInJava2\""))
  }
}
