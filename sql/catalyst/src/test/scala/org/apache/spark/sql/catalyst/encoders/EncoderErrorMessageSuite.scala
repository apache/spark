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

package org.apache.spark.sql.catalyst.encoders

import scala.reflect.ClassTag

import org.apache.spark.{SPARK_DOC_ROOT, SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.Encoders

class NonEncodable(i: Int)

case class ComplexNonEncodable1(name1: NonEncodable)

case class ComplexNonEncodable2(name2: ComplexNonEncodable1)

case class ComplexNonEncodable3(name3: Option[NonEncodable])

case class ComplexNonEncodable4(name4: Array[NonEncodable])

case class ComplexNonEncodable5(name5: Option[Array[NonEncodable]])

class EncoderErrorMessageSuite extends SparkFunSuite {

  // Note: we also test error messages for encoders for private classes in JavaDatasetSuite.
  // That is done in Java because Scala cannot create truly private classes.

  test("primitive types in encoders using Kryo serialization") {
    intercept[SparkUnsupportedOperationException] { Encoders.kryo[Int] }
    intercept[SparkUnsupportedOperationException] { Encoders.kryo[Long] }
    intercept[SparkUnsupportedOperationException] { Encoders.kryo[Char] }
  }

  test("primitive types in encoders using Java serialization") {
    intercept[SparkUnsupportedOperationException] { Encoders.javaSerialization[Int] }
    intercept[SparkUnsupportedOperationException] { Encoders.javaSerialization[Long] }
    intercept[SparkUnsupportedOperationException] { Encoders.javaSerialization[Char] }
  }

  test("nice error message for missing encoder") {
    checkError(
      exception = intercept[
        SparkUnsupportedOperationException](ExpressionEncoder[ComplexNonEncodable1]()),
      condition = "ENCODER_NOT_FOUND",
      parameters = Map(
        "typeName" -> "org.apache.spark.sql.catalyst.encoders.NonEncodable",
        "docroot" -> SPARK_DOC_ROOT)
    )

    checkError(
      exception = intercept[
        SparkUnsupportedOperationException](ExpressionEncoder[ComplexNonEncodable2]()),
      condition = "ENCODER_NOT_FOUND",
      parameters = Map(
        "typeName" -> "org.apache.spark.sql.catalyst.encoders.NonEncodable",
        "docroot" -> SPARK_DOC_ROOT)
    )

    checkError(
      exception = intercept[
        SparkUnsupportedOperationException](ExpressionEncoder[ComplexNonEncodable3]()),
      condition = "ENCODER_NOT_FOUND",
      parameters = Map(
        "typeName" -> "org.apache.spark.sql.catalyst.encoders.NonEncodable",
        "docroot" -> SPARK_DOC_ROOT)
    )

    checkError(
      exception = intercept[
        SparkUnsupportedOperationException](ExpressionEncoder[ComplexNonEncodable4]()),
      condition = "ENCODER_NOT_FOUND",
      parameters = Map(
        "typeName" -> "org.apache.spark.sql.catalyst.encoders.NonEncodable",
        "docroot" -> SPARK_DOC_ROOT)
    )

    checkError(
      exception = intercept[
        SparkUnsupportedOperationException](ExpressionEncoder[ComplexNonEncodable5]()),
      condition = "ENCODER_NOT_FOUND",
      parameters = Map(
        "typeName" -> "org.apache.spark.sql.catalyst.encoders.NonEncodable",
        "docroot" -> SPARK_DOC_ROOT)
    )
  }

  private def clsName[T : ClassTag]: String = implicitly[ClassTag[T]].runtimeClass.getName
}
