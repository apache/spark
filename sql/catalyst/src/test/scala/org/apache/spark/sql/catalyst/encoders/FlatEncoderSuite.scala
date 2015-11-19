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

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.Encoders

class FlatEncoderSuite extends ExpressionEncoderSuite {
  encodeDecodeTest(false, FlatEncoder[Boolean], "primitive boolean")
  encodeDecodeTest(-3.toByte, FlatEncoder[Byte], "primitive byte")
  encodeDecodeTest(-3.toShort, FlatEncoder[Short], "primitive short")
  encodeDecodeTest(-3, FlatEncoder[Int], "primitive int")
  encodeDecodeTest(-3L, FlatEncoder[Long], "primitive long")
  encodeDecodeTest(-3.7f, FlatEncoder[Float], "primitive float")
  encodeDecodeTest(-3.7, FlatEncoder[Double], "primitive double")

  encodeDecodeTest(new java.lang.Boolean(false), FlatEncoder[java.lang.Boolean], "boxed boolean")
  encodeDecodeTest(new java.lang.Byte(-3.toByte), FlatEncoder[java.lang.Byte], "boxed byte")
  encodeDecodeTest(new java.lang.Short(-3.toShort), FlatEncoder[java.lang.Short], "boxed short")
  encodeDecodeTest(new java.lang.Integer(-3), FlatEncoder[java.lang.Integer], "boxed int")
  encodeDecodeTest(new java.lang.Long(-3L), FlatEncoder[java.lang.Long], "boxed long")
  encodeDecodeTest(new java.lang.Float(-3.7f), FlatEncoder[java.lang.Float], "boxed float")
  encodeDecodeTest(new java.lang.Double(-3.7), FlatEncoder[java.lang.Double], "boxed double")

  encodeDecodeTest(BigDecimal("32131413.211321313"), FlatEncoder[BigDecimal], "scala decimal")
  type JDecimal = java.math.BigDecimal
  // encodeDecodeTest(new JDecimal("231341.23123"), FlatEncoder[JDecimal], "java decimal")

  encodeDecodeTest("hello", FlatEncoder[String], "string")
  encodeDecodeTest(Date.valueOf("2012-12-23"), FlatEncoder[Date], "date")
  encodeDecodeTest(Timestamp.valueOf("2016-01-29 10:00:00"), FlatEncoder[Timestamp], "timestamp")
  encodeDecodeTest(Array[Byte](13, 21, -23), FlatEncoder[Array[Byte]], "binary")

  encodeDecodeTest(Seq(31, -123, 4), FlatEncoder[Seq[Int]], "seq of int")
  encodeDecodeTest(Seq("abc", "xyz"), FlatEncoder[Seq[String]], "seq of string")
  encodeDecodeTest(Seq("abc", null, "xyz"), FlatEncoder[Seq[String]], "seq of string with null")
  encodeDecodeTest(Seq.empty[Int], FlatEncoder[Seq[Int]], "empty seq of int")
  encodeDecodeTest(Seq.empty[String], FlatEncoder[Seq[String]], "empty seq of string")

  encodeDecodeTest(Seq(Seq(31, -123), null, Seq(4, 67)),
    FlatEncoder[Seq[Seq[Int]]], "seq of seq of int")
  encodeDecodeTest(Seq(Seq("abc", "xyz"), Seq[String](null), null, Seq("1", null, "2")),
    FlatEncoder[Seq[Seq[String]]], "seq of seq of string")

  encodeDecodeTest(Array(31, -123, 4), FlatEncoder[Array[Int]], "array of int")
  encodeDecodeTest(Array("abc", "xyz"), FlatEncoder[Array[String]], "array of string")
  encodeDecodeTest(Array("a", null, "x"), FlatEncoder[Array[String]], "array of string with null")
  encodeDecodeTest(Array.empty[Int], FlatEncoder[Array[Int]], "empty array of int")
  encodeDecodeTest(Array.empty[String], FlatEncoder[Array[String]], "empty array of string")

  encodeDecodeTest(Array(Array(31, -123), null, Array(4, 67)),
    FlatEncoder[Array[Array[Int]]], "array of array of int")
  encodeDecodeTest(Array(Array("abc", "xyz"), Array[String](null), null, Array("1", null, "2")),
    FlatEncoder[Array[Array[String]]], "array of array of string")

  encodeDecodeTest(Map(1 -> "a", 2 -> "b"), FlatEncoder[Map[Int, String]], "map")
  encodeDecodeTest(Map(1 -> "a", 2 -> null), FlatEncoder[Map[Int, String]], "map with null")
  encodeDecodeTest(Map(1 -> Map("a" -> 1), 2 -> Map("b" -> 2)),
    FlatEncoder[Map[Int, Map[String, Int]]], "map of map")

  // Kryo encoders
  encodeDecodeTest(
    "hello",
    Encoders.kryo[String].asInstanceOf[ExpressionEncoder[String]],
    "kryo string")
  encodeDecodeTest(
    new NotJavaSerializable(15),
    Encoders.kryo[NotJavaSerializable].asInstanceOf[ExpressionEncoder[NotJavaSerializable]],
    "kryo object serialization")
}


class NotJavaSerializable(val value: Int) {
  override def equals(other: Any): Boolean = {
    this.value == other.asInstanceOf[NotJavaSerializable].value
  }
}
