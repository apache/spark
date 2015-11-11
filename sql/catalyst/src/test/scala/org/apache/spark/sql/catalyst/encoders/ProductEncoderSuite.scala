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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.PrimitiveData

class ProductEncoderSuite extends SparkFunSuite {
  test("primitive data") {
    val input = PrimitiveData(1, 2L, 3.1, 4.1f, 5, 6, true)
    val encoder = ProductEncoder[PrimitiveData]
    val boundEnc = encoder.bind()
    val row = boundEnc.toRow(input)
    val output = boundEnc.fromRow(row)
    assert(input == output)
  }

  test("boxed data") {
    val input = BoxedData(1, 2L, 3.1, 4.1f, 5.asInstanceOf[Short], 6.asInstanceOf[Byte], true)
    val encoder = ProductEncoder[BoxedData]
    val boundEnc = encoder.bind()
    val row = boundEnc.toRow(input)
    val output = boundEnc.fromRow(row)
    assert(input == output)
  }

  test("repeated struct") {
    val input = RepeatedStruct(Seq(
      PrimitiveData(1, 2L, 3.1, 4.1f, 5, 6, true),
      PrimitiveData(2, 3L, 4.1, 5.1f, 6, 7, false)))
    val encoder = ProductEncoder[RepeatedStruct]
    val boundEnc = encoder.bind()
    val row = boundEnc.toRow(input)
    val output = boundEnc.fromRow(row)
    assert(input == output)
  }
}

object ProductEncoderSuite {
  case class RepeatedStruct(s: Seq[PrimitiveData])

  case class NestedArray(a: Array[Array[Int]])

  case class BoxedData(
      intField: java.lang.Integer,
      longField: java.lang.Long,
      doubleField: java.lang.Double,
      floatField: java.lang.Float,
      shortField: java.lang.Short,
      byteField: java.lang.Byte,
      booleanField: java.lang.Boolean)

  case class RepeatedData(
      arrayField: Seq[Int],
      arrayFieldContainsNull: Seq[java.lang.Integer],
      mapField: scala.collection.Map[Int, Long],
      mapFieldNull: scala.collection.Map[Int, java.lang.Long],
      structField: PrimitiveData)

  case class SpecificCollection(l: List[Int])
}
