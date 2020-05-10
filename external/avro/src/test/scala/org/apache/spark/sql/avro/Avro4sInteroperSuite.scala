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

package org.apache.spark.sql.avro

import com.sksamuel.avro4s.{Record, RecordFormat}
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow

class Avro4sInteroperSuite extends SparkFunSuite
  with Matchers {

  private def doConvert(avroRecord: Record): InternalRow = {
    val avroSchema = avroRecord.getSchema
    val sqlDataType = SchemaConverters.toSqlType(avroSchema)
      .dataType
    val deserializer = new AvroDeserializer(avroSchema, sqlDataType, rebaseDateTime = false)
    deserializer.deserialize(avroRecord)
      .asInstanceOf[InternalRow]
  }

  case class ScalarSample(
                           int: Int,
                           long: Long,
                           float: Float,
                           double: Double,
                           boolean: Boolean,
                           string: String
                         )

  test("Case class with scalar types") {
    val sample = ScalarSample(
      int = 2000,
      long = 100000L,
      float = 0.5f,
      double = 0.123,
      boolean = true,
      string = "str"
    )
    val sampleRecord: Record = RecordFormat[ScalarSample].to(sample)

    val result = doConvert(sampleRecord)

    result.numFields shouldBe 6
    result.getInt(0) shouldBe 2000
    result.getLong(1) shouldBe 100000L
    result.getFloat(2) shouldBe 0.5f
    result.getDouble(3) shouldBe 0.123
    result.getBoolean(4) shouldBe true
    result.getString(5) shouldBe "str"
  }

  case class SeqSample(
                        string: String,
                        seq: Seq[String]
                      )

  test("Case class with Seq type") {
    val sample = SeqSample(
      "str",
      Seq("a", "b", "c")
    )
    val sampleRecord = RecordFormat[SeqSample].to(sample)

    val result = doConvert(sampleRecord)

    result.numFields shouldBe 2
    result.getString(0) shouldBe "str"
    val resultSeq = result.getArray(1)
    resultSeq.numElements() shouldBe 3
    resultSeq.getUTF8String(0).toString shouldBe "a"
    resultSeq.getUTF8String(1).toString shouldBe "b"
    resultSeq.getUTF8String(2).toString shouldBe "c"
  }

  case class OuterSample(
                        outerString: String,
                        inner: InnerSample
                           )
  case class InnerSample(
                        innerString: String,
                        innerSeq: Seq[String]
                        )

  test("Inner class with Seq type") {
    val sample = OuterSample(
      "outer",
      InnerSample(
        "inner",
        Seq("a", "b", "c")
      )
    )
    val sampleRecord = RecordFormat[OuterSample].to(sample)

    val result = doConvert(sampleRecord)

    result.numFields shouldBe 2
    result.getString(0) shouldBe "outer"
    val innerResult = result.getStruct(1, 2)
    innerResult.numFields shouldBe 2
    innerResult.getString(0) shouldBe "inner"
    val resultSeq = innerResult.getArray(1)
    resultSeq.numElements() shouldBe 3
    resultSeq.getUTF8String(0).toString shouldBe "a"
    resultSeq.getUTF8String(1).toString shouldBe "b"
    resultSeq.getUTF8String(2).toString shouldBe "c"
  }

}

