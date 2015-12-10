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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types._
import org.scalatest.{Matchers, FunSpec}

class RowTest extends FunSpec with Matchers {

  val schema = StructType(
    StructField("col1", StringType) ::
    StructField("col2", DoubleType) ::
    StructField("col3", IntegerType) ::
    Nil)
  val values = Array("value1", 1.0, 1)
  val valuesWithoutCol3 = Array[Any]("value1", 1.0, null)

  val sampleRow: Row = new GenericRowWithSchema(values, schema)
  val sampleRowWithoutCol3: Row = new GenericRowWithSchema(valuesWithoutCol3, schema)
  val noSchemaRow: Row = new GenericRow(values)

  describe("Row (without schema)") {
    it("throws an exception when accessing by field name") {
      intercept[UnsupportedOperationException] {
        noSchemaRow.fieldIndex("col1")
      }
      intercept[UnsupportedOperationException] {
        noSchemaRow.getAs("col1")
      }
    }

    it("getAsOpt[T]() can get values using indices.") {
      noSchemaRow.getAsOpt[String](0) should be (Some("value1"))
      noSchemaRow.getAsOpt[Double](1) should be (Some(1.0))
      noSchemaRow.getAsOpt[Int](2) should be (Some(1))
    }

    it("getAsOpt[T]() cannot get values using field names.") {
      noSchemaRow.getAsOpt[String]("col1") should be (None)
      noSchemaRow.getAsOpt[Double]("col2") should be (None)
      noSchemaRow.getAsOpt[Int]("col3") should be (None)
    }

    it("getAsOpt[T]() returns None for non-existing columns.") {
      noSchemaRow.getAsOpt[String](3) should be (None)
      sampleRow.getAsOpt[Double]("col4") should be (None)
    }
  }

  describe("Row (with schema)") {
    it("fieldIndex(name) returns field index") {
      sampleRow.fieldIndex("col1") shouldBe 0
      sampleRow.fieldIndex("col3") shouldBe 2
    }

    it("getAs[T] retrieves a value by field name") {
      sampleRow.getAs[String]("col1") shouldBe "value1"
      sampleRow.getAs[Int]("col3") shouldBe 1
    }

    it("getAsOpt[T]() can get values using indices.") {
      sampleRow.getAsOpt[String](0) should be (Some("value1"))
      sampleRow.getAsOpt[Double](1) should be (Some(1.0))
      sampleRow.getAsOpt[Int](2) should be (Some(1))
    }

    it("getAsOpt[T]() can get values using field names.") {
      sampleRow.getAsOpt[String]("col1") should be (Some("value1"))
      sampleRow.getAsOpt[Double]("col2") should be (Some(1.0))
      sampleRow.getAsOpt[Int]("col3") should be (Some(1))
    }

    it("getAsOpt[T] retrieves an Optional value if the field name exists else returns None") {
      sampleRow.getAsOpt[String]("col1") shouldBe Some("value1")
      sampleRow.getAsOpt[Int]("col3") shouldBe Some(1)
      sampleRow.getAsOpt[String]("col4") shouldBe None
    }

    it("getAsOpt[T] retrieves an Optional value if class cast is successful else returns None") {
      sampleRow.getAsOpt[String]("col1") shouldBe Some("value1")
      sampleRow.getAsOpt[Int]("col2") shouldBe Some(1)
    }

    it("Accessing non existent field throws an exception") {
      intercept[IllegalArgumentException] {
        sampleRow.getAs[String]("non_existent")
      }
    }

    it("getValuesMap() retrieves values of multiple fields as a Map(field -> value)") {
      val expected = Map(
        "col1" -> "value1",
        "col2" -> 1.0,
        "col3" -> 1
      )
      sampleRow.getValuesMap(List("col1", "col2", "col3")) shouldBe expected
    }

    it("getValuesMap() retrieves null value on non AnyVal Type") {
      val expected = Map(
        "col1" -> "value1",
        "col2" -> 1.0,
        "col3" -> null
      )
      sampleRowWithoutCol3.getValuesMap[String](List("col1", "col2", "col3")) shouldBe expected
    }

    it("getAs() on type extending AnyVal throws an exception when accessing field that is null") {
      intercept[NullPointerException] {
        sampleRowWithoutCol3.getInt(sampleRowWithoutCol3.fieldIndex("col3"))
      }
    }

    it("getAs() on type extending AnyVal does not throw exception when value is null"){
      sampleRowWithoutCol3.getAs[String](sampleRowWithoutCol3.fieldIndex("col3")) shouldBe null
    }
  }

  describe("row equals") {
    val externalRow = Row(1, 2)
    val externalRow2 = Row(1, 2)
    val internalRow = InternalRow(1, 2)
    val internalRow2 = InternalRow(1, 2)

    it("equality check for external rows") {
      externalRow shouldEqual externalRow2
    }

    it("equality check for internal rows") {
      internalRow shouldEqual internalRow2
    }
  }
}

