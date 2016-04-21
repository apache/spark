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

import scala.util.{Failure, Success, Try}

import org.scalatest.{FunSpec, Matchers}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types._

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

    it("getAs[T]() should throw ClassCastException when casting fails.") {
      intercept[ClassCastException] {
        noSchemaRow.getAs[Int](0)
      }
    }

    it("attempt[T]() returns Success for valid indices.") {
      noSchemaRow.attempt[String](0) should be (Success("value1"))
      noSchemaRow.attempt[Double](1) should be (Success(1.0))
      noSchemaRow.attempt[Int](2) should be (Success(1))
    }

    it("attempt[T]() returns Failure for an invalid index.") {
      val value = noSchemaRow.attempt[String](3)
      value.getClass should be (classOf[Failure[String]])
      intercept[IndexOutOfBoundsException] { value.get }
    }

    // TODO: Check why the commented out test works when it should behave the same.
    it("attempt[T]() returns Failure for invalid casts.") {
      val value = noSchemaRow.attempt[Int](0)
      // val value = Try(noSchemaRow.getAs[Int](0))
      value.getClass should be (classOf[Failure[Int]])
      intercept[ClassCastException] { value.get }
    }

    it("attempt[T]() cannot get values using field names.") {
      val value = noSchemaRow.attempt[String]("col1")
      value.getClass should be (classOf[Failure[String]])
      intercept[UnsupportedOperationException] { value.get }
    }

    it("getOption[T]() can get values for valid indices.") {
      noSchemaRow.getOption[String](0) should be (Some("value1"))
      noSchemaRow.getOption[Double](1) should be (Some(1.0))
      noSchemaRow.getOption[Int](2) should be (Some(1))
    }

    it("getOption[T]() returns None for an invalid index.") {
      noSchemaRow.getOption[String](3) should be (None)
    }

    it("getOption[T]() retrieves values for valid casts.") {
      noSchemaRow.getOption[Int](1) should be (Some(1))
      noSchemaRow.getOption[Long](1) should be (Some(1L))
    }

    // TODO: Check why the commented out test works when it should behave the same.
    it("getOption[T]() returns None for invalid casts.") {
      sampleRow.getOption[Int](0) should be (None)
      // noSchemaRow.getOption[Int](0) should be (Some("value1"))
      sampleRow.getOption[String](1) should be (None)
      // noSchemaRow.getOption[String](1) should be (Some(1.0))
    }

    it("getOption[T]() cannot get values using field names.") {
      noSchemaRow.getOption[String]("col1") should be (None)
      noSchemaRow.getOption[Double]("col2") should be (None)
      noSchemaRow.getOption[Int]("col3") should be (None)
    }
  }

  describe("Row (with schema)") {
    it("fieldIndex(name) returns field index") {
      sampleRow.fieldIndex("col1") should be (0)
      sampleRow.fieldIndex("col3") should be (2)
    }

    it("getAs[T] retrieves a value by field name") {
      sampleRow.getAs[String]("col1") should be ("value1")
      sampleRow.getAs[Int]("col3") should be (1)
    }

    it("attempt[T]() returns Success for valid indices.") {
      sampleRow.attempt[String](0) should be (Success("value1"))
      sampleRow.attempt[Double](1) should be (Success(1.0))
      sampleRow.attempt[Int](2) should be (Success(1))
    }

    it("attempt[T]() returns Failure for an invalid index.") {
      val value = sampleRow.attempt[String](3)
      value.getClass should be (classOf[Failure[String]])
      intercept[IndexOutOfBoundsException] { value.get }
    }

    it("attempt[T]() can get values using field names.") {
      sampleRow.attempt[String]("col1") should be (Success("value1"))
      sampleRow.attempt[Double]("col2") should be (Success(1.0))
      sampleRow.attempt[Int]("col3") should be (Success(1))
    }

    it("attempt[T]() returns Failure for an invalid field name.") {
      val value = sampleRow.attempt[String]("col4")
      value.getClass should be (classOf[Failure[String]])
      intercept[IllegalArgumentException] { value.get }
    }

    it("attempt[T]() returns Success for valid casts.") {
      sampleRow.attempt[Int]("col2") should be (Success(1))
      sampleRow.attempt[Long]("col2") should be (Success(1L))
    }

    // TODO: Check why the commented out test works when it should behave the same.
    it("attempt[T]() returns Failure for invalid casts.") {
      val value = sampleRow.attempt[Int]("col1")
      // val value = Try(sampleRow.getAs[Int]("col1"))
      value.getClass should be (classOf[Failure[Int]])
      intercept[ClassCastException] { value.get }
    }

    it("getOption[T]() can get values for valid indices.") {
      sampleRow.getOption[String](0) should be (Some("value1"))
      sampleRow.getOption[Double](1) should be (Some(1.0))
      sampleRow.getOption[Int](2) should be (Some(1))
    }

    it("getOption[T]() returns None for an invalid index.") {
      sampleRow.getOption[String](3) should be (None)
    }

    it("getOption[T]() can get values using field names.") {
      sampleRow.getOption[String]("col1") should be (Some("value1"))
      sampleRow.getOption[Double]("col2") should be (Some(1.0))
      sampleRow.getOption[Int]("col3") should be (Some(1))
    }

    it("getOption[T]() returns None for an invalid field name.") {
      sampleRow.getOption[String]("col4") should be (None)
    }

    it("getOption[T]() retrieves values for valid casts.") {
      sampleRow.getOption[Int]("col2") should be (Some(1))
      sampleRow.getOption[Long]("col2") should be (Some(1L))
    }

    // TODO: Check why the commented out test works when it should behave the same.
    it("getOption[T]() returns None for invalid casts.") {
      sampleRow.getOption[Int]("col1") should be (None)
      // sampleRow.getOption[Int]("col1") should be (Some("value1"))
      sampleRow.getOption[String]("col2") should be (None)
      // sampleRow.getOption[String]("col2") should be (Some(1.0))
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
      sampleRow.getValuesMap(List("col1", "col2", "col3")) should be (expected)
    }

    it("getValuesMap() retrieves null value on non AnyVal Type") {
      val expected = Map(
        "col1" -> "value1",
        "col2" -> 1.0,
        "col3" -> null
      )
      sampleRowWithoutCol3.getValuesMap[String](List("col1", "col2", "col3")) should be (expected)
    }

    it("getAs() on type extending AnyVal throws an exception when accessing field that is null") {
      intercept[NullPointerException] {
        sampleRowWithoutCol3.getInt(sampleRowWithoutCol3.fieldIndex("col3"))
      }
    }

    it("getAs() on type extending AnyVal does not throw exception when value is null") {
      sampleRowWithoutCol3.getAs[String](sampleRowWithoutCol3.fieldIndex("col3")) should be (null)
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

  describe("row immutability") {
    val values = Seq(1, 2, "3", "IV", 6L)
    val externalRow = Row.fromSeq(values)
    val internalRow = InternalRow.fromSeq(values)

    def modifyValues(values: Seq[Any]): Seq[Any] = {
      val array = values.toArray
      array(2) = "42"
      array
    }

    it("copy should return same ref for external rows") {
      externalRow should be theSameInstanceAs externalRow.copy()
    }

    it("copy should return same ref for internal rows") {
      internalRow should be theSameInstanceAs internalRow.copy()
    }

    it("toSeq should not expose internal state for external rows") {
      val modifiedValues = modifyValues(externalRow.toSeq)
      externalRow.toSeq should not equal modifiedValues
    }

    it("toSeq should not expose internal state for internal rows") {
      val modifiedValues = modifyValues(internalRow.toSeq(Seq.empty))
      internalRow.toSeq(Seq.empty) should not equal modifiedValues
    }
  }
}

