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

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types._

class RowTest extends AnyFunSpec with Matchers {

  val schema = StructType(
    StructField("col1", StringType) ::
    StructField("col2", StringType) ::
    StructField("col3", IntegerType) :: Nil)
  val values = Array("value1", "value2", 1)
  val valuesWithoutCol3 = Array[Any](null, "value2", null)

  val sampleRow: Row = new GenericRowWithSchema(values, schema)
  val sampleRowWithoutCol3: Row = new GenericRowWithSchema(valuesWithoutCol3, schema)
  val noSchemaRow: Row = new GenericRow(values)

  describe("Row (without schema)") {
    it("throws an exception when accessing by fieldName") {
      intercept[UnsupportedOperationException] {
        noSchemaRow.fieldIndex("col1")
      }
      intercept[UnsupportedOperationException] {
        noSchemaRow.getAs("col1")
      }
    }
  }

  describe("Row (with schema)") {
    it("fieldIndex(name) returns field index") {
      sampleRow.fieldIndex("col1") shouldBe 0
      sampleRow.fieldIndex("col3") shouldBe 2
    }

    it("getAs[T] retrieves a value by fieldname") {
      sampleRow.getAs[String]("col1") shouldBe "value1"
      sampleRow.getAs[Int]("col3") shouldBe 1
    }

    it("Accessing non existent field throws an exception") {
      intercept[IllegalArgumentException] {
        sampleRow.getAs[String]("non_existent")
      }
    }

    it("getValuesMap() retrieves values of multiple fields as a Map(field -> value)") {
      val expected = Map(
        "col1" -> "value1",
        "col2" -> "value2"
      )
      sampleRow.getValuesMap(List("col1", "col2")) shouldBe expected
    }

    it("getValuesMap() retrieves null value on non AnyVal Type") {
      val expected = Map(
        "col1" -> null,
        "col2" -> "value2"
      )
      sampleRowWithoutCol3.getValuesMap[String](List("col1", "col2")) shouldBe expected
    }

    it("getAs() on type extending AnyVal throws an exception when accessing field that is null") {
      intercept[NullPointerException] {
        sampleRowWithoutCol3.getInt(sampleRowWithoutCol3.fieldIndex("col3"))
      }
    }

    it("getAs() on type extending AnyVal does not throw exception when value is null") {
      sampleRowWithoutCol3.getAs[String](sampleRowWithoutCol3.fieldIndex("col1")) shouldBe null
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
