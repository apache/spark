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
package org.apache.spark.sql.execution.datasources.xml.util

import java.nio.file.Paths

import org.apache.spark.sql.catalyst.xml.XSDToSchema
import org.apache.spark.sql.execution.datasources.xml.TestUtils._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, DecimalType, FloatType, LongType, StringType}

class XSDToSchemaSuite extends SharedSparkSession {

  private val resDir = "test-data/xml-resources/"

  test("Basic parsing") {
    val parsedSchema = XSDToSchema.read(Paths.get(testFile(resDir + "basket.xsd")
      .replace("file:/", "/")))
    val expectedSchema = buildSchema(
      field("basket",
        structField(
          structArray("entry",
            field("key"),
            field("value"))), nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("Relative path parsing") {
    val parsedSchema = XSDToSchema.read(Paths.get(testFile(resDir + "include-example/first.xsd")
      .replace("file:/", "/")))
    val expectedSchema = buildSchema(
      field("basket",
        structField(
          structArray("entry",
            field("key"),
            field("value"))), nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("Test schema types and attributes") {
    val parsedSchema = XSDToSchema.read(Paths.get(testFile(resDir + "catalog.xsd")
      .replace("file:/", "/")))
    val expectedSchema = buildSchema(
      field("catalog",
        structField(
          field("product",
            structField(
              structArray("catalog_item",
                field("item_number", nullable = false),
                field("price", FloatType, nullable = false),
                structArray("size",
                  structArray("color_swatch",
                    field("_VALUE"),
                    field("_image")),
                  field("_description")),
                field("_gender")),
              field("_description"),
              field("_product_image")),
            nullable = false)),
        nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("Test xs:choice nullability") {
    val parsedSchema = XSDToSchema.read(Paths.get(testFile(resDir + "choice.xsd")
      .replace("file:/", "/")))
    val expectedSchema = buildSchema(
      field("el", structField(field("foo"), field("bar"), field("baz")), nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("Two root elements") {
    val parsedSchema = XSDToSchema.read(Paths.get(testFile(resDir + "twoelements.xsd")
      .replace("file:/", "/")))
    val expectedSchema = buildSchema(field("bar", nullable = false), field("foo", nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("xs:any schema") {
    val parsedSchema = XSDToSchema.read(Paths.get(testFile(resDir + "xsany.xsd")
      .replace("file:/", "/")))
    val expectedSchema = buildSchema(
      field("root",
        structField(
          field("foo",
            structField(
              field("xs_any")),
            nullable = false),
          field("bar",
            structField(
              field("xs_any", nullable = false)),
            nullable = false),
          field("baz",
            structField(
              field("xs_any", ArrayType(StringType), nullable = false)),
            nullable = false),
          field("bing",
            structField(
              field("xs_any")),
            nullable = false)),
        nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("Tests xs:long type / Issue 520") {
    val parsedSchema = XSDToSchema.read(Paths.get(testFile(resDir + "long.xsd")
      .replace("file:/", "/")))
    val expectedSchema = buildSchema(
      field("test",
        structField(field("userId", LongType, nullable = false)), nullable = false))
    assert(parsedSchema === expectedSchema)
  }

  test("Test xs:decimal type with restriction[fractionalDigits]") {
    val parsedSchema = XSDToSchema.read(Paths.get(testFile(resDir +
      "decimal-with-restriction.xsd").replace("file:/", "/")))
    val expectedSchema = buildSchema(
      field("decimal_type_3", DecimalType(12, 6), nullable = false),
      field("decimal_type_1", DecimalType(38, 18), nullable = false),
      field("decimal_type_2", DecimalType(38, 2), nullable = false)
    )
    assert(parsedSchema === expectedSchema)
  }

  test("Test ref attribute / Issue 617") {
    val parsedSchema = XSDToSchema.read(Paths.get(testFile(resDir + "ref-attribute.xsd")
      .replace("file:/", "/")))
    val expectedSchema = buildSchema(
      field(
        "book",
        structField(
          field("name", StringType, false),
          field("author", StringType, false),
          field("isbn", StringType, false)
        ),
        false
      ),
      field(
        "bookList",
        structField(
          structArray(
            "book",
            field("name", StringType, false),
            field("author", StringType, false),
            field("isbn", StringType, false)
          )
        ),
        false
      )
    )
    assert(parsedSchema === expectedSchema)
  }

  test("Test complex content with extension element / Issue 554") {
    val parsedSchema = XSDToSchema.read(Paths.get(testFile(resDir +
      "complex-content-extension.xsd").replace("file:/", "/")))

    val expectedSchema = buildSchema(
      field(
        "employee",
        structField(
          field("firstname", StringType, false),
          field("lastname", StringType, false),
          field("address", StringType, false),
          field("city", StringType, false),
          field("country", StringType, false)
        ),
        false
      )
    )
    assert(parsedSchema === expectedSchema)
  }
}
