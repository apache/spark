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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.xml.TestUtils._
import org.apache.spark.sql.types.{ArrayType, DecimalType, FloatType, LongType, StringType}

class XSDToSchemaSuite extends SparkFunSuite {
  
  private val resDir = "src/test/resources"

  test("Basic parsing") {
    val parsedSchema = XSDToSchema.read(Paths.get(s"$resDir/basket.xsd"))
    val expectedSchema = buildSchema(
      field("basket",
        struct(
          structArray("entry",
            field("key"),
            field("value"))), nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("Relative path parsing") {
    val parsedSchema = XSDToSchema.read(Paths.get(s"$resDir/include-example/first.xsd"))
    val expectedSchema = buildSchema(
      field("basket",
        struct(
          structArray("entry",
            field("key"),
            field("value"))), nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("Test schema types and attributes") {
    val parsedSchema = XSDToSchema.read(Paths.get(s"$resDir/catalog.xsd"))
    val expectedSchema = buildSchema(
      field("catalog",
        struct(
          field("product",
            struct(
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
    val parsedSchema = XSDToSchema.read(Paths.get(s"$resDir/choice.xsd"))
    val expectedSchema = buildSchema(
      field("el", struct(field("foo"), field("bar"), field("baz")), nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("Two root elements") {
    val parsedSchema = XSDToSchema.read(Paths.get(s"$resDir/twoelements.xsd"))
    val expectedSchema = buildSchema(field("bar", nullable = false), field("foo", nullable = false))
    assert(expectedSchema === parsedSchema)
  }
  
  test("xs:any schema") {
    val parsedSchema = XSDToSchema.read(Paths.get(s"$resDir/xsany.xsd"))
    val expectedSchema = buildSchema(
      field("root",
        struct(
          field("foo",
            struct(
              field("xs_any")),
            nullable = false),
          field("bar",
            struct(
              field("xs_any", nullable = false)),
            nullable = false),
          field("baz",
            struct(
              field("xs_any", ArrayType(StringType), nullable = false)),
            nullable = false),
          field("bing",
            struct(
              field("xs_any")),
            nullable = false)),
        nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("Tests xs:long type / Issue 520") {
    val parsedSchema = XSDToSchema.read(Paths.get(s"$resDir/long.xsd"))
    val expectedSchema = buildSchema(
      field("test",
        struct(field("userId", LongType, nullable = false)), nullable = false))
    assert(parsedSchema === expectedSchema)
  }

  test("Test xs:decimal type with restriction[fractionalDigits]") {
    val parsedSchema = XSDToSchema.read(Paths.get(s"$resDir/decimal-with-restriction.xsd"))
    val expectedSchema = buildSchema(
      field("decimal_type_3", DecimalType(12, 6), nullable = false),
      field("decimal_type_1", DecimalType(38, 18), nullable = false),
      field("decimal_type_2", DecimalType(38, 2), nullable = false)
    )
    assert(parsedSchema === expectedSchema)
  }

  test("Test ref attribute / Issue 617") {
    val parsedSchema = XSDToSchema.read(Paths.get(s"$resDir/ref-attribute.xsd"))
    val expectedSchema = buildSchema(
      field(
        "book",
        struct(
          field("name", StringType, false),
          field("author", StringType, false),
          field("isbn", StringType, false)
        ),
        false
      ),
      field(
        "bookList",
        struct(
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
    val parsedSchema = XSDToSchema.read(Paths.get(s"$resDir/complex-content-extension.xsd"))

    val expectedSchema = buildSchema(
      field(
        "employee",
        struct(
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
