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

import java.io.FileNotFoundException

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.xml.TestUtils._
import org.apache.spark.sql.execution.datasources.xml.XSDToSchema
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class XSDToSchemaSuite extends SharedSparkSession {

  private val resDir = "test-data/xml-resources/"

  test("Basic parsing") {
    val parsedSchema = XSDToSchema.read(new Path(testFile(resDir + "basket.xsd")))
    val expectedSchema = buildSchema(
      field("basket",
        structField(
          structArray("entry",
            field("key"),
            field("value"))), nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("Relative path parsing") {
    val parsedSchema = XSDToSchema.read(new Path(testFile(resDir + "include-example/first.xsd")))
    val expectedSchema = buildSchema(
      field("basket",
        structField(
          structArray("entry",
            field("key"),
            field("value"))), nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("Test schema types and attributes") {
    val parsedSchema = XSDToSchema.read(new Path(testFile(resDir + "catalog.xsd")))
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
    val parsedSchema = XSDToSchema.read(new Path(testFile(resDir + "choice.xsd")))
    val expectedSchema = buildSchema(
      field("el", structField(field("foo"), field("bar"), field("baz")), nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("Two root elements") {
    val parsedSchema = XSDToSchema.read(new Path(testFile(resDir + "twoelements.xsd")))
    val expectedSchema = buildSchema(field("bar", nullable = false), field("foo", nullable = false))
    assert(expectedSchema === parsedSchema)
  }

  test("xs:any schema") {
    val parsedSchema = XSDToSchema.read(new Path(testFile(resDir + "xsany.xsd")))
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
    val parsedSchema = XSDToSchema.read(new Path(testFile(resDir + "long.xsd")))
    val expectedSchema = buildSchema(
      field("test",
        structField(field("userId", LongType, nullable = false)), nullable = false))
    assert(parsedSchema === expectedSchema)
  }

  test("Test xs:decimal type with restriction[fractionalDigits]") {
    val parsedSchema = XSDToSchema.read(new Path(testFile(resDir + "decimal-with-restriction.xsd")))
    val expectedSchema = buildSchema(
      field("decimal_type_3", DecimalType(12, 6), nullable = false),
      field("decimal_type_1", DecimalType(38, 18), nullable = false),
      field("decimal_type_2", DecimalType(38, 2), nullable = false)
    )
    assert(parsedSchema === expectedSchema)
  }

  test("Test ref attribute / Issue 617") {
    val parsedSchema = XSDToSchema.read(new Path(testFile(resDir + "ref-attribute.xsd")))
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
    val parsedSchema =
      XSDToSchema.read(new Path(testFile(resDir + "complex-content-extension.xsd")))

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

  test("SPARK-45912: Test XSDToSchema when open not found files") {
    intercept[FileNotFoundException] {
      XSDToSchema.read(new Path("/path/not/found"))
    }
  }

  test("Basic DataTypes parsing") {
    val xsdString =
      """<?xml version="1.0" encoding="UTF-8" ?>
        |<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        |  <xs:element name="basket">
        |    <xs:complexType>
        |    <xs:sequence>
        |      <xs:element name="xs_anyType" type="xs:anyType" />
        |      <xs:element name="xs_anySimpleType" type="xs:anySimpleType" />
        |      <xs:element name="xs_anyURI" type="xs:anyURI" />
        |      <xs:element name="xs_base64Binary" type="xs:base64Binary" />
        |      <xs:element name="xs_boolean" type="xs:boolean" />
        |      <xs:element name="xs_byte" type="xs:byte" />
        |      <xs:element name="xs_date" type="xs:date" />
        |      <xs:element name="xs_dateTime" type="xs:dateTime" />
        |      <xs:element name="xs_decimal" type="xs:decimal" />
        |      <xs:element name="xs_double" type="xs:double" />
        |      <xs:element name="xs_duration" type="xs:duration" />
        |      <xs:element name="xs_ENTITIES" type="xs:ENTITIES" />
        |      <xs:element name="xs_ENTITY" type="xs:ENTITY" />
        |      <xs:element name="xs_float" type="xs:float" />
        |      <xs:element name="xs_gDay" type="xs:gDay" />
        |      <xs:element name="xs_gMonth" type="xs:gMonth" />
        |      <xs:element name="xs_gMonthDay" type="xs:gMonthDay" />
        |      <xs:element name="xs_gYear" type="xs:gYear" />
        |      <xs:element name="xs_gYearMonth" type="xs:gYearMonth" />
        |      <xs:element name="xs_hexBinary" type="xs:hexBinary" />
        |      <xs:element name="xs_ID" type="xs:ID" />
        |      <xs:element name="xs_IDREF" type="xs:IDREF" />
        |      <xs:element name="xs_IDREFS" type="xs:IDREFS" />
        |      <xs:element name="xs_int" type="xs:int" />
        |      <xs:element name="xs_integer" type="xs:integer" />
        |      <xs:element name="xs_language" type="xs:language" />
        |      <xs:element name="xs_long" type="xs:long" />
        |      <xs:element name="xs_Name" type="xs:Name" />
        |      <xs:element name="xs_NCName" type="xs:NCName" />
        |      <xs:element name="xs_negativeInteger" type="xs:negativeInteger" />
        |      <xs:element name="xs_NMTOKEN" type="xs:NMTOKEN" />
        |      <xs:element name="xs_NMTOKENS" type="xs:NMTOKENS" />
        |      <xs:element name="xs_nonNegativeInteger" type="xs:nonNegativeInteger" />
        |      <xs:element name="xs_nonPositiveInteger" type="xs:nonPositiveInteger" />
        |      <xs:element name="xs_normalizedString" type="xs:normalizedString" />
        |      <xs:element name="xs_NOTATION" type="xs:NOTATION" />
        |      <xs:element name="xs_positiveInteger" type="xs:positiveInteger" />
        |      <xs:element name="xs_QName" type="xs:QName" />
        |      <xs:element name="xs_short" type="xs:short" />
        |      <xs:element name="xs_string" type="xs:string" />
        |      <xs:element name="xs_time" type="xs:time" />
        |      <xs:element name="xs_token" type="xs:token" />
        |      <xs:element name="xs_unsignedByte" type="xs:unsignedByte" />
        |      <xs:element name="xs_unsignedInt" type="xs:unsignedInt" />
        |      <xs:element name="xs_unsignedLong" type="xs:unsignedLong" />
        |      <xs:element name="xs_unsignedShort" type="xs:unsignedShort" />
        |    </xs:sequence>
        |    </xs:complexType>
        |  </xs:element>
        |</xs:schema>
        |""".stripMargin
    val parsedSchema = XSDToSchema.read(xsdString)
    val expectedSchema = StructType(StructField("basket",
      StructType(
        StructField("xs_anyType", StringType, false) ::
          StructField("xs_anySimpleType", StringType, false) ::
          StructField("xs_anyURI", StringType, false) ::
          StructField("xs_base64Binary", StringType, false) ::
          StructField("xs_boolean", BooleanType, false) ::
          StructField("xs_byte", ByteType, false) ::
          StructField("xs_date", DateType, false) ::
          StructField("xs_dateTime", TimestampType, false) ::
          StructField("xs_decimal", DecimalType(38, 18), false) ::
          StructField("xs_double", DoubleType, false) ::
          StructField("xs_duration", StringType, false) ::
          StructField("xs_ENTITIES", StringType, false) ::
          StructField("xs_ENTITY", StringType, false) ::
          StructField("xs_float", FloatType, false) ::
          StructField("xs_gDay", StringType, false) ::
          StructField("xs_gMonth", StringType, false) ::
          StructField("xs_gMonthDay", StringType, false) ::
          StructField("xs_gYear", StringType, false) ::
          StructField("xs_gYearMonth", StringType, false) ::
          StructField("xs_hexBinary", StringType, false) ::
          StructField("xs_ID", StringType, false) ::
          StructField("xs_IDREF", StringType, false) ::
          StructField("xs_IDREFS", StringType, false) ::
          StructField("xs_int", IntegerType, false) ::
          StructField("xs_integer", DecimalType(38, 0), false) ::
          StructField("xs_language", StringType, false) ::
          StructField("xs_long", LongType, false) ::
          StructField("xs_Name", StringType, false) ::
          StructField("xs_NCName", StringType, false) ::
          StructField("xs_negativeInteger", DecimalType(38, 0), false) ::
          StructField("xs_NMTOKEN", StringType, false) ::
          StructField("xs_NMTOKENS", StringType, false) ::
          StructField("xs_nonNegativeInteger", DecimalType(38, 0), false) ::
          StructField("xs_nonPositiveInteger", DecimalType(38, 0), false) ::
          StructField("xs_normalizedString", StringType, false) ::
          StructField("xs_NOTATION", StringType, false) ::
          StructField("xs_positiveInteger", DecimalType(38, 0), false) ::
          StructField("xs_QName", StringType, false) ::
          StructField("xs_short", ShortType, false) ::
          StructField("xs_string", StringType, false) ::
          StructField("xs_time", StringType, false) ::
          StructField("xs_token", StringType, false) ::
          StructField("xs_unsignedByte", ShortType, false) ::
          StructField("xs_unsignedInt", LongType, false) ::
          StructField("xs_unsignedLong", DecimalType(38, 0), false) ::
          StructField("xs_unsignedShort", IntegerType, false) :: Nil),
      false) :: Nil)
    assert(parsedSchema === expectedSchema)
  }
}
