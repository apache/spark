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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.variant.VariantExpressionEvalUtils
import org.apache.spark.sql.execution.datasources.parquet.SparkShreddingUtils
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.Variant
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class VariantWriteShreddingSuite extends SparkFunSuite with ExpressionEvalHelper {

  private def parseJson(input: String): VariantVal =
    VariantExpressionEvalUtils.parseJson(UTF8String.fromString(input))

  private def toVariant(input: Expression): VariantVal = {
    Cast(input, VariantType, true).eval().asInstanceOf[VariantVal]
  }

  private def untypedValue(input: String): Array[Byte] = {
    val variantVal = parseJson(input)
    variantVal.getValue
  }

  private def untypedValue(input: VariantVal): Array[Byte] = input.getValue

  // Shreds variantVal with the requested schema, and verifies that the result is
  // equal to `expected`.
  private def testWithSchema(variantVal: VariantVal,
                             schema: DataType, expected: Row): Unit = {
    val shreddingSchema = SparkShreddingUtils.variantShreddingSchema(schema)
    val variant = new Variant(variantVal.getValue, variantVal.getMetadata)
    val variantSchema = SparkShreddingUtils.buildVariantSchema(shreddingSchema)
    val actual = SparkShreddingUtils.castShredded(variant, variantSchema)

    val catalystExpected = CatalystTypeConverters.convertToCatalyst(expected)
    if (!checkResult(actual, catalystExpected, shreddingSchema, exprNullable = false)) {
      fail(s"Incorrect evaluation of castShredded: " +
        s"actual: $actual, " +
        s"expected: $expected")
    }
  }

  // Parse the provided JSON into a Variant, shred it to the provided schema, and verify the result.
  private def testWithSchema(input: String, dataType: DataType, expected: Row): Unit = {
    val variantVal = parseJson(input)
    testWithSchema(variantVal, dataType, expected)
  }

  private val emptyMetadata: Array[Byte] = parseJson("null").getMetadata

  test("shredding as fixed numeric types") {
    /* Cast integer to any wider numeric type. */
    testWithSchema("1", IntegerType, Row(emptyMetadata, null, 1))
    testWithSchema("1", LongType, Row(emptyMetadata, null, 1))
    testWithSchema("1", ShortType, Row(emptyMetadata, null, 1))
    testWithSchema("1", ByteType, Row(emptyMetadata, null, 1))

    // Invalid casts
    Seq(StringType, DecimalType(5, 5), TimestampType, DateType, BooleanType, DoubleType, FloatType,
      BinaryType, ArrayType(IntegerType),
      StructType.fromDDL("a int, b int")).foreach { t =>
      testWithSchema("1", t, Row(emptyMetadata, untypedValue("1"), null))
    }

    /* Test conversions between numeric types and scales. */
    testWithSchema("1", DecimalType(5, 2), Row(emptyMetadata, null, Decimal("1")))
    testWithSchema("1", DecimalType(38, 37), Row(emptyMetadata, null, Decimal("1")))
    // Decimals that are effectively storing integers can also be cast to integer.
    testWithSchema("1.0", IntegerType, Row(emptyMetadata, null, 1))
    testWithSchema("1.0000000000000000000000000000000000000", IntegerType,
      Row(emptyMetadata, null, 1))
    // Don't overflow the integer type when converting from decimal.
    testWithSchema("32767.0", ShortType, Row(emptyMetadata, null, 32767))
    testWithSchema("32768.0", ShortType, Row(emptyMetadata, untypedValue("32768.0"), null))
    // Don't overflow decimal type when converting from integer.
    testWithSchema("99999", DecimalType(7, 2), Row(emptyMetadata, null, Decimal("99999.00")))
    testWithSchema("100000", DecimalType(7, 2), Row(emptyMetadata, untypedValue("100000"), null))
    // Allow scale to increase
    testWithSchema("12.34", DecimalType(7, 4), Row(emptyMetadata, null, Decimal("12.3400")))
    // Allow scale to decrease if there are trailing zeros
    testWithSchema("12.3400", DecimalType(4, 2), Row(emptyMetadata, null, Decimal("12.34")))
    testWithSchema("12.3410", DecimalType(4, 2), Row(emptyMetadata, untypedValue("12.3410"), null))

    // The string 1 is not numeric
    testWithSchema("\"1\"", IntegerType, Row(emptyMetadata, untypedValue("\"1\""), null))
    // Decimal would lose information.
    testWithSchema("1.1", IntegerType, Row(emptyMetadata, untypedValue("1.1"), null))
    // Exponential notation is parsed as double, cannot be shredded to other numeric types.
    testWithSchema("1e2", IntegerType, Row(emptyMetadata, untypedValue("1e2"), null))
    // Null is not an integer
    testWithSchema("null", IntegerType, Row(emptyMetadata, untypedValue("null"), null))

    // Overflow leads to storing as unshredded.
    testWithSchema("32767", ShortType, Row(emptyMetadata, null, 32767))
    testWithSchema("32768", ShortType, Row(emptyMetadata, untypedValue("32768"), null))

    testWithSchema("1e2", DoubleType, Row(emptyMetadata, null, 1e2))
    // We currently don't allow shredding double as float.
    testWithSchema("1e2", FloatType, Row(emptyMetadata, untypedValue("1e2"), null))
  }

  test("shredding as other scalar types") {
    // Test types that aren't produced by parseJson
    val floatV = toVariant(Literal(1.2f, FloatType))
    testWithSchema(floatV, FloatType, Row(emptyMetadata, null, 1.2f))
    testWithSchema(floatV, DoubleType, Row(emptyMetadata, untypedValue(floatV), null))

    val booleanV = toVariant(Literal(true, BooleanType))
    testWithSchema(booleanV, BooleanType, Row(emptyMetadata, null, true))
    testWithSchema(booleanV, StringType, Row(emptyMetadata, untypedValue(booleanV), null))

    val binaryV = toVariant(Literal(Array[Byte](-1, -2), BinaryType))
    testWithSchema(binaryV, BinaryType, Row(emptyMetadata, null, Array[Byte](-1, -2)))
    testWithSchema(binaryV, StringType, Row(emptyMetadata, untypedValue(binaryV), null))

    val dateV = toVariant(Literal(0, DateType))
    testWithSchema(dateV, DateType, Row(emptyMetadata, null, 0))
    testWithSchema(dateV, TimestampType, Row(emptyMetadata, untypedValue(dateV), null))

    val timestampV = toVariant(Literal(0L, TimestampType))
    testWithSchema(timestampV, TimestampType, Row(emptyMetadata, null, 0))
    testWithSchema(timestampV, TimestampNTZType, Row(emptyMetadata, untypedValue(timestampV), null))

    val timestampNtzV = toVariant(Literal(0L, TimestampNTZType))
    testWithSchema(timestampNtzV, TimestampNTZType, Row(emptyMetadata, null, 0))
    testWithSchema(timestampNtzV, TimestampType,
        Row(emptyMetadata, untypedValue(timestampNtzV), null))
  }

  test("shredding as object") {
    val obj = parseJson("""{"a": 1, "b": "hello"}""")
    // Can't be cast to scalar or array.
    Seq(IntegerType, LongType, ShortType, ByteType, StringType, DecimalType(5, 5),
        TimestampType, DateType, BooleanType, DoubleType, FloatType, BinaryType,
        ArrayType(IntegerType)).foreach { t =>
      testWithSchema(obj, t, Row(obj.getMetadata, untypedValue(obj), null))
    }

    // Happy path
    testWithSchema(obj, StructType.fromDDL("a int, b string"),
      Row(obj.getMetadata, null, Row(Row(null, 1), Row(null, "hello"))))
    // Missing field.
    testWithSchema(obj, StructType.fromDDL("a int, c string, b string"),
      Row(obj.getMetadata, null, Row(Row(null, 1), Row(null, null), Row(null, "hello"))))
    // "a" is not present in shredding schema.
    testWithSchema(obj, StructType.fromDDL("b string, c string"),
      Row(obj.getMetadata, untypedValue("""{"a": 1}"""), Row(Row(null, "hello"), Row(null, null))))
    // "b" is not present in shredding schema. This case is a bit trickier, because the ID
    // will be 1, not 0, since we'll use the original metadata dictionary that contains a and b.
    // So we need to edit the variant value produced by parseJson.
    val residual = untypedValue("""{"b": "hello"}""")
    // First byte is the type, second is number of fields, and the third is the
    // dictionary ID of the first field.
    residual(2) = 1
    testWithSchema(obj, StructType.fromDDL("a int, c string"),
      Row(obj.getMetadata, residual, Row(Row(null, 1), Row(null, null))))
    // "a" is the wrong type.
    testWithSchema(obj, StructType.fromDDL("a string, b string"),
      Row(obj.getMetadata, null, Row(Row(untypedValue("1"), null), Row(null, "hello"))))
    // Not an object
    testWithSchema(obj, ArrayType(StructType.fromDDL("a int, b string")),
      Row(obj.getMetadata, untypedValue(obj), null))
  }

  test("shredding as array") {
    val arr = parseJson("""[{"a": 1, "b": "hello"}, 2, null, 4]""")
    // Can't be cast to scalar or object.
    Seq(IntegerType, LongType, ShortType, ByteType, StringType, DecimalType(5, 5),
      TimestampType, DateType, BooleanType, DoubleType, FloatType, BinaryType,
      StructType.fromDDL("a int, b string")).foreach { t =>
      testWithSchema(arr, t, Row(arr.getMetadata, untypedValue(arr), null))
    }
    // First element is shredded
    testWithSchema(arr, ArrayType(StructType.fromDDL("a int, b string")),
      Row(arr.getMetadata, null, Array(
        Row(null, Row(Row(null, 1), Row(null, "hello"))),
        Row(untypedValue("2"), null),
        Row(untypedValue("null"), null),
        Row(untypedValue("4"), null)
      )))
    // Second and fourth are shredded
    testWithSchema(arr, ArrayType(LongType),
      Row(arr.getMetadata, null, Array(
        Row(untypedValue("""{"a": 1, "b": "hello"}"""), null),
        Row(null, 2),
        Row(untypedValue("null"), null),
        Row(null, 4)
      )))

    // Fully shredded
    testWithSchema("[1,2,3]", ArrayType(LongType),
      Row(emptyMetadata, null, Array(
        Row(null, 1),
        Row(null, 2),
        Row(null, 3)
      )))
  }

}
