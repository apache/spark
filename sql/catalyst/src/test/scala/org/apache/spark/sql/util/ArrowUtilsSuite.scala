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

package org.apache.spark.sql.util

import java.time.ZoneId

import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.{SparkException, SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.LA
import org.apache.spark.sql.types._

class ArrowUtilsSuite extends SparkFunSuite {

  def roundtrip(dt: DataType): Unit = {
    dt match {
      case schema: StructType =>
        assert(ArrowUtils.fromArrowSchema(
          ArrowUtils.toArrowSchema(schema, null, true, false)) === schema)
      case _ =>
        roundtrip(new StructType().add("value", dt))
    }
  }

  test("simple") {
    roundtrip(BooleanType)
    roundtrip(ByteType)
    roundtrip(ShortType)
    roundtrip(IntegerType)
    roundtrip(LongType)
    roundtrip(FloatType)
    roundtrip(DoubleType)
    roundtrip(StringType)
    roundtrip(BinaryType)
    roundtrip(DecimalType.SYSTEM_DEFAULT)
    roundtrip(DateType)
    roundtrip(GeometryType("ANY"))
    roundtrip(GeometryType(4326))
    roundtrip(GeographyType("ANY"))
    roundtrip(GeographyType(4326))
    roundtrip(YearMonthIntervalType())
    roundtrip(DayTimeIntervalType())
    checkError(
      exception = intercept[SparkException] {
        roundtrip(TimestampType)
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "Missing timezoneId where it is mandatory."))
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        ArrowUtils.fromArrowType(new ArrowType.Int(8, false))
      },
      condition = "UNSUPPORTED_ARROWTYPE",
      parameters = Map("typeName" -> "Int(8, false)")
    )
  }

  test("timestamp") {

    def roundtripWithTz(timeZoneId: String): Unit = {
      val schema = new StructType().add("value", TimestampType)
      val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId, true, false)
      val fieldType = arrowSchema.findField("value").getType.asInstanceOf[ArrowType.Timestamp]
      assert(fieldType.getTimezone() === timeZoneId)
      assert(ArrowUtils.fromArrowSchema(arrowSchema) === schema)
    }

    roundtripWithTz(ZoneId.systemDefault().getId)
    roundtripWithTz("Asia/Tokyo")
    roundtripWithTz("UTC")
    roundtripWithTz(LA.getId)
  }

  test("array") {
    roundtrip(ArrayType(IntegerType, containsNull = true))
    roundtrip(ArrayType(IntegerType, containsNull = false))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = true), containsNull = true))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = true), containsNull = false))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = false))
  }

  test("struct") {
    roundtrip(new StructType())
    roundtrip(new StructType().add("i", IntegerType))
    roundtrip(new StructType().add("arr", ArrayType(IntegerType)))
    roundtrip(new StructType().add("i", IntegerType).add("arr", ArrayType(IntegerType)))
    roundtrip(new StructType().add(
      "struct",
      new StructType().add("i", IntegerType).add("arr", ArrayType(IntegerType))))
  }

  test("metadata should be kept after roundtrip") {
    roundtrip(new StructType()
      .add("i", IntegerType, true, Metadata.empty)
      .add("j", LongType, true,
        new MetadataBuilder()
          .putLong("a", Long.MaxValue).putString("city", "beijing").build())
    )

    roundtrip(new StructType()
      .add("v", VariantType, true,
        new MetadataBuilder()
          .putDouble("a", 1.234).putBoolean("is_geo?", false).build())
      .add("i", GeometryType("ANY"), false,
        new MetadataBuilder()
          .putLongArray("list", Array(1, 2, 3)).putString("is_geo?", "true").build())
      .add("i", GeographyType("ANY"), false,
        new MetadataBuilder()
          .putStringArray("list", Array("x", "y")).putString("is_geo?", "true").build())
    )

    roundtrip(new StructType()
      .add("arr", ArrayType(IntegerType), false,
        new MetadataBuilder()
          .putBoolean("is_array?", true).putString("old_name", "old_arr").build())
      .add("map", MapType(LongType, StringType), false,
        new MetadataBuilder()
          .putBoolean("is_array?", false).putString("old_name", "old_map").build())
      .add("struct",
        new StructType()
          .add("s", IntegerType, true,
            new MetadataBuilder()
              .putDouble("pi", 3.14)
              .putString("what type", "struct").build()),
        false,
        new MetadataBuilder()
          .putBoolean("is_array?", false).putString("old_name", "old_map").build())
      .add("3_dim_array",
        ArrayType(ArrayType(ArrayType(new StructType()
          .add("i", IntegerType, true,
            new MetadataBuilder().putLong("v", 1).putString("type", "data point").build())
          .add("c", StringType, false,
            new MetadataBuilder()
              .putLong("v", 1).putString("city", "singapore").build())))),
        true,
        new MetadataBuilder()
          .putBoolean("is_nested_array?", true).putString("dims", "x-y-z").build())
    )
  }

  test("struct with duplicated field names") {

    def check(dt: DataType, expected: DataType): Unit = {
      val schema = new StructType().add("value", dt)
      intercept[SparkUnsupportedOperationException] {
        ArrowUtils.toArrowSchema(schema, null, true, false)
      }
      assert(ArrowUtils.fromArrowSchema(ArrowUtils.toArrowSchema(schema, null, false, false))
        === new StructType().add("value", expected))
    }

    roundtrip(new StructType().add("i", IntegerType).add("i", StringType))

    check(new StructType().add("i", IntegerType).add("i", StringType),
      new StructType().add("i_0", IntegerType).add("i_1", StringType))
    check(ArrayType(new StructType().add("i", IntegerType).add("i", StringType)),
      ArrayType(new StructType().add("i_0", IntegerType).add("i_1", StringType)))
    check(MapType(StringType, new StructType().add("i", IntegerType).add("i", StringType)),
      MapType(StringType, new StructType().add("i_0", IntegerType).add("i_1", StringType)))
  }
}
