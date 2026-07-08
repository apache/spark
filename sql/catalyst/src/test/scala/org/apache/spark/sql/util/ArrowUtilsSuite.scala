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

import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}

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

  test("timestamp nanos") {
    // NTZ is zone-independent (null Arrow timezone); precision preserved via field metadata.
    Seq(7, 8, 9).foreach { p =>
      val schema = new StructType().add("value", TimestampNTZNanosType(p))
      val arrowSchema = ArrowUtils.toArrowSchema(schema, null, true, false)
      val fieldType = arrowSchema.findField("value").getType.asInstanceOf[ArrowType.Timestamp]
      assert(fieldType.getUnit === TimeUnit.NANOSECOND)
      assert(fieldType.getTimezone === null)
      assert(ArrowUtils.fromArrowSchema(arrowSchema) === schema)
    }

    // LTZ is zone-aware: it requires a non-null session time zone; precision preserved.
    def roundtripLtz(timeZoneId: String): Unit = {
      Seq(7, 8, 9).foreach { p =>
        val schema = new StructType().add("value", TimestampLTZNanosType(p))
        val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId, true, false)
        val fieldType = arrowSchema.findField("value").getType.asInstanceOf[ArrowType.Timestamp]
        assert(fieldType.getUnit === TimeUnit.NANOSECOND)
        assert(fieldType.getTimezone === timeZoneId)
        assert(ArrowUtils.fromArrowSchema(arrowSchema) === schema)
      }
    }
    roundtripLtz(ZoneId.systemDefault().getId)
    roundtripLtz("Asia/Tokyo")
    roundtripLtz("UTC")
    roundtripLtz(LA.getId)

    // LTZ without a time zone is an error, mirroring TimestampType.
    checkError(
      exception = intercept[SparkException] {
        ArrowUtils.toArrowSchema(
          new StructType().add("value", TimestampLTZNanosType(9)), null, true, false)
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "Missing timezoneId where it is mandatory."))

    // Fallback: a nanosecond Arrow timestamp without precision metadata maps to canonical p=9.
    def nanosField(timeZoneId: String): Field = new Field(
      "value",
      new FieldType(true, new ArrowType.Timestamp(TimeUnit.NANOSECOND, timeZoneId), null, null),
      java.util.Collections.emptyList[Field]())
    assert(ArrowUtils.fromArrowField(nanosField(null)) === TimestampNTZNanosType(9))
    assert(ArrowUtils.fromArrowField(nanosField("UTC")) === TimestampLTZNanosType(9))

    // Fallback also covers a present-but-invalid precision key (out of [7, 9] or non-numeric):
    // the value is unusable, so the type maps to the canonical p=9 just like the no-metadata case.
    def nanosFieldWithPrecision(timeZoneId: String, precision: String): Field = new Field(
      "value",
      new FieldType(
        true,
        new ArrowType.Timestamp(TimeUnit.NANOSECOND, timeZoneId),
        null,
        java.util.Collections.singletonMap("SPARK::timestampNanos::precision", precision)),
      java.util.Collections.emptyList[Field]())
    assert(
      ArrowUtils.fromArrowField(nanosFieldWithPrecision(null, "5")) === TimestampNTZNanosType(9))
    assert(
      ArrowUtils.fromArrowField(nanosFieldWithPrecision("UTC", "x")) === TimestampLTZNanosType(9))

    // The precision metadata key does not leak into the reconstructed column Metadata.
    val md = new MetadataBuilder().putString("city", "beijing").build()
    val schemaWithMeta =
      new StructType().add("value", TimestampNTZNanosType(7), nullable = true, md)
    assert(ArrowUtils.fromArrowSchema(
      ArrowUtils.toArrowSchema(schemaWithMeta, null, true, false)) === schemaWithMeta)
  }

  test("timestamp nanos lossless struct") {
    def losslessRoundtrip(schema: StructType, timeZoneId: String = null): Unit = {
      val arrowSchema =
        ArrowUtils.toArrowSchema(schema, timeZoneId, true, false, losslessTimestampNanos = true)
      assert(ArrowUtils.fromArrowSchema(arrowSchema) === schema)
    }

    // Top-level: the lossless mapping is a struct of (epochMicros: int64, nanosWithinMicro:
    // int16); the NTZ/LTZ kind and the precision round-trip through the child field metadata.
    Seq(7, 8, 9).foreach { p =>
      Seq[DataType](TimestampNTZNanosType(p), TimestampLTZNanosType(p)).foreach { dt =>
        val schema = new StructType().add("value", dt)
        val arrowSchema =
          ArrowUtils.toArrowSchema(schema, null, true, false, losslessTimestampNanos = true)
        val field = arrowSchema.findField("value")
        assert(field.getType === ArrowType.Struct.INSTANCE)
        val children = field.getChildren
        assert(children.size() === 2)
        assert(children.get(0).getName === "epochMicros")
        assert(children.get(0).getType === new ArrowType.Int(64, true))
        assert(!children.get(0).isNullable)
        assert(children.get(1).getName === "nanosWithinMicro")
        assert(children.get(1).getType === new ArrowType.Int(16, true))
        assert(!children.get(1).isNullable)
        assert(ArrowUtils.isTimestampNanosStructField(field))
        assert(ArrowUtils.fromArrowSchema(arrowSchema) === schema)
      }
    }

    // Unlike the default int64 mapping, LTZ needs no session time zone: the struct stores the
    // raw value components, which are zone-independent.
    losslessRoundtrip(new StructType().add("value", TimestampLTZNanosType(9)))

    // Nested: the flag must reach nanosecond timestamps inside arrays, structs, and maps.
    losslessRoundtrip(new StructType()
      .add("arr", ArrayType(TimestampNTZNanosType(9)))
      .add("struct", new StructType().add("ts", TimestampLTZNanosType(7)))
      .add("map", MapType(IntegerType, TimestampNTZNanosType(8))))

    // User metadata on the column is preserved alongside the struct tag.
    val md = new MetadataBuilder().putString("city", "beijing").build()
    losslessRoundtrip(new StructType().add("value", TimestampNTZNanosType(7), true, md))

    // An invalid or missing precision on the tagged child falls back to the canonical maximum
    // precision, mirroring the default int64 mapping's fallback.
    def taggedStructField(precision: Option[String]): Field = {
      val microsMd = new java.util.HashMap[String, String]()
      microsMd.put("SPARK::timestampNanos::struct", "ntz")
      precision.foreach(p => microsMd.put("SPARK::timestampNanos::precision", p))
      new Field(
        "value",
        new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
        java.util.Arrays.asList(
          new Field(
            "epochMicros",
            new FieldType(false, new ArrowType.Int(64, true), null, microsMd),
            java.util.Collections.emptyList[Field]()),
          new Field(
            "nanosWithinMicro",
            new FieldType(false, new ArrowType.Int(16, true), null, null),
            java.util.Collections.emptyList[Field]())))
    }
    assert(ArrowUtils.fromArrowField(taggedStructField(Some("5"))) === TimestampNTZNanosType(9))
    assert(ArrowUtils.fromArrowField(taggedStructField(None)) === TimestampNTZNanosType(9))

    // A plain struct that merely uses the same child names, but carries no tag, stays a struct.
    val untagged = new StructType().add(
      "value",
      new StructType()
        .add("epochMicros", LongType, nullable = false)
        .add("nanosWithinMicro", ShortType, nullable = false))
    losslessRoundtrip(untagged)
    assert(
      ArrowUtils.fromArrowSchema(ArrowUtils.toArrowSchema(untagged, null, true, false)) ===
        untagged)

    // The default mapping is untouched when the flag is off: still a single nanosecond timestamp.
    val defaultSchema = ArrowUtils.toArrowSchema(
      new StructType().add("value", TimestampNTZNanosType(9)), null, true, false)
    assert(defaultSchema.findField("value").getType.isInstanceOf[ArrowType.Timestamp])
  }

  test("time") {
    // Arrow's Time type has no precision field, so TIME(p) precision is preserved via field
    // metadata; the Arrow type itself stays Time(NANOSECOND, 64).
    Seq(0, 3, 6, 7, 9).foreach { p =>
      val schema = new StructType().add("value", TimeType(p))
      val arrowSchema = ArrowUtils.toArrowSchema(schema, null, true, false)
      val fieldType = arrowSchema.findField("value").getType.asInstanceOf[ArrowType.Time]
      assert(fieldType.getUnit === TimeUnit.NANOSECOND)
      assert(fieldType.getBitWidth === 8 * 8)
      assert(ArrowUtils.fromArrowSchema(arrowSchema) === schema)
    }

    // Fallback: a nanosecond Arrow time without precision metadata maps to canonical TIME(6).
    def timeField: Field = new Field(
      "value",
      new FieldType(true, new ArrowType.Time(TimeUnit.NANOSECOND, 8 * 8), null, null),
      java.util.Collections.emptyList[Field]())
    assert(ArrowUtils.fromArrowField(timeField) === TimeType(TimeType.MICROS_PRECISION))

    // Fallback also covers a present-but-invalid precision key (out of [0, 9] or non-numeric):
    // the value is unusable, so the type maps to the canonical TIME(6) just like the no-metadata
    // case.
    def timeFieldWithPrecision(precision: String): Field = new Field(
      "value",
      new FieldType(
        true,
        new ArrowType.Time(TimeUnit.NANOSECOND, 8 * 8),
        null,
        java.util.Collections.singletonMap("SPARK::time::precision", precision)),
      java.util.Collections.emptyList[Field]())
    val micros = TimeType(TimeType.MICROS_PRECISION)
    assert(ArrowUtils.fromArrowField(timeFieldWithPrecision("-1")) === micros)
    assert(ArrowUtils.fromArrowField(timeFieldWithPrecision("10")) === micros)
    assert(ArrowUtils.fromArrowField(timeFieldWithPrecision("x")) === micros)

    // The precision metadata key does not leak into the reconstructed column Metadata.
    val md = new MetadataBuilder().putString("city", "beijing").build()
    val schemaWithMeta = new StructType().add("value", TimeType(3), nullable = true, md)
    assert(ArrowUtils.fromArrowSchema(
      ArrowUtils.toArrowSchema(schemaWithMeta, null, true, false)) === schemaWithMeta)
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
