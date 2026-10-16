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

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.types.{IntervalUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, Field, FieldType}

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
        ArrowUtils.toArrowSchema(schema, timeZoneId, true, false, losslessInternalTypes = true)
      assert(ArrowUtils.fromArrowSchema(arrowSchema) === schema)
    }

    // Top-level: the lossless mapping is a struct of (epochMicros: int64, nanosWithinMicro:
    // int16); the NTZ/LTZ kind and the precision round-trip through the child field metadata.
    Seq(7, 8, 9).foreach { p =>
      Seq[DataType](TimestampNTZNanosType(p), TimestampLTZNanosType(p)).foreach { dt =>
        val schema = new StructType().add("value", dt)
        val arrowSchema =
          ArrowUtils.toArrowSchema(schema, null, true, false, losslessInternalTypes = true)
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

    // Only the exact canonical shape is recognized: the struct writer fills children
    // positionally while ArrowColumnVector reads them by name, so a tagged-but-non-canonical
    // schema (reordered, wrong width, or extra children) must NOT be treated as a nanosecond
    // timestamp -- it falls back to generic (order-faithful) struct handling.
    def taggedNanosStruct(children: Seq[(String, Int)]): Field = {
      val fields = children.map { case (name, bitWidth) =>
        val md = if (name == "epochMicros") {
          java.util.Collections.singletonMap("SPARK::timestampNanos::struct", "ntz")
        } else {
          null
        }
        new Field(
          name,
          new FieldType(false, new ArrowType.Int(bitWidth, true), null, md),
          java.util.Collections.emptyList[Field]())
      }
      new Field(
        "value",
        new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
        fields.asJava)
    }
    // Reordered children.
    assert(!ArrowUtils.isTimestampNanosStructField(
      taggedNanosStruct(Seq("nanosWithinMicro" -> 16, "epochMicros" -> 64))))
    // Wrong child width.
    assert(!ArrowUtils.isTimestampNanosStructField(
      taggedNanosStruct(Seq("epochMicros" -> 64, "nanosWithinMicro" -> 32))))
    // Extra child.
    assert(!ArrowUtils.isTimestampNanosStructField(
      taggedNanosStruct(Seq("epochMicros" -> 64, "nanosWithinMicro" -> 16, "extra" -> 32))))
    // The canonical shape built by the same helper is recognized (sanity check of the helper).
    assert(ArrowUtils.isTimestampNanosStructField(
      taggedNanosStruct(Seq("epochMicros" -> 64, "nanosWithinMicro" -> 16))))

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

  test("calendar interval lossless struct") {
    def losslessRoundtrip(schema: StructType): Unit = {
      val arrowSchema =
        ArrowUtils.toArrowSchema(schema, null, true, false, losslessInternalTypes = true)
      assert(ArrowUtils.fromArrowSchema(arrowSchema) === schema)
    }

    // Top-level: the lossless mapping is a struct of the type's own components
    // (months: int32, days: int32, microseconds: int64), tagged through child field metadata.
    val schema = new StructType().add("value", CalendarIntervalType)
    val arrowSchema =
      ArrowUtils.toArrowSchema(schema, null, true, false, losslessInternalTypes = true)
    val field = arrowSchema.findField("value")
    assert(field.getType === ArrowType.Struct.INSTANCE)
    val children = field.getChildren
    assert(children.size() === 3)
    assert(children.get(0).getName === "months")
    assert(children.get(0).getType === new ArrowType.Int(32, true))
    assert(!children.get(0).isNullable)
    assert(children.get(1).getName === "days")
    assert(children.get(1).getType === new ArrowType.Int(32, true))
    assert(!children.get(1).isNullable)
    assert(children.get(2).getName === "microseconds")
    assert(children.get(2).getType === new ArrowType.Int(64, true))
    assert(!children.get(2).isNullable)
    assert(ArrowUtils.isCalendarIntervalStructField(field))
    assert(ArrowUtils.fromArrowSchema(arrowSchema) === schema)

    // Nested: the flag must reach intervals inside arrays, structs, and maps.
    losslessRoundtrip(new StructType()
      .add("arr", ArrayType(CalendarIntervalType))
      .add("struct", new StructType().add("i", CalendarIntervalType))
      .add("map", MapType(IntegerType, CalendarIntervalType)))

    // User metadata on the column is preserved alongside the struct tag.
    val md = new MetadataBuilder().putString("city", "beijing").build()
    losslessRoundtrip(new StructType().add("value", CalendarIntervalType, true, md))

    // A plain struct that merely uses the same child names, but carries no tag, stays a struct.
    val untagged = new StructType().add(
      "value",
      new StructType()
        .add("months", IntegerType, nullable = false)
        .add("days", IntegerType, nullable = false)
        .add("microseconds", LongType, nullable = false))
    losslessRoundtrip(untagged)
    assert(
      ArrowUtils.fromArrowSchema(ArrowUtils.toArrowSchema(untagged, null, true, false)) ===
        untagged)

    // Only the exact canonical shape is recognized: CalendarIntervalStructWriter fills children
    // positionally while ArrowColumnVector reads them by name, so a tagged-but-reordered schema
    // (which would silently swap months and days) or other non-canonical shapes must NOT be
    // treated as a CalendarInterval.
    def taggedIntervalStruct(children: Seq[(String, Int)]): Field = {
      val fields = children.map { case (name, bitWidth) =>
        val md = if (name == "months") {
          java.util.Collections.singletonMap("SPARK::calendarInterval::struct", "true")
        } else {
          null
        }
        new Field(
          name,
          new FieldType(false, new ArrowType.Int(bitWidth, true), null, md),
          java.util.Collections.emptyList[Field]())
      }
      new Field(
        "value",
        new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
        fields.asJava)
    }
    // Reordered children.
    assert(!ArrowUtils.isCalendarIntervalStructField(
      taggedIntervalStruct(Seq("days" -> 32, "months" -> 32, "microseconds" -> 64))))
    // Wrong child width.
    assert(!ArrowUtils.isCalendarIntervalStructField(
      taggedIntervalStruct(Seq("months" -> 32, "days" -> 64, "microseconds" -> 64))))
    // Extra child.
    assert(!ArrowUtils.isCalendarIntervalStructField(
      taggedIntervalStruct(
        Seq("months" -> 32, "days" -> 32, "microseconds" -> 64, "extra" -> 32))))
    // Missing child.
    assert(!ArrowUtils.isCalendarIntervalStructField(
      taggedIntervalStruct(Seq("months" -> 32, "days" -> 32))))
    // The canonical shape built by the same helper is recognized (sanity check of the helper).
    assert(ArrowUtils.isCalendarIntervalStructField(
      taggedIntervalStruct(Seq("months" -> 32, "days" -> 32, "microseconds" -> 64))))

    // The default mapping is untouched when the flag is off: still IntervalMonthDayNano.
    val defaultSchema = ArrowUtils.toArrowSchema(
      new StructType().add("value", CalendarIntervalType), null, true, false)
    val defaultType = defaultSchema.findField("value").getType
    assert(defaultType.isInstanceOf[ArrowType.Interval])
    assert(
      defaultType.asInstanceOf[ArrowType.Interval].getUnit === IntervalUnit.MONTH_DAY_NANO)
  }

  test("isCompatibleWithDeclaredField requires congruence with the declared field tree") {
    def declared(schema: StructType, largeVarTypes: Boolean = false): Field = {
      ArrowUtils.toArrowSchema(schema, "UTC", true, largeVarTypes).findField("value")
    }
    def actual(
        schema: StructType,
        lossless: Boolean = false,
        largeVarTypes: Boolean = false): Field = {
      ArrowUtils
        .toArrowSchema(schema, "UTC", true, largeVarTypes, losslessInternalTypes = lossless)
        .findField("value")
    }
    def compat(a: Field, d: Field): Boolean = ArrowUtils.isCompatibleWithDeclaredField(a, d)

    // Identical construction is congruent, for every kind of type the schema can declare,
    // including the standard nanos/interval encodings, tagged struct types, and nested trees.
    Seq[DataType](
      IntegerType,
      StringType,
      TimestampNTZNanosType(9),
      CalendarIntervalType,
      GeometryType(4326),
      VariantType,
      ArrayType(TimestampNTZNanosType(9)),
      new StructType().add("i", CalendarIntervalType),
      MapType(IntegerType, StringType)).foreach { dt =>
      val s = new StructType().add("value", dt)
      assert(compat(actual(s), declared(s)), dt.toString)
    }

    // The lossless internal encodings are structs where the declared schema has the
    // interchange types: incongruent in both directions.
    Seq[DataType](
      TimestampNTZNanosType(9),
      CalendarIntervalType,
      ArrayType(TimestampLTZNanosType(7))).foreach { dt =>
      val s = new StructType().add("value", dt)
      assert(!compat(actual(s, lossless = true), declared(s)), dt.toString)
    }

    // Var-width offset width must agree with the declared field, in both directions and nested.
    Seq[DataType](StringType, BinaryType, ArrayType(StringType)).foreach { dt =>
      val s = new StructType().add("value", dt)
      assert(compat(actual(s), declared(s)), dt.toString)
      assert(!compat(actual(s), declared(s, largeVarTypes = true)), dt.toString)
      assert(!compat(actual(s, largeVarTypes = true), declared(s)), dt.toString)
      assert(
        compat(actual(s, largeVarTypes = true), declared(s, largeVarTypes = true)),
        dt.toString)
    }

    // A tagged Geometry struct with an extra child is read back as GeometryType by the
    // recognizers (which tolerate extra children), but the declared canonical field has exactly
    // two children -- forwarding the three-child body verbatim would shift every following
    // column's buffers (the extra child's buffers would be consumed as the next argument's
    // data), so it must be incongruent.
    val geometrySchema = new StructType().add("value", GeometryType(4326))
    val canonicalGeometry = declared(geometrySchema)
    val extraChild = new Field(
      "extra",
      new FieldType(true, new ArrowType.Int(32, true), null, null),
      java.util.Collections.emptyList[Field]())
    val paddedChildren = new java.util.ArrayList[Field](canonicalGeometry.getChildren)
    paddedChildren.add(extraChild)
    val paddedGeometry = new Field(
      canonicalGeometry.getName,
      new FieldType(
        canonicalGeometry.isNullable,
        canonicalGeometry.getType,
        null,
        canonicalGeometry.getMetadata),
      paddedChildren)
    assert(ArrowUtils.fromArrowField(paddedGeometry) === GeometryType(4326))
    assert(!compat(paddedGeometry, canonicalGeometry))

    // List-like types Spark never declares are incongruent with the declared List.
    val arraySchema = new StructType().add("value", ArrayType(IntegerType))
    val declaredList = declared(arraySchema)
    val intChild = new Field(
      "element",
      new FieldType(true, new ArrowType.Int(32, true), null, null),
      java.util.Collections.emptyList[Field]())
    Seq[ArrowType](
      ArrowType.LargeList.INSTANCE,
      ArrowType.ListView.INSTANCE,
      ArrowType.LargeListView.INSTANCE,
      new ArrowType.FixedSizeList(4)).foreach { arrowType =>
      val listLike = new Field(
        "value",
        new FieldType(true, arrowType, null, null),
        java.util.Collections.singletonList(intChild))
      assert(!compat(listLike, declaredList), arrowType.toString)
    }

    // The timestamp timezone LABEL does not affect the physical layout and differs across
    // Spark's own paths (a worker's output comes back labeled UTC while the next stream
    // declares the session time zone), so it must not cause a reject. Presence-vs-absence must
    // still match: it distinguishes TimestampType from TimestampNTZType, whose values are
    // interpreted differently. The unit must also match: both units are int64, but forwarding
    // one under the other reinterprets every value by a factor of 1000.
    def tsField(tz: String): Field = new Field(
      "value",
      new FieldType(true, new ArrowType.Timestamp(TimeUnit.MICROSECOND, tz), null, null),
      java.util.Collections.emptyList[Field]())
    assert(compat(tsField("UTC"), tsField("America/Los_Angeles")))
    assert(compat(tsField("America/Los_Angeles"), tsField("UTC")))
    assert(!compat(tsField(null), tsField("UTC")))
    assert(!compat(tsField("UTC"), tsField(null)))
    val tsNanoField = new Field(
      "value",
      new FieldType(true, new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), null, null),
      java.util.Collections.emptyList[Field]())
    assert(!compat(tsNanoField, tsField("UTC")))

    // Map equality includes keysSorted, which has no layout impact and must not cause a reject.
    val mapSchema = new StructType().add("value", MapType(IntegerType, StringType))
    val declaredMap = declared(mapSchema)
    val sortedMap = new Field(
      declaredMap.getName,
      new FieldType(declaredMap.isNullable, new ArrowType.Map(true), null, null),
      declaredMap.getChildren)
    assert(compat(sortedMap, declaredMap))

    // An Arrow Map binds key/value semantics to the entry-struct children by name (the key
    // comes first and MapVector addresses the children as "key"/"value"), yet Arrow tolerates
    // vectors whose entry children sit in the opposite order. With int keys and int values the
    // swapped tree is positionally identical to the canonical one, so only the names reveal the
    // swap -- forwarding the buffers verbatim under the canonical header would silently decode
    // {k: v} as {v: k}. Map entry names must therefore match the declared ones, at every
    // nesting level.
    def swapEntries(mapField: Field): Field = {
      val entries = mapField.getChildren.get(0)
      val swapped = new Field(
        entries.getName,
        new FieldType(entries.isNullable, entries.getType, null, entries.getMetadata),
        entries.getChildren.asScala.reverse.asJava)
      new Field(
        mapField.getName,
        new FieldType(mapField.isNullable, mapField.getType, null, mapField.getMetadata),
        java.util.Collections.singletonList(swapped))
    }
    val intMapSchema = new StructType().add("value", MapType(IntegerType, IntegerType, false))
    val declaredIntMap = declared(intMapSchema)
    assert(compat(declaredIntMap, declaredIntMap))
    assert(!compat(swapEntries(declaredIntMap), declaredIntMap))
    val nestedMapSchema = new StructType()
      .add("value", MapType(IntegerType, MapType(IntegerType, IntegerType, false), false))
    val declaredNestedMap = declared(nestedMapSchema)
    val nestedEntries = declaredNestedMap.getChildren.get(0)
    val innerSwappedEntries = new Field(
      nestedEntries.getName,
      new FieldType(nestedEntries.isNullable, nestedEntries.getType, null,
        nestedEntries.getMetadata),
      java.util.Arrays.asList(
        nestedEntries.getChildren.get(0),
        swapEntries(nestedEntries.getChildren.get(1))))
    val innerSwappedMap = new Field(
      declaredNestedMap.getName,
      new FieldType(declaredNestedMap.isNullable, declaredNestedMap.getType, null,
        declaredNestedMap.getMetadata),
      java.util.Collections.singletonList(innerSwappedEntries))
    assert(compat(declaredNestedMap, declaredNestedMap))
    assert(!compat(innerSwappedMap, declaredNestedMap))

    // A view-typed field is incongruent with the declared Utf8.
    val stringSchema = new StructType().add("value", StringType)
    val viewField = new Field(
      "value",
      new FieldType(true, ArrowType.Utf8View.INSTANCE, null, null),
      java.util.Collections.emptyList[Field]())
    assert(!compat(viewField, declared(stringSchema)))

    // A dictionary-encoded field is incongruent even though its value type matches the declared
    // field: its record batches carry indices whose values live in separate dictionary batches
    // the UDF stream never writes.
    val dictField = new Field(
      "value",
      new FieldType(
        true,
        ArrowType.Utf8.INSTANCE,
        new DictionaryEncoding(1L, false, new ArrowType.Int(32, true)),
        null),
      java.util.Collections.emptyList[Field]())
    assert(!compat(dictField, declared(stringSchema)))
    // The same field without the dictionary is congruent, isolating the dictionary as the
    // reason. Names and metadata are ignored: only the physical layout matters.
    val plainField = new Field(
      "renamed",
      new FieldType(true, ArrowType.Utf8.INSTANCE, null, null),
      java.util.Collections.emptyList[Field]())
    assert(compat(plainField, declared(stringSchema)))
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
