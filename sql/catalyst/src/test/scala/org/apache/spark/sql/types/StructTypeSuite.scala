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

package org.apache.spark.sql.types

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.{SparkException, SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{caseInsensitiveResolution, caseSensitiveResolution}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.{ColumnDefinition, DefaultValueExpression}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{ResolveDefaultColumns, ResolveDefaultColumnsUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DayTimeIntervalType => DT}
import org.apache.spark.sql.types.{YearMonthIntervalType => YM}
import org.apache.spark.sql.types.DayTimeIntervalType._
import org.apache.spark.sql.types.StructType.fromDDL
import org.apache.spark.sql.types.YearMonthIntervalType._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.unsafe.types.UTF8String.LongWrapper

class StructTypeSuite extends SparkFunSuite with SQLHelper {

  private val s = StructType.fromDDL("a INT, b STRING")

  private val UNICODE_COLLATION = "UNICODE"
  private val UTF8_LCASE_COLLATION = "UTF8_LCASE"
  private val mapper = new ObjectMapper()

  test("lookup a single missing field should output existing fields") {
    checkError(
      exception = intercept[SparkIllegalArgumentException](s("c")),
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`c`", "fields" -> "`a`, `b`"))
  }

  test("lookup a set of missing fields should output existing fields") {
    checkError(
      exception = intercept[SparkIllegalArgumentException](s(Set("a", "c"))),
      condition = "NONEXISTENT_FIELD_NAME_IN_LIST",
      parameters = Map("nonExistFields" -> "`c`", "fieldNames" -> "`a`, `b`"))
  }

  test("lookup fieldIndex for missing field should output existing fields") {
    checkError(
      exception = intercept[SparkIllegalArgumentException](s.fieldIndex("c")),
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`c`", "fields" -> "`a`, `b`"))
  }

  test("SPARK-24849: toDDL - simple struct") {
    val struct = StructType(Seq(StructField("a", IntegerType)))

    assert(struct.toDDL == "a INT")
  }

  test("SPARK-24849: round trip toDDL - fromDDL") {
    val struct = new StructType().add("a", IntegerType).add("b", StringType)

    assert(fromDDL(struct.toDDL) === struct)
  }

  test("SPARK-24849: round trip fromDDL - toDDL") {
    val struct = "a MAP<INT, STRING>,b INT"

    assert(fromDDL(struct).toDDL === struct)
  }

  test("SPARK-24849: toDDL must take into account case of fields.") {
    val struct = new StructType()
      .add("metaData", new StructType().add("eventId", StringType))

    assert(struct.toDDL == "metaData STRUCT<eventId: STRING>")
  }

  test("SPARK-24849: toDDL should output field's comment") {
    val struct = StructType(Seq(
      StructField("b", BooleanType).withComment("Field's comment")))

    assert(struct.toDDL == """b BOOLEAN COMMENT 'Field\'s comment'""")
  }

  private val nestedStruct = new StructType()
    .add(StructField("a", new StructType()
      .add(StructField("b", new StructType()
        .add(StructField("c", StringType
        ).withComment("Deep Nested comment"))
      ).withComment("Nested comment"))
    ).withComment("comment"))

  test("SPARK-33846: toDDL should output nested field's comment") {
    val ddl = "a STRUCT<b: STRUCT<c: STRING COMMENT 'Deep Nested comment'> " +
      "COMMENT 'Nested comment'> COMMENT 'comment'"
    assert(nestedStruct.toDDL == ddl)
  }

  test("SPARK-33846: fromDDL should parse nested field's comment") {
    val ddl = "`a` STRUCT<`b`: STRUCT<`c`: STRING COMMENT 'Deep Nested comment'> " +
      "COMMENT 'Nested comment'> COMMENT 'comment'"
    assert(StructType.fromDDL(ddl) == nestedStruct)
  }

  test("SPARK-33846: round trip toDDL -> fromDDL - nested struct") {
    assert(StructType.fromDDL(nestedStruct.toDDL) == nestedStruct)
  }

  test("SPARK-35706: make the ':' in STRUCT data type definition optional") {
    val ddl = "`a` STRUCT<`b` STRUCT<`c` STRING COMMENT 'Deep Nested comment'> " +
      "COMMENT 'Nested comment'> COMMENT 'comment'"
    assert(StructType.fromDDL(ddl) == nestedStruct)
  }

  private val structWithEmptyString = new StructType()
    .add(StructField("a b", StringType).withComment("comment"))

  test("SPARK-33846: empty string in a column's name should be respected by toDDL") {
    val ddl = "`a b` STRING COMMENT 'comment'"

    assert(structWithEmptyString.toDDL == ddl)
  }

  test("SPARK-33846: empty string in a column's name should be respected by fromDDL") {
    val ddl = "`a b` STRING COMMENT 'comment'"

    assert(StructType.fromDDL(ddl) == structWithEmptyString)
  }

  test("Print up to the given level") {
    val schema = StructType.fromDDL(
      "c1 INT, c2 STRUCT<c3: INT, c4: STRUCT<c5: INT, c6: INT>>")

    assert(5 == schema.treeString(2).split("\n").length)
    assert(3 == schema.treeString(1).split("\n").length)
    assert(7 == schema.treeString.split("\n").length)
    assert(7 == schema.treeString(0).split("\n").length)
    assert(7 == schema.treeString(-1).split("\n").length)

    val schema2 = StructType.fromDDL(
      "c1 INT, c2 ARRAY<STRUCT<c3: INT>>, c4 STRUCT<c5: INT, c6: ARRAY<ARRAY<INT>>>")
    assert(4 == schema2.treeString(1).split("\n").length)
    assert(7 == schema2.treeString(2).split("\n").length)
    assert(9 == schema2.treeString(3).split("\n").length)
    assert(10 == schema2.treeString(4).split("\n").length)
    assert(10 == schema2.treeString(0).split("\n").length)

    val schema3 = StructType.fromDDL(
      "c1 MAP<INT, STRUCT<c2: MAP<INT, INT>>>, c3 STRUCT<c4: MAP<INT, MAP<INT, INT>>>")
    assert(3 == schema3.treeString(1).split("\n").length)
    assert(6 == schema3.treeString(2).split("\n").length)
    assert(9 == schema3.treeString(3).split("\n").length)
    assert(13 == schema3.treeString(4).split("\n").length)
    assert(13 == schema3.treeString(0).split("\n").length)
  }

  test("interval keyword in schema string") {
    val interval = "a INTERVAL"
    assert(fromDDL(interval).toDDL === interval)
  }

  test("find missing (nested) fields") {
    val schema = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT, c4: STRUCT<c5: INT, c6: INT>>")
    val resolver = SQLConf.get.resolver

    val source1 = StructType.fromDDL("c1 INT")
    val missing1 = StructType.fromDDL("c2 STRUCT<c3: INT, c4: STRUCT<c5: INT, c6: INT>>")
    assert(StructType.findMissingFields(source1, schema, resolver)
      .exists(e => DataTypeUtils.sameType(e, missing1)))

    val source2 = StructType.fromDDL("c1 INT, c3 STRING")
    val missing2 = StructType.fromDDL("c2 STRUCT<c3: INT, c4: STRUCT<c5: INT, c6: INT>>")
    assert(StructType.findMissingFields(source2, schema, resolver)
      .exists(e => DataTypeUtils.sameType(e, missing2)))

    val source3 = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT>")
    val missing3 = StructType.fromDDL("c2 STRUCT<c4: STRUCT<c5: INT, c6: INT>>")
    assert(StructType.findMissingFields(source3, schema, resolver)
      .exists(e => DataTypeUtils.sameType(e, missing3)))

    val source4 = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT, c4: STRUCT<c6: INT>>")
    val missing4 = StructType.fromDDL("c2 STRUCT<c4: STRUCT<c5: INT>>")
    assert(StructType.findMissingFields(source4, schema, resolver)
      .exists(e => DataTypeUtils.sameType(e, missing4)))
  }

  test("find missing (nested) fields: array and map") {
    val resolver = SQLConf.get.resolver

    val schemaWithArray = StructType.fromDDL("c1 INT, c2 ARRAY<STRUCT<c3: INT, c4: LONG>>")
    val source5 = StructType.fromDDL("c1 INT")
    val missing5 = StructType.fromDDL("c2 ARRAY<STRUCT<c3: INT, c4: LONG>>")
    assert(
      StructType.findMissingFields(source5, schemaWithArray, resolver)
        .exists(e => DataTypeUtils.sameType(e, missing5)))

    val schemaWithMap1 = StructType.fromDDL(
      "c1 INT, c2 MAP<STRUCT<c3: INT, c4: LONG>, STRING>, c3 LONG")
    val source6 = StructType.fromDDL("c1 INT, c3 LONG")
    val missing6 = StructType.fromDDL("c2 MAP<STRUCT<c3: INT, c4: LONG>, STRING>")
    assert(
      StructType.findMissingFields(source6, schemaWithMap1, resolver)
        .exists(e => DataTypeUtils.sameType(e, missing6)))

    val schemaWithMap2 = StructType.fromDDL(
      "c1 INT, c2 MAP<STRING, STRUCT<c3: INT, c4: LONG>>, c3 STRING")
    val source7 = StructType.fromDDL("c1 INT, c3 STRING")
    val missing7 = StructType.fromDDL("c2 MAP<STRING, STRUCT<c3: INT, c4: LONG>>")
    assert(
      StructType.findMissingFields(source7, schemaWithMap2, resolver)
        .exists(e => DataTypeUtils.sameType(e, missing7)))

    // Unsupported: nested struct in array, map
    val source8 = StructType.fromDDL("c1 INT, c2 ARRAY<STRUCT<c3: INT>>")
    // `findMissingFields` doesn't support looking into nested struct in array type.
    assert(StructType.findMissingFields(source8, schemaWithArray, resolver).isEmpty)

    val source9 = StructType.fromDDL("c1 INT, c2 MAP<STRUCT<c3: INT>, STRING>, c3 LONG")
    // `findMissingFields` doesn't support looking into nested struct in map type.
    assert(StructType.findMissingFields(source9, schemaWithMap1, resolver).isEmpty)

    val source10 = StructType.fromDDL("c1 INT, c2 MAP<STRING, STRUCT<c3: INT>>, c3 STRING")
    // `findMissingFields` doesn't support looking into nested struct in map type.
    assert(StructType.findMissingFields(source10, schemaWithMap2, resolver).isEmpty)
  }

  test("find missing (nested) fields: case sensitive cases") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val schema = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT, C4: STRUCT<C5: INT, c6: INT>>")
      val resolver = SQLConf.get.resolver

      val source1 = StructType.fromDDL("c1 INT, C2 LONG")
      val missing1 = StructType.fromDDL("c2 STRUCT<c3: INT, C4: STRUCT<C5: INT, c6: INT>>")
      assert(StructType.findMissingFields(source1, schema, resolver)
        .exists(e => DataTypeUtils.sameType(e, missing1)))

      val source2 = StructType.fromDDL("c2 LONG")
      val missing2 = StructType.fromDDL("c1 INT")
      assert(StructType.findMissingFields(source2, schema, resolver)
        .exists(e => DataTypeUtils.sameType(e, missing2)))

      val source3 = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT, C4: STRUCT<c5: INT>>")
      val missing3 = StructType.fromDDL("c2 STRUCT<C4: STRUCT<C5: INT, c6: INT>>")
      assert(StructType.findMissingFields(source3, schema, resolver)
        .exists(e => DataTypeUtils.sameType(e, missing3)))

      val source4 = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT, C4: STRUCT<C5: Int>>")
      val missing4 = StructType.fromDDL("c2 STRUCT<C4: STRUCT<c6: INT>>")
      assert(StructType.findMissingFields(source4, schema, resolver)
        .exists(e => DataTypeUtils.sameType(e, missing4)))
    }
  }

  test("SPARK-35285: ANSI interval types in schema") {
    val yearMonthInterval = "ymi INTERVAL YEAR TO MONTH"
    assert(fromDDL(yearMonthInterval).toDDL === yearMonthInterval)

    val dayTimeInterval = "dti INTERVAL DAY TO SECOND"
    assert(fromDDL(dayTimeInterval).toDDL === dayTimeInterval)
  }

  test("SPARK-35774: Prohibit the case start/end are the same with unit-to-unit interval syntax") {
    def checkIntervalDDL(start: Byte, end: Byte, fieldToString: Byte => String): Unit = {
      val startUnit = fieldToString(start)
      val endUnit = fieldToString(end)
      if (start < end) {
        fromDDL(s"x INTERVAL $startUnit TO $endUnit")
      } else {
        intercept[ParseException] {
          fromDDL(s"x INTERVAL $startUnit TO $endUnit")
        }
      }
    }

    for (start <- YM.yearMonthFields; end <- YM.yearMonthFields) {
      checkIntervalDDL(start, end, YM.fieldToString)
    }
    for (start <- DT.dayTimeFields; end <- DT.dayTimeFields) {
      checkIntervalDDL(start, end, DT.fieldToString)
    }
  }

  test("findNestedField") {
    val innerStruct = new StructType()
      .add("s11", "int")
      .add("s12", "int")
    val input = new StructType()
      .add("s1", innerStruct)
      .add("s2", new StructType().add("x", "int").add("X", "int"))
      .add("m1", MapType(IntegerType, IntegerType))
      .add("m2", MapType(
        new StructType().add("a", "int"),
        new StructType().add("b", "int")
      ))
      .add("m3", MapType(IntegerType, MapType(
        IntegerType,
        new StructType().add("ma", IntegerType))
      ))
      .add("a1", ArrayType(IntegerType))
      .add("a2", ArrayType(new StructType().add("c", "int")))
      .add("a3", ArrayType(ArrayType(new StructType().add("d", "int"))))

    def check(field: Seq[String], expect: Option[(Seq[String], StructField)]): Unit = {
      val res = input.findNestedField(field, resolver = caseInsensitiveResolution)
      assert(res == expect)
    }

    def caseSensitiveCheck(field: Seq[String], expect: Option[(Seq[String], StructField)]): Unit = {
      val res = input.findNestedField(field, resolver = caseSensitiveResolution)
      assert(res == expect)
    }

    def checkCollection(field: Seq[String], expect: Option[(Seq[String], StructField)]): Unit = {
      val res = input.findNestedField(field,
        includeCollections = true, resolver = caseInsensitiveResolution)
      assert(res == expect)
    }

    // struct type
    check(Seq("non_exist"), None)
    check(Seq("S1"), Some(Nil -> StructField("s1", innerStruct)))
    caseSensitiveCheck(Seq("S1"), None)
    check(Seq("s1", "S12"), Some(Seq("s1") -> StructField("s12", IntegerType)))
    caseSensitiveCheck(Seq("s1", "S12"), None)
    check(Seq("S1.non_exist"), None)
    var e = intercept[AnalysisException] {
      check(Seq("S1", "S12", "S123"), None)
    }
    checkError(
      exception = e,
      condition = "INVALID_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "`S1`.`S12`.`S123`",
        "path" -> "`s1`.`s12`"))

    // ambiguous name
    e = intercept[AnalysisException] {
      check(Seq("S2", "x"), None)
    }
    checkError(
      exception = e,
      condition = "AMBIGUOUS_COLUMN_OR_FIELD",
      parameters = Map("name" -> "`S2`.`x`", "n" -> "2"))
    caseSensitiveCheck(Seq("s2", "x"), Some(Seq("s2") -> StructField("x", IntegerType)))

    // simple map type
    e = intercept[AnalysisException] {
      check(Seq("m1", "key"), None)
    }
    checkError(
      exception = e,
      condition = "INVALID_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "`m1`.`key`",
        "path" -> "`m1`"))
    checkCollection(Seq("m1", "key"), Some(Seq("m1") -> StructField("key", IntegerType, false)))
    checkCollection(Seq("M1", "value"), Some(Seq("m1") -> StructField("value", IntegerType)))
    e = intercept[AnalysisException] {
      checkCollection(Seq("M1", "key", "name"), None)
    }
    checkError(
      exception = e,
      condition = "INVALID_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "`M1`.`key`.`name`",
        "path" -> "`m1`.`key`"))
    e = intercept[AnalysisException] {
      checkCollection(Seq("M1", "value", "name"), None)
    }
    checkError(
      exception = e,
      condition = "INVALID_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "`M1`.`value`.`name`",
        "path" -> "`m1`.`value`"))

    // map of struct
    checkCollection(Seq("M2", "key", "A"),
      Some(Seq("m2", "key") -> StructField("a", IntegerType)))
    checkCollection(Seq("M2", "key", "non_exist"), None)
    checkCollection(Seq("M2", "value", "b"),
      Some(Seq("m2", "value") -> StructField("b", IntegerType)))
    checkCollection(Seq("M2", "value", "non_exist"), None)
    e = intercept[AnalysisException] {
      checkCollection(Seq("m2", "key", "A", "name"), None)
    }
    checkError(
      exception = e,
      condition = "INVALID_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "`m2`.`key`.`A`.`name`",
        "path" -> "`m2`.`key`.`a`"))
    e = intercept[AnalysisException] {
      checkCollection(Seq("M2", "value", "b", "name"), None)
    }
    checkError(
      exception = e,
      condition = "INVALID_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "`M2`.`value`.`b`.`name`",
        "path" -> "`m2`.`value`.`b`"))
    // simple array type
    e = intercept[AnalysisException] {
      check(Seq("A1", "element"), None)
    }
    checkError(
      exception = e,
      condition = "INVALID_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "`A1`.`element`",
        "path" -> "`a1`"))
    checkCollection(Seq("A1", "element"), Some(Seq("a1") -> StructField("element", IntegerType)))
    e = intercept[AnalysisException] {
      checkCollection(Seq("A1", "element", "name"), None)
    }
    checkError(
      exception = e,
      condition = "INVALID_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "`A1`.`element`.`name`",
        "path" -> "`a1`.`element`"))

    // array of struct
    checkCollection(Seq("A2", "element", "C"),
      Some(Seq("a2", "element") -> StructField("c", IntegerType)))
    checkCollection(Seq("A2", "element", "non_exist"), None)
    e = intercept[AnalysisException] {
      checkCollection(Seq("a2", "element", "C", "name"), None)
    }
    checkError(
      exception = e,
      condition = "INVALID_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "`a2`.`element`.`C`.`name`",
        "path" -> "`a2`.`element`.`c`"))

    // nested maps
    checkCollection(Seq("M3", "value", "value", "MA"),
      Some(Seq("m3", "value", "value") -> StructField("ma", IntegerType)))
    checkCollection(Seq("M3", "value", "value", "non_exist"), None)
    e = intercept[AnalysisException] {
      checkCollection(Seq("M3", "value", "value", "MA", "name"), None)
    }
    checkError(
      exception = e,
      condition = "INVALID_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "`M3`.`value`.`value`.`MA`.`name`",
        "path" -> "`m3`.`value`.`value`.`ma`"))

    // nested arrays
    checkCollection(Seq("A3", "element", "element", "D"),
      Some(Seq("a3", "element", "element") -> StructField("d", IntegerType)))
    checkCollection(Seq("A3", "element", "element", "non_exist"), None)
    e = intercept[AnalysisException] {
      checkCollection(Seq("A3", "element", "element", "D", "name"), None)
    }
    checkError(
      exception = e,
      condition = "INVALID_FIELD_NAME",
      parameters = Map(
        "fieldName" -> "`A3`.`element`.`element`.`D`.`name`",
        "path" -> "`a3`.`element`.`element`.`d`")
    )
  }

  test("SPARK-36807: Merge ANSI interval types to a tightest common type") {
    Seq(
      (YM(YEAR), YM(YEAR)) -> YM(YEAR),
      (YM(YEAR), YM(MONTH)) -> YM(YEAR, MONTH),
      (YM(MONTH), YM(MONTH)) -> YM(MONTH),
      (YM(YEAR, MONTH), YM(YEAR)) -> YM(YEAR, MONTH),
      (YM(YEAR, MONTH), YM(YEAR, MONTH)) -> YM(YEAR, MONTH),
      (DT(DAY), DT(DAY)) -> DT(DAY),
      (DT(SECOND), DT(SECOND)) -> DT(SECOND),
      (DT(DAY), DT(SECOND)) -> DT(DAY, SECOND),
      (DT(HOUR, SECOND), DT(DAY, MINUTE)) -> DT(DAY, SECOND),
      (DT(HOUR, MINUTE), DT(DAY, SECOND)) -> DT(DAY, SECOND)
    ).foreach { case ((i1, i2), expected) =>
      val st1 = new StructType().add("interval", i1)
      val st2 = new StructType().add("interval", i2)
      val expectedStruct = new StructType().add("interval", expected)
      assert(st1.merge(st2) === expectedStruct)
      assert(st2.merge(st1) === expectedStruct)
    }
  }

  test("SPARK-37076: Implement StructType.toString explicitly for Scala 2.13") {
    val struct = StructType(StructField("a", IntegerType) :: Nil)
    assert(struct.toString() === "StructType(StructField(a,IntegerType,true))")
  }

  test("SPARK-37191: Merge DecimalType") {
    val source1 = StructType.fromDDL("c1 DECIMAL(12, 2)")
      .merge(StructType.fromDDL("c1 DECIMAL(12, 2)"))
    assert(source1 === StructType.fromDDL("c1 DECIMAL(12, 2)"))

    val source2 = StructType.fromDDL("c1 DECIMAL(12, 2)")
      .merge(StructType.fromDDL("c1 DECIMAL(17, 2)"))
    assert(source2 === StructType.fromDDL("c1 DECIMAL(17, 2)"))

    val source3 = StructType.fromDDL("c1 DECIMAL(17, 2)")
      .merge(StructType.fromDDL("c1 DECIMAL(12, 2)"))
    assert(source3 === StructType.fromDDL("c1 DECIMAL(17, 2)"))

    // Invalid merge cases:

    checkError(
      exception = intercept[SparkException] {
        StructType.fromDDL("c1 DECIMAL(10, 5)").merge(StructType.fromDDL("c1 DECIMAL(12, 2)"))
      },
      condition = "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE",
      parameters = Map("left" -> "\"DECIMAL(10,5)\"", "right" -> "\"DECIMAL(12,2)\"")
    )

    checkError(
      exception = intercept[SparkException] {
        StructType.fromDDL("c1 DECIMAL(12, 5)").merge(StructType.fromDDL("c1 DECIMAL(12, 2)"))
      },
      condition = "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE",
      parameters = Map("left" -> "\"DECIMAL(12,5)\"", "right" -> "\"DECIMAL(12,2)\"")
    )
  }

  test("SPARK-39143: Test parsing default column values out of struct types") {
    // Positive test: the StructType.defaultValues evaluation is successful.
    val source1 = StructType(Array(
      StructField("c1", LongType, true,
        new MetadataBuilder()
          .putString(ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY, "CAST(42 AS BIGINT)")
          .putString(
            ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY, "CAST(42 AS BIGINT)")
          .build()),
      StructField("c2", StringType, true,
        new MetadataBuilder()
          .putString(ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY, "'abc'")
          .putString(ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY, "'abc'")
          .build()),
      StructField("c3", BooleanType)))
    assert(ResolveDefaultColumns.existenceDefaultValues(source1).length == 3)
    assert(ResolveDefaultColumns.existenceDefaultValues(source1)(0) == 42)
    assert(ResolveDefaultColumns.existenceDefaultValues(source1)(1) == UTF8String.fromString("abc"))
    assert(ResolveDefaultColumns.existenceDefaultValues(source1)(2) == null)

    // Positive test: StructType.defaultValues works because the existence default value parses and
    // resolves successfully, then evaluates to a non-literal expression: this is constant-folded at
    // reference time.
    val source2 = StructType(
      Array(StructField("c1", IntegerType, true,
        new MetadataBuilder()
        .putString(ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY, "1 + 1")
          .putString(ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY, "1 + 1")
          .build())))
    assert(ResolveDefaultColumns.existenceDefaultValues(source2).length == 1)
    assert(ResolveDefaultColumns.existenceDefaultValues(source2)(0) == 2)

    // Negative test: StructType.defaultValues fails because the existence default value fails to
    // parse.
    val source3 = StructType(Array(
      StructField("c1", IntegerType, true,
        new MetadataBuilder()
          .putString(ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY, "invalid")
          .putString(ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY, "invalid")
          .build())))

    checkError(
      exception = intercept[AnalysisException]{
        ResolveDefaultColumns.existenceDefaultValues(source3)
      },
      condition = "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION",
      parameters = Map("statement" -> "", "colName" -> "`c1`", "defaultValue" -> "invalid"))

    // Negative test: StructType.defaultValues fails because the existence default value fails to
    // resolve.
    val source4 = StructType(Array(
      StructField("c1", IntegerType, true,
        new MetadataBuilder()
          .putString(
            ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY,
            "(SELECT 'abc' FROM missingtable)")
          .putString(
            ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY,
            "(SELECT 'abc' FROM missingtable)")
          .build())))

    checkError(
      exception = intercept[AnalysisException]{
        ResolveDefaultColumns.existenceDefaultValues(source4)
      },
      condition = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
      parameters = Map("statement" -> "",
        "colName" -> "`c1`",
        "defaultValue" -> "(SELECT 'abc' FROM missingtable)"))
  }

  test("SPARK-46629: Test STRUCT DDL with NOT NULL round trip") {
    val struct = StructType(
      Seq(
        StructField(
          "b",
          StructType(
            Seq(StructField("c", StringType, nullable = false).withComment("struct comment"))),
          nullable = false),
        StructField("b", StringType, nullable = false),
        StructField("c", StringType).withComment("nullable comment")))
    assert(
      struct.toDDL == "b STRUCT<c: STRING NOT NULL COMMENT 'struct comment'> NOT NULL," +
        "b STRING NOT NULL,c STRING COMMENT 'nullable comment'")
    assert(fromDDL(struct.toDDL) === struct)
  }

  test("simple struct with collations to json") {
    val simpleStruct = StructType(
      StructField("c1", StringType(UNICODE_COLLATION)) :: Nil)

    val expectedJson =
      s"""
         |{
         |  "type": "struct",
         |  "fields": [
         |    {
         |      "name": "c1",
         |      "type": "string",
         |      "nullable": true,
         |      "metadata": {
         |        "${DataType.COLLATIONS_METADATA_KEY}": {
         |          "c1": "icu.$UNICODE_COLLATION"
         |        }
         |      }
         |    }
         |  ]
         |}
         |""".stripMargin

    assert(mapper.readTree(simpleStruct.json) == mapper.readTree(expectedJson))
  }

  test("nested struct with collations to json") {
    val nestedStruct = StructType(
      StructField("nested", StructType(
        StructField("c1", StringType(UTF8_LCASE_COLLATION)) :: Nil)) :: Nil)

    val expectedJson =
      s"""
         |{
         |  "type": "struct",
         |  "fields": [
         |    {
         |      "name": "nested",
         |      "type": {
         |        "type": "struct",
         |        "fields": [
         |          {
         |            "name": "c1",
         |            "type": "string",
         |            "nullable": true,
         |            "metadata": {
         |              "${DataType.COLLATIONS_METADATA_KEY}": {
         |                "c1": "spark.$UTF8_LCASE_COLLATION"
         |              }
         |            }
         |          }
         |        ]
         |      },
         |      "nullable": true,
         |      "metadata": {}
         |    }
         |  ]
         |}
         |""".stripMargin

    assert(mapper.readTree(nestedStruct.json) == mapper.readTree(expectedJson))
  }

  test("array with collations in schema to json") {
    val arrayInSchema = StructType(
      StructField("arrayField", ArrayType(StringType(UNICODE_COLLATION))) :: Nil)

    val expectedJson =
      s"""
         |{
         |  "type": "struct",
         |  "fields": [
         |    {
         |      "name": "arrayField",
         |      "type": {
         |        "type": "array",
         |        "elementType": "string",
         |        "containsNull": true
         |      },
         |      "nullable": true,
         |      "metadata": {
         |        "${DataType.COLLATIONS_METADATA_KEY}": {
         |          "arrayField.element": "icu.$UNICODE_COLLATION"
         |        }
         |      }
         |    }
         |  ]
         |}
         |""".stripMargin

    assert(mapper.readTree(arrayInSchema.json) == mapper.readTree(expectedJson))
  }

  test("map with collations in schema to json") {
    val arrayInSchema = StructType(
      StructField("mapField",
        MapType(StringType(UNICODE_COLLATION), StringType(UNICODE_COLLATION))) :: Nil)

    val expectedJson =
      s"""
         |{
         |  "type": "struct",
         |  "fields": [
         |    {
         |      "name": "mapField",
         |      "type": {
         |        "type": "map",
         |        "keyType": "string",
         |        "valueType": "string",
         |        "valueContainsNull": true
         |      },
         |      "nullable": true,
         |      "metadata": {
         |        "${DataType.COLLATIONS_METADATA_KEY}": {
         |          "mapField.key": "icu.$UNICODE_COLLATION",
         |          "mapField.value": "icu.$UNICODE_COLLATION"
         |        }
         |      }
         |    }
         |  ]
         |}
         |""".stripMargin

    assert(mapper.readTree(arrayInSchema.json) == mapper.readTree(expectedJson))
  }

  test("nested array with collations in map to json" ) {
    val mapWithNestedArray = StructType(
      StructField("column", ArrayType(MapType(
        StringType(UNICODE_COLLATION),
        ArrayType(ArrayType(ArrayType(StringType(UNICODE_COLLATION))))))) :: Nil)

    val expectedJson =
      s"""
         |{
         |  "type": "struct",
         |  "fields": [
         |    {
         |      "name": "column",
         |      "type": {
         |        "type": "array",
         |        "elementType": {
         |          "type": "map",
         |          "keyType": "string",
         |          "valueType": {
         |            "type": "array",
         |            "elementType": {
         |              "type": "array",
         |              "elementType": {
         |                "type": "array",
         |                "elementType": "string",
         |                "containsNull": true
         |              },
         |              "containsNull": true
         |            },
         |            "containsNull": true
         |          },
         |          "valueContainsNull": true
         |        },
         |        "containsNull": true
         |      },
         |      "nullable": true,
         |      "metadata": {
         |        "${DataType.COLLATIONS_METADATA_KEY}": {
         |          "column.element.key": "icu.$UNICODE_COLLATION",
         |          "column.element.value.element.element.element": "icu.$UNICODE_COLLATION"
         |        }
         |      }
         |    }
         |  ]
         |}
         |""".stripMargin

    assert(
      mapper.readTree(mapWithNestedArray.json) == mapper.readTree(expectedJson))
  }

  test("SPARK-51208: ColumnDefinition.toV1Column should preserve EXISTS_DEFAULT resolution") {

    def validateConvertedDefaults(
        colName: String,
        dataType: DataType,
        defaultSQL: String,
        expectedExists: String): Unit = {
      val existsDefault = ResolveDefaultColumns.analyze(colName, dataType, defaultSQL, "")
      val col =
        ColumnDefinition(colName, dataType, true, None,
          Some(DefaultValueExpression(existsDefault, defaultSQL)))

      val structField = col.toV1Column
      assert(
        structField.metadata.getString(
          ResolveDefaultColumnsUtils.CURRENT_DEFAULT_COLUMN_METADATA_KEY) ==
          defaultSQL)
      val existsSQL = structField.metadata.getString(
          ResolveDefaultColumnsUtils.EXISTS_DEFAULT_COLUMN_METADATA_KEY)
      assert(existsSQL == expectedExists)
      assert(Literal.fromSQL(existsSQL).resolved)
    }

    validateConvertedDefaults("c1", StringType, "current_catalog()", "'spark_catalog'")
    validateConvertedDefaults("c2", VariantType, "parse_json('1')", "PARSE_JSON('1')")
    validateConvertedDefaults("c3", VariantType, "parse_json('{\"k\": \"v\"}')",
      "PARSE_JSON('{\"k\":\"v\"}')")
    validateConvertedDefaults("c4", VariantType, "parse_json(null)", "CAST(NULL AS VARIANT)")
    validateConvertedDefaults("c5", IntegerType, "1 + 1", "2")
  }

  test("SPARK-51119: Add fallback to process unresolved EXISTS_DEFAULT") {
    val source = StructType(
      Array(
        StructField("c0", VariantType, true,
          new MetadataBuilder()
            .putString(ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY,
              "parse_json(null)")
            .putString(ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY,
              "parse_json(null)")
            .build()),
        StructField("c1", StringType, true,
          new MetadataBuilder()
            .putString(ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY,
              "current_catalog()")
            .putString(ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY,
              "current_catalog()")
            .build()),
        StructField("c2", StringType, true,
          new MetadataBuilder()
            .putString(ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY,
              "CAST(CURRENT_TIMESTAMP AS BIGINT)")
            .putString(ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY,
              "CAST(CURRENT_TIMESTAMP AS BIGINT)")
            .build())))
    val res = ResolveDefaultColumns.existenceDefaultValues(source)
    assert(res(0) == null)
    assert(res(1) == UTF8String.fromString("spark_catalog"))

    val res2Wrapper = new LongWrapper
    assert(res(2).asInstanceOf[UTF8String].toLong(res2Wrapper))
    assert(res2Wrapper.value > 0)
  }
}
