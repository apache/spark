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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{caseInsensitiveResolution, caseSensitiveResolution}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DayTimeIntervalType => DT}
import org.apache.spark.sql.types.{YearMonthIntervalType => YM}
import org.apache.spark.sql.types.StructType.fromDDL

class StructTypeSuite extends SparkFunSuite with SQLHelper {

  private val s = StructType.fromDDL("a INT, b STRING")

  test("lookup a single missing field should output existing fields") {
    val e = intercept[IllegalArgumentException](s("c")).getMessage
    assert(e.contains("Available: a, b"))
  }

  test("lookup a set of missing fields should output existing fields") {
    val e = intercept[IllegalArgumentException](s(Set("a", "c"))).getMessage
    assert(e.contains("Available: a, b"))
  }

  test("lookup fieldIndex for missing field should output existing fields") {
    val e = intercept[IllegalArgumentException](s.fieldIndex("c")).getMessage
    assert(e.contains("Available: a, b"))
  }

  test("SPARK-24849: toDDL - simple struct") {
    val struct = StructType(Seq(StructField("a", IntegerType)))

    assert(struct.toDDL == "`a` INT")
  }

  test("SPARK-24849: round trip toDDL - fromDDL") {
    val struct = new StructType().add("a", IntegerType).add("b", StringType)

    assert(fromDDL(struct.toDDL) === struct)
  }

  test("SPARK-24849: round trip fromDDL - toDDL") {
    val struct = "`a` MAP<INT, STRING>,`b` INT"

    assert(fromDDL(struct).toDDL === struct)
  }

  test("SPARK-24849: toDDL must take into account case of fields.") {
    val struct = new StructType()
      .add("metaData", new StructType().add("eventId", StringType))

    assert(struct.toDDL == "`metaData` STRUCT<`eventId`: STRING>")
  }

  test("SPARK-24849: toDDL should output field's comment") {
    val struct = StructType(Seq(
      StructField("b", BooleanType).withComment("Field's comment")))

    assert(struct.toDDL == """`b` BOOLEAN COMMENT 'Field\'s comment'""")
  }

  private val nestedStruct = new StructType()
    .add(StructField("a", new StructType()
      .add(StructField("b", new StructType()
        .add(StructField("c", StringType
        ).withComment("Deep Nested comment"))
      ).withComment("Nested comment"))
    ).withComment("comment"))

  test("SPARK-33846: toDDL should output nested field's comment") {
    val ddl = "`a` STRUCT<`b`: STRUCT<`c`: STRING COMMENT 'Deep Nested comment'> " +
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
    val interval = "`a` INTERVAL"
    assert(fromDDL(interval).toDDL === interval)
  }

  test("find missing (nested) fields") {
    val schema = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT, c4: STRUCT<c5: INT, c6: INT>>")
    val resolver = SQLConf.get.resolver

    val source1 = StructType.fromDDL("c1 INT")
    val missing1 = StructType.fromDDL("c2 STRUCT<c3: INT, c4: STRUCT<c5: INT, c6: INT>>")
    assert(StructType.findMissingFields(source1, schema, resolver)
      .exists(_.sameType(missing1)))

    val source2 = StructType.fromDDL("c1 INT, c3 STRING")
    val missing2 = StructType.fromDDL("c2 STRUCT<c3: INT, c4: STRUCT<c5: INT, c6: INT>>")
    assert(StructType.findMissingFields(source2, schema, resolver)
      .exists(_.sameType(missing2)))

    val source3 = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT>")
    val missing3 = StructType.fromDDL("c2 STRUCT<c4: STRUCT<c5: INT, c6: INT>>")
    assert(StructType.findMissingFields(source3, schema, resolver)
      .exists(_.sameType(missing3)))

    val source4 = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT, c4: STRUCT<c6: INT>>")
    val missing4 = StructType.fromDDL("c2 STRUCT<c4: STRUCT<c5: INT>>")
    assert(StructType.findMissingFields(source4, schema, resolver)
      .exists(_.sameType(missing4)))
  }

  test("find missing (nested) fields: array and map") {
    val resolver = SQLConf.get.resolver

    val schemaWithArray = StructType.fromDDL("c1 INT, c2 ARRAY<STRUCT<c3: INT, c4: LONG>>")
    val source5 = StructType.fromDDL("c1 INT")
    val missing5 = StructType.fromDDL("c2 ARRAY<STRUCT<c3: INT, c4: LONG>>")
    assert(
      StructType.findMissingFields(source5, schemaWithArray, resolver)
        .exists(_.sameType(missing5)))

    val schemaWithMap1 = StructType.fromDDL(
      "c1 INT, c2 MAP<STRUCT<c3: INT, c4: LONG>, STRING>, c3 LONG")
    val source6 = StructType.fromDDL("c1 INT, c3 LONG")
    val missing6 = StructType.fromDDL("c2 MAP<STRUCT<c3: INT, c4: LONG>, STRING>")
    assert(
      StructType.findMissingFields(source6, schemaWithMap1, resolver)
        .exists(_.sameType(missing6)))

    val schemaWithMap2 = StructType.fromDDL(
      "c1 INT, c2 MAP<STRING, STRUCT<c3: INT, c4: LONG>>, c3 STRING")
    val source7 = StructType.fromDDL("c1 INT, c3 STRING")
    val missing7 = StructType.fromDDL("c2 MAP<STRING, STRUCT<c3: INT, c4: LONG>>")
    assert(
      StructType.findMissingFields(source7, schemaWithMap2, resolver)
        .exists(_.sameType(missing7)))

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
        .exists(_.sameType(missing1)))

      val source2 = StructType.fromDDL("c2 LONG")
      val missing2 = StructType.fromDDL("c1 INT")
      assert(StructType.findMissingFields(source2, schema, resolver)
        .exists(_.sameType(missing2)))

      val source3 = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT, C4: STRUCT<c5: INT>>")
      val missing3 = StructType.fromDDL("c2 STRUCT<C4: STRUCT<C5: INT, c6: INT>>")
      assert(StructType.findMissingFields(source3, schema, resolver)
        .exists(_.sameType(missing3)))

      val source4 = StructType.fromDDL("c1 INT, c2 STRUCT<c3: INT, C4: STRUCT<C5: Int>>")
      val missing4 = StructType.fromDDL("c2 STRUCT<C4: STRUCT<c6: INT>>")
      assert(StructType.findMissingFields(source4, schema, resolver)
        .exists(_.sameType(missing4)))
    }
  }

  test("SPARK-35285: ANSI interval types in schema") {
    val yearMonthInterval = "`ymi` INTERVAL YEAR TO MONTH"
    assert(fromDDL(yearMonthInterval).toDDL === yearMonthInterval)

    val dayTimeInterval = "`dti` INTERVAL DAY TO SECOND"
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
      .add("a1", ArrayType(IntegerType))
      .add("a2", ArrayType(new StructType().add("c", "int")))

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
    assert(e.getMessage.contains("Field name S1.S12.S123 is invalid: s1.s12 is not a struct"))

    // ambiguous name
    e = intercept[AnalysisException] {
      check(Seq("S2", "x"), None)
    }
    assert(e.getMessage.contains(
      "Field name S2.x is ambiguous and has 2 matching fields in the struct"))
    caseSensitiveCheck(Seq("s2", "x"), Some(Seq("s2") -> StructField("x", IntegerType)))

    // simple map type
    e = intercept[AnalysisException] {
      check(Seq("m1", "key"), None)
    }
    assert(e.getMessage.contains("Field name m1.key is invalid: m1 is not a struct"))
    checkCollection(Seq("m1", "key"), Some(Seq("m1") -> StructField("key", IntegerType, false)))
    checkCollection(Seq("M1", "value"), Some(Seq("m1") -> StructField("value", IntegerType)))
    e = intercept[AnalysisException] {
      checkCollection(Seq("M1", "key", "name"), None)
    }
    assert(e.getMessage.contains("Field name M1.key.name is invalid: m1.key is not a struct"))
    e = intercept[AnalysisException] {
      checkCollection(Seq("M1", "value", "name"), None)
    }
    assert(e.getMessage.contains("Field name M1.value.name is invalid: m1.value is not a struct"))

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
    assert(e.getMessage.contains("Field name m2.key.A.name is invalid: m2.key.a is not a struct"))
    e = intercept[AnalysisException] {
      checkCollection(Seq("M2", "value", "b", "name"), None)
    }
    assert(e.getMessage.contains(
      "Field name M2.value.b.name is invalid: m2.value.b is not a struct"))

    // simple array type
    e = intercept[AnalysisException] {
      check(Seq("A1", "element"), None)
    }
    assert(e.getMessage.contains("Field name A1.element is invalid: a1 is not a struct"))
    checkCollection(Seq("A1", "element"), Some(Seq("a1") -> StructField("element", IntegerType)))
    e = intercept[AnalysisException] {
      checkCollection(Seq("A1", "element", "name"), None)
    }
    assert(e.getMessage.contains(
      "Field name A1.element.name is invalid: a1.element is not a struct"))

    // array of struct
    checkCollection(Seq("A2", "element", "C"),
      Some(Seq("a2", "element") -> StructField("c", IntegerType)))
    checkCollection(Seq("A2", "element", "non_exist"), None)
    e = intercept[AnalysisException] {
      checkCollection(Seq("a2", "element", "C", "name"), None)
    }
    assert(e.getMessage.contains(
      "Field name a2.element.C.name is invalid: a2.element.c is not a struct"))
  }
}
