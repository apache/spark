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
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
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

  test("SPARK-35290: Struct merging case insensitive") {
    val schema1 = StructType.fromDDL("a1 INT, a2 STRING, nested STRUCT<b1: INT, b2: STRING>")
    val schema2 = StructType.fromDDL("A2 STRING, a3 DOUBLE, nested STRUCT<B2: STRING, b3: DOUBLE>")
    val resolver = SQLConf.get.resolver

    assert(schema1.merge(schema2, resolver).sameType(StructType.fromDDL(
      "a1 INT, a2 STRING, nested STRUCT<b1: INT, b2: STRING, b3: DOUBLE>, a3 DOUBLE"
    )))

    assert(schema2.merge(schema1, resolver).sameType(StructType.fromDDL(
      "a2 STRING, a3 DOUBLE, nested STRUCT<b2: STRING, b3: DOUBLE, b1: INT>, a1 INT"
    )))
  }

  test("SPARK-35290: Struct merging case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val schema1 = StructType.fromDDL("a1 INT, a2 STRING, nested STRUCT<b1: INT, b2: STRING>")
      val schema2 = StructType.fromDDL(
        "A2 STRING, a3 DOUBLE, nested STRUCT<B2: STRING, b3: DOUBLE>")
      val resolver = SQLConf.get.resolver

      assert(schema1.merge(schema2, resolver).sameType(StructType.fromDDL(
        "a1 INT, a2 STRING, nested STRUCT<b1: INT, b2: STRING, B2: STRING, b3: DOUBLE>, " +
          "A2 STRING, a3 DOUBLE"
      )))

      assert(schema2.merge(schema1, resolver).sameType(StructType.fromDDL(
        "A2 STRING, a3 DOUBLE, nested STRUCT<B2: STRING, b3: DOUBLE, b1: INT, b2: STRING>, " +
          "a1 INT, a2 STRING"
      )))
    }
  }

  test("SPARK-35285: ANSI interval types in schema") {
    val yearMonthInterval = "`ymi` INTERVAL YEAR TO MONTH"
    assert(fromDDL(yearMonthInterval).toDDL === yearMonthInterval)

    val dayTimeInterval = "`dti` INTERVAL DAY TO SECOND"
    assert(fromDDL(dayTimeInterval).toDDL === dayTimeInterval)
  }
}
