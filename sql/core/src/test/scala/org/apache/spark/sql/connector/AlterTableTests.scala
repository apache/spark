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

package org.apache.spark.sql.connector

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.CatalogV2Util.withDefaultOwnership
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

trait AlterTableTests extends SharedSparkSession {

  protected def getTableMetadata(tableName: String): Table

  protected val catalogAndNamespace: String

  protected val v2Format: String

  private def fullTableName(tableName: String): String = {
    if (catalogAndNamespace.isEmpty) {
      s"default.$tableName"
    } else {
      s"${catalogAndNamespace}table_name"
    }
  }

  test("AlterTable: table does not exist") {
    val t2 = s"${catalogAndNamespace}fake_table"
    withTable(t2) {
      sql(s"CREATE TABLE $t2 (id int) USING $v2Format")
      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE ${catalogAndNamespace}table_name DROP COLUMN id")
      }

      assert(exc.getMessage.contains(s"${catalogAndNamespace}table_name"))
      assert(exc.getMessage.contains("Table not found"))
    }
  }

  test("AlterTable: change rejected by implementation") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[SparkException] {
        sql(s"ALTER TABLE $t DROP COLUMN id")
      }

      assert(exc.getMessage.contains("Unsupported table change"))
      assert(exc.getMessage.contains("Cannot drop all fields")) // from the implementation

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType().add("id", IntegerType))
    }
  }

  test("AlterTable: add top-level column") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN data string")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType().add("id", IntegerType).add("data", StringType))
    }
  }

  test("AlterTable: add column with NOT NULL") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN data string NOT NULL")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType, nullable = false))))
    }
  }

  test("AlterTable: add column with comment") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN data string COMMENT 'doc'")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType).withComment("doc"))))
    }
  }

  test("AlterTable: add column with interval type") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      val e1 =
        intercept[AnalysisException](sql(s"ALTER TABLE $t ADD COLUMN data interval"))
      assert(e1.getMessage.contains("Cannot use interval type in the table schema."))
      val e2 =
        intercept[AnalysisException](sql(s"ALTER TABLE $t ADD COLUMN point.z interval"))
      assert(e2.getMessage.contains("Cannot use interval type in the table schema."))
    }
  }

  test("AlterTable: add column with position") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (point struct<x: int>) USING $v2Format")

      sql(s"ALTER TABLE $t ADD COLUMN a string FIRST")
      val tableName = fullTableName(t)
      assert(getTableMetadata(tableName).schema == new StructType()
        .add("a", StringType)
        .add("point", new StructType().add("x", IntegerType)))

      sql(s"ALTER TABLE $t ADD COLUMN b string AFTER point")
      assert(getTableMetadata(tableName).schema == new StructType()
        .add("a", StringType)
        .add("point", new StructType().add("x", IntegerType))
        .add("b", StringType))

      val e1 = intercept[AnalysisException](
        sql(s"ALTER TABLE $t ADD COLUMN c string AFTER non_exist"))
      assert(e1.getMessage().contains("Couldn't find the reference column"))

      sql(s"ALTER TABLE $t ADD COLUMN point.y int FIRST")
      assert(getTableMetadata(tableName).schema == new StructType()
        .add("a", StringType)
        .add("point", new StructType()
          .add("y", IntegerType)
          .add("x", IntegerType))
        .add("b", StringType))

      sql(s"ALTER TABLE $t ADD COLUMN point.z int AFTER x")
      assert(getTableMetadata(tableName).schema == new StructType()
        .add("a", StringType)
        .add("point", new StructType()
          .add("y", IntegerType)
          .add("x", IntegerType)
          .add("z", IntegerType))
        .add("b", StringType))

      val e2 = intercept[AnalysisException](
        sql(s"ALTER TABLE $t ADD COLUMN point.x2 int AFTER non_exist"))
      assert(e2.getMessage().contains("Couldn't find the reference column"))
    }
  }

  test("SPARK-30814: add column with position referencing new columns being added") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (a string, b int, point struct<x: double, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMNS (x int AFTER a, y int AFTER x, z int AFTER y)")

      val tableName = fullTableName(t)
      assert(getTableMetadata(tableName).schema === new StructType()
        .add("a", StringType)
        .add("x", IntegerType)
        .add("y", IntegerType)
        .add("z", IntegerType)
        .add("b", IntegerType)
        .add("point", new StructType()
          .add("x", DoubleType)
          .add("y", DoubleType)))

      sql(s"ALTER TABLE $t ADD COLUMNS (point.z double AFTER x, point.zz double AFTER z)")
      assert(getTableMetadata(tableName).schema === new StructType()
        .add("a", StringType)
        .add("x", IntegerType)
        .add("y", IntegerType)
        .add("z", IntegerType)
        .add("b", IntegerType)
        .add("point", new StructType()
          .add("x", DoubleType)
          .add("z", DoubleType)
          .add("zz", DoubleType)
          .add("y", DoubleType)))

      // The new column being referenced should come before being referenced.
      val e = intercept[AnalysisException](
        sql(s"ALTER TABLE $t ADD COLUMNS (yy int AFTER xx, xx int)"))
      assert(e.getMessage().contains("Couldn't find the reference column for AFTER xx at root"))
    }
  }

  test("AlterTable: add multiple columns") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMNS data string COMMENT 'doc', ts timestamp")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType).withComment("doc"),
        StructField("ts", TimestampType))))
    }
  }

  test("AlterTable: add nested column") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN point.z double")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType),
          StructField("z", DoubleType)))))
    }
  }

  test("AlterTable: add nested column to map key") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<struct<x: double, y: double>, bigint>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN points.key.z double")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType),
          StructField("z", DoubleType))), LongType)))
    }
  }

  test("AlterTable: add nested column to map value") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: double, y: double>>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN points.value.z double")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StringType, StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType),
          StructField("z", DoubleType))))))
    }
  }

  test("AlterTable: add nested column to array element") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN points.element.z double")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType),
          StructField("z", DoubleType))))))
    }
  }

  test("AlterTable: add complex column") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN points array<struct<x: double, y: double>>")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType))))))
    }
  }

  test("AlterTable: add nested column with comment") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN points.element.z double COMMENT 'doc'")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType),
          StructField("z", DoubleType).withComment("doc"))))))
    }
  }

  test("AlterTable: add nested column parent must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ADD COLUMN point.z double")
      }

      assert(exc.getMessage.contains("Missing field point"))
    }
  }

  test("AlterTable: add column - new column should not exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(
        s"""CREATE TABLE $t (
           |id int,
           |point struct<x: double, y: double>,
           |arr array<struct<x: double, y: double>>,
           |mk map<struct<x: double, y: double>, string>,
           |mv map<string, struct<x: double, y: double>>
           |)
           |USING $v2Format""".stripMargin)

      Seq("id", "point.x", "arr.element.x", "mk.key.x", "mv.value.x").foreach { field =>

        val e = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t ADD COLUMNS $field double")
        }
        assert(e.getMessage.contains("add"))
        assert(e.getMessage.contains(s"$field already exists"))
      }
    }
  }

  test("SPARK-36372: Adding duplicate columns should not be allowed") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ADD COLUMNS (data string, data1 string, data string)")
      }
      assert(e.message.contains("Found duplicate column(s) in the user specified columns: `data`"))
    }
  }

  test("SPARK-36372: Adding duplicate nested columns should not be allowed") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ADD COLUMNS (point.z double, point.z double, point.xx double)")
      }
      assert(e.message.contains(
        "Found duplicate column(s) in the user specified columns: `point.z`"))
    }
  }

  test("AlterTable: update column type int -> long") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN id TYPE bigint")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType().add("id", LongType))
    }
  }

  test("AlterTable: update column type to interval") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      (DataTypeTestUtils.dayTimeIntervalTypes ++ DataTypeTestUtils.yearMonthIntervalTypes)
        .foreach {
          case d: DataType => d.typeName
            val e = intercept[AnalysisException](
              sql(s"ALTER TABLE $t ALTER COLUMN id TYPE ${d.typeName}"))
            assert(e.getMessage.contains("id to interval type"))
        }
    }
  }

  test("AlterTable: SET/DROP NOT NULL") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint NOT NULL) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN id SET NOT NULL")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)
      assert(table.name === tableName)
      assert(table.schema === new StructType().add("id", LongType, nullable = false))

      sql(s"ALTER TABLE $t ALTER COLUMN id DROP NOT NULL")
      val table2 = getTableMetadata(tableName)
      assert(table2.name === tableName)
      assert(table2.schema === new StructType().add("id", LongType))

      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN id SET NOT NULL")
      }
      assert(e.message.contains("Cannot change nullable column to non-nullable"))
    }
  }

  test("AlterTable: update nested type float -> double") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: float, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN point.x TYPE double")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)
      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType)))))
    }
  }

  test("AlterTable: update column with struct type fails") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN point TYPE struct<x: double, y: double, z: double>")
      }

      assert(exc.getMessage.contains("point"))
      assert(exc.getMessage.contains("update a struct by updating its fields"))

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType)))))
    }
  }

  test("AlterTable: update column with array type fails") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<int>) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN points TYPE array<long>")
      }

      assert(exc.getMessage.contains("update the element by updating points.element"))

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(IntegerType)))
    }
  }

  test("AlterTable: update column array element type") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<int>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.element TYPE long")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(LongType)))
    }
  }

  test("AlterTable: update column with map type fails") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, m map<string, int>) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN m TYPE map<string, long>")
      }

      assert(exc.getMessage.contains("update a map by updating m.key or m.value"))

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("m", MapType(StringType, IntegerType)))
    }
  }

  test("AlterTable: update column map value type") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, m map<string, int>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN m.value TYPE long")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("m", MapType(StringType, LongType)))
    }
  }

  test("AlterTable: update nested type in map key") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<struct<x: float, y: double>, bigint>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.key.x TYPE double")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType))), LongType)))
    }
  }

  test("AlterTable: update nested type in map value") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: float, y: double>>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.value.x TYPE double")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StringType, StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType))))))
    }
  }

  test("AlterTable: update nested type in array") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: float, y: double>>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.element.x TYPE double")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType))))))
    }
  }

  test("AlterTable: update column must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN data TYPE string")
      }

      assert(exc.getMessage.contains("Missing field data"))
    }
  }

  test("AlterTable: nested update column must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN point.x TYPE double")
      }

      assert(exc.getMessage.contains("Missing field point.x"))
    }
  }

  test("AlterTable: update column type must be compatible") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN id TYPE boolean")
      }

      assert(exc.getMessage.contains("id"))
      assert(exc.getMessage.contains("int cannot be cast to boolean"))
    }
  }

  test("AlterTable: update column comment") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN id COMMENT 'doc'")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === StructType(Seq(StructField("id", IntegerType).withComment("doc"))))
    }
  }

  test("AlterTable: update column position") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (a int, b int, point struct<x: int, y: int, z: int>) USING $v2Format")

      sql(s"ALTER TABLE $t ALTER COLUMN b FIRST")
      val tableName = fullTableName(t)
      assert(getTableMetadata(tableName).schema == new StructType()
        .add("b", IntegerType)
        .add("a", IntegerType)
        .add("point", new StructType()
          .add("x", IntegerType)
          .add("y", IntegerType)
          .add("z", IntegerType)))

      sql(s"ALTER TABLE $t ALTER COLUMN b AFTER point")
      assert(getTableMetadata(tableName).schema == new StructType()
        .add("a", IntegerType)
        .add("point", new StructType()
          .add("x", IntegerType)
          .add("y", IntegerType)
          .add("z", IntegerType))
        .add("b", IntegerType))

      val e1 = intercept[AnalysisException](
        sql(s"ALTER TABLE $t ALTER COLUMN b AFTER non_exist"))
      assert(e1.getMessage.contains("Missing field non_exist"))

      sql(s"ALTER TABLE $t ALTER COLUMN point.y FIRST")
      assert(getTableMetadata(tableName).schema == new StructType()
        .add("a", IntegerType)
        .add("point", new StructType()
          .add("y", IntegerType)
          .add("x", IntegerType)
          .add("z", IntegerType))
        .add("b", IntegerType))

      sql(s"ALTER TABLE $t ALTER COLUMN point.y AFTER z")
      assert(getTableMetadata(tableName).schema == new StructType()
        .add("a", IntegerType)
        .add("point", new StructType()
          .add("x", IntegerType)
          .add("z", IntegerType)
          .add("y", IntegerType))
        .add("b", IntegerType))

      val e2 = intercept[AnalysisException](
        sql(s"ALTER TABLE $t ALTER COLUMN point.y AFTER non_exist"))
      assert(e2.getMessage.contains("Missing field point.non_exist"))

      // `AlterTable.resolved` checks column existence.
      intercept[AnalysisException](
        sql(s"ALTER TABLE $t ALTER COLUMN a.y AFTER x"))
    }
  }

  test("AlterTable: update nested column comment") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN point.y COMMENT 'doc'")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType).withComment("doc")))))
    }
  }

  test("AlterTable: update nested column comment in map key") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<struct<x: double, y: double>, bigint>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.key.y COMMENT 'doc'")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType).withComment("doc"))), LongType)))
    }
  }

  test("AlterTable: update nested column comment in map value") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: double, y: double>>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.value.y COMMENT 'doc'")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StringType, StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType).withComment("doc"))))))
    }
  }

  test("AlterTable: update nested column comment in array") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.element.y COMMENT 'doc'")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType).withComment("doc"))))))
    }
  }

  test("AlterTable: comment update column must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN data COMMENT 'doc'")
      }

      assert(exc.getMessage.contains("Missing field data"))
    }
  }

  test("AlterTable: nested comment update column must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN point.x COMMENT 'doc'")
      }

      assert(exc.getMessage.contains("Missing field point.x"))
    }
  }

  test("AlterTable: rename column") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t RENAME COLUMN id TO user_id")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType().add("user_id", IntegerType))
    }
  }

  test("AlterTable: rename nested column") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t RENAME COLUMN point.y TO t")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", StructType(Seq(
          StructField("x", DoubleType),
          StructField("t", DoubleType)))))
    }
  }

  test("AlterTable: rename nested column in map key") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point map<struct<x: double, y: double>, bigint>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t RENAME COLUMN point.key.y TO t")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", MapType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("t", DoubleType))), LongType)))
    }
  }

  test("AlterTable: rename nested column in map value") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: double, y: double>>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t RENAME COLUMN points.value.y TO t")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StringType, StructType(Seq(
          StructField("x", DoubleType),
          StructField("t", DoubleType))))))
    }
  }

  test("AlterTable: rename nested column in array element") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING $v2Format")
      sql(s"ALTER TABLE $t RENAME COLUMN points.element.y TO t")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("t", DoubleType))))))
    }
  }

  test("AlterTable: rename column must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t RENAME COLUMN data TO some_string")
      }

      assert(exc.getMessage.contains("Missing field data"))
    }
  }

  test("AlterTable: nested rename column must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t RENAME COLUMN point.x TO z")
      }

      assert(exc.getMessage.contains("Missing field point.x"))
    }
  }

  test("AlterTable: rename column - new name should not exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(
        s"""CREATE TABLE $t (
           |id int,
           |user_id int,
           |point struct<x: double, y: double>,
           |arr array<struct<x: double, y: double>>,
           |mk map<struct<x: double, y: double>, string>,
           |mv map<string, struct<x: double, y: double>>
           |)
           |USING $v2Format""".stripMargin)

      Seq(
        "id" -> "user_id",
        "point.x" -> "y",
        "arr.element.x" -> "y",
        "mk.key.x" -> "y",
        "mv.value.x" -> "y").foreach { case (field, newName) =>

        val e = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t RENAME COLUMN $field TO $newName")
        }
        assert(e.getMessage.contains("rename"))
        assert(e.getMessage.contains((field.split("\\.").init :+ newName).mkString(".")))
        assert(e.getMessage.contains("already exists"))
      }
    }
  }

  test("AlterTable: drop column") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, data string) USING $v2Format")
      sql(s"ALTER TABLE $t DROP COLUMN data")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType().add("id", IntegerType))
    }
  }

  test("AlterTable: drop nested column") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double, t: double>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t DROP COLUMN point.t")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType)))))
    }
  }

  test("AlterTable: drop nested column in map key") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point map<struct<x: double, y: double>, bigint>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t DROP COLUMN point.key.y")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", MapType(StructType(Seq(
          StructField("x", DoubleType))), LongType)))
    }
  }

  test("AlterTable: drop nested column in map value") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: double, y: double>>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t DROP COLUMN points.value.y")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StringType, StructType(Seq(
          StructField("x", DoubleType))))))
    }
  }

  test("AlterTable: drop nested column in array element") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING $v2Format")
      sql(s"ALTER TABLE $t DROP COLUMN points.element.y")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType))))))
    }
  }

  test("AlterTable: drop column must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP COLUMN data")
      }

      assert(exc.getMessage.contains("Missing field data"))
    }
  }

  test("AlterTable: nested drop column must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP COLUMN point.x")
      }

      assert(exc.getMessage.contains("Missing field point.x"))
    }
  }

  test("AlterTable: set location") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t SET LOCATION 's3://bucket/path'")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.properties ===
        withDefaultOwnership(Map("provider" -> v2Format, "location" -> "s3://bucket/path")).asJava)
    }
  }

  test("AlterTable: set partition location") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t PARTITION(ds='2017-06-10') SET LOCATION 's3://bucket/path'")
      }
      assert(exc.getMessage.contains(
        "ALTER TABLE SET LOCATION does not support partition for v2 tables"))
    }
  }

  test("AlterTable: set table property") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t SET TBLPROPERTIES ('test'='34')")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.properties ===
        withDefaultOwnership(Map("provider" -> v2Format, "test" -> "34")).asJava)
    }
  }

  test("AlterTable: remove table property") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format TBLPROPERTIES('test' = '34')")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.properties ===
        withDefaultOwnership(Map("provider" -> v2Format, "test" -> "34")).asJava)

      sql(s"ALTER TABLE $t UNSET TBLPROPERTIES ('test')")

      val updated = getTableMetadata(tableName)

      assert(updated.name === tableName)
      assert(updated.properties === withDefaultOwnership(Map("provider" -> v2Format)).asJava)
    }
  }

  test("AlterTable: replace columns") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (col1 int, col2 int COMMENT 'c2') USING $v2Format")
      sql(s"ALTER TABLE $t REPLACE COLUMNS (col2 string, col3 int COMMENT 'c3')")

      val tableName = fullTableName(t)
      val table = getTableMetadata(tableName)

      assert(table.name === tableName)
      assert(table.schema === StructType(Seq(
        StructField("col2", StringType),
        StructField("col3", IntegerType).withComment("c3"))))
    }
  }
}
