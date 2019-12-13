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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
      assert(table.schema === new StructType().add("id", IntegerType))
    }
  }

  test("AlterTable: add top-level column") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN data string")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
      assert(table.schema === new StructType().add("id", IntegerType).add("data", StringType))
    }
  }

  test("AlterTable: add column with comment") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN data string COMMENT 'doc'")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
      assert(table.schema === StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType).withComment("doc"))))
    }
  }

  test("AlterTable: add column with position") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (point struct<x: int>) USING $v2Format")

      sql(s"ALTER TABLE $t ADD COLUMN a string FIRST")
      assert(getTableMetadata(t).schema == new StructType()
        .add("a", StringType)
        .add("point", new StructType().add("x", IntegerType)))

      sql(s"ALTER TABLE $t ADD COLUMN b string AFTER point")
      assert(getTableMetadata(t).schema == new StructType()
        .add("a", StringType)
        .add("point", new StructType().add("x", IntegerType))
        .add("b", StringType))

      val e1 = intercept[SparkException](
        sql(s"ALTER TABLE $t ADD COLUMN c string AFTER non_exist"))
      assert(e1.getMessage().contains("AFTER column not found"))

      sql(s"ALTER TABLE $t ADD COLUMN point.y int FIRST")
      assert(getTableMetadata(t).schema == new StructType()
        .add("a", StringType)
        .add("point", new StructType()
          .add("y", IntegerType)
          .add("x", IntegerType))
        .add("b", StringType))

      sql(s"ALTER TABLE $t ADD COLUMN point.z int AFTER x")
      assert(getTableMetadata(t).schema == new StructType()
        .add("a", StringType)
        .add("point", new StructType()
          .add("y", IntegerType)
          .add("x", IntegerType)
          .add("z", IntegerType))
        .add("b", StringType))

      val e2 = intercept[SparkException](
        sql(s"ALTER TABLE $t ADD COLUMN point.x2 int AFTER non_exist"))
      assert(e2.getMessage().contains("AFTER column not found"))
    }
  }

  test("AlterTable: add multiple columns") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMNS data string COMMENT 'doc', ts timestamp")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      assert(exc.getMessage.contains("point"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: update column type int -> long") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN id TYPE bigint")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
      assert(table.schema === new StructType().add("id", LongType))
    }
  }

  test("AlterTable: update nested type float -> double") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: float, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN point.x TYPE double")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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
      assert(exc.getMessage.contains("update a struct by adding, deleting, or updating its fields"))

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      assert(exc.getMessage.contains("data"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: nested update column must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN point.x TYPE double")
      }

      assert(exc.getMessage.contains("point.x"))
      assert(exc.getMessage.contains("missing field"))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
      assert(table.schema === StructType(Seq(StructField("id", IntegerType).withComment("doc"))))
    }
  }

  test("AlterTable: update column position") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (a int, b int, point struct<x: int, y: int, z: int>) USING $v2Format")

      sql(s"ALTER TABLE $t ALTER COLUMN b FIRST")
      assert(getTableMetadata(t).schema == new StructType()
        .add("b", IntegerType)
        .add("a", IntegerType)
        .add("point", new StructType()
          .add("x", IntegerType)
          .add("y", IntegerType)
          .add("z", IntegerType)))

      sql(s"ALTER TABLE $t ALTER COLUMN b AFTER point")
      assert(getTableMetadata(t).schema == new StructType()
        .add("a", IntegerType)
        .add("point", new StructType()
          .add("x", IntegerType)
          .add("y", IntegerType)
          .add("z", IntegerType))
        .add("b", IntegerType))

      val e1 = intercept[SparkException](
        sql(s"ALTER TABLE $t ALTER COLUMN b AFTER non_exist"))
      assert(e1.getMessage.contains("AFTER column not found"))

      sql(s"ALTER TABLE $t ALTER COLUMN point.y FIRST")
      assert(getTableMetadata(t).schema == new StructType()
        .add("a", IntegerType)
        .add("point", new StructType()
          .add("y", IntegerType)
          .add("x", IntegerType)
          .add("z", IntegerType))
        .add("b", IntegerType))

      sql(s"ALTER TABLE $t ALTER COLUMN point.y AFTER z")
      assert(getTableMetadata(t).schema == new StructType()
        .add("a", IntegerType)
        .add("point", new StructType()
          .add("x", IntegerType)
          .add("z", IntegerType)
          .add("y", IntegerType))
        .add("b", IntegerType))

      val e2 = intercept[SparkException](
        sql(s"ALTER TABLE $t ALTER COLUMN point.y AFTER non_exist"))
      assert(e2.getMessage.contains("AFTER column not found"))

      // `AlterTable.resolved` checks column existence.
      intercept[AnalysisException](
        sql(s"ALTER TABLE $t ALTER COLUMN a.y AFTER x"))
    }
  }

  test("AlterTable: update column type and comment") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN id TYPE bigint COMMENT 'doc'")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
      assert(table.schema === StructType(Seq(StructField("id", LongType).withComment("doc"))))
    }
  }

  test("AlterTable: update nested column comment") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN point.y COMMENT 'doc'")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      assert(exc.getMessage.contains("data"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: nested comment update column must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN point.x COMMENT 'doc'")
      }

      assert(exc.getMessage.contains("point.x"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: rename column") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t RENAME COLUMN id TO user_id")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
      assert(table.schema === new StructType().add("user_id", IntegerType))
    }
  }

  test("AlterTable: rename nested column") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t RENAME COLUMN point.y TO t")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      assert(exc.getMessage.contains("data"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: nested rename column must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t RENAME COLUMN point.x TO z")
      }

      assert(exc.getMessage.contains("point.x"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: drop column") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, data string) USING $v2Format")
      sql(s"ALTER TABLE $t DROP COLUMN data")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
      assert(table.schema === new StructType().add("id", IntegerType))
    }
  }

  test("AlterTable: drop nested column") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double, t: double>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t DROP COLUMN point.t")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
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

      assert(exc.getMessage.contains("data"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: nested drop column must exist") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP COLUMN point.x")
      }

      assert(exc.getMessage.contains("point.x"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: set location") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t SET LOCATION 's3://bucket/path'")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
      assert(table.properties ===
        Map("provider" -> v2Format, "location" -> "s3://bucket/path").asJava)
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

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
      assert(table.properties === Map("provider" -> v2Format, "test" -> "34").asJava)
    }
  }

  test("AlterTable: remove table property") {
    val t = s"${catalogAndNamespace}table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format TBLPROPERTIES('test' = '34')")

      val table = getTableMetadata(t)

      assert(table.name === fullTableName(t))
      assert(table.properties === Map("provider" -> v2Format, "test" -> "34").asJava)

      sql(s"ALTER TABLE $t UNSET TBLPROPERTIES ('test')")

      val updated = getTableMetadata(t)

      assert(updated.name === fullTableName(t))
      assert(updated.properties === Map("provider" -> v2Format).asJava)
    }
  }

}
