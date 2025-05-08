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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.connector.catalog.{Column, Table}
import org.apache.spark.sql.connector.catalog.CatalogV2Util.withDefaultOwnership
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

trait AlterTableTests extends SharedSparkSession with QueryErrorsBase {

  protected def getTableMetadata(tableName: String): Table

  protected val catalogAndNamespace: String

  protected val v2Format: String

  private def fullTableName(tableName: String): String = {
    if (catalogAndNamespace.isEmpty) {
      s"default.$tableName"
    } else {
      s"$catalogAndNamespace$tableName"
    }
  }

  private def prependCatalogName(tableName: String): String = {
    if (catalogAndNamespace.isEmpty) {
      s"spark_catalog.$tableName"
    } else {
      tableName
    }
  }

  test("AlterTable: table does not exist") {
    val t2 = s"${catalogAndNamespace}fake_table"
    val quoted = UnresolvedAttribute.parseAttributeName(s"${catalogAndNamespace}table_name")
      .map(part => quoteIdentifier(part)).mkString(".")
    withTable(t2) {
      sql(s"CREATE TABLE $t2 (id int) USING $v2Format")
      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE ${catalogAndNamespace}table_name DROP COLUMN id")
      }

      checkErrorTableNotFound(exc, quoted,
        ExpectedContext(s"${catalogAndNamespace}table_name", 12,
          11 + s"${catalogAndNamespace}table_name".length))
    }
  }

  test("AlterTable: change rejected by implementation") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val exc = intercept[SparkException] {
        sql(s"ALTER TABLE $t DROP COLUMN id")
      }

      assert(exc.getMessage.contains("Unsupported table change"))
      assert(exc.getMessage.contains("Cannot drop all fields")) // from the implementation

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType().add("id", IntegerType))
    }
  }

  test("AlterTable: add top-level column") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN data string")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType().add("id", IntegerType).add("data", StringType))
    }
  }

  test("AlterTable: add column with NOT NULL") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN data string NOT NULL")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType, nullable = false))))
    }
  }

  test("AlterTable: add column with comment") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN data string COMMENT 'doc'")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType).withComment("doc"))))
    }
  }

  test("AlterTable: add column with interval type") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      val e1 =
        intercept[ParseException](sql(s"ALTER TABLE $t ADD COLUMN data interval"))
      assert(e1.getMessage.contains("Cannot use \"INTERVAL\" type in the table schema."))
      val e2 =
        intercept[ParseException](sql(s"ALTER TABLE $t ADD COLUMN point.z interval"))
      assert(e2.getMessage.contains("Cannot use \"INTERVAL\" type in the table schema."))
    }
  }

  test("AlterTable: add column with position") {
    val t = fullTableName("table_name")
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

      val e1 = intercept[AnalysisException](
        sql(s"ALTER TABLE $t ADD COLUMN c string AFTER non_exist"))
      checkError(
        exception = e1,
        condition = "FIELD_NOT_FOUND",
        parameters = Map("fieldName" -> "`c`", "fields" -> "a, point, b")
      )

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

      val e2 = intercept[AnalysisException](
        sql(s"ALTER TABLE $t ADD COLUMN point.x2 int AFTER non_exist"))
      checkError(
        exception = e2,
        condition = "FIELD_NOT_FOUND",
        parameters = Map("fieldName" -> "`x2`", "fields" -> "y, x, z")
      )
    }
  }

  test("SPARK-30814: add column with position referencing new columns being added") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (a string, b int, point struct<x: double, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMNS (x int AFTER a, y int AFTER x, z int AFTER y)")

      assert(getTableMetadata(t).schema === new StructType()
        .add("a", StringType)
        .add("x", IntegerType)
        .add("y", IntegerType)
        .add("z", IntegerType)
        .add("b", IntegerType)
        .add("point", new StructType()
          .add("x", DoubleType)
          .add("y", DoubleType)))

      sql(s"ALTER TABLE $t ADD COLUMNS (point.z double AFTER x, point.zz double AFTER z)")
      assert(getTableMetadata(t).schema === new StructType()
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
      checkError(
        exception = e,
        condition = "FIELD_NOT_FOUND",
        parameters = Map("fieldName" -> "`yy`", "fields" -> "a, x, y, z, b, point")
      )
    }
  }

  test("AlterTable: add multiple columns") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMNS data string COMMENT 'doc', ts timestamp")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType).withComment("doc"),
        StructField("ts", TimestampType))))
    }
  }

  test("AlterTable: add nested column") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN point.z double")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType),
          StructField("z", DoubleType)))))
    }
  }

  test("AlterTable: add nested column to map key") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<struct<x: double, y: double>, bigint>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN points.key.z double")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType),
          StructField("z", DoubleType))), LongType)))
    }
  }

  test("AlterTable: add nested column to map value") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: double, y: double>>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN points.value.z double")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StringType, StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType),
          StructField("z", DoubleType))))))
    }
  }

  test("AlterTable: add nested column to array element") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN points.element.z double")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType),
          StructField("z", DoubleType))))))
    }
  }

  test("SPARK-39383 DEFAULT columns on V2 data sources with ALTER TABLE ADD/ALTER COLUMN") {
    withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> s"$v2Format, ") {
      val t = fullTableName("table_name")
      withTable("t") {
        sql(s"create table $t (a string) using $v2Format")
        sql(s"alter table $t add column (b int default 2 + 3)")

        val table = getTableMetadata(t)

        assert(table.name === t)
        assert(table.schema === new StructType()
          .add("a", StringType)
          .add(StructField("b", IntegerType)
            .withCurrentDefaultValue("2 + 3")
            .withExistenceDefaultValue("5")))

        sql(s"alter table $t alter column b set default 2 + 3")

        assert(
          getTableMetadata(t).schema === new StructType()
            .add("a", StringType)
            .add(StructField("b", IntegerType)
              .withCurrentDefaultValue("2 + 3")
              .withExistenceDefaultValue("5")))

        sql(s"alter table $t alter column b drop default")

        assert(
          getTableMetadata(t).schema === new StructType()
            .add("a", StringType)
            .add(StructField("b", IntegerType)
              .withExistenceDefaultValue("5")))
      }
    }
  }

  test("SPARK-45075: ALTER COLUMN with invalid default value") {
    withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> s"$v2Format, ") {
      withTable("t") {
        sql(s"create table t(i boolean) using $v2Format")
        // The default value fails to analyze.
        checkError(
          exception = intercept[AnalysisException] {
            sql("alter table t add column s bigint default badvalue")
          },
          condition = "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION",
          parameters = Map(
            "statement" -> "ALTER TABLE",
            "colName" -> "`s`",
            "defaultValue" -> "badvalue"))

        sql("alter table t add column s bigint default 3L")
        checkError(
          exception = intercept[AnalysisException] {
            sql("alter table t alter column s set default badvalue")
          },
          condition = "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION",
          parameters = Map(
            "statement" -> "ALTER TABLE ALTER COLUMN",
            "colName" -> "`s`",
            "defaultValue" -> "badvalue"))
      }
    }
  }

  test("AlterTable: add complex column") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN points array<struct<x: double, y: double>>")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType))))))
    }
  }

  test("AlterTable: add nested column with comment") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING $v2Format")
      sql(s"ALTER TABLE $t ADD COLUMN points.element.z double COMMENT 'doc'")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType),
          StructField("z", DoubleType).withComment("doc"))))))
    }
  }

  test("AlterTable: add nested column parent must exist") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val sqlText = s"ALTER TABLE $t ADD COLUMN point.z double"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`point`",
          "proposal" -> "`id`"),
        context = ExpectedContext(
          fragment = "point.z double",
          start = 24 + t.length,
          stop = 37 + t.length))
    }
  }

  test("AlterTable: add column - new column should not exist") {
    val t = fullTableName("table_name")
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
        val expectedParameters = Map(
          "op" -> "add",
          "fieldNames" -> s"${toSQLId(field)}",
          "struct" ->
            """"STRUCT<id: INT, point: STRUCT<x: DOUBLE, y: DOUBLE>,
              |arr: ARRAY<STRUCT<x: DOUBLE, y: DOUBLE>>,
              |mk: MAP<STRUCT<x: DOUBLE, y: DOUBLE>, STRING>,
              |mv: MAP<STRING, STRUCT<x: DOUBLE, y: DOUBLE>>>"""".stripMargin.replace("\n", " "))
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"ALTER TABLE $t ADD COLUMNS $field double")
          },
          condition = "FIELD_ALREADY_EXISTS",
          parameters = expectedParameters,
          context = ExpectedContext(
            fragment = s"ALTER TABLE $t ADD COLUMNS $field double",
            start = 0,
            stop = 31 + t.length + field.length)
        )
      }
    }
  }

  test("SPARK-36372: Adding duplicate columns should not be allowed") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t ADD COLUMNS (data string, data1 string, data string)")
        },
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`data`"))
    }
  }

  test("SPARK-36372: Adding duplicate nested columns should not be allowed") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t ADD COLUMNS (point.z double, point.z double, point.xx double)")
        },
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> toSQLId("point.z")))
    }
  }

  test("AlterTable: update column type int -> long") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN id TYPE bigint")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType().add("id", LongType))
    }
  }

  test("AlterTable: update column type to interval") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      (DataTypeTestUtils.dayTimeIntervalTypes ++ DataTypeTestUtils.yearMonthIntervalTypes)
        .foreach {
          case d: DataType => d.typeName
            val sqlText = s"ALTER TABLE $t ALTER COLUMN id TYPE ${d.typeName}"

            checkError(
              exception = intercept[AnalysisException] {
                sql(sqlText)
              },
              condition = "CANNOT_UPDATE_FIELD.INTERVAL_TYPE",
              parameters = Map(
                "table" -> s"${toSQLId(prependCatalogName(t))}",
                "fieldName" -> "`id`"),
              context = ExpectedContext(
                fragment = sqlText,
                start = 0,
                stop = 33 + d.typeName.length + t.length)
            )
        }
    }
  }

  test("AlterTable: SET/DROP NOT NULL") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint NOT NULL) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN id SET NOT NULL")

      val table = getTableMetadata(t)
      assert(table.name === t)
      assert(table.schema === new StructType().add("id", LongType, nullable = false))

      sql(s"ALTER TABLE $t ALTER COLUMN id DROP NOT NULL")
      val table2 = getTableMetadata(t)
      assert(table2.name === t)
      assert(table2.schema === new StructType().add("id", LongType))

      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN id SET NOT NULL")
      }
      assert(e.message.contains("Cannot change nullable column to non-nullable"))
    }
  }

  test("AlterTable: update nested type float -> double") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: float, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN point.x TYPE double")

      val table = getTableMetadata(t)
      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType)))))
    }
  }

  test("AlterTable: update column with struct type fails") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      val sqlText =
        s"ALTER TABLE $t ALTER COLUMN point TYPE struct<x: double, y: double, z: double>"

      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "CANNOT_UPDATE_FIELD.STRUCT_TYPE",
        parameters = Map(
          "table" -> s"${toSQLId(prependCatalogName(t))}",
          "fieldName" -> "`point`"),
        context = ExpectedContext(
          fragment = sqlText,
          start = 0,
          stop = 75 + t.length)
      )

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType)))))
    }
  }

  test("AlterTable: update column with array type fails") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<int>) USING $v2Format")
      val sqlText = s"ALTER TABLE $t ALTER COLUMN points TYPE array<long>"

      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "CANNOT_UPDATE_FIELD.ARRAY_TYPE",
        parameters = Map(
          "table" -> s"${toSQLId(prependCatalogName(t))}",
          "fieldName" -> "`points`"),
        context = ExpectedContext(
          fragment = sqlText,
          start = 0,
          stop = 48 + t.length)
      )

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(IntegerType)))
    }
  }

  test("AlterTable: update column array element type") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<int>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.element TYPE long")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(LongType)))
    }
  }

  test("AlterTable: update column with map type fails") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, m map<string, int>) USING $v2Format")
      val sqlText = s"ALTER TABLE $t ALTER COLUMN m TYPE map<string, long>"

      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "CANNOT_UPDATE_FIELD.MAP_TYPE",
        parameters = Map(
          "table" -> s"${toSQLId(prependCatalogName(t))}",
          "fieldName" -> "`m`"),
        context = ExpectedContext(
          fragment = sqlText,
          start = 0,
          stop = 49 + t.length)
      )

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("m", MapType(StringType, IntegerType)))
    }
  }

  test("AlterTable: update column map value type") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, m map<string, int>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN m.value TYPE long")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("m", MapType(StringType, LongType)))
    }
  }

  test("AlterTable: update nested type in map key") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<struct<x: float, y: double>, bigint>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.key.x TYPE double")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType))), LongType)))
    }
  }

  test("AlterTable: update nested type in map value") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: float, y: double>>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.value.x TYPE double")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StringType, StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType))))))
    }
  }

  test("AlterTable: update nested type in array") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: float, y: double>>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.element.x TYPE double")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType))))))
    }
  }

  test("AlterTable: update column must exist") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val sqlText = s"ALTER TABLE $t ALTER COLUMN data TYPE string"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`data`",
          "proposal" -> "`id`"),
        context = ExpectedContext(fragment = sqlText, start = 0, stop = sqlText.length - 1))
    }
  }

  test("AlterTable: nested update column must exist") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val sqlText = s"ALTER TABLE $t ALTER COLUMN point.x TYPE double"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`point`.`x`",
          "proposal" -> "`id`"),
        context = ExpectedContext(fragment = sqlText, start = 0, stop = sqlText.length - 1))
    }
  }

  test("AlterTable: update column type must be compatible") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      val sql1 = s"ALTER TABLE $t ALTER COLUMN id TYPE boolean"
      checkErrorMatchPVals(
        exception = intercept[AnalysisException] {
          sql(sql1)
        },
        condition = "NOT_SUPPORTED_CHANGE_COLUMN",
        sqlState = None,
        parameters = Map(
          "originType" -> "\"INT\"",
          "newType" -> "\"BOOLEAN\"",
          "newName" -> "`id`",
          "originName" -> "`id`",
          "table" -> ".*table_name.*"),
        context = ExpectedContext(
          fragment = sql1,
          start = 0,
          stop = sql1.length - 1)
      )
    }
  }

  test("AlterTable: update column comment") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN id COMMENT 'doc'")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === StructType(Seq(StructField("id", IntegerType).withComment("doc"))))
    }
  }

  test("AlterTable: update column position") {
    val t = fullTableName("table_name")
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

      val sqlText1 = s"ALTER TABLE $t ALTER COLUMN b AFTER non_exist"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText1)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`non_exist`",
          "proposal" -> "`a`, `point`, `b`"),
        context = ExpectedContext(fragment = sqlText1, start = 0, stop = sqlText1.length - 1))

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

      val sqlText2 = s"ALTER TABLE $t ALTER COLUMN point.y AFTER non_exist"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText2)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`point`.`non_exist`",
          "proposal" -> "`a`, `point`, `b`"),
        context = ExpectedContext(fragment = sqlText2, start = 0, stop = sqlText2.length - 1))

      // `AlterTable.resolved` checks column existence.
      intercept[AnalysisException](
        sql(s"ALTER TABLE $t ALTER COLUMN a.y AFTER x"))
    }
  }

  test("AlterTable: update nested column comment") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN point.y COMMENT 'doc'")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType).withComment("doc")))))
    }
  }

  test("AlterTable: update nested column comment in map key") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<struct<x: double, y: double>, bigint>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.key.y COMMENT 'doc'")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType).withComment("doc"))), LongType)))
    }
  }

  test("AlterTable: update nested column comment in map value") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: double, y: double>>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.value.y COMMENT 'doc'")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StringType, StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType).withComment("doc"))))))
    }
  }

  test("AlterTable: update nested column comment in array") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING $v2Format")
      sql(s"ALTER TABLE $t ALTER COLUMN points.element.y COMMENT 'doc'")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType).withComment("doc"))))))
    }
  }

  test("AlterTable: comment update column must exist") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val sqlText = s"ALTER TABLE $t ALTER COLUMN data COMMENT 'doc'"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`data`",
          "proposal" -> "`id`"),
        context = ExpectedContext(fragment = sqlText, start = 0, stop = sqlText.length - 1))
    }
  }

  test("AlterTable: nested comment update column must exist") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val sqlText = s"ALTER TABLE $t ALTER COLUMN point.x COMMENT 'doc'"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`point`.`x`",
          "proposal" -> "`id`"),
        context = ExpectedContext(fragment = sqlText, start = 0, stop = sqlText.length - 1))
    }
  }

  test("AlterTable: alter multiple columns/fields in the same command") {
    withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> s"$v2Format, ") {
      val t = fullTableName("table_name")
      withTable(t) {
        sql(s"""CREATE TABLE $t (
             |  id int,
             |  data string,
             |  ts timestamp NOT NULL,
             |  points array<struct<x: double, y: double>>) USING $v2Format""".stripMargin)
        sql(s"""ALTER TABLE $t ALTER COLUMN
             |  id TYPE bigint,
             |  data FIRST,
             |  ts DROP NOT NULL,
             |  points.element.x SET DEFAULT (1.0 + 2.0),
             |  points.element.y COMMENT 'comment on y'""".stripMargin)

        val table = getTableMetadata(t)

        assert(table.name === t)
        assert(
          table.columns() === Array(
            Column.create("data", StringType),
            Column.create("id", LongType),
            Column.create("ts", TimestampType, true),
            Column.create("points", ArrayType(
              StructType(
                Seq(
                  StructField("x", DoubleType).withCurrentDefaultValue("(1.0 + 2.0)"),
                  StructField("y", DoubleType).withComment("comment on y")
                ))))))
      }
    }
  }

  test("AlterTable: cannot alter the same column in the same command") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, data string, ts timestamp) USING $v2Format")
      val sqlText = s"ALTER TABLE $t ALTER COLUMN id TYPE bigint, id COMMENT 'id', data FIRST"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "NOT_SUPPORTED_CHANGE_SAME_COLUMN",
        parameters = Map(
          "table" -> s"${toSQLId(prependCatalogName(t))}",
          "fieldName" -> "`id`"),
        context = ExpectedContext(fragment = sqlText, start = 0, stop = sqlText.length - 1)
      )
    }
  }

  test("AlterTable: cannot alter parent and child fields in the same command") {
    withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> s"$v2Format, ") {
      val t = fullTableName("table_name")
      withTable(t) {
        sql(s"CREATE TABLE $t (parent array<struct<child: int>>) USING $v2Format")
        val sqlText = s"""ALTER TABLE $t ALTER COLUMN
                         | parent.element.child TYPE string,
                         | parent SET DEFAULT array(struct(1000))""".stripMargin

        checkError(
          exception = intercept[AnalysisException] {
            sql(sqlText)
          },
          condition = "NOT_SUPPORTED_CHANGE_SAME_COLUMN",
          parameters = Map(
            "table" -> s"${toSQLId(prependCatalogName(t))}",
            "fieldName" -> "`parent`"),
          context = ExpectedContext(
            fragment = sqlText,
            start = 0,
            stop = sqlText.length - 1)
        )
      }
    }
  }

  test("AlterTable: rename column") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t RENAME COLUMN id TO user_id")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType().add("user_id", IntegerType))
    }
  }

  test("AlterTable: rename nested column") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING $v2Format")
      sql(s"ALTER TABLE $t RENAME COLUMN point.y TO t")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", StructType(Seq(
          StructField("x", DoubleType),
          StructField("t", DoubleType)))))
    }
  }

  test("AlterTable: rename nested column in map key") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point map<struct<x: double, y: double>, bigint>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t RENAME COLUMN point.key.y TO t")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", MapType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("t", DoubleType))), LongType)))
    }
  }

  test("AlterTable: rename nested column in map value") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: double, y: double>>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t RENAME COLUMN points.value.y TO t")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StringType, StructType(Seq(
          StructField("x", DoubleType),
          StructField("t", DoubleType))))))
    }
  }

  test("AlterTable: rename nested column in array element") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING $v2Format")
      sql(s"ALTER TABLE $t RENAME COLUMN points.element.y TO t")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType),
          StructField("t", DoubleType))))))
    }
  }

  test("AlterTable: rename column must exist") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val sqlText = s"ALTER TABLE $t RENAME COLUMN data TO some_string"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`data`",
          "proposal" -> "`id`"),
        context = ExpectedContext(fragment = sqlText, start = 0, stop = sqlText.length - 1))
    }
  }

  test("AlterTable: nested rename column must exist") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val sqlText = s"ALTER TABLE $t RENAME COLUMN point.x TO z"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`point`.`x`",
          "proposal" -> "`id`"),
        context = ExpectedContext(fragment = sqlText, start = 0, stop = sqlText.length - 1))
    }
  }

  test("AlterTable: rename column - new name should not exist") {
    val t = fullTableName("table_name")
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
        ("id" -> "user_id") -> "user_id",
        ("point.x" -> "y") -> "point.y",
        ("arr.element.x" -> "y") -> "arr.element.y",
        ("mk.key.x" -> "y") -> "mk.key.y",
        ("mv.value.x" -> "y") -> "mv.value.y").foreach { case ((field, newName), expectedName) =>

        val expectedStruct =
          """"
            |STRUCT<id: INT, user_id: INT,
            | point: STRUCT<x: DOUBLE, y: DOUBLE>,
            | arr: ARRAY<STRUCT<x: DOUBLE, y: DOUBLE>>,
            | mk: MAP<STRUCT<x: DOUBLE, y: DOUBLE>, STRING>,
            | mv: MAP<STRING, STRUCT<x: DOUBLE, y: DOUBLE>>>
            |"""".stripMargin.replace("\n", "")
        val expectedStop = if (expectedName == "user_id") {
          39 + t.length
        } else {
          31 + t.length + expectedName.length
        }

        checkError(
          exception = intercept[AnalysisException] {
            sql(s"ALTER TABLE $t RENAME COLUMN $field TO $newName")
          },
          condition = "FIELD_ALREADY_EXISTS",
          parameters = Map(
            "op" -> "rename",
            "fieldNames" -> s"${toSQLId(expectedName)}",
            "struct" -> expectedStruct),
          context = ExpectedContext(
            fragment = s"ALTER TABLE $t RENAME COLUMN $field TO $newName",
            start = 0,
            stop = expectedStop)
        )
      }
    }
  }

  test("AlterTable: drop column") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, data string) USING $v2Format")
      sql(s"ALTER TABLE $t DROP COLUMN data")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType().add("id", IntegerType))
    }
  }

  test("AlterTable: drop nested column") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double, t: double>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t DROP COLUMN point.t")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", StructType(Seq(
          StructField("x", DoubleType),
          StructField("y", DoubleType)))))
    }
  }

  test("AlterTable: drop nested column in map key") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point map<struct<x: double, y: double>, bigint>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t DROP COLUMN point.key.y")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("point", MapType(StructType(Seq(
          StructField("x", DoubleType))), LongType)))
    }
  }

  test("AlterTable: drop nested column in map value") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: double, y: double>>) " +
        s"USING $v2Format")
      sql(s"ALTER TABLE $t DROP COLUMN points.value.y")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", MapType(StringType, StructType(Seq(
          StructField("x", DoubleType))))))
    }
  }

  test("AlterTable: drop nested column in array element") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING $v2Format")
      sql(s"ALTER TABLE $t DROP COLUMN points.element.y")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === new StructType()
        .add("id", IntegerType)
        .add("points", ArrayType(StructType(Seq(
          StructField("x", DoubleType))))))
    }
  }

  test("AlterTable: drop column must exist if required") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val sqlText = s"ALTER TABLE $t DROP COLUMN data"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`data`",
          "proposal" -> "`id`"),
        context = ExpectedContext(fragment = sqlText, start = 0, stop = sqlText.length - 1))

      // with if exists it should pass
      sql(s"ALTER TABLE $t DROP COLUMN IF EXISTS data")
      val table = getTableMetadata(t)
      assert(table.schema == new StructType().add("id", IntegerType))
    }
  }

  test("AlterTable: nested drop column must exist if required") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")

      val sqlText = s"ALTER TABLE $t DROP COLUMN point.x"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`point`.`x`",
          "proposal" -> "`id`"),
        context = ExpectedContext(fragment = sqlText, start = 0, stop = sqlText.length - 1))

      // with if exists it should pass
      sql(s"ALTER TABLE $t DROP COLUMN IF EXISTS point.x")
      val table = getTableMetadata(t)
      assert(table.schema == new StructType().add("id", IntegerType))
    }
  }

  test("AlterTable: drop mixed existing/non-existing columns using IF EXISTS") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, name string, points array<struct<x: double, y: double>>) " +
        s"USING $v2Format")

      // with if exists it should pass
      sql(s"ALTER TABLE $t DROP COLUMNS IF EXISTS " +
        s"names, name, points.element.z, id, points.element.x")
      val table = getTableMetadata(t)
      assert(table.schema == new StructType()
        .add("points", ArrayType(StructType(Seq(StructField("y", DoubleType))))))
    }
  }

  test("AlterTable: set table property") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format")
      sql(s"ALTER TABLE $t SET TBLPROPERTIES ('test'='34')")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.properties ===
        withDefaultOwnership(Map("provider" -> v2Format, "test" -> "34")).asJava)
    }
  }

  test("AlterTable: remove table property") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING $v2Format TBLPROPERTIES('test' = '34')")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.properties ===
        withDefaultOwnership(Map("provider" -> v2Format, "test" -> "34")).asJava)

      sql(s"ALTER TABLE $t UNSET TBLPROPERTIES ('test')")

      val updated = getTableMetadata(t)

      assert(updated.name === t)
      assert(updated.properties === withDefaultOwnership(Map("provider" -> v2Format)).asJava)
    }
  }

  test("AlterTable: replace columns") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (col1 int, col2 int COMMENT 'c2') USING $v2Format")
      sql(s"ALTER TABLE $t REPLACE COLUMNS (col2 string, col3 int COMMENT 'c3')")

      val table = getTableMetadata(t)

      assert(table.name === t)
      assert(table.schema === StructType(Seq(
        StructField("col2", StringType),
        StructField("col3", IntegerType).withComment("c3"))))
    }
  }

  test("SPARK-36449: Replacing columns with duplicate name should not be allowed") {
    val t = fullTableName("table_name")
    withTable(t) {
      sql(s"CREATE TABLE $t (data string) USING $v2Format")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t REPLACE COLUMNS (data string, data1 string, data string)")
        },
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`data`"))
    }
  }

  test("Alter column type between string and char/varchar") {
    val types = Seq(
      ("STRING", "\"STRING\""),
      ("STRING COLLATE UTF8_LCASE", "\"STRING COLLATE UTF8_LCASE\""),
      ("CHAR(5)", "\"CHAR\\(5\\)\""),
      ("VARCHAR(5)", "\"VARCHAR\\(5\\)\""))
    types.flatMap { a => types.map { b => (a, b) } }
      .filter { case (a, b) => a != b }
      .filter { case ((a, _), (b, _)) => !a.startsWith("STRING") || !b.startsWith("STRING") }
      .foreach { case ((from, originType), (to, newType)) =>
        val t = "table_name"
        withTable(t) {
          sql(s"CREATE TABLE $t (id $from) USING PARQUET")
          val sql1 = s"ALTER TABLE $t ALTER COLUMN id TYPE $to"
          checkErrorMatchPVals(
            exception = intercept[AnalysisException] {
              sql(sql1)
            },
            condition = "NOT_SUPPORTED_CHANGE_COLUMN",
            sqlState = None,
            parameters = Map(
              "originType" -> originType,
              "newType" -> newType,
              "newName" -> "`id`",
              "originName" -> "`id`",
              "table" -> ".*table_name.*"),
            context = ExpectedContext(
              fragment = sql1,
              start = 0,
              stop = sql1.length - 1)
          )
        }
      }
  }
}
