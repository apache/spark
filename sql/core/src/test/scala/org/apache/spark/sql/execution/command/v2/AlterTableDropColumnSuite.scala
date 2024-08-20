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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{Column, Identifier, Table}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, MapType, StringType, StructField, StructType}

/**
 * The class contains tests for the `ALTER TABLE .. DROP (COLUMN | COLUMNS)` command to
 * check V2 table catalogs.
 */
class AlterTableDropColumnSuite
  extends command.AlterTableDropColumnSuiteBase with CommandSuiteBase {

  private def getTableMetadata(tableIndent: TableIdentifier): Table = {
    val nameParts = tableIndent.nameParts
    val v2Catalog = spark.sessionState.catalogManager.catalog(nameParts.head).asTableCatalog
    val namespace = nameParts.drop(1).init.toArray
    v2Catalog.loadTable(Identifier.of(namespace, nameParts.last))
  }

  test("table does not exist") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int) $defaultUsing")
      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER TABLE does_not_exist DROP COLUMN id")
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`does_not_exist`"),
        context = ExpectedContext(fragment = "does_not_exist", start = 12, stop = 25)
      )
    }
  }

  test("drop column") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, data string) $defaultUsing")
      sql(s"ALTER TABLE $t DROP COLUMN data")

      val table = getTableMetadata(TableIdentifier("tbl", Some("ns"), Some(catalog)))
      assert(table.name === t)
      assert(table.columns() === Array(Column.create("id", IntegerType)))
    }
  }

  test("drop nested column") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double, z: double>) $defaultUsing")
      sql(s"ALTER TABLE $t DROP COLUMN point.z")

      val table = getTableMetadata(TableIdentifier("tbl", Some("ns"), Some(catalog)))
      assert(table.name === t)
      assert(table.columns() === Array(
        Column.create("id", IntegerType),
        Column.create("point",
          StructType(Seq(StructField("x", DoubleType), StructField("y", DoubleType))))))
    }
  }

  test("drop nested column in map key") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, point map<struct<x: double, y: double>, bigint>) " +
        s"$defaultUsing")
      sql(s"ALTER TABLE $t DROP COLUMN point.key.y")

      val table = getTableMetadata(TableIdentifier("tbl", Some("ns"), Some(catalog)))
      assert(table.name === t)
      assert(table.columns() === Array(
        Column.create("id", IntegerType),
        Column.create("point", MapType(StructType(Seq(StructField("x", DoubleType))), LongType))))
    }
  }

  test("drop nested column in map value") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, point map<string, struct<x: double, y: double>>) " +
        s"$defaultUsing")
      sql(s"ALTER TABLE $t DROP COLUMN point.value.y")

      val table = getTableMetadata(TableIdentifier("tbl", Some("ns"), Some(catalog)))
      assert(table.name === t)
      assert(table.columns() === Array(
        Column.create("id", IntegerType),
        Column.create("point", MapType(StringType, StructType(Seq(StructField("x", DoubleType)))))))
    }
  }

  test("drop nested column in array element") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) $defaultUsing")
      sql(s"ALTER TABLE $t DROP COLUMN points.element.y")

      val table = getTableMetadata(TableIdentifier("tbl", Some("ns"), Some(catalog)))
      assert(table.name === t)
      assert(table.columns() === Array(
        Column.create("id", IntegerType),
        Column.create("points", ArrayType(StructType(Seq(StructField("x", DoubleType)))))))
    }
  }

  test("drop column must exist if required") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int) $defaultUsing")

      val sqlText = s"ALTER TABLE $t DROP COLUMN does_not_exist"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`does_not_exist`",
          "proposal" -> "`id`"),
        context = ExpectedContext(fragment = sqlText, start = 0, stop = 57))
    }
  }

  test("nested drop column must exist if required") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int) $defaultUsing")

      val sqlText = s"ALTER TABLE $t DROP COLUMN point.does_not_exist"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`point`.`does_not_exist`",
          "proposal" -> "`id`"),
        context = ExpectedContext(fragment = sqlText, start = 0, stop = 63))

      // with if exists it should pass
      sql(s"ALTER TABLE $t DROP COLUMN IF EXISTS point.does_not_exist")
      val table = getTableMetadata(TableIdentifier("tbl", Some("ns"), Some(catalog)))
      assert(table.name === t)
      assert(table.columns() === Array(Column.create("id", IntegerType)))
    }
  }

  test("drop mixed existing/non-existing columns using IF EXISTS") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, name string, points array<struct<x: double, y: double>>) " +
        s"$defaultUsing")
      // with if exists it should pass
      sql(s"ALTER TABLE $t DROP COLUMNS IF EXISTS " +
        s"names, name, points.element.z, id, points.element.x")
      val table = getTableMetadata(TableIdentifier("tbl", Some("ns"), Some(catalog)))
      assert(table.name === t)
      assert(table.columns() === Array(Column.create("points",
        ArrayType(StructType(Seq(StructField("y", DoubleType)))))))
    }
  }
}
