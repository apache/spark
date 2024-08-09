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
 * The class contains tests for the `ALTER TABLE .. RENAME COLUMN` command
 * to check V2 table catalogs.
 */
class AlterTableRenameColumnSuite
  extends command.AlterTableRenameColumnSuiteBase with CommandSuiteBase {

  private def getTableMetadata(tableIndent: TableIdentifier): Table = {
    val nameParts = tableIndent.nameParts
    val v2Catalog = spark.sessionState.catalogManager.catalog(nameParts.head).asTableCatalog
    val namespace = nameParts.drop(1).init.toArray
    v2Catalog.loadTable(Identifier.of(namespace, nameParts.last))
  }

  test("table does not exist") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 int, col2 string, a int, b int) $defaultUsing")
      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER TABLE does_not_exist RENAME COLUMN col1 TO col3")
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`does_not_exist`"),
        context = ExpectedContext(fragment = "does_not_exist", start = 12, stop = 25)
      )
    }
  }

  test("rename column") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 int, col2 string, a int, b int) $defaultUsing")
      sql(s"ALTER TABLE $t RENAME COLUMN col1 to col3")

      val table = getTableMetadata(TableIdentifier("tbl", Some("ns"), Some(catalog)))
      assert(table.name === t)
      assert(table.columns() === Array(
        Column.create("col3", IntegerType),
        Column.create("col2", StringType),
        Column.create("a", IntegerType),
        Column.create("b", IntegerType)))
    }
  }

  test("rename nested column") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double, z: double>) $defaultUsing")
      // don't write as: s"ALTER TABLE $t RENAME COLUMN point.z to point.w",
      // otherwise, it will throw an exception
      sql(s"ALTER TABLE $t RENAME COLUMN point.z to w")

      val table = getTableMetadata(TableIdentifier("tbl", Some("ns"), Some(catalog)))
      assert(table.name === t)
      assert(table.columns() === Array(
        Column.create("id", IntegerType),
        Column.create("point",
          StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType),
            StructField("w", DoubleType))
          )
        ))
      )
    }
  }

  test("rename nested column in map key") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, point map<struct<x: double, y: double>, bigint>) " +
        s"$defaultUsing")
      sql(s"ALTER TABLE $t RENAME COLUMN point.key.y to z")

      val table = getTableMetadata(TableIdentifier("tbl", Some("ns"), Some(catalog)))
      assert(table.name === t)
      assert(table.columns() === Array(
        Column.create("id", IntegerType),
        Column.create("point",
          MapType(
            StructType(Seq(StructField("x", DoubleType), StructField("z", DoubleType))),
            LongType
          )
        ))
      )
    }
  }

  test("rename nested column in map value") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, point map<string, struct<x: double, y: double>>) " +
        s"$defaultUsing")
      sql(s"ALTER TABLE $t RENAME COLUMN point.value.y to z")

      val table = getTableMetadata(TableIdentifier("tbl", Some("ns"), Some(catalog)))
      assert(table.name === t)
      assert(table.columns() === Array(
        Column.create("id", IntegerType),
        Column.create("point",
          MapType(
            StringType,
            StructType(Seq(StructField("x", DoubleType), StructField("z", DoubleType)))
          )
        ))
      )
    }
  }

  test("rename nested column in array element") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) $defaultUsing")
      sql(s"ALTER TABLE $t RENAME COLUMN points.element.y to z")

      val table = getTableMetadata(TableIdentifier("tbl", Some("ns"), Some(catalog)))
      assert(table.name === t)
      assert(table.columns() === Array(
        Column.create("id", IntegerType),
        Column.create("points",
          ArrayType(StructType(Seq(StructField("x", DoubleType), StructField("z", DoubleType))))))
      )
    }
  }

  test("rename column must exist if required") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int) $defaultUsing")

      val sqlText = s"ALTER TABLE $t RENAME COLUMN does_not_exist to x"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`does_not_exist`",
          "proposal" -> "`id`"))
    }
  }

  test("nested rename column must exist if required") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int) $defaultUsing")

      val sqlText = s"ALTER TABLE $t RENAME COLUMN point.does_not_exist TO x"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`point`.`does_not_exist`",
          "proposal" -> "`id`"))
    }
  }
}
