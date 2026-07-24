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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.connector.catalog.CatalogV2Util
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}

abstract class DeltaBasedNestedRowIdTableSuiteBase(splitUpdates: Boolean)
  extends RowLevelOperationSuiteBase {

  import testImplicits._

  override protected def extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props.put("nested-row-id", "true")
    if (splitUpdates) props.put("split-updates", "true")
    props
  }

  // mirror the metadata marker on file-source row IDs
  private val nestedPkMetadata =
    new MetadataBuilder().putString(METADATA_COL_ATTR_KEY, "row_index").build()

  private val tableSchema = StructType(Seq(
    StructField("pk", IntegerType, nullable = false),
    StructField("nested", StructType(Seq(
      StructField("pk", IntegerType, nullable = false, metadata = nestedPkMetadata))),
      nullable = false),
    StructField("id", IntegerType),
    StructField("dep", StringType)))

  // use different top-level and nested PK values to expose name-based binding
  private val initialRows =
    """{ "pk": 10, "nested": { "pk": 1 }, "id": 1, "dep": "hr" }
      |{ "pk": 20, "nested": { "pk": 2 }, "id": 2, "dep": "software" }
      |{ "pk": 30, "nested": { "pk": 3 }, "id": 3, "dep": "hr" }
      |""".stripMargin

  private def createNestedRowIdTable(): Unit = {
    createTable(CatalogV2Util.structTypeToV2Columns(tableSchema))
    append(tableSchema.toDDL, initialRows)
  }

  private def checkTable(expected: Seq[Row]): Unit = {
    checkAnswer(
      sql(s"SELECT pk, nested.pk, id, dep FROM $tableNameAsString ORDER BY pk"),
      expected)
  }

  test("delete with nested row id") {
    createNestedRowIdTable()
    // use a multi-value IN to force the row-level delta path instead of metadata-only deleteWhere
    sql(s"DELETE FROM $tableNameAsString WHERE id IN (1, 100)")
    checkTable(Seq(
      Row(20, 2, 2, "software"),
      Row(30, 3, 3, "hr")))
  }

  test("update with nested row id") {
    createNestedRowIdTable()
    sql(s"UPDATE $tableNameAsString SET dep = 'it' WHERE id = 1")
    checkTable(Seq(
      Row(10, 1, 1, "it"),
      Row(20, 2, 2, "software"),
      Row(30, 3, 3, "hr")))
  }

  test("update replacing the struct that holds the nested row id") {
    createNestedRowIdTable()
    sql(
      s"""UPDATE $tableNameAsString
         |SET nested = named_struct('pk', 11)
         |WHERE id = 1
         |""".stripMargin)
    checkTable(Seq(
      Row(10, 11, 1, "hr"),
      Row(20, 2, 2, "software"),
      Row(30, 3, 3, "hr")))
  }

  test("merge update with nested row id") {
    withTempView("source") {
      createNestedRowIdTable()
      Seq((1, "it")).toDF("id", "dep").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.dep = s.dep
           |""".stripMargin)
      checkTable(Seq(
        Row(10, 1, 1, "it"),
        Row(20, 2, 2, "software"),
        Row(30, 3, 3, "hr")))
    }
  }

  test("merge delete with nested row id") {
    withTempView("source") {
      createNestedRowIdTable()
      Seq(2).toDF("id").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN DELETE
           |""".stripMargin)
      checkTable(Seq(
        Row(10, 1, 1, "hr"),
        Row(30, 3, 3, "hr")))
    }
  }

  test("merge not matched by source update with nested row id") {
    withTempView("source") {
      createNestedRowIdTable()
      Seq(1).toDF("id").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.dep = 'gone'
           |""".stripMargin)
      checkTable(Seq(
        Row(10, 1, 1, "hr"),
        Row(20, 2, 2, "gone"),
        Row(30, 3, 3, "gone")))
    }
  }

  test("merge with matched update and not-matched insert with nested row id") {
    withTempView("source") {
      createNestedRowIdTable()
      Seq((1, "it"), (4, "new")).toDF("id", "dep").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.dep = s.dep
           |WHEN NOT MATCHED THEN
           |INSERT (pk, nested, id, dep) VALUES (s.id * 10, named_struct('pk', s.id), s.id, s.dep)
           |""".stripMargin)
      checkTable(Seq(
        Row(10, 1, 1, "it"),
        Row(20, 2, 2, "software"),
        Row(30, 3, 3, "hr"),
        Row(40, 4, 4, "new")))
    }
  }
}

class DeltaBasedNestedRowIdTableSuite
  extends DeltaBasedNestedRowIdTableSuiteBase(splitUpdates = false)

class DeltaBasedNestedRowIdUpdateAsDeleteAndInsertTableSuite
  extends DeltaBasedNestedRowIdTableSuiteBase(splitUpdates = true)
