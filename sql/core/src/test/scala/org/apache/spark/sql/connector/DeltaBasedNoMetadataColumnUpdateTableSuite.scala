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
import org.apache.spark.sql.connector.catalog.Update

class DeltaBasedNoMetadataColumnUpdateTableSuite extends UpdateTableSuiteBase
  with ColumnUpdateTableSharedTests {

  override protected def deltaUpdate: Boolean = true

  override protected def extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update", "true")
    props.put("no-metadata", "true")
    props
  }

  override protected def reqAttrsTableProps(reqAttrs: String): java.util.Map[String, String] = {
    java.util.Map.of("column-update-req-attrs", reqAttrs, "no-metadata", "true")
  }

  override protected def fullWidthTableProps: java.util.Map[String, String] = {
    java.util.Map.of("supports-deltas", "true", "no-metadata", "true")
  }

  override protected def scanMetadataColumns: String = ""

  test("column-update: narrow rows reach the delta writer without metadata") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

    assert(!table.lastWriteInfo.metadataSchema().isPresent)
    checkLastColumnUpdateWriteLog(
      WriteLogEntry(operation = Update, id = Some(1), data = Some(Row(1, "hr", -1))))
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 200, "software") :: Nil)
  }

  test("column-update: a reassigned row ID reaches the delta writer without metadata") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET pk = 10 WHERE pk = 1")

    checkLastColumnUpdateWriteLog(
      WriteLogEntry(operation = Update, id = Some(1), data = Some(Row(10, "hr"))))
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(2, 200, "software") :: Row(10, 100, "hr") :: Nil)
  }
}
