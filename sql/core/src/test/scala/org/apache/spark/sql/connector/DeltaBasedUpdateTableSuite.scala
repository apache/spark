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
import org.apache.spark.sql.types.StructType

class DeltaBasedUpdateTableSuite extends DeltaBasedUpdateTableSuiteBase {

  import testImplicits._

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props
  }

  test("update handles metadata columns correctly") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE id IN (1, 100)")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)

    checkLastWriteInfo(
      expectedRowSchema = StructType(table.schema.map {
        case attr if attr.name == "id" => attr.copy(nullable = false) // input is a constant
        case attr => attr
      }),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))

    checkLastWriteLog(
      updateWriteLogEntry(id = 1, metadata = Row("hr", null), data = Row(1, -1, "hr")))
  }

  test("update with subquery handles metadata columns correctly") {
    withTempView("updated_dep") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "software" }
          |{ "pk": 3, "id": 3, "dep": "hr" }
          |""".stripMargin)

      val updatedIdDF = Seq(Some("hr"), Some("it")).toDF()
      updatedIdDF.createOrReplaceTempView("updated_dep")

      sql(
        s"""UPDATE $tableNameAsString
           |SET id = -1
           |WHERE
           | id IN (1, 20)
           | AND
           | dep IN (SELECT * FROM updated_dep)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)

      checkLastWriteInfo(
        expectedRowSchema = StructType(table.schema.map {
          case attr if attr.name == "id" => attr.copy(nullable = false) // input is a constant
          case attr => attr
        }),
        expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
        expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))

      checkLastWriteLog(
        updateWriteLogEntry(id = 1, metadata = Row("hr", null), data = Row(1, -1, "hr")))
    }
  }
}
