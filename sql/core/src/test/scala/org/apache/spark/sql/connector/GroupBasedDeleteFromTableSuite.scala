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

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class GroupBasedDeleteFromTableSuite extends DeleteFromTableSuiteBase {

  import testImplicits._

  test("delete preserves metadata columns for carried-over records") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |{ "pk": 4, "id": 4, "dep": "hr" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE id IN (1, 100)")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, "software") :: Row(3, 3, "hr") :: Row(4, 4, "hr") :: Nil)

    checkLastWriteInfo(
      expectedRowSchema = table.schema,
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD))))

    checkLastWriteLog(
      writeWithMetadataLogEntry(metadata = Row("hr", 1), data = Row(3, 3, "hr")),
      writeWithMetadataLogEntry(metadata = Row("hr", 2), data = Row(4, 4, "hr")))
  }

  test("delete with nondeterministic conditions") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[AnalysisException](
        sql(s"DELETE FROM $tableNameAsString WHERE id <= 1 AND rand() > 0.5")),
      condition = "INVALID_NON_DETERMINISTIC_EXPRESSIONS",
      parameters = Map(
        "sqlExprs" -> "\"((id <= 1) AND (rand() > 0.5))\", \"((id <= 1) AND (rand() > 0.5))\""),
      context = ExpectedContext(
        fragment = "DELETE FROM cat.ns1.test_table WHERE id <= 1 AND rand() > 0.5",
        start = 0,
        stop = 60)
    )
  }

  test("delete with IN predicate and runtime group filtering") {
    createAndInitTable("id INT, salary INT, dep STRING",
      """{ "id": 1, "salary": 300, "dep": 'hr' }
        |{ "id": 2, "salary": 150, "dep": 'software' }
        |{ "id": 3, "salary": 120, "dep": 'hr' }
        |""".stripMargin)

    executeAndCheckScans(
      s"DELETE FROM $tableNameAsString WHERE salary IN (300, 400, 500)",
      primaryScanSchema = "id INT, salary INT, dep STRING, _partition STRING, index INT",
      groupFilterScanSchema = Some("salary INT, dep STRING"))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 150, "software") :: Row(3, 120, "hr") :: Nil)

    checkReplacedPartitions(Seq("hr"))
  }

  test("delete with subqueries and runtime group filtering") {
    withTempView("deleted_id", "deleted_dep") {
      createAndInitTable("id INT, salary INT, dep STRING",
        """{ "id": 1, "salary": 300, "dep": 'hr' }
          |{ "id": 2, "salary": 150, "dep": 'software' }
          |{ "id": 3, "salary": 120, "dep": 'hr' }
          |{ "id": 4, "salary": 150, "dep": 'software' }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(2), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      val deletedDepDF = Seq(Some("software"), None).toDF()
      deletedDepDF.createOrReplaceTempView("deleted_dep")

      executeAndCheckScans(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id IN (SELECT * FROM deleted_id)
           | AND
           | dep IN (SELECT * FROM deleted_dep)
           |""".stripMargin,
        primaryScanSchema = "id INT, salary INT, dep STRING, _partition STRING, index INT",
        groupFilterScanSchema = Some("id INT, dep STRING"))

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 300, "hr") :: Row(3, 120, "hr") :: Row(4, 150, "software") :: Nil)

      checkReplacedPartitions(Seq("software"))
    }
  }

  test("delete runtime group filtering (DPP enabled)") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
      checkDeleteRuntimeGroupFiltering()
    }
  }

  test("delete runtime group filtering (DPP disabled)") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
      checkDeleteRuntimeGroupFiltering()
    }
  }

  test("delete runtime group filtering (AQE enabled)") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      checkDeleteRuntimeGroupFiltering()
    }
  }

  test("delete runtime group filtering (AQE disabled)") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      checkDeleteRuntimeGroupFiltering()
    }
  }

  private def checkDeleteRuntimeGroupFiltering(): Unit = {
    withTempView("deleted_id") {
      createAndInitTable("id INT, salary INT, dep STRING",
        """{ "id": 1, "salary": 300, "dep": 'hr' }
          |{ "id": 2, "salary": 150, "dep": 'software' }
          |{ "id": 3, "salary": 120, "dep": 'hr' }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(1), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      executeAndCheckScans(
        s"DELETE FROM $tableNameAsString WHERE id IN (SELECT * FROM deleted_id)",
        primaryScanSchema = "id INT, salary INT, dep STRING, _partition STRING, index INT",
        groupFilterScanSchema = Some("id INT, dep STRING"))

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, 150, "software") :: Row(3, 120, "hr") :: Nil)

      checkReplacedPartitions(Seq("hr"))
    }
  }
}
