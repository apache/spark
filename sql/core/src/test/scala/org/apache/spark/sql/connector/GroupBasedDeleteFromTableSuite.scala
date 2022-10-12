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
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

class GroupBasedDeleteFromTableSuite extends DeleteFromTableSuiteBase {

  import testImplicits._

  test("delete with IN predicate and runtime group filtering") {
    createAndInitTable("id INT, salary INT, dep STRING",
      """{ "id": 1, "salary": 300, "dep": 'hr' }
        |{ "id": 2, "salary": 150, "dep": 'software' }
        |{ "id": 3, "salary": 120, "dep": 'hr' }
        |""".stripMargin)

    executeDeleteAndCheckScans(
      s"DELETE FROM $tableNameAsString WHERE salary IN (300, 400, 500)",
      primaryScanSchema = "id INT, salary INT, dep STRING, _partition STRING",
      groupFilterScanSchema = "salary INT, dep STRING")

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

      executeDeleteAndCheckScans(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id IN (SELECT * FROM deleted_id)
           | AND
           | dep IN (SELECT * FROM deleted_dep)
           |""".stripMargin,
        primaryScanSchema = "id INT, salary INT, dep STRING, _partition STRING",
        groupFilterScanSchema = "id INT, dep STRING")

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

      executeDeleteAndCheckScans(
        s"DELETE FROM $tableNameAsString WHERE id IN (SELECT * FROM deleted_id)",
        primaryScanSchema = "id INT, salary INT, dep STRING, _partition STRING",
        groupFilterScanSchema = "id INT, dep STRING")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, 150, "software") :: Row(3, 120, "hr") :: Nil)

      checkReplacedPartitions(Seq("hr"))
    }
  }

  private def executeDeleteAndCheckScans(
      query: String,
      primaryScanSchema: String,
      groupFilterScanSchema: String): Unit = {

    val executedPlan = executeAndKeepPlan {
      sql(query)
    }

    val primaryScan = collect(executedPlan) {
      case s: BatchScanExec => s
    }.head
    assert(primaryScan.schema.sameType(StructType.fromDDL(primaryScanSchema)))

    primaryScan.runtimeFilters match {
      case Seq(DynamicPruningExpression(child: InSubqueryExec)) =>
        val groupFilterScan = collect(child.plan) {
          case s: BatchScanExec => s
        }.head
        assert(groupFilterScan.schema.sameType(StructType.fromDDL(groupFilterScanSchema)))

      case _ =>
        fail("could not find group filter scan")
    }
  }

  private def checkReplacedPartitions(expectedPartitions: Seq[Any]): Unit = {
    val actualPartitions = table.replacedPartitions.map {
      case Seq(partValue: UTF8String) => partValue.toString
      case Seq(partValue) => partValue
      case other => fail(s"expected only one partition value: $other" )
    }
    assert(actualPartitions == expectedPartitions, "replaced partitions must match")
  }
}
