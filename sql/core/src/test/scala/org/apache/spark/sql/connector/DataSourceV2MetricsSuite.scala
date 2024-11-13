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

import java.util

import scala.jdk.CollectionConverters.MapHasAsJava

import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Identifier, InMemoryTable, InMemoryTableCatalog, StagedTable, StagedTableWithCommitMetrics, StagingInMemoryTableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.CommandResultExec

class StagingInMemoryTableCatalogWithMetrics extends StagingInMemoryTableCatalog {

  override def supportsCommitMetrics(): Boolean = true

  private class TestStagedTableWithMetric(
      ident: Identifier,
      delegateTable: InMemoryTable
  ) extends TestStagedTable(ident, delegateTable) with StagedTableWithCommitMetrics {

    override def commitStagedChanges(): Unit = {
      tables.put(ident, delegateTable)
    }

    override def getCommitMetrics: util.Map[String, java.lang.Long] = {
      StagingInMemoryTableCatalogWithMetrics.testMetrics
        .asInstanceOf[Map[String, java.lang.Long]].asJava
    }
  }

  override def stageCreate(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    new TestStagedTableWithMetric(
      ident,
      new InMemoryTable(s"$name.${ident.quoted}",
        CatalogV2Util.v2ColumnsToStructType(columns), partitions, properties))
  }

  override def stageReplace(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    stageCreate(ident, columns, partitions, properties)

  override def stageCreateOrReplace(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    stageCreate(ident, columns, partitions, properties)
}

object StagingInMemoryTableCatalogWithMetrics {

  val testMetrics: Map[String, Long] = Map(
    "numFiles" -> 1337,
    "numOutputRows" -> 1338,
    "numOutputBytes" -> 1339)
}

class DataSourceV2MetricsSuite extends DatasourceV2SQLBase {

  private val testCatalog = "test_catalog"
  private val atomicTestCatalog = "atomic_test_catalog"
  private val nonExistingTable = "non_existing_table"
  private val existingTable = "existing_table"

  private def commands(catalogName: String) = Seq(
    s"CREATE TABLE $catalogName.$nonExistingTable AS SELECT * FROM $existingTable",
    s"CREATE OR REPLACE TABLE $catalogName.$nonExistingTable AS SELECT * FROM $existingTable",
    s"REPLACE TABLE $catalogName.$existingTable AS SELECT * FROM $existingTable",
    s"REPLACE TABLE $catalogName.$existingTable (id bigint, data string)")

  private def catalogCommitMetricsTest(
      testName: String, catalogName: String)(testFunction: String => Unit): Unit = {
    commands(catalogName).foreach { command =>
      test(s"$testName - $command") {
        registerCatalog(testCatalog, classOf[InMemoryTableCatalog])
        registerCatalog(atomicTestCatalog, classOf[StagingInMemoryTableCatalogWithMetrics])
        withTable(existingTable, s"$catalogName.$existingTable") {
          sql(s"CREATE TABLE $existingTable (id bigint, data string)")
          sql(s"CREATE TABLE $catalogName.$existingTable (id bigint, data string)")

          testFunction(command)
        }
      }
    }
  }

  catalogCommitMetricsTest(
      "No metrics in the plan if the catalog does not support them", testCatalog) { command =>
    val df = sql(command)
    val metrics = df.queryExecution.executedPlan match {
      case c: CommandResultExec => c.commandPhysicalPlan.metrics
    }

    assert(metrics.isEmpty)
  }

  catalogCommitMetricsTest(
      "Plan metrics values are the values from the catalog", atomicTestCatalog) { command =>
    val df = sql(command)
    val metrics = df.queryExecution.executedPlan match {
      case c: CommandResultExec => c.commandPhysicalPlan.metrics
    }

    assert(metrics.size === StagingInMemoryTableCatalogWithMetrics.testMetrics.size)
    StagingInMemoryTableCatalogWithMetrics.testMetrics.foreach { case (k, v) =>
      assert(metrics(k).value === v)
    }
  }
}
