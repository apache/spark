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

import org.apache.spark.sql.QueryTest.withQueryExecutionsCaptured
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTable, InMemoryTableCatalog, StagedTable, StagingInMemoryTableCatalog, TableInfo}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomSumMetric, CustomTaskMetric}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec, AtomicReplaceTableExec, CreateTableAsSelectExec, ReplaceTableAsSelectExec, ReplaceTableExec}

class StagingInMemoryTableCatalogWithMetrics extends StagingInMemoryTableCatalog {

  case class TestSupportedCommitMetric(name: String, description: String) extends CustomSumMetric

  override def supportedCustomMetrics(): Array[CustomMetric] = Array(
    TestSupportedCommitMetric("numFiles", "number of written files"),
    TestSupportedCommitMetric("numOutputRows", "number of output rows"),
    TestSupportedCommitMetric("numOutputBytes", "written output"))

  private class TestStagedTableWithMetric(
      ident: Identifier,
      delegateTable: InMemoryTable
  ) extends TestStagedTable(ident, delegateTable) with StagedTable {

    private var stagedChangesCommitted = false

    override def commitStagedChanges(): Unit = {
      tables.put(ident, delegateTable)
      stagedChangesCommitted = true
    }

    override def reportDriverMetrics: Array[CustomTaskMetric] = {
      assert(stagedChangesCommitted)
      StagingInMemoryTableCatalogWithMetrics.testMetrics
    }
  }

  override def stageCreate(ident: Identifier, tableInfo: TableInfo): StagedTable =
    new TestStagedTableWithMetric(
      ident,
      new InMemoryTable(s"$name.${ident.quoted}",
        tableInfo.schema(), tableInfo.partitions(), tableInfo.properties()))

  override def stageReplace(ident: Identifier, tableInfo: TableInfo): StagedTable =
    stageCreate(ident, tableInfo)

  override def stageCreateOrReplace(ident: Identifier, tableInfo: TableInfo) : StagedTable =
    stageCreate(ident, tableInfo)
}

object StagingInMemoryTableCatalogWithMetrics {

  case class TestCustomTaskMetric(name: String, value: Long) extends CustomTaskMetric

  val testMetrics: Array[CustomTaskMetric] = Array(
    TestCustomTaskMetric("numFiles", 1337),
    TestCustomTaskMetric("numOutputRows", 1338),
    TestCustomTaskMetric("numOutputBytes", 1339))
}

class DataSourceV2MetricsSuite extends DatasourceV2SQLBase {

  private val testCatalog = "test_catalog"
  private val atomicTestCatalog = "atomic_test_catalog"
  private val nonExistingTable = "non_existing_table"
  private val existingTable = "existing_table"

  private def captureStagedTableWrite(thunk: => Unit): SparkPlan = {
    val physicalPlans = withQueryExecutionsCaptured(spark)(thunk).map(_.executedPlan)
    val stagedTableWrites = physicalPlans.filter {
      case _: AtomicCreateTableAsSelectExec | _: CreateTableAsSelectExec |
           _: AtomicReplaceTableAsSelectExec | _: ReplaceTableAsSelectExec |
           _: AtomicReplaceTableExec | _: ReplaceTableExec => true
      case _ => false
    }
    assert(stagedTableWrites.size === 1)
    stagedTableWrites.head
  }

  private def commands: Seq[String => Unit] = Seq(
    { catalogName =>
      sql(s"CREATE TABLE $catalogName.$nonExistingTable AS SELECT * FROM $existingTable") },
    { catalogName =>
      spark.table(existingTable).write.saveAsTable(s"$catalogName.$nonExistingTable") },
    { catalogName =>
      sql(s"CREATE OR REPLACE TABLE $catalogName.$nonExistingTable " +
          s"AS SELECT * FROM $existingTable") },
    { catalogName =>
      sql(s"REPLACE TABLE $catalogName.$existingTable AS SELECT * FROM $existingTable") },
    { catalogName =>
        spark.table(existingTable)
          .write.mode("overwrite").saveAsTable(s"$catalogName.$existingTable") },
    { catalogName =>
      sql(s"REPLACE TABLE $catalogName.$existingTable (id bigint, data string)") })

  private def catalogCommitMetricsTest(
      testName: String, catalogName: String)(testFunction: SparkPlan => Unit): Unit = {
    commands.foreach { command =>
      test(s"$testName - $command") {
        registerCatalog(testCatalog, classOf[InMemoryTableCatalog])
        registerCatalog(atomicTestCatalog, classOf[StagingInMemoryTableCatalogWithMetrics])
        withTable(existingTable, s"$catalogName.$existingTable") {
          sql(s"CREATE TABLE $existingTable (id bigint, data string)")
          sql(s"CREATE TABLE $catalogName.$existingTable (id bigint, data string)")

          testFunction(captureStagedTableWrite(command(catalogName)))
        }
      }
    }
  }

  catalogCommitMetricsTest(
      "No metrics in the plan if the catalog does not support them", testCatalog) { sparkPlan =>
    val metrics = sparkPlan.metrics

    assert(metrics.isEmpty)
  }

  catalogCommitMetricsTest(
      "Plan metrics values are the values from the catalog", atomicTestCatalog) { sparkPlan =>
    val metrics = sparkPlan.metrics

    assert(metrics.size === StagingInMemoryTableCatalogWithMetrics.testMetrics.length)
    StagingInMemoryTableCatalogWithMetrics.testMetrics.foreach(customTaskMetric =>
      assert(metrics(customTaskMetric.name()).value === customTaskMetric.value()))
  }
}
