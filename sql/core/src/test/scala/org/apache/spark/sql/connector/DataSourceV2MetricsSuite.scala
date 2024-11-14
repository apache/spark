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

import scala.jdk.CollectionConverters.IterableHasAsJava

import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Identifier, InMemoryTable, InMemoryTableCatalog, StagedTable, StagedTableWithCommitMetrics, StagingInMemoryTableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomSumMetric, CustomTaskMetric}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec, AtomicReplaceTableExec, CreateTableAsSelectExec, ReplaceTableAsSelectExec, ReplaceTableExec}
import org.apache.spark.sql.util.QueryExecutionListener

class StagingInMemoryTableCatalogWithMetrics extends StagingInMemoryTableCatalog {

  override def supportedCommitMetrics(): java.lang.Iterable[CustomMetric] = java.util.List.of(
    new CustomSumMetric {
      override def name(): String = "numFiles"
      override def description(): String = "number of written files"
    },
    new CustomSumMetric {
      override def name(): String = "numOutputRows"
      override def description(): String = "number of output rows"
    },
    new CustomSumMetric {
      override def name(): String = "numOutputBytes"
      override def description(): String = "written output"
    })

  private class TestStagedTableWithMetric(
      ident: Identifier,
      delegateTable: InMemoryTable
  ) extends TestStagedTable(ident, delegateTable) with StagedTableWithCommitMetrics {

    override def commitStagedChanges(): Unit = {
      tables.put(ident, delegateTable)
    }

    override def getCommitMetrics: java.lang.Iterable[CustomTaskMetric] = {
      StagingInMemoryTableCatalogWithMetrics.testMetrics.asJava
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

  val testMetrics: Seq[CustomTaskMetric] = Seq(
    new CustomTaskMetric {
      override def name(): String = "numFiles"
      override def value(): Long = 1337
    },
    new CustomTaskMetric {
      override def name(): String = "numOutputRows"
      override def value(): Long = 1338
    },
    new CustomTaskMetric {
      override def name(): String = "numOutputBytes"
      override def value(): Long = 1339
    })
}

class DataSourceV2MetricsSuite extends DatasourceV2SQLBase {

  private val testCatalog = "test_catalog"
  private val atomicTestCatalog = "atomic_test_catalog"
  private val nonExistingTable = "non_existing_table"
  private val existingTable = "existing_table"

  private def captureExecutedPlan(command: => Unit): SparkPlan = {
    var commandPlan: Option[SparkPlan] = None
    var otherPlans = Seq.empty[SparkPlan]

    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        qe.executedPlan match {
          case _: CreateTableAsSelectExec | _: AtomicCreateTableAsSelectExec
               | _: ReplaceTableAsSelectExec | _: AtomicReplaceTableAsSelectExec
               | _: ReplaceTableExec | _: AtomicReplaceTableExec =>
            assert(commandPlan.isEmpty)
            commandPlan = Some(qe.executedPlan)
          case _ =>
            otherPlans :+= qe.executedPlan
        }
      }

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    spark.listenerManager.register(listener)

    command

    sparkContext.listenerBus.waitUntilEmpty()

    assert(commandPlan.nonEmpty, s"No command plan found, but saw $otherPlans")
    commandPlan.get
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

          testFunction(captureExecutedPlan(command(catalogName)))
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

    assert(metrics.size === StagingInMemoryTableCatalogWithMetrics.testMetrics.size)
    StagingInMemoryTableCatalogWithMetrics.testMetrics.foreach(customTaskMetric =>
      assert(metrics(customTaskMetric.name()).value === customTaskMetric.value()))
  }
}
