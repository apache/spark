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

package org.apache.spark.sql.execution

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, DataWritingCommandExec}
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * Stores information about a SQL SparkPlan.
 */
@DeveloperApi
class SparkPlanInfo(
    val nodeName: String,
    val simpleString: String,
    val children: Seq[SparkPlanInfo],
    val metadata: Map[String, String],
    val metrics: Seq[SQLMetricInfo]) {

  override def hashCode(): Int = {
    // hashCode of simpleString should be good enough to distinguish the plans from each other
    // within a plan
    simpleString.hashCode
  }

  override def equals(other: Any): Boolean = other match {
    case o: SparkPlanInfo =>
      nodeName == o.nodeName && simpleString == o.simpleString && children == o.children
    case _ => false
  }
}

private[execution] object SparkPlanInfo {

  def fromSparkPlan(plan: SparkPlan): SparkPlanInfo = {
    val children = plan match {
      case ReusedExchangeExec(_, child) => child :: Nil
      case _ => plan.children ++ plan.subqueries
    }
    val metrics = plan.metrics.toSeq.map { case (key, metric) =>
      new SQLMetricInfo(metric.name.getOrElse(key), metric.id, metric.metricType)
    }

    def makeOutputMetadata(
        path: Option[Path],
        outputColumnNames: Seq[String]): Map[String, String] = {
      val pathString = path match {
        case Some(p) if p != null => p.toString
        case _ => ""
      }
      Map("OutputPath" -> pathString,
        "OutputColumnNames" -> outputColumnNames.mkString("[", ", ", "]")
      )
    }

    def reflectTable(write: DataWritingCommand, className: String, field: String): CatalogTable = {
      val tableField = Utils.classForName(className).getDeclaredField(field)
      tableField.setAccessible(true)
      tableField.get(write).asInstanceOf[CatalogTable]
    }

    // dump the file scan metadata (e.g file path) to event log
    var metadata = plan match {
      case fileScan: FileSourceScanExec => fileScan.metadata
      case DataWritingCommandExec(i: InsertIntoHadoopFsRelationCommand, _) =>
        makeOutputMetadata(Some(i.outputPath), i.outputColumnNames)
      case DataWritingCommandExec(c: CreateDataSourceTableAsSelectCommand, _) =>
        makeOutputMetadata(c.table.storage.locationUri.map(new Path(_)), c.outputColumnNames)
      case DataWritingCommandExec(d: DataWritingCommand, _)
          if d.getClass.getCanonicalName == CREATE_HIVE_TABLE_AS_SELECT_COMMAND =>
        val table = reflectTable(d, CREATE_HIVE_TABLE_AS_SELECT_COMMAND, "tableDesc")
        makeOutputMetadata(table.storage.locationUri.map(new Path(_)), d.outputColumnNames)
      case DataWritingCommandExec(d: DataWritingCommand, _)
          if d.getClass.getCanonicalName == INSERT_INTO_HIVE_DIR_COMMAND =>
        val table = reflectTable(d, INSERT_INTO_HIVE_DIR_COMMAND, "table")
        makeOutputMetadata(table.storage.locationUri.map(new Path(_)), d.outputColumnNames)
      case DataWritingCommandExec(d: DataWritingCommand, _)
        if d.getClass.getCanonicalName == INSERT_INTO_HIVE_TABLE =>
        val table = reflectTable(d, INSERT_INTO_HIVE_TABLE, "table")
        makeOutputMetadata(table.storage.locationUri.map(new Path(_)), d.outputColumnNames)
      case _ => Map[String, String]()
    }

    new SparkPlanInfo(plan.nodeName, plan.simpleString, children.map(fromSparkPlan),
      metadata, metrics)
  }

  private val CREATE_HIVE_TABLE_AS_SELECT_COMMAND =
    "org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand"
  private val INSERT_INTO_HIVE_DIR_COMMAND =
    "org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand"
  private val INSERT_INTO_HIVE_TABLE =
    "org.apache.spark.sql.hive.execution.InsertIntoHiveTable"
}
