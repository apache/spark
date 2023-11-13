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

import java.util.Collections

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{DataFrame, Encoders, QueryTest}
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Identifier, InMemoryRowLevelOperationTable, InMemoryRowLevelOperationTableCatalog}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{identity, reference}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.{InSubqueryExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

abstract class RowLevelOperationSuiteBase
  extends QueryTest with SharedSparkSession with BeforeAndAfter with AdaptiveSparkPlanHelper {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  before {
    spark.conf.set("spark.sql.catalog.cat", classOf[InMemoryRowLevelOperationTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf("spark.sql.catalog.cat")
  }

  protected val namespace: Array[String] = Array("ns1")
  protected val ident: Identifier = Identifier.of(namespace, "test_table")
  protected val tableNameAsString: String = "cat." + ident.toString

  protected def extraTableProps: java.util.Map[String, String] = {
    Collections.emptyMap[String, String]
  }

  protected def catalog: InMemoryRowLevelOperationTableCatalog = {
    val catalog = spark.sessionState.catalogManager.catalog("cat")
    catalog.asTableCatalog.asInstanceOf[InMemoryRowLevelOperationTableCatalog]
  }

  protected def table: InMemoryRowLevelOperationTable = {
    catalog.loadTable(ident).asInstanceOf[InMemoryRowLevelOperationTable]
  }

  protected def createTable(schemaString: String): Unit = {
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schemaString))
    createTable(columns)
  }

  protected def createTable(columns: Array[Column]): Unit = {
    val transforms = Array[Transform](identity(reference(Seq("dep"))))
    catalog.createTable(ident, columns, transforms, extraTableProps)
  }

  protected def createAndInitTable(schemaString: String, jsonData: String): Unit = {
    createTable(schemaString)
    append(schemaString, jsonData)
  }

  protected def append(schemaString: String, jsonData: String): Unit = {
    withSQLConf(SQLConf.LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION.key -> "true") {
      val df = toDF(jsonData, schemaString)
      df.coalesce(1).writeTo(tableNameAsString).append()
    }
  }

  private def toDF(jsonData: String, schemaString: String = null): DataFrame = {
    val jsonRows = jsonData.split("\\n").filter(str => str.trim.nonEmpty)
    val jsonDS = spark.createDataset(jsonRows.toImmutableArraySeq)(Encoders.STRING)
    if (schemaString == null) {
      spark.read.json(jsonDS)
    } else {
      spark.read.schema(schemaString).json(jsonDS)
    }
  }

  // executes an operation and keeps the executed plan
  protected def executeAndKeepPlan(func: => Unit): SparkPlan = {
    var executedPlan: SparkPlan = null

    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        executedPlan = qe.executedPlan
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
      }
    }
    spark.listenerManager.register(listener)

    func

    sparkContext.listenerBus.waitUntilEmpty()

    stripAQEPlan(executedPlan)
  }

  protected def executeAndCheckScan(
      query: String,
      expectedScanSchema: String): Unit = {

    val executedPlan = executeAndKeepPlan {
      sql(query)
    }

    val scan = collect(executedPlan) {
      case s: BatchScanExec => s
    }.head
    assert(DataTypeUtils.sameType(scan.schema, StructType.fromDDL(expectedScanSchema)))
  }

  protected def executeAndCheckScans(
      query: String,
      primaryScanSchema: String,
      groupFilterScanSchema: Option[String]): Unit = {

    val executedPlan = executeAndKeepPlan {
      sql(query)
    }

    val primaryScan = collect(executedPlan) {
      case s: BatchScanExec => s
    }.head
    assert(DataTypeUtils.sameType(primaryScan.schema, StructType.fromDDL(primaryScanSchema)))

    val groupFilterScan = primaryScan.runtimeFilters match {
      case Seq(DynamicPruningExpression(child: InSubqueryExec)) =>
        find(child.plan) {
          case _: BatchScanExec => true
          case _ => false
        }
      case _ =>
        None
    }

    groupFilterScanSchema match {
      case Some(filterSchema) =>
        assert(groupFilterScan.isDefined, "could not find group filter scan")
        assert(DataTypeUtils.sameType(groupFilterScan.get.schema, StructType.fromDDL(filterSchema)))

      case None =>
        assert(groupFilterScan.isEmpty, "should not be any group filter scans")
    }
  }

  protected def checkReplacedPartitions(expectedPartitions: Seq[Any]): Unit = {
    val actualPartitions = table.replacedPartitions.map {
      case Seq(partValue: UTF8String) => partValue.toString
      case Seq(partValue) => partValue
      case other => fail(s"expected only one partition value: $other" )
    }
    assert(actualPartitions == expectedPartitions, "replaced partitions must match")
  }
}
