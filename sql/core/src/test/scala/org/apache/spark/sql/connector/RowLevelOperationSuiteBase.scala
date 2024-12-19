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

import org.apache.spark.sql.{DataFrame, Encoders, QueryTest, Row}
import org.apache.spark.sql.QueryTest.sameRows
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, GenericRowWithSchema}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Delete, Identifier, InMemoryRowLevelOperationTable, InMemoryRowLevelOperationTableCatalog, Insert, MetadataColumn, Operation, Reinsert, Update, Write}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{identity, reference}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.{InSubqueryExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}
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

  protected final val PK_FIELD = StructField("pk", IntegerType, nullable = false)
  protected final val PARTITION_FIELD = StructField(
    "_partition",
    StringType,
    nullable = false,
    metadata = new MetadataBuilder()
      .putString(METADATA_COL_ATTR_KEY, "_partition")
      .putString("comment", "Partition key used to store the row")
      .putBoolean(MetadataColumn.PRESERVE_ON_UPDATE, value = true)
      .putBoolean(MetadataColumn.PRESERVE_ON_REINSERT, value = true)
      .build())
  protected final val PARTITION_FIELD_NULLABLE = PARTITION_FIELD.copy(nullable = true)
  protected final val INDEX_FIELD = StructField(
    "index",
    IntegerType,
    nullable = false,
    metadata = new MetadataBuilder()
      .putString(METADATA_COL_ATTR_KEY, "index")
      .putString("comment", "Metadata column used to conflict with a data column")
      .putBoolean(MetadataColumn.PRESERVE_ON_DELETE, value = false)
      .putBoolean(MetadataColumn.PRESERVE_ON_UPDATE, value = false)
      .build())
  protected final val INDEX_FIELD_NULLABLE = INDEX_FIELD.copy(nullable = true)

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

  protected def checkLastWriteInfo(
      expectedRowSchema: StructType = new StructType(),
      expectedRowIdSchema: Option[StructType] = None,
      expectedMetadataSchema: Option[StructType] = None): Unit = {
    val info = table.lastWriteInfo
    assert(info.schema == expectedRowSchema, "row schema must match")
    val actualRowIdSchema = Option(info.rowIdSchema.orElse(null))
    assert(actualRowIdSchema == expectedRowIdSchema, "row ID schema must match")
    val actualMetadataSchema = Option(info.metadataSchema.orElse(null))
    assert(actualMetadataSchema == expectedMetadataSchema, "metadata schema must match")
  }

  protected def checkLastWriteLog(expectedEntries: WriteLogEntry*): Unit = {
    val entryType = new StructType()
      .add(StructField("operation", StringType))
      .add(StructField("id", IntegerType))
      .add(StructField(
        "metadata",
        new StructType(Array(
          StructField("_partition", StringType),
          StructField("_index", IntegerType)))))
      .add(StructField("data", table.schema))

    val expectedEntriesAsRows = expectedEntries.map { entry =>
      new GenericRowWithSchema(
        values = Array(
          entry.operation.toString,
          entry.id.orNull,
          entry.metadata.orNull,
          entry.data.orNull),
        schema = entryType)
    }

    val encoder = ExpressionEncoder(entryType)
    val deserializer = encoder.resolveAndBind().createDeserializer()
    val actualEntriesAsRows = table.lastWriteLog.map(deserializer)

    sameRows(expectedEntriesAsRows, actualEntriesAsRows) match {
      case Some(errMsg) => fail(s"Write log contains unexpected entries: $errMsg")
      case None => // OK
    }
  }

  protected def writeLogEntry(data: Row): WriteLogEntry = {
    WriteLogEntry(operation = Write, data = Some(data))
  }

  protected def writeWithMetadataLogEntry(metadata: Row, data: Row): WriteLogEntry = {
    WriteLogEntry(operation = Write, metadata = Some(metadata), data = Some(data))
  }

  protected def deleteWriteLogEntry(id: Int, metadata: Row): WriteLogEntry = {
    WriteLogEntry(operation = Delete, id = Some(id), metadata = Some(metadata))
  }

  protected def updateWriteLogEntry(id: Int, metadata: Row, data: Row): WriteLogEntry = {
    WriteLogEntry(operation = Update, id = Some(id), metadata = Some(metadata), data = Some(data))
  }

  protected def reinsertWriteLogEntry(metadata: Row, data: Row): WriteLogEntry = {
    WriteLogEntry(operation = Reinsert, metadata = Some(metadata), data = Some(data))
  }

  protected def insertWriteLogEntry(data: Row): WriteLogEntry = {
    WriteLogEntry(operation = Insert, data = Some(data))
  }

  case class WriteLogEntry(
      operation: Operation,
      id: Option[Int] = None,
      metadata: Option[Row] = None,
      data: Option[Row] = None)
}
