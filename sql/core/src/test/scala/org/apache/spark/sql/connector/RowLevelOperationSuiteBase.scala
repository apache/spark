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

import scala.jdk.CollectionConverters._

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.QueryTest.{sameRows, withQueryExecutionsCaptured}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression, GenericRowWithSchema}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReplaceData, RowLevelWrite, WriteDelta}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Delete, Identifier, InMemoryRowLevelOperationTable, InMemoryRowLevelOperationTableCatalog, Insert, MetadataColumn, Operation, Reinsert, RowLevelOperationWithOptions, Table, TableInfo, Txn, TxnTable, Update, Write, WriteColumnUpdate}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{identity, reference}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.{InSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

abstract class RowLevelOperationSuiteBase
  extends SharedSparkSession with BeforeAndAfter with AdaptiveSparkPlanHelper {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  before {
    spark.conf.set("spark.sql.catalog.cat", classOf[InMemoryRowLevelOperationTableCatalog].getName)
    spark.conf.set(
      "spark.sql.catalog.cat.tableStateOptionKeys", "load-option,targetLoadOption")
  }

  after {
    catalog.nextTxnRejectRegisteredScansAttempt = false
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf("spark.sql.catalog.cat")
    spark.sessionState.conf.unsetConf("spark.sql.catalog.cat.tableStateOptionKeys")
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
  protected final val DEP_FIELD = StructField("dep", StringType, nullable = true)

  protected val namespace: Array[String] = Array("ns1")
  protected val ident: Identifier = Identifier.of(namespace, "test_table")
  protected val tableNameAsString: String = "cat." + ident.toString
  protected val sourceNameAsString: String = "cat.ns1.source_table"

  protected def extraTableProps: java.util.Map[String, String] = {
    Collections.emptyMap[String, String]
  }

  /** True for the *NoMetadata* test variants - the writer doesn't request any required
   * distribution / ordering and so MergeRowsExec / writer can run in the same stage as the
   * preceding join. */
  protected def noMetadata: Boolean =
    extraTableProps.getOrDefault("no-metadata", "false") == "true"

  // `extraTableProps` as the TBLPROPERTIES clause of a CREATE TABLE statement
  protected def extraTablePropertiesClause: String = {
    if (extraTableProps.isEmpty) {
      ""
    } else {
      extraTableProps.asScala
        .map { case (key, value) => s"'$key' = '$value'" }
        .mkString("TBLPROPERTIES (", ", ", ")")
    }
  }

  protected def catalog: InMemoryRowLevelOperationTableCatalog = {
    val catalog = spark.sessionState.catalogManager.catalog("cat")
    catalog.asTableCatalog.asInstanceOf[InMemoryRowLevelOperationTableCatalog]
  }

  protected def table: InMemoryRowLevelOperationTable = {
    // Use liveTable, not loadTable, because loadTable returns a snapshot copy.
    // Tests mutate state through this accessor (e.g., `table.increaseVersion()`) and need
    // those mutations to be observable to subsequent loads.
    catalog.liveTable(ident).asInstanceOf[InMemoryRowLevelOperationTable]
  }

  protected def createTable(schemaString: String): Unit = {
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schemaString))
    createTable(columns)
  }

  protected def createTable(schemaString: String, transforms: Array[Transform]): Unit = {
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schemaString))
    createTable(columns, transforms)
  }

  protected def createTable(columns: Array[Column]): Unit = {
    val transforms = Array[Transform](identity(reference(Seq("dep"))))
    createTable(columns, transforms)
  }

  protected def createTable(
      columns: Array[Column],
      transforms: Array[Transform],
      tableProps: java.util.Map[String, String] = extraTableProps): Unit = {
    val tableInfo = new TableInfo.Builder()
      .withColumns(columns)
      .withPartitions(transforms)
      .withProperties(tableProps)
      .build()
    catalog.createTable(ident, tableInfo)
  }

  protected def createAndInitTable(schemaString: String, jsonData: String): Unit = {
    createAndInitTable(schemaString, jsonData, extraTableProps)
  }

  protected def createAndInitTable(
      schemaString: String,
      jsonData: String,
      tableProps: java.util.Map[String, String]): Unit = {
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schemaString))
    createTable(columns, Array[Transform](identity(reference(Seq("dep")))), tableProps)
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
    withQueryExecutionsCaptured(spark)(func) match {
      case Seq(qe) => stripAQEPlan(qe.executedPlan)
      case other => fail(s"expected only one query execution, but got ${other.size}")
    }
  }

  protected def executeTransaction(func: => Unit): (Txn, Map[String, TxnTable]) = {
    val tables = withQueryExecutionsCaptured(spark)(func).flatMap { qe =>
      collectWithSubqueries(qe.executedPlan) {
        case BatchScanExec(_, _, _, _, table: TxnTable, _, _, _) => table
        case BatchScanExec(_, _, _, _, RowLevelOperationTable(table: TxnTable, _), _, _, _) => table
      }
    }
    (catalog.lastTransaction, indexByName(tables))
  }

  protected def indexByName[T <: Table](tables: Seq[T]): Map[String, T] = {
    tables.groupBy(_.name).map {
      case (name, sameNameTables) =>
        val Seq(table) = sameNameTables.distinct
        name -> table
    }
  }

  // executes an operation and extracts conditions from ReplaceData or WriteDelta
  protected def executeAndKeepConditions(func: => Unit): (Expression, Option[Expression]) = {
    val Seq(qe) = withQueryExecutionsCaptured(spark)(func)
    qe.optimizedPlan.collectFirst {
      case rd: ReplaceData => (rd.condition, rd.groupFilterCondition)
      case wd: WriteDelta => (wd.condition, wd.groupFilterCondition)
    }.getOrElse(fail("couldn't find row-level operation in optimized plan"))
  }

  // executes an operation and extracts the analyzed ReplaceData or WriteDelta
  protected def executeAndKeepAnalyzedWrite(func: => Unit): RowLevelWrite = {
    val Seq(qe) = withQueryExecutionsCaptured(spark)(func)
    qe.analyzed.collectFirst {
      case w: RowLevelWrite => w
    }.getOrElse(fail("couldn't find row-level operation in analyzed plan"))
  }

  // checks that a column-update write narrows only the write relation to the declared columns
  protected def checkColumnUpdateWrite(write: RowLevelWrite, declaredNames: String*): Unit = {
    assert(write.table.output.map(_.name) == declaredNames,
      s"write relation must have the declared columns: ${write.table.output}")
    assert(write.originalTable.output.map(_.name) == table.schema.fieldNames.toSeq,
      s"original table must have every column: ${write.originalTable.output}")
    assert(table.lastWriteInfo.schema.isEmpty,
      s"row schema must be empty: ${table.lastWriteInfo.schema}")
    checkLastColumnUpdateSchema(declaredNames: _*)
  }

  // Asserts the given SQL options reached every V2 layer that should carry them: the target
  // catalog load, the rewritten DataSourceV2Relation, the RowLevelOperationInfo passed to the
  // operation builder, and the write builder's LogicalWriteInfo.
  protected def checkRowLevelOperationOptions(
      func: => Unit,
      expectedOptions: (String, String)*): Unit = {
    val Seq(qe) = withQueryExecutionsCaptured(spark)(func)
    val writeRelation = qe.optimizedPlan.collectFirst {
      case rd: ReplaceData => rd.table
      case wd: WriteDelta => wd.table
    }.getOrElse(fail("couldn't find row-level operation in optimized plan"))
      .asInstanceOf[DataSourceV2Relation]
    assert(writeRelation.catalog.nonEmpty, "expected a catalog-backed V2 write relation")
    assert(writeRelation.identifier.nonEmpty, "expected a catalog-backed V2 write identifier")
    val operation = writeRelation.table.asInstanceOf[RowLevelOperationTable].operation
      .asInstanceOf[RowLevelOperationWithOptions]
    assertLastTransactionWriteLoadOptions(expectedOptions: _*)

    expectedOptions.foreach { case (key, value) =>
      assert(writeRelation.options.get(key) === value, s"relation option '$key'")
      assert(operation.options.get(key) === value, s"row-level operation option '$key'")
      assert(table.lastWriteInfo.options().get(key) === value, s"write option '$key'")
    }
  }

  protected def assertLastTransactionWriteLoadOptions(
      expectedOptions: (String, String)*): Unit = {
    val stateKeys = catalog.tableStateOptionKeys().asScala
    val expectedStateOptions = expectedOptions.filter { case (key, _) =>
      stateKeys.exists(_.equalsIgnoreCase(key))
    }
    val targetLoads = catalog.lastTransaction.catalog.loadTableCalls.filter {
      case (context, _) => !context.writePrivileges().isEmpty
    }
    assert(targetLoads.nonEmpty, "target loadTable did not receive write privileges")
    targetLoads.foreach { case (_, options) =>
      assert(options.size() === expectedStateOptions.size)
      expectedStateOptions.foreach { case (key, value) =>
        assert(options.get(key) === value, s"table-state option '$key'")
      }
      expectedOptions.diff(expectedStateOptions).foreach { case (key, _) =>
        assert(options.get(key) === null, s"non-state load option '$key'")
      }
    }
  }

  protected def assertNoScanPlanning(plan: LogicalPlan): Unit = {
    val relations = plan.collect { case r: DataSourceV2Relation => r }
    assert(relations.nonEmpty, "plan must contain relations")
    val scans = plan.collect { case s: DataSourceV2ScanRelation => s }
    assert(scans.isEmpty, "plan must not contain scan relations")
  }

  // the schemas of the scans of the target table, directly or through a row-level operation, in
  // the plan of the query
  private def executeAndCollectScanSchemas(query: String): Seq[StructType] = {
    val executedPlan = executeAndKeepPlan {
      sql(query)
    }
    val targetName = table.name
    val schemas = collect(executedPlan) {
      case scan: BatchScanExec if scan.table.name == targetName => scan.schema
    }
    assert(schemas.nonEmpty, s"no scan of the target table for: $query")
    schemas
  }

  /**
   * Executes the query and asserts the exact schema of its scans of the target table, including
   * the spelling of field names but ignoring nullability.
   */
  protected def executeAndCheckScan(
      query: String,
      expectedScanSchema: String): Unit = {
    executeAndCollectScanSchemas(query).foreach { schema =>
      assert(DataTypeUtils.equalsIgnoreNullability(schema, StructType.fromDDL(expectedScanSchema)),
        s"scan read ${schema.toDDL} but expected $expectedScanSchema")
    }
  }

  /** Executes the query and asserts that its table scans read all of the given columns. */
  protected def executeAndCheckScanIncludes(query: String, includedNames: String*): Unit = {
    executeAndCollectScanSchemas(query).foreach { schema =>
      val missing = includedNames.toSet.diff(schema.fieldNames.toSet)
      assert(missing.isEmpty,
        s"scan must include ${missing.mkString("[", ", ", "]")} " +
          s"but read ${schema.fieldNames.mkString("[", ", ", "]")}")
    }
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
    assert(DataTypeUtils.equalsIgnoreNullability(
      primaryScan.schema, StructType.fromDDL(primaryScanSchema)))

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
        assert(DataTypeUtils.equalsIgnoreNullability(
          groupFilterScan.get.schema, StructType.fromDDL(filterSchema)))

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
    assert(actualPartitions.toSet == expectedPartitions.toSet, "replaced partitions must match")
  }

  protected def checkLastWriteInfo(
      expectedRowSchema: StructType = new StructType(),
      expectedRowIdSchema: Option[StructType] = None,
      expectedMetadataSchema: Option[StructType] = None,
      expectedColumnUpdateSchema: Option[StructType] = None): Unit = {
    val info = table.lastWriteInfo
    assert(info.schema == expectedRowSchema, "row schema must match")
    val actualRowIdSchema = Option(info.rowIdSchema.orElse(null))
    assert(actualRowIdSchema == expectedRowIdSchema, "row ID schema must match")
    val actualMetadataSchema = Option(info.metadataSchema.orElse(null))
    assert(actualMetadataSchema == expectedMetadataSchema, "metadata schema must match")
    val actualColumnUpdateSchema = Option(info.columnUpdateSchema.orElse(null))
    assert(actualColumnUpdateSchema == expectedColumnUpdateSchema,
      "column update schema must match")
  }

  /**
   * Asserts that the column names in RowLevelOperationInfo.updatedColumns() received by the
   * last operation match exactly the expected set.  Order is ignored.
   */
  protected def checkLastUpdatedColumns(expectedNames: String*): Unit = {
    assert(table.lastUpdatedColumns != null, "no row-level operation was built")
    val actual = table.lastUpdatedColumns.map(_.describe()).toSet
    val expected = expectedNames.toSet
    assert(actual == expected,
      s"updatedColumns mismatch: expected ${expected.mkString("[", ", ", "]")} " +
        s"but got ${actual.mkString("[", ", ", "]")}")
  }

  /** Asserts that the last write is narrow and its column update schema has the given names. */
  protected def checkLastColumnUpdateSchema(expectedNames: String*): Unit = {
    val actual = lastColumnUpdateSchema.fieldNames.toSeq
    assert(actual == expectedNames,
      s"column update schema mismatch: expected ${expectedNames.mkString("[", ", ", "]")} " +
        s"but got ${actual.mkString("[", ", ", "]")}")
  }

  private def lastColumnUpdateSchema: StructType = {
    val info = table.lastWriteInfo
    assert(info.columnUpdateSchema.isPresent,
      s"the last write has no column update schema, its row schema is ${info.schema.toDDL}")
    info.columnUpdateSchema.get
  }

  protected def checkLastWriteLog(expectedEntries: WriteLogEntry*): Unit = {
    checkWriteLog(table.schema, expectedEntries)
  }

  /** Checks the last write log, reading row data in the layout of `columnUpdateSchema()`. */
  protected def checkLastColumnUpdateWriteLog(expectedEntries: WriteLogEntry*): Unit = {
    checkWriteLog(lastColumnUpdateSchema, expectedEntries)
  }

  private def checkWriteLog(dataSchema: StructType, expectedEntries: Seq[WriteLogEntry]): Unit = {
    val entryType = new StructType()
      .add(StructField("operation", StringType))
      .add(StructField("id", IntegerType))
      .add(StructField(
        "metadata",
        new StructType(Array(
          StructField("_partition", StringType),
          StructField("_index", IntegerType)))))
      .add(StructField("data", dataSchema))

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

  protected def writeColumnUpdateLogEntry(data: Row): WriteLogEntry = {
    WriteLogEntry(operation = WriteColumnUpdate, data = Some(data))
  }

  protected def writeColumnUpdateWithMetadataLogEntry(metadata: Row, data: Row): WriteLogEntry = {
    WriteLogEntry(operation = WriteColumnUpdate, metadata = Some(metadata), data = Some(data))
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
