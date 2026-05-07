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

package org.apache.spark.sql.execution.datasources.v2

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, ProjectingInternalRow}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, TableSpec, UnaryNode}
import org.apache.spark.sql.catalyst.transactions.TransactionUtils
import org.apache.spark.sql.catalyst.util.{removeInternalMetadata, CharVarcharUtils, ReplaceDataProjections, WriteDeltaProjections}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{COPY_OPERATION, DELETE_OPERATION, INSERT_OPERATION, REINSERT_OPERATION, UPDATE_OPERATION}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Identifier, StagedTable, StagingTableCatalog, Table, TableCatalog, TableInfo, TableWritePrivilege}
import org.apache.spark.sql.connector.catalog.transactions.Transaction
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, DeleteSummaryImpl, DeltaWrite, DeltaWriter, MergeSummaryImpl, PhysicalWriteInfoImpl, RowLevelOperation, RowLevelOperationTable, UpdateSummaryImpl, Write, WriterCommitMessage, WriteSummary}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command._
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SchemaValidationMode.PROHIBIT_CHANGES
import org.apache.spark.util.{LongAccumulator, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * Deprecated logical plan for writing data into data source v2. This is being replaced by more
 * specific logical plans, like [[org.apache.spark.sql.catalyst.plans.logical.AppendData]].
 */
@deprecated("Use specific logical plans like AppendData instead", "2.4.0")
case class WriteToDataSourceV2(
    relation: Option[DataSourceV2Relation],
    batchWrite: BatchWrite,
    query: LogicalPlan,
    customMetrics: Seq[CustomMetric]) extends UnaryNode {
  override def child: LogicalPlan = query
  override def output: Seq[Attribute] = Nil
  override protected def withNewChildInternal(newChild: LogicalPlan): WriteToDataSourceV2 =
    copy(query = newChild)
}

/**
 * Physical plan node for v2 create table as select when the catalog does not support staging
 * the table creation.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * If either table creation or the append fails, the table will be deleted. This implementation is
 * not atomic; for an atomic variant for catalogs that support the appropriate features, see
 * CreateTableAsSelectStagingExec.
 */
case class CreateTableAsSelectExec(
    catalog: TableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    ifNotExists: Boolean,
    transaction: Option[Transaction] = None)
  extends V2CreateTableAsSelectBaseExec with TransactionalExec {

  override def withTransaction(txn: Option[Transaction]): CreateTableAsSelectExec =
    copy(transaction = txn)

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return Nil
      }
      throw QueryCompilationErrors.tableAlreadyExistsError(ident)
    }
    val tableInfo = new TableInfo.Builder()
      .withColumns(getV2Columns(query.schema, catalog.useNullableQuerySchema))
      .withPartitions(partitioning.toArray)
      .withProperties(properties.asJava)
      .build()
    val table = Option(catalog.createTable(ident, tableInfo))
      .getOrElse(catalog.loadTable(ident, Set(TableWritePrivilege.INSERT).asJava))
    val result = writeToTable(catalog, table, writeOptions, ident, query, overwrite = false)
    transaction.foreach(TransactionUtils.commit)
    result
  }
}

/**
 * Physical plan node for v2 create table as select, when the catalog is determined to support
 * staging table creation.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * The CTAS operation is atomic. The creation of the table is staged and the commit of the write
 * should bundle the commitment of the metadata and the table contents in a single unit. If the
 * write fails, the table is instructed to roll back all staged changes.
 */
case class AtomicCreateTableAsSelectExec(
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    ifNotExists: Boolean)
  extends V2CreateTableAsSelectBaseExec {

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override val metrics: Map[String, SQLMetric] =
    DataSourceV2Utils.commitMetrics(sparkContext, catalog)

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return Nil
      }
      throw QueryCompilationErrors.tableAlreadyExistsError(ident)
    }
    val tableInfo = new TableInfo.Builder()
      .withColumns(getV2Columns(query.schema, catalog.useNullableQuerySchema))
      .withPartitions(partitioning.toArray)
      .withProperties(properties.asJava)
      .build()
    val stagedTable = Option(catalog.stageCreate(ident, tableInfo)
    ).getOrElse(catalog.loadTable(ident, Set(TableWritePrivilege.INSERT).asJava))
    writeToTable(catalog, stagedTable, writeOptions, ident, query, overwrite = false)
  }
}

/**
 * Physical plan node for v2 replace table as select when the catalog does not support staging
 * table replacement.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * If the table exists, its contents and schema should be replaced with the schema and the contents
 * of the query. This is a non-atomic implementation that drops the table and then runs non-atomic
 * CTAS. For an atomic implementation for catalogs with the appropriate support, see
 * ReplaceTableAsSelectStagingExec.
 */
case class ReplaceTableAsSelectExec(
    catalog: TableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Identifier) => Unit,
    transaction: Option[Transaction] = None)
  extends V2CreateTableAsSelectBaseExec with TransactionalExec {

  override def withTransaction(txn: Option[Transaction]): ReplaceTableAsSelectExec =
    copy(transaction = txn)

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override protected def run(): Seq[InternalRow] = {
    // Note that this operation is potentially unsafe, but these are the strict semantics of
    // RTAS if the catalog does not support atomic operations.
    //
    // There are numerous cases we concede to where the table will be dropped and irrecoverable:
    //
    // 1. Creating the new table fails,
    // 2. Writing to the new table fails,
    // 3. The table returned by catalog.createTable doesn't support writing.
    //
    // RTAS must refresh and pin versions in query to read from original table versions instead of
    // newly created empty table that is meant to serve as target for append/overwrite
    val refreshedQuery = V2TableRefreshUtil.refresh(
      session,
      query,
      versionedOnly = true,
      schemaValidationMode = PROHIBIT_CHANGES)
    if (catalog.tableExists(ident)) {
      invalidateCache(catalog, ident)
      catalog.dropTable(ident)
    } else if (!orCreate) {
      throw QueryCompilationErrors.cannotReplaceMissingTableError(
        ident,
        CatalogV2Util.searchPathForTableIdentifier(catalog, ident))
    }
    val tableInfo = new TableInfo.Builder()
      .withColumns(getV2Columns(refreshedQuery.schema, catalog.useNullableQuerySchema))
      .withPartitions(partitioning.toArray)
      .withProperties(properties.asJava)
      .build()
    val table = Option(catalog.createTable(ident, tableInfo))
      .getOrElse(catalog.loadTable(ident, Set(TableWritePrivilege.INSERT).asJava))
    val result = writeToTable(
      catalog, table, writeOptions, ident, refreshedQuery,
      overwrite = true, refreshPhaseEnabled = false)
    transaction.foreach(TransactionUtils.commit)
    result
  }
}

/**
 *
 * Physical plan node for v2 replace table as select when the catalog supports staging
 * table replacement.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * If the table exists, its contents and schema should be replaced with the schema and the contents
 * of the query. This implementation is atomic. The table replacement is staged, and the commit
 * operation at the end should perform the replacement of the table's metadata and contents. If the
 * write fails, the table is instructed to roll back staged changes and any previously written table
 * is left untouched.
 */
case class AtomicReplaceTableAsSelectExec(
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Identifier) => Unit)
  extends V2CreateTableAsSelectBaseExec {

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override val metrics: Map[String, SQLMetric] =
    DataSourceV2Utils.commitMetrics(sparkContext, catalog)

  override protected def run(): Seq[InternalRow] = {
    val columns = getV2Columns(query.schema, catalog.useNullableQuerySchema)
    if (catalog.tableExists(ident)) {
      invalidateCache(catalog, ident)
    }
    val staged = if (orCreate) {
      val tableInfo = new TableInfo.Builder()
        .withColumns(columns)
        .withPartitions(partitioning.toArray)
        .withProperties(properties.asJava)
        .build()
      catalog.stageCreateOrReplace(ident, tableInfo)
    } else if (catalog.tableExists(ident)) {
      try {
        val tableInfo = new TableInfo.Builder()
          .withColumns(columns)
          .withPartitions(partitioning.toArray)
          .withProperties(properties.asJava)
          .build()
        catalog.stageReplace(ident, tableInfo)
      } catch {
        case e: NoSuchTableException =>
          throw QueryCompilationErrors.cannotReplaceMissingTableError(
            ident,
            CatalogV2Util.searchPathForTableIdentifier(catalog, ident),
            Some(e))
      }
    } else {
      throw QueryCompilationErrors.cannotReplaceMissingTableError(
        ident,
        CatalogV2Util.searchPathForTableIdentifier(catalog, ident))
    }
    val table = Option(staged).getOrElse(
      catalog.loadTable(ident, Set(TableWritePrivilege.INSERT).asJava))
    writeToTable(catalog, table, writeOptions, ident, query, overwrite = true)
  }
}

/**
 * Physical plan node for append into a v2 table.
 *
 * Rows in the output data set are appended.
 */
case class AppendDataExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write,
    tableName: String,
    transaction: Option[Transaction] = None) extends V2ExistingTableWriteExec {
  override def withTransaction(txn: Option[Transaction]): AppendDataExec = copy(transaction = txn)
  override protected def withNewChildInternal(newChild: SparkPlan): AppendDataExec =
    copy(query = newChild)
}

/**
 * Physical plan node for overwrite into a v2 table.
 *
 * Overwrites data in a table matched by a set of filters. Rows matching all of the filters will be
 * deleted and rows in the output data set are appended.
 *
 * This plan is used to implement SaveMode.Overwrite. The behavior of SaveMode.Overwrite is to
 * truncate the table -- delete all rows -- and append the output data set. This uses the filter
 * AlwaysTrue to delete all rows.
 */
case class OverwriteByExpressionExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write,
    tableName: String,
    transaction: Option[Transaction] = None) extends V2ExistingTableWriteExec {
  override def withTransaction(txn: Option[Transaction]): OverwriteByExpressionExec =
    copy(transaction = txn)
  override protected def withNewChildInternal(newChild: SparkPlan): OverwriteByExpressionExec =
    copy(query = newChild)
}

/**
 * Physical plan node for dynamic partition overwrite into a v2 table.
 *
 * Dynamic partition overwrite is the behavior of Hive INSERT OVERWRITE ... PARTITION queries, and
 * Spark INSERT OVERWRITE queries when spark.sql.sources.partitionOverwriteMode=dynamic. Each
 * partition in the output data set replaces the corresponding existing partition in the table or
 * creates a new partition. Existing partitions for which there is no data in the output data set
 * are not modified.
 */
case class OverwritePartitionsDynamicExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write,
    tableName: String,
    transaction: Option[Transaction] = None) extends V2ExistingTableWriteExec {
  override def withTransaction(txn: Option[Transaction]): OverwritePartitionsDynamicExec =
    copy(transaction = txn)
  override protected def withNewChildInternal(newChild: SparkPlan): OverwritePartitionsDynamicExec =
    copy(query = newChild)
}

/**
 * Physical plan node to replace data in existing tables.
 */
case class ReplaceDataExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    projections: ReplaceDataProjections,
    write: Write,
    rowLevelCommand: RowLevelOperation.Command,
    tableName: String,
    transaction: Option[Transaction] = None) extends RowLevelWriteExec {

  override def writingTask: WritingSparkTask[_] = {
    projections.metadataProjection match {
      case Some(metadataProj) =>
        DataAndMetadataWritingSparkTask(projections.rowProjection, metadataProj, sparkMetrics)
      case None =>
        DataWithProjectionWritingSparkTask(projections.rowProjection, sparkMetrics)
    }
  }

  override def withTransaction(txn: Option[Transaction]): ReplaceDataExec = copy(transaction = txn)
  override protected def withNewChildInternal(newChild: SparkPlan): ReplaceDataExec = {
    copy(query = newChild)
  }

  override protected def getWriteSummary(query: SparkPlan): Option[WriteSummary] = {
    if (rowLevelCommand == DELETE) {
      // DELETE ReplaceData plans filter out the deleted rows early in the plan, and they don't
      // reach this node. We need to calculate this value as numScannedRows - numCopiedRows.
      val numScannedRows = collectFirst(query) {
        case b: BatchScanExec if b.table.isInstanceOf[RowLevelOperationTable] =>
          getMetricValue(b.metrics, "numOutputRows")
      }
      val numCopiedRows = getMetricValue(metrics, "numCopiedRows")
      val numDeletedRows = if (numScannedRows.exists(_ >= 0) && numCopiedRows >= 0) {
        numScannedRows.get - numCopiedRows
      } else {
        // One of the metrics couldn't be found, also mark numDeletedRows as not found.
        -1L
      }
      metrics("numDeletedRows").set(numDeletedRows)
    }
    super.getWriteSummary(query)
  }
}

/**
 * Physical plan node to write a delta of rows to an existing table.
 */
case class WriteDeltaExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    projections: WriteDeltaProjections,
    write: DeltaWrite,
    rowLevelCommand: RowLevelOperation.Command,
    tableName: String,
    transaction: Option[Transaction] = None) extends RowLevelWriteExec {

  override lazy val writingTask: WritingSparkTask[_] = {
    if (projections.metadataProjection.isDefined) {
      DeltaWithMetadataWritingSparkTask(projections, sparkMetrics)
    } else {
      DeltaWritingSparkTask(projections, sparkMetrics)
    }
  }

  override def withTransaction(txn: Option[Transaction]): WriteDeltaExec = copy(transaction = txn)
  override protected def withNewChildInternal(newChild: SparkPlan): WriteDeltaExec = {
    copy(query = newChild)
  }
}

case class WriteToDataSourceV2Exec(
    batchWrite: BatchWrite,
    refreshCache: () => Unit,
    query: SparkPlan,
    writeMetrics: Seq[CustomMetric],
    transaction: Option[Transaction] = None) extends V2TableWriteExec with TransactionalExec {

  override def withTransaction(txn: Option[Transaction]): WriteToDataSourceV2Exec =
    copy(transaction = txn)

  override def stringArgs: Iterator[Any] = Iterator(batchWrite, query)

  override lazy val customMetrics: Map[String, SQLMetric] =
    createCustomMetrics(writeMetrics.toArray)

  override protected def run(): Seq[InternalRow] = {
    val writtenRows = writeWithV2(batchWrite)
    transaction.foreach(TransactionUtils.commit)
    refreshCache()
    writtenRows
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WriteToDataSourceV2Exec =
    copy(query = newChild)
}

/**
 * Trait for physical plan nodes that write to an existing table as part of a transaction.
 * The [[transaction]] is injected post-planning by [[QueryExecution]].
 */
trait TransactionalExec extends SparkPlan {
  def transaction: Option[Transaction]
  def withTransaction(txn: Option[Transaction]): SparkPlan
}

trait V2ExistingTableWriteExec extends V2TableWriteExec with TransactionalExec {
  def refreshCache: () => Unit
  def write: Write
  def tableName: String

  override def stringArgs: Iterator[Any] = Iterator(query, write)

  override def nodeName: String = s"${super.nodeName} $tableName"

  override lazy val customMetrics: Map[String, SQLMetric] =
    createCustomMetrics(write.supportedCustomMetrics())

  override protected def run(): Seq[InternalRow] = {
    val writtenRows = try {
      writeWithV2(write.toBatch)
    } finally {
      postDriverMetrics(write.reportDriverMetrics())
    }
    transaction.foreach(TransactionUtils.commit)
    refreshCache()
    writtenRows
  }
}

/**
 * A trait for row-level write operations (UPDATE, DELETE, MERGE).
 */
trait RowLevelWriteExec extends V2ExistingTableWriteExec {
  def rowLevelCommand: RowLevelOperation.Command

  override protected lazy val sparkMetrics: Map[String, SQLMetric] = rowLevelCommand match {
    case UPDATE =>
      Map(
        "numUpdatedRows" -> SQLMetrics.createMetric(sparkContext, "number of updated rows"),
        "numCopiedRows" -> SQLMetrics.createMetric(sparkContext, "number of copied rows"))
    case DELETE =>
      Map(
        "numDeletedRows" -> SQLMetrics.createMetric(sparkContext, "number of deleted rows"),
        "numCopiedRows" -> SQLMetrics.createMetric(sparkContext, "number of copied rows"))
    case _ => Map.empty
  }

  /**
   * Returns the value of the named metric, or -1 if the metric is not found.
   */
  protected def getMetricValue(metrics: Map[String, SQLMetric], name: String): Long = {
    metrics.get(name).map(_.value).getOrElse(-1L)
  }

  override protected def getWriteSummary(query: SparkPlan): Option[WriteSummary] = {
    rowLevelCommand match {
      case MERGE =>
        collectFirst(query) { case m: MergeRowsExec => m }.map { n =>
          val metrics = n.metrics
          MergeSummaryImpl(
            getMetricValue(metrics, "numTargetRowsCopied"),
            getMetricValue(metrics, "numTargetRowsDeleted"),
            getMetricValue(metrics, "numTargetRowsUpdated"),
            getMetricValue(metrics, "numTargetRowsInserted"),
            getMetricValue(metrics, "numTargetRowsMatchedUpdated"),
            getMetricValue(metrics, "numTargetRowsMatchedDeleted"),
            getMetricValue(metrics, "numTargetRowsNotMatchedBySourceUpdated"),
            getMetricValue(metrics, "numTargetRowsNotMatchedBySourceDeleted"))
        }
      case UPDATE =>
        Some(UpdateSummaryImpl(
          getMetricValue(sparkMetrics, "numUpdatedRows"),
          getMetricValue(sparkMetrics, "numCopiedRows")))
      case DELETE =>
        Some(DeleteSummaryImpl(
          getMetricValue(sparkMetrics, "numDeletedRows"),
          getMetricValue(sparkMetrics, "numCopiedRows")))
    }
  }
}

/**
 * The base physical plan for writing data into data source v2.
 */
trait V2TableWriteExec
  extends V2CommandExec
  with UnaryExecNode
  with AdaptiveSparkPlanHelper
  with SupportsCustomDriverMetrics {

  def query: SparkPlan
  def writingTask: WritingSparkTask[_] = DataWritingSparkTask

  var commitProgress: Option[StreamWriterCommitProgress] = None

  override def child: SparkPlan = query
  override def output: Seq[Attribute] = Nil

  override def customMetrics: Map[String, SQLMetric] = Map.empty

  protected def writeWithV2(batchWrite: BatchWrite): Seq[InternalRow] = {
    val rdd: RDD[InternalRow] = {
      val tempRdd = query.execute()
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      if (tempRdd.partitions.length == 0) {
        sparkContext.parallelize(Array.empty[InternalRow].toImmutableArraySeq, 1)
      } else {
        tempRdd
      }
    }
    // introduce a local var to avoid serializing the whole class
    val task = writingTask
    val writerFactory = batchWrite.createBatchWriterFactory(
      PhysicalWriteInfoImpl(rdd.getNumPartitions))
    val useCommitCoordinator = batchWrite.useCommitCoordinator
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(log"Start processing data source write support: " +
      log"${MDC(LogKeys.BATCH_WRITE, batchWrite)}. The input RDD has " +
      log"${MDC(LogKeys.COUNT, messages.length)} partitions.")

    // Avoid object not serializable issue.
    val writeMetrics: Map[String, SQLMetric] = customMetrics

    try {
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          task.run(writerFactory, context, iter, useCommitCoordinator, writeMetrics),
        rdd.partitions.indices,
        (index, result: DataWritingSparkTaskResult) => {
          val commitMessage = result.writerCommitMessage
          messages(index) = commitMessage
          totalNumRowsAccumulator.add(result.numRows)
          batchWrite.onDataWriterCommit(commitMessage)
        }
      )

      val writeSummary = getWriteSummary(query)
      logInfo(log"Data source write support ${MDC(LogKeys.BATCH_WRITE, batchWrite)} is committing.")
      writeSummary match {
        case Some(summary) => batchWrite.commit(messages, summary)
        case None => batchWrite.commit(messages)
      }
      logInfo(log"Data source write support ${MDC(LogKeys.BATCH_WRITE, batchWrite)} committed.")
      commitProgress = Some(StreamWriterCommitProgress(totalNumRowsAccumulator.value))
    } catch {
      case cause: Throwable =>
        logError(
          log"Data source write support ${MDC(LogKeys.BATCH_WRITE, batchWrite)} is aborting.")
        try {
          batchWrite.abort(messages)
        } catch {
          case t: Throwable =>
            logError(log"Data source write support ${MDC(LogKeys.BATCH_WRITE, batchWrite)} " +
              log"failed to abort.")
            cause.addSuppressed(t)
            throw QueryExecutionErrors.writingJobFailedError(cause)
        }
        logError(log"Data source write support ${MDC(LogKeys.BATCH_WRITE, batchWrite)} aborted.")
        throw cause
    }

    Nil
  }

  protected def getWriteSummary(query: SparkPlan): Option[WriteSummary] = None
}

trait WritingSparkTask[W <: DataWriter[InternalRow]] extends Logging with Serializable {

  protected def write(writer: W, iter: java.util.Iterator[InternalRow]): Unit

  def run(
      writerFactory: DataWriterFactory,
      context: TaskContext,
      iter: Iterator[InternalRow],
      useCommitCoordinator: Boolean,
      customMetrics: Map[String, SQLMetric]): DataWritingSparkTaskResult = {
    val stageId = context.stageId()
    val stageAttempt = context.stageAttemptNumber()
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()
    val attemptId = context.attemptNumber()
    val dataWriter = writerFactory.createWriter(partId, taskId).asInstanceOf[W]

    val iterWithMetrics = IteratorWithMetrics(iter, dataWriter, customMetrics)

    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      write(dataWriter, iterWithMetrics)

      CustomMetrics.updateMetrics(
        dataWriter.currentMetricsValues.toImmutableArraySeq, customMetrics)

      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(stageId, stageAttempt, partId, attemptId)
        if (commitAuthorized) {
          logInfo(log"Commit authorized for partition ${MDC(LogKeys.PARTITION_ID, partId)} " +
            log"(task ${MDC(LogKeys.TASK_ID, taskId)}, " +
            log"attempt ${MDC(LogKeys.TASK_ATTEMPT_ID, attemptId)}, " +
            log"stage ${MDC(LogKeys.STAGE_ID, stageId)}." +
            log"${MDC(LogKeys.STAGE_ATTEMPT_ID, stageAttempt)})")

          dataWriter.commit()
        } else {
          val commitDeniedException = QueryExecutionErrors.commitDeniedError(
            partId, taskId, attemptId, stageId, stageAttempt)
          logInfo(log"${MDC(LogKeys.ERROR, commitDeniedException.getMessage)}")
          // throwing CommitDeniedException will trigger the catch block for abort
          throw commitDeniedException
        }

      } else {
        logInfo(log"Writer for partition ${MDC(LogKeys.PARTITION_ID, context.partitionId())} " +
          log"is committing.")
        dataWriter.commit()
      }

      logInfo(log"Committed partition ${MDC(LogKeys.PARTITION_ID, partId)} " +
        log"(task ${MDC(LogKeys.TASK_ID, taskId)}, " +
        log"attempt ${MDC(LogKeys.TASK_ATTEMPT_ID, attemptId)}, " +
        log"stage ${MDC(LogKeys.STAGE_ID, stageId)}." +
        log"${MDC(LogKeys.STAGE_ATTEMPT_ID, stageAttempt)})")

      DataWritingSparkTaskResult(iterWithMetrics.count, msg)

    })(catchBlock = {
      // If there is an error, abort this writer
      logError(log"Aborting commit for partition ${MDC(LogKeys.PARTITION_ID, partId)} " +
        log"(task ${MDC(LogKeys.TASK_ID, taskId)}, " +
        log"attempt ${MDC(LogKeys.TASK_ATTEMPT_ID, attemptId)}, " +
        log"stage ${MDC(LogKeys.STAGE_ID, stageId)}." +
        log"${MDC(LogKeys.STAGE_ATTEMPT_ID, stageAttempt)})")
      dataWriter.abort()
      logError(log"Aborted commit for partition ${MDC(LogKeys.PARTITION_ID, partId)} " +
        log"(task ${MDC(LogKeys.TASK_ID, taskId)}, " +
        log"attempt ${MDC(LogKeys.TASK_ATTEMPT_ID, attemptId)}, " +
        log"stage ${MDC(LogKeys.STAGE_ID, stageId)}." +
        log"${MDC(LogKeys.STAGE_ATTEMPT_ID, stageAttempt)})")
    }, finallyBlock = {
      dataWriter.close()
    })
  }

  private case class IteratorWithMetrics(
      iter: Iterator[InternalRow],
      dataWriter: W,
      customMetrics: Map[String, SQLMetric]) extends java.util.Iterator[InternalRow] {
    var count = 0L

    override def hasNext: Boolean = iter.hasNext

    override def next(): InternalRow = {
      if (count % CustomMetrics.NUM_ROWS_PER_UPDATE == 0) {
        CustomMetrics.updateMetrics(
          dataWriter.currentMetricsValues.toImmutableArraySeq, customMetrics)
      }
      count += 1
      iter.next()
    }
  }
}

case class DataAndMetadataWritingSparkTask(
    dataProj: ProjectingInternalRow,
    metadataProj: ProjectingInternalRow,
    sparkMetrics: Map[String, SQLMetric])
  extends WritingSparkTask[DataWriter[InternalRow]] {

  override protected def write(
      writer: DataWriter[InternalRow], iter: java.util.Iterator[InternalRow]): Unit = {
    var numUpdatedRows = 0L
    var numCopiedRows = 0L

    while (iter.hasNext) {
      val row = iter.next()
      val operation = row.getInt(0)

      operation match {
        case UPDATE_OPERATION =>
          numUpdatedRows += 1L
          dataProj.project(row)
          metadataProj.project(row)
          writer.write(metadataProj, dataProj)

        case COPY_OPERATION =>
          numCopiedRows += 1L
          dataProj.project(row)
          metadataProj.project(row)
          writer.write(metadataProj, dataProj)

        case INSERT_OPERATION =>
          dataProj.project(row)
          writer.write(dataProj)

        case other =>
          throw new SparkException(s"Unexpected operation ID: $other")
      }
    }

    sparkMetrics.get("numUpdatedRows").foreach(_.add(numUpdatedRows))
    sparkMetrics.get("numCopiedRows").foreach(_.add(numCopiedRows))
  }
}

case class DataWithProjectionWritingSparkTask(
    dataProj: ProjectingInternalRow,
    sparkMetrics: Map[String, SQLMetric])
  extends WritingSparkTask[DataWriter[InternalRow]] {

  override protected def write(
      writer: DataWriter[InternalRow], iter: java.util.Iterator[InternalRow]): Unit = {
    var numUpdatedRows = 0L
    var numCopiedRows = 0L

    while (iter.hasNext) {
      val row = iter.next()
      val operation = row.getInt(0)

      operation match {
        case UPDATE_OPERATION =>
          numUpdatedRows += 1L
          dataProj.project(row)
          writer.write(dataProj)

        case COPY_OPERATION =>
          numCopiedRows += 1L
          dataProj.project(row)
          writer.write(dataProj)

        case INSERT_OPERATION =>
          dataProj.project(row)
          writer.write(dataProj)

        case other =>
          throw new SparkException(s"Unexpected operation ID: $other")
      }
    }

    sparkMetrics.get("numUpdatedRows").foreach(_.add(numUpdatedRows))
    sparkMetrics.get("numCopiedRows").foreach(_.add(numCopiedRows))
  }
}

object DataWritingSparkTask extends WritingSparkTask[DataWriter[InternalRow]] {
  override protected def write(
      writer: DataWriter[InternalRow], iter: java.util.Iterator[InternalRow]): Unit = {
    writer.writeAll(iter)
  }
}

case class DeltaWritingSparkTask(
    projections: WriteDeltaProjections,
    sparkMetrics: Map[String, SQLMetric])
  extends WritingSparkTask[DeltaWriter[InternalRow]] {

  private lazy val rowProjection = projections.rowProjection.orNull
  private lazy val rowIdProjection = projections.rowIdProjection

  override protected def write(
      writer: DeltaWriter[InternalRow], iter: java.util.Iterator[InternalRow]): Unit = {
    var numUpdatedRows = 0L
    var numDeletedRows = 0L

    while (iter.hasNext) {
      val row = iter.next()
      val operation = row.getInt(0)

      operation match {
        case DELETE_OPERATION =>
          numDeletedRows += 1L
          rowIdProjection.project(row)
          writer.delete(null, rowIdProjection)

        case UPDATE_OPERATION =>
          numUpdatedRows += 1L
          rowProjection.project(row)
          rowIdProjection.project(row)
          writer.update(null, rowIdProjection, rowProjection)

        case REINSERT_OPERATION =>
          // When representUpdateAsDeleteAndInsert is true, each logical update is split
          // into a DELETE and a REINSERT. Count the REINSERT as one updated row.
          numUpdatedRows += 1L
          rowProjection.project(row)
          writer.reinsert(null, rowProjection)

        case INSERT_OPERATION =>
          rowProjection.project(row)
          writer.insert(rowProjection)

        case other =>
          throw new SparkException(s"Unexpected operation ID: $other")
      }
    }

    sparkMetrics.get("numUpdatedRows").foreach(_.add(numUpdatedRows))
    sparkMetrics.get("numDeletedRows").foreach(_.add(numDeletedRows))
  }
}

case class DeltaWithMetadataWritingSparkTask(
    projections: WriteDeltaProjections,
    sparkMetrics: Map[String, SQLMetric])
  extends WritingSparkTask[DeltaWriter[InternalRow]] {

  private lazy val rowProjection = projections.rowProjection.orNull
  private lazy val rowIdProjection = projections.rowIdProjection
  private lazy val metadataProjection = projections.metadataProjection.orNull

  override protected def write(
      writer: DeltaWriter[InternalRow], iter: java.util.Iterator[InternalRow]): Unit = {
    var numUpdatedRows = 0L
    var numDeletedRows = 0L

    while (iter.hasNext) {
      val row = iter.next()
      val operation = row.getInt(0)

      operation match {
        case DELETE_OPERATION =>
          numDeletedRows += 1L
          rowIdProjection.project(row)
          metadataProjection.project(row)
          writer.delete(metadataProjection, rowIdProjection)

        case UPDATE_OPERATION =>
          numUpdatedRows += 1L
          rowProjection.project(row)
          rowIdProjection.project(row)
          metadataProjection.project(row)
          writer.update(metadataProjection, rowIdProjection, rowProjection)

        case REINSERT_OPERATION =>
          // When representUpdateAsDeleteAndInsert is true, each logical update is split
          // into a DELETE and a REINSERT. Count the REINSERT as one updated row.
          numUpdatedRows += 1L
          rowProjection.project(row)
          metadataProjection.project(row)
          writer.reinsert(metadataProjection, rowProjection)

        case INSERT_OPERATION =>
          rowProjection.project(row)
          writer.insert(rowProjection)

        case other =>
          throw new SparkException(s"Unexpected operation ID: $other")
      }
    }

    sparkMetrics.get("numUpdatedRows").foreach(_.add(numUpdatedRows))
    sparkMetrics.get("numDeletedRows").foreach(_.add(numDeletedRows))
  }
}

private[v2] trait V2CreateTableAsSelectBaseExec extends LeafV2CommandExec {
  override def output: Seq[Attribute] = Nil

  protected def getV2Columns(schema: StructType, forceNullable: Boolean): Array[Column] = {
    val rawSchema = CharVarcharUtils.getRawSchema(removeInternalMetadata(schema), conf)
    val tableSchema = if (forceNullable) rawSchema.asNullable else rawSchema
    CatalogV2Util.structTypeToV2Columns(tableSchema)
  }

  protected def writeToTable(
      catalog: TableCatalog,
      table: Table,
      writeOptions: Map[String, String],
      ident: Identifier,
      query: LogicalPlan,
      overwrite: Boolean,
      refreshPhaseEnabled: Boolean = true): Seq[InternalRow] = {
    Utils.tryWithSafeFinallyAndFailureCallbacks({
      val relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
      val writeCommand = if (overwrite) {
        OverwriteByExpression.byPosition(relation, query, Literal.TrueLiteral, writeOptions)
      } else {
        AppendData.byPosition(relation, query, writeOptions)
      }
      QueryExecution.runCommand(
        session,
        writeCommand,
        "inner data writing for CTAS/RTAS",
        refreshPhaseEnabled)
      DataSourceV2Utils.commitStagedChanges(sparkContext, table, metrics)
      Nil
    })(catchBlock = {
      table match {
        // Failure rolls back the staged writes and metadata changes.
        case st: StagedTable => st.abortStagedChanges()
        case _ => catalog.dropTable(ident)
      }
    })
  }
}

private[v2] case class DataWritingSparkTaskResult(
    numRows: Long,
    writerCommitMessage: WriterCommitMessage)

/**
 * Sink progress information collected after commit.
 */
private[sql] case class StreamWriterCommitProgress(numOutputRows: Long)

