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

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, TableSpec, UnaryNode}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, StagedTable, StagingTableCatalog, SupportsWrite, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, LogicalWriteInfoImpl, PhysicalWriteInfoImpl, V1Write, Write, WriterCommitMessage}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric, SQLMetrics}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.{LongAccumulator, Utils}

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
    plan: LogicalPlan,
    query: SparkPlan,
    tableSpec: TableSpec,
    writeOptions: CaseInsensitiveStringMap,
    ifNotExists: Boolean) extends TableWriteExecHelper {

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return Nil
      }

      throw QueryCompilationErrors.tableAlreadyExistsError(ident)
    }

    val schema = CharVarcharUtils.getRawSchema(query.schema, conf).asNullable
    val table = catalog.createTable(ident, schema,
      partitioning.toArray, properties.asJava)
    writeToTable(catalog, table, writeOptions, ident)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CreateTableAsSelectExec =
    copy(query = newChild)
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
    plan: LogicalPlan,
    query: SparkPlan,
    tableSpec: TableSpec,
    writeOptions: CaseInsensitiveStringMap,
    ifNotExists: Boolean) extends TableWriteExecHelper {

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return Nil
      }

      throw QueryCompilationErrors.tableAlreadyExistsError(ident)
    }
    val schema = CharVarcharUtils.getRawSchema(query.schema, conf).asNullable
    val stagedTable = catalog.stageCreate(
      ident, schema, partitioning.toArray, properties.asJava)
    writeToTable(catalog, stagedTable, writeOptions, ident)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): AtomicCreateTableAsSelectExec =
    copy(query = newChild)
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
    plan: LogicalPlan,
    query: SparkPlan,
    tableSpec: TableSpec,
    writeOptions: CaseInsensitiveStringMap,
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Table, Identifier) => Unit) extends TableWriteExecHelper {

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
    if (catalog.tableExists(ident)) {
      val table = catalog.loadTable(ident)
      invalidateCache(catalog, table, ident)
      catalog.dropTable(ident)
    } else if (!orCreate) {
      throw QueryCompilationErrors.cannotReplaceMissingTableError(ident)
    }
    val schema = CharVarcharUtils.getRawSchema(query.schema, conf).asNullable
    val table = catalog.createTable(
      ident, schema, partitioning.toArray, properties.asJava)
    writeToTable(catalog, table, writeOptions, ident)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ReplaceTableAsSelectExec =
    copy(query = newChild)
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
    plan: LogicalPlan,
    query: SparkPlan,
    tableSpec: TableSpec,
    writeOptions: CaseInsensitiveStringMap,
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Table, Identifier) => Unit) extends TableWriteExecHelper {

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override protected def run(): Seq[InternalRow] = {
    val schema = CharVarcharUtils.getRawSchema(query.schema, conf).asNullable
    if (catalog.tableExists(ident)) {
      val table = catalog.loadTable(ident)
      invalidateCache(catalog, table, ident)
    }
    val staged = if (orCreate) {
      catalog.stageCreateOrReplace(
        ident, schema, partitioning.toArray, properties.asJava)
    } else if (catalog.tableExists(ident)) {
      try {
        catalog.stageReplace(
          ident, schema, partitioning.toArray, properties.asJava)
      } catch {
        case e: NoSuchTableException =>
          throw QueryCompilationErrors.cannotReplaceMissingTableError(ident, Some(e))
      }
    } else {
      throw QueryCompilationErrors.cannotReplaceMissingTableError(ident)
    }
    writeToTable(catalog, staged, writeOptions, ident)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): AtomicReplaceTableAsSelectExec =
    copy(query = newChild)
}

/**
 * Physical plan node for append into a v2 table.
 *
 * Rows in the output data set are appended.
 */
case class AppendDataExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write) extends V2ExistingTableWriteExec {
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
    write: Write) extends V2ExistingTableWriteExec {
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
    write: Write) extends V2ExistingTableWriteExec {
  override protected def withNewChildInternal(newChild: SparkPlan): OverwritePartitionsDynamicExec =
    copy(query = newChild)
}

/**
 * Physical plan node to replace data in existing tables.
 */
case class ReplaceDataExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write) extends V2ExistingTableWriteExec {

  override val stringArgs: Iterator[Any] = Iterator(query, write)

  override protected def withNewChildInternal(newChild: SparkPlan): ReplaceDataExec = {
    copy(query = newChild)
  }
}

case class WriteToDataSourceV2Exec(
    batchWrite: BatchWrite,
    refreshCache: () => Unit,
    query: SparkPlan,
    writeMetrics: Seq[CustomMetric]) extends V2TableWriteExec {

  override val customMetrics: Map[String, SQLMetric] = writeMetrics.map { customMetric =>
    customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
  }.toMap

  override protected def run(): Seq[InternalRow] = {
    val writtenRows = writeWithV2(batchWrite)
    refreshCache()
    writtenRows
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WriteToDataSourceV2Exec =
    copy(query = newChild)
}

trait V2ExistingTableWriteExec extends V2TableWriteExec {
  def refreshCache: () => Unit
  def write: Write

  override val customMetrics: Map[String, SQLMetric] =
    write.supportedCustomMetrics().map { customMetric =>
      customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
    }.toMap

  override protected def run(): Seq[InternalRow] = {
    val writtenRows = writeWithV2(write.toBatch)
    refreshCache()
    writtenRows
  }
}

/**
 * The base physical plan for writing data into data source v2.
 */
trait V2TableWriteExec extends V2CommandExec with UnaryExecNode {
  def query: SparkPlan

  var commitProgress: Option[StreamWriterCommitProgress] = None

  override def child: SparkPlan = query
  override def output: Seq[Attribute] = Nil

  protected val customMetrics: Map[String, SQLMetric] = Map.empty

  override lazy val metrics = customMetrics

  protected def writeWithV2(batchWrite: BatchWrite): Seq[InternalRow] = {
    val rdd: RDD[InternalRow] = {
      val tempRdd = query.execute()
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      if (tempRdd.partitions.length == 0) {
        sparkContext.parallelize(Array.empty[InternalRow], 1)
      } else {
        tempRdd
      }
    }
    val writerFactory = batchWrite.createBatchWriterFactory(
      PhysicalWriteInfoImpl(rdd.getNumPartitions))
    val useCommitCoordinator = batchWrite.useCommitCoordinator
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(s"Start processing data source write support: $batchWrite. " +
      s"The input RDD has ${messages.length} partitions.")

    // Avoid object not serializable issue.
    val writeMetrics: Map[String, SQLMetric] = customMetrics

    try {
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          DataWritingSparkTask.run(writerFactory, context, iter, useCommitCoordinator,
            writeMetrics),
        rdd.partitions.indices,
        (index, result: DataWritingSparkTaskResult) => {
          val commitMessage = result.writerCommitMessage
          messages(index) = commitMessage
          totalNumRowsAccumulator.add(result.numRows)
          batchWrite.onDataWriterCommit(commitMessage)
        }
      )

      logInfo(s"Data source write support $batchWrite is committing.")
      batchWrite.commit(messages)
      logInfo(s"Data source write support $batchWrite committed.")
      commitProgress = Some(StreamWriterCommitProgress(totalNumRowsAccumulator.value))
    } catch {
      case cause: Throwable =>
        logError(s"Data source write support $batchWrite is aborting.")
        try {
          batchWrite.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source write support $batchWrite failed to abort.")
            cause.addSuppressed(t)
            throw QueryExecutionErrors.writingJobFailedError(cause)
        }
        logError(s"Data source write support $batchWrite aborted.")
        throw cause
    }

    Nil
  }
}

object DataWritingSparkTask extends Logging {
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
    val dataWriter = writerFactory.createWriter(partId, taskId)

    var count = 0L
    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iter.hasNext) {
        if (count % CustomMetrics.NUM_ROWS_PER_UPDATE == 0) {
          CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)
        }

        // Count is here.
        count += 1
        dataWriter.write(iter.next())
      }

      CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)

      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(stageId, stageAttempt, partId, attemptId)
        if (commitAuthorized) {
          logInfo(s"Commit authorized for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)")
          dataWriter.commit()
        } else {
          val commitDeniedException = QueryExecutionErrors.commitDeniedError(
            partId, taskId, attemptId, stageId, stageAttempt)
          logInfo(commitDeniedException.getMessage)
          // throwing CommitDeniedException will trigger the catch block for abort
          throw commitDeniedException
        }

      } else {
        logInfo(s"Writer for partition ${context.partitionId()} is committing.")
        dataWriter.commit()
      }

      logInfo(s"Committed partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")

      DataWritingSparkTaskResult(count, msg)

    })(catchBlock = {
      // If there is an error, abort this writer
      logError(s"Aborting commit for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)")
      dataWriter.abort()
      logError(s"Aborted commit for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)")
    }, finallyBlock = {
      dataWriter.close()
    })
  }
}

private[v2] trait TableWriteExecHelper extends V2TableWriteExec with SupportsV1Write {
  protected def writeToTable(
      catalog: TableCatalog,
      table: Table,
      writeOptions: CaseInsensitiveStringMap,
      ident: Identifier): Seq[InternalRow] = {
    Utils.tryWithSafeFinallyAndFailureCallbacks({
      table match {
        case table: SupportsWrite =>
          val info = LogicalWriteInfoImpl(
            queryId = UUID.randomUUID().toString,
            query.schema,
            writeOptions)
          val writeBuilder = table.newWriteBuilder(info)

          val write = writeBuilder.build()
          val writtenRows = write match {
            case v1: V1Write => writeWithV1(v1.toInsertableRelation)
            case v2 => writeWithV2(v2.toBatch)
          }

          table match {
            case st: StagedTable => st.commitStagedChanges()
            case _ =>
          }
          writtenRows

        case _ =>
          // Table does not support writes - staged changes are also rolled back below if table
          // is staging.
          throw QueryExecutionErrors.unsupportedTableWritesError(ident)
      }
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

