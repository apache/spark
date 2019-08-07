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
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.v2.{Identifier, StagingTableCatalog, TableCatalog}
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.sources.{AlwaysTrue, Filter}
import org.apache.spark.sql.sources.v2.{StagedTable, SupportsWrite}
import org.apache.spark.sql.sources.v2.writer.{BatchWrite, DataWriterFactory, SupportsDynamicOverwrite, SupportsOverwrite, SupportsTruncate, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.{LongAccumulator, Utils}

/**
 * Deprecated logical plan for writing data into data source v2. This is being replaced by more
 * specific logical plans, like [[org.apache.spark.sql.catalyst.plans.logical.AppendData]].
 */
@deprecated("Use specific logical plans like AppendData instead", "2.4.0")
case class WriteToDataSourceV2(batchWrite: BatchWrite, query: LogicalPlan)
  extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil
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
    query: SparkPlan,
    properties: Map[String, String],
    writeOptions: CaseInsensitiveStringMap,
    ifNotExists: Boolean) extends V2TableWriteExec {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits.IdentifierHelper

  override protected def doExecute(): RDD[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return sparkContext.parallelize(Seq.empty, 1)
      }

      throw new TableAlreadyExistsException(ident)
    }

    Utils.tryWithSafeFinallyAndFailureCallbacks({
      catalog.createTable(
        ident, query.schema, partitioning.toArray, properties.asJava) match {
        case table: SupportsWrite =>
          val batchWrite = table.newWriteBuilder(writeOptions)
            .withInputDataSchema(query.schema)
            .withQueryId(UUID.randomUUID().toString)
            .buildForBatch()

          doWrite(batchWrite)

        case _ =>
          // table does not support writes
          throw new SparkException(
            s"Table implementation does not support writes: ${ident.quoted}")
      }
    })(catchBlock = {
      catalog.dropTable(ident)
    })
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
    query: SparkPlan,
    properties: Map[String, String],
    writeOptions: CaseInsensitiveStringMap,
    ifNotExists: Boolean) extends AtomicTableWriteExec {

  override protected def doExecute(): RDD[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return sparkContext.parallelize(Seq.empty, 1)
      }

      throw new TableAlreadyExistsException(ident)
    }
    val stagedTable = catalog.stageCreate(
      ident, query.schema, partitioning.toArray, properties.asJava)
    writeToStagedTable(stagedTable, writeOptions, ident)
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
    query: SparkPlan,
    properties: Map[String, String],
    writeOptions: CaseInsensitiveStringMap,
    orCreate: Boolean) extends AtomicTableWriteExec {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits.IdentifierHelper

  override protected def doExecute(): RDD[InternalRow] = {
    // Note that this operation is potentially unsafe, but these are the strict semantics of
    // RTAS if the catalog does not support atomic operations.
    //
    // There are numerous cases we concede to where the table will be dropped and irrecoverable:
    //
    // 1. Creating the new table fails,
    // 2. Writing to the new table fails,
    // 3. The table returned by catalog.createTable doesn't support writing.
    if (catalog.tableExists(ident)) {
      catalog.dropTable(ident)
    } else if (!orCreate) {
      throw new CannotReplaceMissingTableException(ident)
    }
    val createdTable = catalog.createTable(
      ident, query.schema, partitioning.toArray, properties.asJava)
    Utils.tryWithSafeFinallyAndFailureCallbacks({
      createdTable match {
        case table: SupportsWrite =>
          val batchWrite = table.newWriteBuilder(writeOptions)
            .withInputDataSchema(query.schema)
            .withQueryId(UUID.randomUUID().toString)
            .buildForBatch()

          doWrite(batchWrite)

        case _ =>
          // table does not support writes
          throw new SparkException(
            s"Table implementation does not support writes: ${ident.quoted}")
      }
    })(catchBlock = {
      catalog.dropTable(ident)
    })
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
 * operation at the end should perform tne replacement of the table's metadata and contents. If the
 * write fails, the table is instructed to roll back staged changes and any previously written table
 * is left untouched.
 */
case class AtomicReplaceTableAsSelectExec(
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: SparkPlan,
    properties: Map[String, String],
    writeOptions: CaseInsensitiveStringMap,
    orCreate: Boolean) extends AtomicTableWriteExec {

  override protected def doExecute(): RDD[InternalRow] = {
    val staged = if (orCreate) {
      catalog.stageCreateOrReplace(
        ident, query.schema, partitioning.toArray, properties.asJava)
    } else if (catalog.tableExists(ident)) {
      try {
        catalog.stageReplace(
          ident, query.schema, partitioning.toArray, properties.asJava)
      } catch {
        case e: NoSuchTableException =>
          throw new CannotReplaceMissingTableException(ident, Some(e))
      }
    } else {
      throw new CannotReplaceMissingTableException(ident)
    }
    writeToStagedTable(staged, writeOptions, ident)
  }
}

/**
 * Physical plan node for append into a v2 table.
 *
 * Rows in the output data set are appended.
 */
case class AppendDataExec(
    table: SupportsWrite,
    writeOptions: CaseInsensitiveStringMap,
    query: SparkPlan) extends V2TableWriteExec with BatchWriteHelper {

  override protected def doExecute(): RDD[InternalRow] = {
    val batchWrite = newWriteBuilder().buildForBatch()
    doWrite(batchWrite)
  }
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
    table: SupportsWrite,
    deleteWhere: Array[Filter],
    writeOptions: CaseInsensitiveStringMap,
    query: SparkPlan) extends V2TableWriteExec with BatchWriteHelper {

  private def isTruncate(filters: Array[Filter]): Boolean = {
    filters.length == 1 && filters(0).isInstanceOf[AlwaysTrue]
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val batchWrite = newWriteBuilder() match {
      case builder: SupportsTruncate if isTruncate(deleteWhere) =>
        builder.truncate().buildForBatch()

      case builder: SupportsOverwrite =>
        builder.overwrite(deleteWhere).buildForBatch()

      case _ =>
        throw new SparkException(s"Table does not support overwrite by expression: $table")
    }

    doWrite(batchWrite)
  }
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
    table: SupportsWrite,
    writeOptions: CaseInsensitiveStringMap,
    query: SparkPlan) extends V2TableWriteExec with BatchWriteHelper {

  override protected def doExecute(): RDD[InternalRow] = {
    val batchWrite = newWriteBuilder() match {
      case builder: SupportsDynamicOverwrite =>
        builder.overwriteDynamicPartitions().buildForBatch()

      case _ =>
        throw new SparkException(s"Table does not support dynamic partition overwrite: $table")
    }

    doWrite(batchWrite)
  }
}

case class WriteToDataSourceV2Exec(
    batchWrite: BatchWrite,
    query: SparkPlan) extends V2TableWriteExec {

  def writeOptions: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()

  override protected def doExecute(): RDD[InternalRow] = {
    doWrite(batchWrite)
  }
}

/**
 * Helper for physical plans that build batch writes.
 */
trait BatchWriteHelper {
  def table: SupportsWrite
  def query: SparkPlan
  def writeOptions: CaseInsensitiveStringMap

  def newWriteBuilder(): WriteBuilder = {
    table.newWriteBuilder(writeOptions)
        .withInputDataSchema(query.schema)
        .withQueryId(UUID.randomUUID().toString)
  }
}

/**
 * The base physical plan for writing data into data source v2.
 */
trait V2TableWriteExec extends UnaryExecNode {
  def query: SparkPlan

  var commitProgress: Option[StreamWriterCommitProgress] = None

  override def child: SparkPlan = query
  override def output: Seq[Attribute] = Nil

  protected def doWrite(batchWrite: BatchWrite): RDD[InternalRow] = {
    val writerFactory = batchWrite.createBatchWriterFactory()
    val useCommitCoordinator = batchWrite.useCommitCoordinator
    val rdd = query.execute()
    // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
    // partition rdd to make sure we at least set up one write task to write the metadata.
    val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
      sparkContext.parallelize(Array.empty[InternalRow], 1)
    } else {
      rdd
    }
    val messages = new Array[WriterCommitMessage](rddWithNonEmptyPartitions.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(s"Start processing data source write support: $batchWrite. " +
      s"The input RDD has ${messages.length} partitions.")

    try {
      sparkContext.runJob(
        rddWithNonEmptyPartitions,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          DataWritingSparkTask.run(writerFactory, context, iter, useCommitCoordinator),
        rddWithNonEmptyPartitions.partitions.indices,
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
            throw new SparkException("Writing job failed.", cause)
        }
        logError(s"Data source write support $batchWrite aborted.")
        cause match {
          // Only wrap non fatal exceptions.
          case NonFatal(e) => throw new SparkException("Writing job aborted.", e)
          case _ => throw cause
        }
    }

    sparkContext.emptyRDD
  }
}

object DataWritingSparkTask extends Logging {
  def run(
      writerFactory: DataWriterFactory,
      context: TaskContext,
      iter: Iterator[InternalRow],
      useCommitCoordinator: Boolean): DataWritingSparkTaskResult = {
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
        // Count is here.
        count += 1
        dataWriter.write(iter.next())
      }

      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(stageId, stageAttempt, partId, attemptId)
        if (commitAuthorized) {
          logInfo(s"Commit authorized for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)")
          dataWriter.commit()
        } else {
          val message = s"Commit denied for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)"
          logInfo(message)
          // throwing CommitDeniedException will trigger the catch block for abort
          throw new CommitDeniedException(message, stageId, partId, attemptId)
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
    })
  }
}

private[v2] trait AtomicTableWriteExec extends V2TableWriteExec {
  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits.IdentifierHelper

  protected def writeToStagedTable(
      stagedTable: StagedTable,
      writeOptions: CaseInsensitiveStringMap,
      ident: Identifier): RDD[InternalRow] = {
    Utils.tryWithSafeFinallyAndFailureCallbacks({
      stagedTable match {
        case table: SupportsWrite =>
          val batchWrite = table.newWriteBuilder(writeOptions)
            .withInputDataSchema(query.schema)
            .withQueryId(UUID.randomUUID().toString)
            .buildForBatch()

          val writtenRows = doWrite(batchWrite)
          stagedTable.commitStagedChanges()
          writtenRows
        case _ =>
          // Table does not support writes - staged changes are also rolled back below.
          throw new SparkException(
            s"Table implementation does not support writes: ${ident.quoted}")
      }
    })(catchBlock = {
      // Failure rolls back the staged writes and metadata changes.
      stagedTable.abortStagedChanges()
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
