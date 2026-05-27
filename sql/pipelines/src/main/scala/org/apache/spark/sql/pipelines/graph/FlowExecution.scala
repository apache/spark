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

package org.apache.spark.sql.pipelines.graph

import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.{AnalysisException, Dataset, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRowLevelOperations, TableCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.pipelines.autocdc.{
  AutoCdcReservedNames,
  ChangeArgs,
  Scd1BatchProcessor,
  Scd1ForeachBatchHandler
}
import org.apache.spark.sql.pipelines.graph.QueryOrigin.ExceptionHelpers
import org.apache.spark.sql.pipelines.util.SparkSessionUtils
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.ThreadUtils

/**
 * A flow's execution may complete for two reasons:
 * 1. it may finish performing all of its necessary work, or
 * 2. it may be interrupted by a request from a user to stop it.
 *
 * We use this result to disambiguate these two cases, using 'ExecutionResult.FINISHED'
 * for the former and 'ExecutionResult.STOPPED' for the latter.
 */
sealed trait ExecutionResult
object ExecutionResult {
  case object FINISHED extends ExecutionResult
  case object STOPPED extends ExecutionResult
}

/** A `FlowExecution` specifies how to execute a flow and manages its execution. */
trait FlowExecution {

  /** Identifier of this physical flow */
  def identifier: TableIdentifier

  /**
   * Returns the user-visible name of this flow.
   */
  final def displayName: String = identifier.unquotedString

  /**
   * SparkSession to execute this physical flow with.
   *
   * The default value for streaming flows is the pipeline's spark session because the source
   * dataframe is resolved using the pipeline's spark session, and a new session will be started
   * implicitly by the streaming query.
   *
   * The default value for batch flows is a cloned spark session from the pipeline's spark session.
   *
   * Please make sure that the execution thread runs in a different spark session than the
   * pipeline's spark session.
   */
  protected def spark: SparkSession = updateContext.spark

  /**
   * Origin to use when recording events for this flow.
   */
  def getOrigin: QueryOrigin

  /**
   * Returns true if and only if this `FlowExecution` has been completed with
   * either success or an exception.
   */
  def isCompleted: Boolean = _future.exists(_.isCompleted)

  /** Returns true iff this `FlowExecution` executes using Spark Structured Streaming. */
  def isStreaming: Boolean

  /** Retrieves the future that can be used to track execution status. */
  def getFuture: Future[ExecutionResult] = {
    _future.getOrElse(
      throw new IllegalStateException(s"FlowExecution $identifier has not been executed.")
    )
  }

  /** Tracks the currently running future. */
  private final var _future: Option[Future[ExecutionResult]] = None

  /** Context about this pipeline update. */
  def updateContext: PipelineUpdateContext

  /** The thread execution context for the current `FlowExecution`. */
  implicit val executionContext: ExecutionContext = {
    ExecutionContext.fromExecutor(FlowExecution.threadPool)
  }

  /**
   * Stops execution of this `FlowExecution`. If you override this, please be sure to
   * call `super.stop()` at the beginning of your method, so we can properly handle errors
   * when a user tries to stop a flow.
   */
  def stop(): Unit = {
    stopped.set(true)
  }

  /** Returns an optional exception that occurred during execution, if any. */
  def exception: Option[Throwable] = _future.flatMap(_.value).flatMap(_.failed.toOption)

  /**
   * Executes this FlowExecution synchronously to perform its intended update.
   * This method should be overridden by subclasses to provide the actual execution logic.
   *
   * @return a Future that completes when the execution is finished or stopped.
   */
  def executeInternal(): Future[Unit]

  /**
   * Executes this FlowExecution asynchronously to perform its intended update. A future that can be
   * used to track execution status is saved, and can be retrieved with `getFuture`.
   */
  final def executeAsync(): Unit = {
    if (_future.isDefined) {
      throw new IllegalStateException(
        s"FlowExecution ${identifier.unquotedString} has already been executed."
      )
    }

    val queryOrigin = QueryOrigin(filePath = getOrigin.filePath)

    _future = try {
      Option(
        executeInternal()
          .map(_ => ExecutionResult.FINISHED)
          .recover {
            case _: Throwable if stopped.get() =>
              ExecutionResult.STOPPED
          }
      )
    } catch {
      case NonFatal(e) =>
        // Add query origin to exceptions raised while starting a flow
        throw e.addOrigin(queryOrigin)
    }
  }

  /** The destination that this `FlowExecution` is writing to. */
  def destination: Output

  /** Whether this `FlowExecution` has been stopped. Set by `FlowExecution.stop()`. */
  private val stopped: AtomicBoolean = new AtomicBoolean(false)
}

object FlowExecution {

  /** A thread pool used to execute `FlowExecutions`. */
  private val threadPool: ThreadPoolExecutor = {
    ThreadUtils.newDaemonCachedThreadPool("FlowExecution")
  }
}

/** A 'FlowExecution' that processes data statefully using Structured Streaming. */
trait StreamingFlowExecution extends FlowExecution with Logging {

  /** The `ResolvedFlow` that this `StreamingFlowExecution` is executing. */
  def flow: ResolvedFlow

  /** Structured Streaming checkpoint. */
  def checkpointPath: String

  /** Structured Streaming trigger. */
  def trigger: Trigger

  def isStreaming: Boolean = true

  /** Spark confs that must be set when starting this flow. */
  protected def sqlConf: Map[String, String]

  /** Starts a stream and returns its streaming query. */
  protected def startStream(): StreamingQuery

  private var _streamingQuery: Option[StreamingQuery] = None

  /** Visible for testing */
  def getStreamingQuery: StreamingQuery =
    _streamingQuery.getOrElse(
      throw new IllegalStateException("StreamingPhysicalFlow has not been started")
    )

  /**
   * Executes this `StreamingFlowExecution` by starting its stream with the correct scheduling pool
   * and confs.
   */
  override final def executeInternal(): Future[Unit] = {
    logInfo(
      log"Starting ${MDC(LogKeys.TABLE_NAME, identifier)} with " +
      log"checkpoint location ${MDC(LogKeys.CHECKPOINT_PATH, checkpointPath)}"
    )
    val streamingQuery = SparkSessionUtils.withSqlConf(spark, sqlConf.toList: _*)(startStream())
    _streamingQuery = Option(streamingQuery)
    Future(streamingQuery.awaitTermination())
  }
}

/** A `StreamingFlowExecution` that writes a streaming `DataFrame` to a `Table`. */
class StreamingTableWrite(
    val identifier: TableIdentifier,
    val flow: ResolvedFlow,
    val graph: DataflowGraph,
    val updateContext: PipelineUpdateContext,
    val checkpointPath: String,
    val trigger: Trigger,
    val destination: Table,
    val sqlConf: Map[String, String]
) extends StreamingFlowExecution {

  override def getOrigin: QueryOrigin = flow.origin

  def startStream(): StreamingQuery = {
    val data = graph.reanalyzeFlow(flow).df
    val dataStreamWriter = data
      .writeStream
      .queryName(displayName)
      .option("checkpointLocation", checkpointPath)
      .trigger(trigger)
      .outputMode(OutputMode.Append())
    destination.format.foreach(dataStreamWriter.format)
    dataStreamWriter.toTable(destination.identifier.unquotedString)
  }
}

/** A `FlowExecution` that writes a batch `DataFrame` to a `Table`. */
class BatchTableWrite(
    val identifier: TableIdentifier,
    val flow: ResolvedFlow,
    val graph: DataflowGraph,
    val destination: Table,
    val updateContext: PipelineUpdateContext,
    val sqlConf: Map[String, String]
) extends FlowExecution {

  override final def isStreaming: Boolean = false
  override def getOrigin: QueryOrigin = flow.origin

  def executeInternal(): Future[Unit] = {
    SparkSessionUtils.withSqlConf(spark, sqlConf.toList: _*) {
      updateContext.flowProgressEventLogger.recordRunning(flow = flow)
      val data = graph.reanalyzeFlow(flow).df
      Future {
        val dataFrameWriter = data.write
        destination.format.foreach(dataFrameWriter.format)

        // In "append" mode with saveAsTable, partition/cluster columns must be specified in query
        // because the format and options of the existing table is used, and the table could
        // have been created with partition columns.
        destination.clusterCols.foreach { clusterCols =>
          dataFrameWriter.clusterBy(clusterCols.head, clusterCols.tail: _*)
        }
        destination.partitionCols.foreach { partitionCols =>
          dataFrameWriter.partitionBy(partitionCols: _*)
        }

        dataFrameWriter
          .mode("append")
          .saveAsTable(destination.identifier.unquotedString)
      }
    }
  }
}

/** A `StreamingFlowExecution` that writes a streaming `DataFrame` to a `Sink`. */
class SinkWrite(
  val identifier: TableIdentifier,
  val flow: ResolvedFlow,
  val graph: DataflowGraph,
  val updateContext: PipelineUpdateContext,
  val checkpointPath: String,
  val trigger: Trigger,
  val destination: Sink,
  val sqlConf: Map[String, String]
) extends StreamingFlowExecution {

  override def getOrigin: QueryOrigin = flow.origin

  def startStream(): StreamingQuery = {
    val data = graph.reanalyzeFlow(flow).df
    data.writeStream
      .queryName(displayName)
      .option("checkpointLocation", checkpointPath)
      .trigger(trigger)
      .outputMode(OutputMode.Append())
      .format(destination.format)
      .options(destination.options)
      .start()
  }
}

object AutoCdcAuxiliaryTable {
  /**
   * Helper for deriving the auxiliary AutoCDC catalog table identifier from a target table. The
   * derived name is anchored on [[AutoCdcReservedNames.prefix]] so it is unambiguously
   * AutoCDC-managed and cannot collide with a user-managed table.
   */
  def identifier(destination: TableIdentifier): TableIdentifier = TableIdentifier(
    table = s"${AutoCdcReservedNames.prefix}aux_state_${destination.table}",
    database = destination.database,
    catalog = destination.catalog
  )

  /**
   * Reserved table property key set on the auxiliary table to record which SCD strategy it
   * serves.
   */
  val scdTypePropertyKey: String = s"${PipelinesTableProperties.pipelinesPrefix}autocdc.scd_type"
}

/**
 * Base trait for AutoCDC merge-based write flows.
 */
trait AutoCdcMergeWriteBase {
  /** The spark session the AutoCDC flow is going to be planned in. */
  protected def spark: SparkSession

  /** The destination (target) table entity the AutoCDC flow will be writing to. */
  protected def destination: Table

  /** The AutoCDC flow's [[ChangeArgs]] (keys, sequencing, columnSelection, ...). */
  protected def changeArgs: ChangeArgs

  /** Full schema of the auxiliary table for this SCD type. */
  protected def auxiliaryTableSchema: StructType

  /**
   * Idempotently create the auxiliary table for [[destination]] if it does not already exist
   * and return its [[TableIdentifier]].
   *
   * Note that this is `CREATE TABLE IF NOT EXISTS`: when the aux table already exists, its
   * schema is left untouched and `auxiliaryTableSchema` is ignored. For SCD1, they keys must be
   * invariant across executions and the CDC metadata will always be present, so this is correct.
   */
  protected def createAuxiliaryTableIfNotExists(spark: SparkSession): TableIdentifier = {
    val auxIdent = AutoCdcAuxiliaryTable.identifier(destination.identifier)
    // The auxiliary table inherits the target's format so MERGE semantics line up. When the
    // target's format is unspecified (None), omit the USING clause and fall back to the
    // session's default source provider.
    val usingClause = destination.format.map(fmt => s"USING $fmt").getOrElse("")
    val tblPropertiesClause =
      s"TBLPROPERTIES ('${AutoCdcAuxiliaryTable.scdTypePropertyKey}' = " +
        s"'${changeArgs.storedAsScdType.label}')"
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS
         |${auxIdent.quotedString}
         |(${auxiliaryTableSchema.toDDL}) $usingClause $tblPropertiesClause""".stripMargin
    )
    auxIdent
  }

  /**
   * Validate that the target table's underlying connector implements
   * [[SupportsRowLevelOperations]], which is the V2 connector contract for MERGE/UPDATE/DELETE
   * with rewrite - all operations that the AutoCDC transformation executes.
   */
  protected def requireDestinationSupportsRowLevelOps(): Unit = {
    val (catalog, v2Identifier) = resolveTableCatalog(spark, destination.identifier)
    val destinationTable = catalog.loadTable(v2Identifier)

    if (!destinationTable.isInstanceOf[SupportsRowLevelOperations]) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE",
        messageParameters = Map(
          "tableName" -> destination.identifier.quotedString,
          "format" -> destination.format.orElse(
              Option(
                destinationTable.properties.get(TableCatalog.PROP_PROVIDER)
              )
            )
            .getOrElse("<unknown>")
        )
      )
    }
  }

  private def resolveTableCatalog(
      spark: SparkSession,
      ident: TableIdentifier): (TableCatalog, Identifier) = {
    val catalogManager = spark.sessionState.catalogManager
    val catalogPlugin = ident.catalog
      .map(catalogManager.catalog)
      .getOrElse(catalogManager.currentCatalog)
    val catalog = catalogPlugin match {
      case t: TableCatalog => t
      case _ => throw QueryCompilationErrors.missingCatalogTablesAbilityError(catalogPlugin)
    }
    val namespace = ident.database.getOrElse(
      throw SparkException.internalError(
        s"Cannot resolve table identifier ${ident.quotedString}: namespace is unspecified."
      )
    )
    (catalog, Identifier.of(Array(namespace), ident.table))
  }
}

/**
 * A [[StreamingFlowExecution]] that applies a CDC event stream to a target [[Table]] via
 * SCD Type 1 MERGE semantics.
 */
class Scd1MergeStreamingWrite(
    val identifier: TableIdentifier,
    val flow: AutoCdcMergeFlow,
    val graph: DataflowGraph,
    val updateContext: PipelineUpdateContext,
    val checkpointPath: String,
    val trigger: Trigger,
    val destination: Table,
    val sqlConf: Map[String, String]
) extends StreamingFlowExecution with AutoCdcMergeWriteBase {

  requireDestinationSupportsRowLevelOps()

  override def getOrigin: QueryOrigin = flow.origin

  override protected def changeArgs: ChangeArgs = flow.changeArgs

  override def startStream(): StreamingQuery = {
    val sourceChangeDataFeed = graph.reanalyzeFlow(flow).df

    // The auxiliary table is created here (at flow execution) rather than during flow resolution
    // or dataset materialization for two reasons:
    //   1. It is an internal state store: we deliberately keep it out of the graph registration
    //      context's table set so that it is invisible to other flows and the [[DatasetManager]]
    //      will never materialize it.
    //   2. Its format must match the target table's, which only exists after the target is
    //      materialized. Flow resolution must also stay side-effect free (e.g. for dry runs).
    val auxiliaryTableIdentifier = createAuxiliaryTableIfNotExists(spark = updateContext.spark)

    val foreachBatchHandler = Scd1ForeachBatchHandler(
      batchProcessor = Scd1BatchProcessor(
        changeArgs = flow.changeArgs,
        resolvedSequencingType = flow.sequencingType
      ),
      auxiliaryTableIdentifier = auxiliaryTableIdentifier,
      targetTableIdentifier = destination.identifier
    )

    sourceChangeDataFeed.writeStream
      .queryName(displayName)
      .option("checkpointLocation", checkpointPath)
      .trigger(trigger)
      .foreachBatch((batch: Dataset[Row], batchId: Long) => {
        foreachBatchHandler.execute(batch, batchId)
      })
      .start()
  }

  override protected lazy val auxiliaryTableSchema: StructType =
    // SCD1's auxiliary table is just keys + the CDC metadata struct; no user data columns. Keys
    // come first, in `changeArgs.keys` declaration order, to anchor the per-key sequence
    // watermark used to gate out-of-order events.
    StructType(autoCdcKeyFields :+ cdcMetadataField)

  /**
   * AutoCDC key columns resolved out of the flow's augmented schema, in
   * `changeArgs.keys` declaration order. Keys are guaranteed to be present in the schema
   * because [[AutoCdcMergeFlow.schema]] validates that.
   */
  private lazy val autoCdcKeyFields: Seq[StructField] = {
    val resolver = updateContext.spark.sessionState.conf.resolver
    val targetTableSchema = flow.schema
    flow.changeArgs.keys.map { key =>
      targetTableSchema.fields
        .find(field => resolver(field.name, key.name))
        .getOrElse(
          throw SparkException.internalError(
            s"Key column '${key.name}' was not found in the AutoCDC flow's selected schema."
          )
        )
    }
  }

  /** CDC metadata field resolved out of the flow's augmented schema. */
  private lazy val cdcMetadataField: StructField = {
    val resolver = updateContext.spark.sessionState.conf.resolver
    flow.schema.fields
      .find(field => resolver(field.name, Scd1BatchProcessor.cdcMetadataColName))
      .getOrElse(
        throw SparkException.internalError(
          s"CDC metadata column '${Scd1BatchProcessor.cdcMetadataColName}' was not found in the " +
          s"AutoCDC flow's target table schema."
        )
      )
  }
}
