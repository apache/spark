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

import org.scalatest.{BeforeAndAfterEach, Suite}

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.connector.catalog.SharedTablesInMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.functions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.autocdc.{
  ChangeArgs,
  ColumnSelection,
  Scd1BatchProcessor,
  ScdType,
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.common.RunState
import org.apache.spark.sql.pipelines.logging.RunProgress
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Shared helpers for AutoCDC end-to-end graph-execution test suites.
 */
trait AutoCdcGraphExecutionTestMixin extends BeforeAndAfterEach {
  self: Suite with ExecutionTest with SharedSparkSession =>

  /** v2 catalog name registered for AutoCDC E2E tests. Tests qualify tables as `cat.ns1.t`. */
  protected val catalog: String = "cat"

  /** Namespace under [[catalog]] used by AutoCDC E2E tests. */
  protected val namespace: String = "ns1"

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set(
      s"spark.sql.catalog.$catalog",
      classOf[SharedTablesInMemoryRowLevelOperationTableCatalog].getName
    )
    // Disable per-flow retries so failure-path tests (e.g. KEY_SCHEMA_DRIFT, INCOMPATIBLE_DATA)
    // surface the AnalysisException after the first attempt instead of going through the default
    // 2 retries, which would otherwise emit duplicate FAILED events and inflate test runtime
    // without changing the asserted outcome.
    spark.conf.set(SQLConf.PIPELINES_MAX_FLOW_RETRY_ATTEMPTS.key, "0")
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.$namespace")
  }

  override protected def afterEach(): Unit = {
    SharedTablesInMemoryRowLevelOperationTableCatalog.reset()
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$catalog")
    spark.sessionState.conf.unsetConf(SQLConf.PIPELINES_MAX_FLOW_RETRY_ATTEMPTS.key)
    super.afterEach()
  }

  /**
   * Run a pipeline to completion. If any flow emitted a [[RunProgress]] event with state
   * [[RunState.FAILED]], collect every error from the event buffer and throw a single
   * exception listing them, so that test failures surface meaningful stack traces instead of
   * generic "test exited normally but flow failed" errors.
   */
  protected def runPipeline(ctx: TestGraphRegistrationContext): Unit = {
    val updateCtx = TestPipelineUpdateContext(spark, ctx.toDataflowGraph, storageRoot)
    updateCtx.pipelineExecution.runPipeline()
    updateCtx.pipelineExecution.awaitCompletion()

    if (updateCtx.eventBuffer.getEvents.exists(_.details == RunProgress(RunState.FAILED))) {
      val errors = updateCtx.eventBuffer.getEvents.flatMap(_.error)
      val ex = new RuntimeException(
        s"Pipeline run failed with ${errors.size} error(s):\n" +
        errors.map { e =>
          val stackSnippet = e.getStackTrace
            .map(f => s"    at $f")
            .mkString("\n")
          s"  ${e.getClass.getSimpleName}: ${e.getMessage}\n$stackSnippet"
        }.mkString("\n")
      )
      errors.foreach(ex.addSuppressed)
      throw ex
    }
  }

  /**
   * Walk every [[Throwable]] reachable from `failure` via [[Throwable#getSuppressed]] and
   * [[Throwable#getCause]] for the first [[SparkThrowable]] whose
   * [[SparkThrowable#getCondition]] equals `condition`, then run [[checkError]] against that
   * exception with all of its other arguments propagated through.
   */
  protected def checkErrorInPipelineFailure(
      failure: Throwable,
      condition: String,
      sqlState: Option[String] = None,
      parameters: Map[String, String] = Map.empty,
      matchPVals: Boolean = false,
      queryContext: Array[ExpectedContext] = Array.empty): Unit = {

    def causeChain(t: Throwable): Iterator[Throwable] =
      Iterator.iterate[Throwable](t)(_.getCause).takeWhile(_ != null)

    def reachable: Iterator[Throwable] =
      (Iterator(failure) ++ failure.getSuppressed.iterator).flatMap(causeChain)

    val matched = reachable.collectFirst {
      case t: SparkThrowable if t.getCondition == condition => t
    }
    assert(
      matched.isDefined,
      s"Expected a SparkThrowable with condition '$condition' reachable from the runPipeline " +
      s"failure chain, got top-level: ${failure.getMessage}; chain:\n" +
      reachable
        .map(t => s"  ${t.getClass.getSimpleName}: ${t.getMessage}")
        .mkString("\n")
    )
    checkError(
      exception = matched.get,
      condition = condition,
      sqlState = sqlState,
      parameters = parameters,
      matchPVals = matchPVals,
      queryContext = queryContext
    )
  }

  /**
   * DDL fragment for the AutoCDC metadata column appended to every SCD1 target table. Use
   * inside a `CREATE TABLE` statement, for example:
   *   `CREATE TABLE t (id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)`
   *
   * Assumes sequence type is BIGINT (Long).
   */
  protected val cdcMetadataDdl: String = {
    val col = Scd1BatchProcessor.cdcMetadataColName
    val del = Scd1BatchProcessor.cdcDeleteSequenceFieldName
    val ups = Scd1BatchProcessor.cdcUpsertSequenceFieldName
    s"$col STRUCT<$del:BIGINT,$ups:BIGINT> NOT NULL"
  }

  /**
   * Insert a pre-existing row into a target table, populating the CDC metadata struct so the
   * row looks as if a previous AutoCDC run upserted it at sequencing version [[sequence]].
   *
   * @param table     Fully-qualified table name (catalog.schema.table).
   * @param colValues Comma-separated SQL literals for the user-defined columns, in declared
   *                  order, excluding the trailing CDC metadata column.
   * @param sequence  Value to seed `_cdc_metadata.upsertSequence` with. The
   *                  `deleteSequence` field is left NULL.
   */
  protected def insertPreloadedRow(table: String, colValues: String, sequence: Long): Unit = {
    val del = Scd1BatchProcessor.cdcDeleteSequenceFieldName
    val ups = Scd1BatchProcessor.cdcUpsertSequenceFieldName
    spark.sql(
      s"INSERT INTO $table SELECT $colValues, " +
      s"named_struct('$del', CAST(NULL AS BIGINT), '$ups', CAST($sequence AS BIGINT))"
    )
  }

  /** Catalog identifier of the AutoCDC auxiliary table for [[targetTableName]]. */
  protected def auxTableNameFor(targetTableName: String): String = {
    val targetIdent = fullyQualifiedIdentifier(targetTableName, Some(catalog), Some(namespace))
    AutoCdcAuxiliaryTable.identifier(targetIdent).unquotedString
  }

  /**
   * Construct an [[AutoCdcFlow]] targeting `catalog.namespace.${target}` from the given
   * query and CDC knobs.
   */
  protected def autoCdcFlow(
      name: String,
      target: String,
      query: FlowFunction,
      keys: Seq[String],
      sequencing: Column,
      columnSelection: Option[ColumnSelection] = None,
      deleteCondition: Option[Column] = None,
      scdType: ScdType = ScdType.Type1
  ): AutoCdcFlow = AutoCdcFlow(
    identifier = fullyQualifiedIdentifier(name, Some(catalog), Some(namespace)),
    destinationIdentifier = fullyQualifiedIdentifier(target, Some(catalog), Some(namespace)),
    func = query,
    queryContext = QueryContext(
      currentCatalog = Some(catalog),
      currentDatabase = Some(namespace)
    ),
    origin = QueryOrigin.empty,
    changeArgs = ChangeArgs(
      keys = keys.map(UnqualifiedColumnName(_)),
      sequencing = sequencing,
      columnSelection = columnSelection,
      deleteCondition = deleteCondition,
      storedAsScdType = scdType
    )
  )

  /**
   * Build a single-flow AutoCDC pipeline: a [[TestGraphRegistrationContext]] that registers
   * `target` under [[catalog]].[[namespace]] and one [[autoCdcFlow]] writing into it from
   * `sourceDf`. Covers the common single-table/single-flow shape used across the AutoCDC E2E
   * suites; tests that need multiple flows or non-AutoCDC datasets build the context inline.
   */
  protected def singleAutoCdcFlowPipeline(
      flowName: String,
      target: String,
      sourceDf: DataFrame,
      keys: Seq[String],
      sequencing: Column = functions.col("version"),
      columnSelection: Option[ColumnSelection] = None,
      deleteCondition: Option[Column] = None,
      scdType: ScdType = ScdType.Type1): TestGraphRegistrationContext =
    new TestGraphRegistrationContext(spark) {
      registerTable(target, catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = flowName,
        target = target,
        query = dfFlowFunc(sourceDf),
        keys = keys,
        sequencing = sequencing,
        columnSelection = columnSelection,
        deleteCondition = deleteCondition,
        scdType = scdType
      ))
    }

  /** Build a target row's `_cdc_metadata` struct value. */
  protected def cdcMeta(deleteSeq: Option[Long], upsertSeq: Option[Long]): Row =
    Row(deleteSeq.orNull, upsertSeq.orNull)
}
