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

import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{functions => F, AnalysisException, Column}
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.pipelines.AnalysisWarning
import org.apache.spark.sql.pipelines.autocdc.{
  AutoCdcReservedNames,
  CaseSensitivityLabels,
  ChangeArgs,
  ColumnSelection,
  Scd1BatchProcessor,
  ScdType
}
import org.apache.spark.sql.pipelines.util.InputReadOptions
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Contains the catalog and database context information for query execution.
 */
case class QueryContext(currentCatalog: Option[String], currentDatabase: Option[String])

/**
 * A [[Flow]] is a node of data transformation in a dataflow graph. It describes the movement
 * of data into a particular dataset.
 */
trait Flow extends GraphElement with Logging {

  /** The [[FlowFunction]] containing the user's query. */
  def func: FlowFunction

  val identifier: TableIdentifier

  /**
   * The dataset that this Flow represents a write to.
   */
  val destinationIdentifier: TableIdentifier

  /**
   * Whether this is a ONCE flow. ONCE flows should run only once per full refresh.
   */
  def once: Boolean = false

  /** The current query context (catalog and database) when the query is defined. */
  def queryContext: QueryContext

  def sqlConf: Map[String, String]
}

/** A wrapper for a resolved internal input that includes the alias provided by the user. */
case class ResolvedInput(input: Input, aliasIdentifier: AliasIdentifier)

/** A wrapper for the lambda function that defines a [[Flow]]. */
trait FlowFunction extends Logging {

  /**
   * This function defines the transformations performed by a flow, expressed as a DataFrame.
   *
   * @param allInputs the set of identifiers for all the [[Input]]s defined in the
   *                  [[DataflowGraph]].
   * @param availableInputs the list of all [[Input]]s available to this flow
   * @param configuration the spark configurations that apply to this flow.
   * @param queryContext The context of the query being evaluated.
   * @param queryOrigin The source code location of the flow definition this flow function was
   *                    instantiated from.
   * @return the inputs actually used, and the DataFrame expression for the flow
   */
  def call(
      allInputs: Set[TableIdentifier],
      availableInputs: Seq[Input],
      configuration: Map[String, String],
      queryContext: QueryContext,
      queryOrigin: QueryOrigin
  ): FlowFunctionResult
}

/**
 * Holds the DataFrame returned by a [[FlowFunction]] along with the inputs used to
 * construct it.
 * @param batchInputs the complete inputs read by the flow
 * @param streamingInputs the incremental inputs read by the flow
 * @param usedExternalInputs the identifiers of the external inputs read by the flow
 * @param dataFrame the DataFrame expression executed by the flow if the flow can be resolved
 */
case class FlowFunctionResult(
    requestedInputs: Set[TableIdentifier],
    batchInputs: Set[ResolvedInput],
    streamingInputs: Set[ResolvedInput],
    usedExternalInputs: Set[TableIdentifier],
    dataFrame: Try[DataFrame],
    sqlConf: Map[String, String],
    analysisWarnings: Seq[AnalysisWarning] = Nil) {

  /**
   * Returns the names of all of the [[Input]]s used when resolving this [[Flow]]. If the
   * flow failed to resolve, we return the all the datasets that were requested when evaluating the
   * flow.
   */
  def inputs: Set[TableIdentifier] = {
    (batchInputs ++ streamingInputs).map(_.input.identifier)
  }

  /** Returns errors that occurred when attempting to analyze this [[Flow]]. */
  def failure: Seq[Throwable] = {
    dataFrame.failed.toOption.toSeq
  }

  /** Whether this [[Flow]] is successfully analyzed. */
  final def resolved: Boolean = failure.isEmpty // don't override this, override failure
}

/** A [[Flow]] whose output schema and dependencies aren't known. */
sealed trait UnresolvedFlow extends Flow {
  /** Returns a copy of this flow with the given SQL confs overriding the existing ones. */
  def withSqlConf(newSqlConf: Map[String, String]): UnresolvedFlow
}

/**
 * An [[UnresolvedFlow]] whose execution-type has not yet been determined.
 *
 * In some cases, we know the execution-type for an [[UnresolvedFlow]] even before flow analysis
 * and resolution. For example an AutoCDCFlow is a special unresolved-but-typed flow; we know a
 * flow will be an AutoCDC flow immediately on construction, because it has its own special
 * registration API. Such flows are considered "typed flows", but there isn't any semantic reason
 * yet to explicitly introduce a `TypedFlow` trait/class.
 */
case class UntypedFlow(
    identifier: TableIdentifier,
    destinationIdentifier: TableIdentifier,
    func: FlowFunction,
    queryContext: QueryContext,
    sqlConf: Map[String, String],
    override val once: Boolean,
    override val origin: QueryOrigin
) extends UnresolvedFlow {
  override def withSqlConf(newSqlConf: Map[String, String]): UntypedFlow =
    copy(sqlConf = newSqlConf)
}

/**
 * An unresolved but typed flow that applies a CDC event stream to a target table via MERGE.
 *
 * [[AutoCdcFlow]] is a typed flow because it is only supported for streaming, and not as a once
 * flow. Therefore by definition it is a streaming-type flow.
 *
 * In the future once-support for [[AutoCdcFlow]] may be added.
 */
case class AutoCdcFlow(
    identifier: TableIdentifier,
    destinationIdentifier: TableIdentifier,
    func: FlowFunction,
    queryContext: QueryContext,
    sqlConf: Map[String, String] = Map.empty,
    comment: Option[String] = None,
    override val origin: QueryOrigin,
    changeArgs: ChangeArgs
) extends UnresolvedFlow {
  override val once: Boolean = false

  override def withSqlConf(newSqlConf: Map[String, String]): AutoCdcFlow =
    copy(sqlConf = newSqlConf)
}

/**
 * A [[Flow]] whose flow function has been invoked, meaning either:
 *  - Its output schema and dependencies are known.
 *  - It failed to resolve.
 */
trait ResolutionCompletedFlow extends Flow {
  def flow: UnresolvedFlow
  def funcResult: FlowFunctionResult

  val identifier: TableIdentifier = flow.identifier
  val destinationIdentifier: TableIdentifier = flow.destinationIdentifier
  def func: FlowFunction = flow.func
  def queryContext: QueryContext = flow.queryContext
  def sqlConf: Map[String, String] = funcResult.sqlConf
  def origin: QueryOrigin = flow.origin
}

/** A [[Flow]] whose flow function has failed to resolve. */
class ResolutionFailedFlow(
    val flow: UnresolvedFlow,
    val funcResult: FlowFunctionResult)
  extends ResolutionCompletedFlow {
  assert(!funcResult.resolved)

  def failure: Seq[Throwable] = funcResult.failure
}

/** A [[Flow]] whose flow function has successfully resolved. */
trait ResolvedFlow extends ResolutionCompletedFlow with Input {
  assert(funcResult.resolved)

  /** The logical plan for this flow's query. */
  def df: DataFrame = funcResult.dataFrame.get

  /** Returns the schema of the output of this [[Flow]]. */
  def schema: StructType = df.schema
  override def load(readOptions: InputReadOptions): DataFrame = df
  def inputs: Set[TableIdentifier] = funcResult.inputs
}

/** A [[Flow]] that represents stateful movement of data to some target. */
class StreamingFlow(
    val flow: UnresolvedFlow,
    val funcResult: FlowFunctionResult,
    val mustBeAppend: Boolean = false
) extends ResolvedFlow

/** A [[Flow]] that declares exactly what data should be in the target table. */
class CompleteFlow(
    val flow: UnresolvedFlow,
    val funcResult: FlowFunctionResult,
    val mustBeAppend: Boolean = false
) extends ResolvedFlow

/** A [[Flow]] that reads source[s] completely and appends data to the target, just once.
 */
class AppendOnceFlow(
    val flow: UnresolvedFlow,
    val funcResult: FlowFunctionResult
) extends ResolvedFlow {

  override val once = true
}

/**
 * A resolved flow that applies a CDC event stream to a target table via MERGE, in accordance to
 * the configured [[flow.changeArgs]].
 */
class AutoCdcMergeFlow(
    val flow: AutoCdcFlow,
    val funcResult: FlowFunctionResult
) extends ResolvedFlow {
  requireReservedPrefixAbsentInSourceColumns()

  def changeArgs: ChangeArgs = flow.changeArgs

  /** The user-selected projection of [[df.schema]] (i.e. before the SCD metadata column). */
  private val userSelectedSchema: StructType = {
    val selectedSchema = ColumnSelection.applyToSchema(
      schemaName = "changeDataFeed",
      schema = df.schema,
      columnSelection = changeArgs.columnSelection,
      caseSensitive = spark.sessionState.conf.caseSensitiveAnalysis
    )
    requireKeysPresentInSelectedSchema(selectedSchema)
    selectedSchema
  }

  /** The DataType of the sequencing expression, derived once from the source change feed. */
  private[graph] val sequencingType: DataType =
    df.select(changeArgs.sequencing).schema.head.dataType

  /**
   * Returns the augmented output schema of this flow, which can differ from the schema of the
   * source change-data-feed dataframe.
   *
   * The source dataframe's schema describes the incoming CDC events; the augmented schema here
   * applies the user-specified [[ColumnSelection]] and appends the SCD-specific metadata
   * columns that the AutoCDC MERGE engine projects onto the target table. Downstream
   * dependencies in the pipeline see this augmented schema.
   */
  override val schema: StructType = changeArgs.storedAsScdType match {
    case ScdType.Type1 =>
      // SCD1 produces a target table with all the user-selected output columns and a projected
      // CDC operational metadata column at the end.
      StructType(
        userSelectedSchema.fields :+ StructField(
          Scd1BatchProcessor.cdcMetadataColName,
          Scd1BatchProcessor.cdcMetadataColSchema(sequencingType),
          nullable = false
        )
      )
    case ScdType.Type2 =>
      throw new AnalysisException(
        errorClass = "AUTOCDC_SCD2_NOT_SUPPORTED",
        messageParameters = Map.empty
      )
  }

  /**
   * Returns an empty dataframe whose schema matches [[AutoCdcMergeFlow.schema]].
   *
   * Today, [[AutoCdcMergeFlow.load]] is not actually ever called during graph analysis or
   * execution. An AutoCdcMergeFlow can only be an input to a streaming table (not an MV or
   * persisted/temp view), and streaming tables take a [[VirtualTableInput]] as input, not
   * the producing [[Flow]] directly. [[VirtualTableInput]] overrides its own [[load]] to do
   * schema inference on its input flows, rather than a transitive [[Flow.load]].
   *
   * The [[AutoCdcMergeFlow.load]] implementation exists solely for API consistency.
   */
  override def load(readOptions: InputReadOptions): DataFrame = changeArgs.storedAsScdType match {
    case ScdType.Type1 =>
      val userSelectedCols: Seq[Column] = userSelectedSchema.fieldNames.toSeq.map(F.col)
      val emptyCdcMetadataCol: Column = Scd1BatchProcessor.constructCdcMetadataCol(
        deleteSequence = F.lit(null),
        upsertSequence = F.lit(null),
        sequencingType = sequencingType
      ).as(Scd1BatchProcessor.cdcMetadataColName)

      df.select(userSelectedCols :+ emptyCdcMetadataCol: _*)
    case ScdType.Type2 =>
      throw new AnalysisException(
        errorClass = "AUTOCDC_SCD2_NOT_SUPPORTED",
        messageParameters = Map.empty
      )
  }

  /**
   * Validate that the resolved source dataframe for the AutoCDC flow does not contain any column
   * names that use the reserved Spark AutoCDC prefix.
   */
  private def requireReservedPrefixAbsentInSourceColumns(): Unit = {
    val resolver = spark.sessionState.conf.resolver
    val reservedPrefix = AutoCdcReservedNames.prefix

    def nameContainsReservedPrefix(name: String): Boolean = {
      name.length >= reservedPrefix.length && resolver(
        name.substring(0, reservedPrefix.length),
        reservedPrefix
      )
    }

    df.schema.fieldNames.find(nameContainsReservedPrefix).foreach { conflictingColumnName =>
      throw new AnalysisException(
        errorClass = "AUTOCDC_RESERVED_COLUMN_NAME_PREFIX_CONFLICT",
        messageParameters = Map(
          "caseSensitivity" -> CaseSensitivityLabels.of(
            spark.sessionState.conf.caseSensitiveAnalysis
          ),
          "columnName" -> conflictingColumnName,
          "schemaName" -> "changeDataFeed",
          "reservedColumnNamePrefix" -> reservedPrefix
        )
      )
    }
  }

  /**
   * Validate all keys specified in changeArgs are actually present in the user-selected schema.
   */
  private def requireKeysPresentInSelectedSchema(selectedSchema: StructType): Unit = {
    val resolver = spark.sessionState.conf.resolver

    changeArgs.keys
      .find(key => !selectedSchema.fieldNames.exists(name => resolver(name, key.name)))
      .foreach { missingKey =>
        throw new AnalysisException(
          errorClass = "AUTOCDC_KEY_NOT_IN_SELECTED_SCHEMA",
          messageParameters = Map(
            "caseSensitivity" -> CaseSensitivityLabels.of(
              spark.sessionState.conf.caseSensitiveAnalysis
            ),
            "keyColumnName" -> missingKey.name
          )
        )
      }
  }
}
