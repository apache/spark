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

package org.apache.spark.sql.execution.streaming.runtime

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Cast, Expression, Generator, NamedExpression, OuterReference, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.optimizer.ReplaceExpressions
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.TreePattern.{CURRENT_LIKE, RUNTIME_REPLACEABLE}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{truncatedString, CharVarcharUtils}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetSeqMetadata, OffsetSeqMetadataBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AnyTimeType, DataType, DatetimeType, StringType, StructType, VariantType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

/**
 * Pushes the top-level columns that the optimized plan of a streaming query reads from each Data
 * Source V2 streaming source down to the scan of the source, as `V2ScanRelationPushDown` does for
 * the scans of batch queries, if the table of the source has the capability
 * `STREAMING_COLUMN_PRUNING`.
 *
 * The stream of a source is created from its scan, and has to exist before any micro-batch is
 * planned, so the scan is built when the query starts. The query is optimized once at that point,
 * by the optimizer of its micro-batches and with their configuration, and the scan of each source
 * reads the columns that the planner takes from the optimized plan for the relation of the source
 * (see `DataSourceV2Strategy`): those of the projections and the filters over each occurrence of
 * the relation.
 *
 * The plan of each micro-batch is the one without this pushdown: the optimizer keeps deciding
 * which columns every operator gets, and in which order, including the stateful ones. Only the
 * scan node of the physical plan outputs the columns that the scan reads, and the planner checks
 * that the micro-batch reads no other column of the relation. A scan cannot read other columns
 * once its stream exists, so a micro-batch that does fails with STREAMING_SCAN_MISSING_COLUMNS,
 * as when the configuration of the optimizer changes in the session of the query after it starts
 * so that its plan reads other columns of a source.
 *
 * The other operations that the scans of batch queries can apply, such as aggregates and limits,
 * apply to the data of all the micro-batches of a streaming query, which the scan of a micro-batch
 * does not read, so they are not pushed down.
 */
private[sql] object StreamingV2ScanPushDown extends Logging {

  /**
   * The scan of a streaming source, and its stream.
   *
   * @param scanOutput the columns of the relation that the scan reads, in the order of its read
   *                   schema, unless these are all its columns in their order
   */
  case class StreamingScan(
      scan: Scan,
      stream: SparkDataStream,
      scanOutput: Option[Seq[AttributeReference]])

  /**
   * Returns the name of a streaming source in the errors of its scan: the parts of its identifier,
   * with its catalog, or the name of its table.
   */
  def sourceName(
      table: Table,
      catalog: Option[CatalogPlugin],
      identifier: Option[Identifier]): Seq[String] = (catalog, identifier) match {
    case (Some(catalog), Some(identifier)) =>
      (catalog.name +: identifier.namespace.toSeq) :+ identifier.name
    case _ => Seq(table.name())
  }

  /**
   * Returns the columns that each streaming relation of Data Source V2 needs to read, from `plan`
   * optimized as the micro-batches are (see `forStartup`). A relation without an entry reads all
   * its columns, as all do if the optimization fails.
   *
   * @param session the session of the stream
   * @param offsetLogMetadata the metadata of the last entry of the offset log of the query, if any
   */
  def requiredColumnsOfSources(
      session: SparkSession,
      plan: LogicalPlan,
      offsetLogMetadata: Option[OffsetSeqMetadataBase]): Map[StreamingRelationV2, AttributeSet] = {
    val conf = session.sessionState.conf.clone()
    offsetLogMetadata.foreach(OffsetSeqMetadata.setConfWithoutLogging(_, conf))
    conf.setConf(SQLConf.CBO_ENABLED, false)
    session.withActive {
      SQLConf.withExistingConf(conf) {
        try {
          // The plan of a micro-batch is normalized before it is optimized, see `QueryExecution`.
          val normalized = QueryExecution.normalize(session, plan)
          val optimized = session.sessionState.optimizer.execute(forStartup(normalized))
          val planned = forPlanning(optimized)
          val occurrences = mutable.ArrayBuffer.empty[(StreamingRelationV2, AttributeSet)]
          // Visits the nodes as the planner plans them: the projections and the filters over a
          // relation are planned with its scan, and the other nodes on their own.
          def visit(node: LogicalPlan): Unit = node match {
            case ScanOfStream(project, filters, relation: StreamingRelationV2) =>
              occurrences += relation -> AttributeSet((project ++ filters).flatMap(_.references))
            case _ =>
              node.children.foreach(visit)
          }
          visit(planned)
          val relations = plan.collect { case relation: StreamingRelationV2 => relation }.toSet
          val unknown = occurrences.map(_._1).filterNot(relations.contains)
          if (unknown.nonEmpty) {
            // The optimizer does not copy streaming relations: `DeduplicateRelations` keeps
            // streaming plans as they are, so all the references to a common table expression,
            // for example, have the same relation. A copy would share the scan of its relation in
            // the micro-batches, so the scan would have to read what the copy reads too.
            val message = log"The optimized streaming query reads the streaming relations " +
              log"${MDC(LogKeys.STREAMING_TABLE, unknown.distinct.mkString(", "))} that its " +
              log"plan does not have, so the scans of its sources read all their columns."
            if (Utils.isTesting) {
              throw SparkException.internalError(message.message)
            }
            logWarning(message)
            Map.empty
          } else {
            // The scan of a relation reads the rows of all its occurrences, so it reads the
            // columns that any of them reads.
            occurrences.toSeq.groupMap(_._1)(_._2).map { case (relation, columns) =>
              relation -> AttributeSet.fromAttributeSets(columns)
            }
          }
        } catch {
          // The tests fail instead, so that they catch a regression of the optimization.
          case NonFatal(e) if !Utils.isTesting =>
            logWarning(log"Could not optimize the streaming query to push down the columns " +
              log"that it reads to the scans of its sources, which read all their columns.", e)
            Map.empty
        }
      }
    }
  }

  /**
   * Returns `plan` for the optimization when the query starts, so that the columns that it reads
   * from its streaming relations do not depend on when it is optimized:
   *  - The subtrees without a streaming relation, such as the static side of a join or the plan
   *    of a static common table expression, and the subtrees of the plans of subqueries without
   *    an outer reference, become placeholders of their outputs, so that nothing of them folds
   *    into the plan: a micro-batch applies the cache, which may change after the query starts,
   *    and their scans are neither built nor planned.
   *  - The expressions whose values may depend on when they are evaluated or on the state of the
   *    session, such as the current time or the conversions to dates and timestamps (see
   *    `makesDateOrTimestamp`), become placeholders that are not deterministic for the optimizer,
   *    as the current time of a micro-batch, and keep their children.
   * Visible for tests.
   */
  private[sql] def forStartup(plan: LogicalPlan): LogicalPlan = withOpaqueSubqueries(
    plan.transformDown {
      case relation: BatchRelation => relation
      // `WithCTE` keeps the definitions of its common table expressions as they are.
      case definition: CTERelationDef if !definition.isStreaming =>
        definition.copy(child = BatchRelation(definition.child))
      case node if !node.isStreaming => BatchRelation(node)
    }).transformUpWithSubqueries {
    case node =>
      // The expressions that ReplaceExpressions replaces with others when the optimization starts
      // are replaced first, as the optimizer does, so that the conversions that they stand for,
      // such as the cast of to_date('today'), are replaced below.
      val replaced = if (node.containsPattern(RUNTIME_REPLACEABLE)) {
        node.mapExpressions(ReplaceExpressions.replace)
      } else {
        node
      }
      replaced.transformExpressionsUp {
        // The children of an expression are replaced before it, as are the subqueries, so an
        // expression that still has the pattern CURRENT_LIKE has it itself: its value is the
        // current time, or comes from the state of the session, as the current database does.
        case e if e.containsPattern(CURRENT_LIKE) => StartupValue(e, e.children)
        case e if makesDateOrTimestamp(e) => StartupValue(e, e.children)
      }
  }

  /**
   * Returns `plan`, optimized when the query starts, as the planner of a micro-batch gets it: with
   * the values of the expressions that depend on when they are evaluated, which are deterministic.
   * Visible for tests.
   */
  private[sql] def forPlanning(plan: LogicalPlan): LogicalPlan = plan.transformUpWithSubqueries {
    case node => node.transformExpressions {
      case value: StartupValue => value.asPlanned
    }
  }

  /**
   * Returns `plan` with the subtrees of the plans of its subqueries that have no outer reference
   * replaced by placeholders of their outputs, also in the subqueries of the others. The others
   * keep the columns of the plan that they reference, which the plan reads.
   */
  private def withOpaqueSubqueries(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case node => node.transformExpressions {
      case subquery: SubqueryExpression =>
        subquery.withNewPlan(withOpaqueSubqueries(withOpaqueSubtrees(subquery.plan)._1))
    }
  }

  // Returns `plan` with its subtrees that have no outer reference replaced by placeholders, and
  // whether it has one, which each node finds once, from its children.
  private def withOpaqueSubtrees(plan: LogicalPlan): (LogicalPlan, Boolean) = {
    val (children, outer) = plan.children.map(withOpaqueSubtrees).unzip
    if (outer.contains(true) || plan.expressions.exists(hasOuterReference)) {
      plan.withNewChildren(children) -> true
    } else {
      BatchRelation(plan) -> false
    }
  }

  // Whether `expression` references the columns of an outer query, also in its subqueries.
  private def hasOuterReference(expression: Expression): Boolean = expression.exists {
    case _: OuterReference => true
    case subquery: SubqueryExpression =>
      subquery.plan.exists(_.expressions.exists(hasOuterReference))
    case _ => false
  }

  /**
   * Matches the projections and the filters over a relation that the planner plans with the scan
   * of the relation, as `DataSourceV2Strategy` does for the scans of streaming sources, so that
   * the columns that a scan reads are those that its micro-batches plan.
   */
  private[sql] object ScanOfStream {
    def unapply(plan: LogicalPlan): Option[(Seq[NamedExpression], Seq[Expression], LogicalPlan)] =
      PhysicalOperation.unapply(plan)
  }

  /**
   * Returns whether `expression` may make a date or a timestamp of a value without a date, which
   * takes the date of when it is evaluated: a string with only a time such as '12:00:00', a
   * special string such as 'today' or 'now', a time, or a variant with such a string. The
   * optimizer evaluates such an expression once its children are constant, which they may only
   * become during the optimization, as when an alias or an equality propagates a string, so the
   * expression counts whatever its children are, if its type has a date or a timestamp, of any
   * precision and time zone, at any depth: also the elements of an array, the keys or the values
   * of a map, or the fields of a struct, and the type of one of its children has a string, a time
   * or a variant at any depth.
   *
   * These are the casts, and the other conversions, such as `from_json` or `variant_get`, which
   * only count if no child has a date or a timestamp: the expressions that have one, such as
   * `named_struct`, `element_at` or an alias, pass on the dates and the timestamps of their
   * children, whose own conversions are replaced. The expressions that the optimizer does not
   * evaluate, such as aggregate functions or generators, which their operators need as they are,
   * do not count.
   */
  private def makesDateOrTimestamp(expression: Expression): Boolean = expression match {
    case _: Unevaluable | _: AggregateFunction | _: Generator => false
    case cast: Cast =>
      hasDateOrTimestamp(cast.dataType) && hasStringTimeOrVariant(cast.child.dataType)
    case e =>
      e.children.nonEmpty && hasDateOrTimestamp(e.dataType) &&
        !e.children.exists(child => hasDateOrTimestamp(child.dataType)) &&
        e.children.exists(child => hasStringTimeOrVariant(child.dataType))
  }

  private def hasDateOrTimestamp(dataType: DataType): Boolean = dataType.existsRecursively {
    case _: AnyTimeType => false
    case _: DatetimeType => true
    case _ => false
  }

  private def hasStringTimeOrVariant(dataType: DataType): Boolean = dataType.existsRecursively {
    case _: StringType | _: AnyTimeType | VariantType => true
    case _ => false
  }

  /**
   * Builds the scan of a streaming source, and creates its stream with `createStream`. If the
   * table of the source has the capability `STREAMING_COLUMN_PRUNING`, its scan builder supports
   * the pushdown, and `conf` enables it, the scan only reads the columns of `output` that the query
   * reads (see `requiredColumnsOfSources`). `requiredColumns` is only evaluated for such a scan.
   * Otherwise, the scan reads all the columns of `output`, as the scans of streaming queries did
   * before this pushdown.
   *
   * The streams of the scans of a table with the capability return the columns of the read
   * schemas of the scans, in their order, so Spark asks each scan of such a table for its read
   * schema before it creates the stream, whether the scan reads all the columns or not. A pruned
   * scan must read the required columns and only columns of `output`, each once and with its
   * type, as `SupportsPushDownRequiredColumns` requires. If the scan builder or the scan fails
   * with the pruning, or the read schema of a pruned scan cannot be read or does not follow
   * `SupportsPushDownRequiredColumns`, a scan of all the columns is built instead, from a new scan
   * builder, as without this pushdown. If the read schema of a scan of all the columns of such a
   * table does not follow the relation, the query fails with STREAMING_SCAN_INVALID_READ_SCHEMA,
   * unless `conf` disables the column pruning, with which the scan node outputs the columns of
   * the relation, as it did before the capability.
   *
   * The stream is created once, from the scan that the source reads, outside of the fallback: a
   * stream can start to use the checkpoint of the source, or other resources, before it fails, so
   * a failure of `createStream` fails the query, as it did without this pushdown.
   *
   * @param output the output of the relation of the source, with the metadata columns that the
   *               query reads, which the analyzer added to it, so that connectors implementing
   *               `SupportsMetadataColumns` include them in `readSchema()`
   * @param conf the configuration of the query
   * @param requiredColumns the columns that the query reads from the relation, or None to read
   *                        all its columns
   * @param source the name of the source in the errors of its scan, see `sourceName`
   */
  def buildScan(
      table: SupportsRead,
      options: CaseInsensitiveStringMap,
      output: Seq[AttributeReference],
      conf: SQLConf,
      requiredColumns: => Option[AttributeSet],
      source: Seq[String])(
      createStream: Scan => SparkDataStream): StreamingScan = {
    val returnsReadSchema = table.capabilities().contains(TableCapability.STREAMING_COLUMN_PRUNING)
    val builder = table.newScanBuilder(options)
    val prunesColumns = returnsReadSchema && conf.streamingV2ColumnPruningEnabled &&
      builder.isInstanceOf[SupportsPushDownRequiredColumns]
    val requiredOutput = (if (prunesColumns) requiredColumns else None)
      .map(columns => output.filter(columns.contains))
      .getOrElse(output)

    // Builds the scan of `columns` with `builder`.
    def build(builder: ScanBuilder, columns: Seq[AttributeReference]): Scan = {
      builder match {
        case pruning: SupportsPushDownRequiredColumns =>
          pruning.pruneColumns(DataTypeUtils.fromAttributes(columns))
        case _ =>
      }
      builder.build()
    }
    // The scan of all the columns, as without this pushdown, and the columns that its scan node
    // outputs.
    def scanOfAllColumns(builder: ScanBuilder): (Scan, Seq[AttributeReference]) = {
      val scan = build(builder, output)
      if (returnsReadSchema) {
        // The readers of the stream return the columns of the read schema, so a read schema that
        // does not follow the relation fails the query when it starts: the scan node cannot
        // output the columns of the relation in its place. Without the column pruning, the scan
        // node outputs them instead, as it did before the capability.
        def invalid(reason: String, cause: Throwable): (Scan, Seq[AttributeReference]) = {
          if (conf.streamingV2ColumnPruningEnabled) {
            throw QueryExecutionErrors.streamingScanInvalidReadSchemaError(source, reason, cause)
          }
          logWarning(log"The read schema of the scan of all the columns of the streaming table " +
            log"${MDC(LogKeys.STREAMING_TABLE, table)} ${MDC(LogKeys.REASON, reason)}. Its scan " +
            log"node outputs the columns of the relation, as the column pruning is disabled.")
          scan -> output
        }
        Try(readOutput(scan.readSchema(), output, output)) match {
          case Success(Right(columns)) => scan -> columns
          case Success(Left(reason)) => invalid(reason, null)
          case Failure(e) => invalid(s"could not be read: $e", e)
        }
      } else {
        scan -> output
      }
    }

    val (scan, scanOutput) = if (requiredOutput.length == output.length) {
      scanOfAllColumns(builder)
    } else {
      // Logs why the pruned scan could not be built, and builds the scan of all the columns
      // instead.
      def fallBack(reason: String, cause: Option[Throwable]): (Scan, Seq[AttributeReference]) = {
        val message = log"The scan of the streaming table " +
          log"${MDC(LogKeys.STREAMING_TABLE, table)} could not be built to read only the columns " +
          log"${MDC(LogKeys.COLUMN_NAMES, requiredOutput.map(_.name).mkString(", "))}: " +
          log"${MDC(LogKeys.REASON, reason)}. It reads all the columns instead."
        cause match {
          case Some(e) => logWarning(message, e)
          case None => logWarning(message)
        }
        scanOfAllColumns(table.newScanBuilder(options))
      }
      // The pruned scan and its columns, or why its read schema does not follow
      // `SupportsPushDownRequiredColumns`.
      Try {
        val scan = build(builder, requiredOutput)
        readOutput(scan.readSchema(), output, requiredOutput).map(scan -> _)
      } match {
        case Success(Right(pruned)) => pruned
        case Success(Left(reason)) => fallBack(s"its read schema $reason", None)
        case Failure(e) => fallBack(s"it failed with $e", Some(e))
      }
    }
    StreamingScan(scan, createStream(scan),
      Some(scanOutput).filterNot(_.map(_.exprId) == output.map(_.exprId)))
  }

  /**
   * Returns the columns of `output` that a scan built to read `requiredOutput` reads, in the
   * order of its read schema, or why the read schema does not follow
   * `SupportsPushDownRequiredColumns`: each of its fields must be a column of `output`, once, with
   * the type of the column, and it must have the columns of `requiredOutput`. The readers return
   * strings for char and varchar columns, so these compare as strings, whether the relation or
   * the read schema has them as strings or with their types. The columns keep their nullability,
   * as the plans of the micro-batches are optimized with them.
   */
  private def readOutput(
      readSchema: StructType,
      output: Seq[AttributeReference],
      requiredOutput: Seq[AttributeReference]): Either[String, Seq[AttributeReference]] = {
    val columns = output.map(column => column.name -> column).toMap
    val fields = readSchema.fields.toSeq
    val names = fields.map(_.name)
    val unknown = names.filterNot(columns.contains)
    val repeated = names.diff(names.distinct).distinct
    lazy val retyped = fields.filterNot { field =>
      DataType.equalsIgnoreNullability(
        CharVarcharUtils.replaceCharVarcharWithStringAlways(field.dataType),
        CharVarcharUtils.replaceCharVarcharWithStringAlways(columns(field.name).dataType))
    }
    lazy val readColumns = names.map(columns)
    lazy val missing = requiredOutput.filterNot(AttributeSet(readColumns).contains)
    if (unknown.nonEmpty) {
      Left(s"${readSchema.simpleString} has the columns ${unknown.mkString(", ")}, which the " +
        "relation does not output")
    } else if (repeated.nonEmpty) {
      Left(s"${readSchema.simpleString} has the columns ${repeated.mkString(", ")} more than once")
    } else if (retyped.nonEmpty) {
      Left(s"${readSchema.simpleString} has the columns ${retyped.map(_.name).mkString(", ")} " +
        "with other types than the relation outputs them with")
    } else if (missing.nonEmpty) {
      Left(s"${readSchema.simpleString} does not have the columns " +
        s"${missing.map(_.name).mkString(", ")}")
    } else {
      Right(readColumns)
    }
  }

  /**
   * Stands for `plan`, a subtree without a streaming relation, in the plan that is optimized when
   * the query starts, which is never executed. It keeps `plan` canonicalized, which is not a child,
   * so that the placeholders of different plans differ, while those of the same plan do not, as
   * the plans do in the micro-batches.
   */
  private case class BatchRelation(output: Seq[Attribute], plan: LogicalPlan)
    extends LeafNode with MultiInstanceRelation {
    override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))
    override def computeStats(): Statistics = Statistics(sizeInBytes = conf.defaultSizeInBytes)
    override def simpleString(maxFields: Int): String =
      "BatchRelation" + truncatedString(output, "[", ", ", "]", maxFields)
  }

  private object BatchRelation {
    def apply(plan: LogicalPlan): BatchRelation = BatchRelation(plan.output, plan.canonicalized)
  }

  /**
   * Stands for `expression`, whose value depends on when it is evaluated, in the plan that is
   * optimized when the query starts, which is never executed. It keeps the children of the
   * expression, and the expression itself, which is not one of its children, so that the
   * placeholders of different expressions, such as `current_user()` and `current_database()`,
   * differ. It is not deterministic for the optimizer, as the current time of a micro-batch is
   * not, so that the optimizer neither folds it nor moves operators around it as it could with a
   * value. It is deterministic once `planned`, as the values that the planner of a micro-batch
   * gets are, unless the expression itself is not, as a nondeterministic UDF.
   */
  private case class StartupValue(
      expression: Expression,
      children: Seq[Expression],
      planned: Boolean = false)
    extends Expression with Unevaluable {
    override val dataType: DataType = expression.dataType
    override val nullable: Boolean = expression.nullable
    override lazy val deterministic: Boolean =
      planned && expression.deterministic && children.forall(_.deterministic)

    // The placeholder once planned, with the placeholders in `expression`, which is not a child.
    def asPlanned: StartupValue = copy(
      expression = expression.transform { case value: StartupValue => value.asPlanned },
      planned = true)

    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
  }
}
