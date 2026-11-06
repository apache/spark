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

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, ResolvedInlineTable}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, AttributeSet, Cast, Expression, ExpressionSet, Generator, PredicateHelper, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.optimizer.{InferFiltersFromConstraints, InferFiltersFromGenerate, ReplaceExpressions}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, Filter, Join, LateralJoin, LeafNode, LogicalPlan, Project, SetOperation, Statistics, SubqueryAlias}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.TreePattern.{CURRENT_LIKE, RUNTIME_REPLACEABLE}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, PushDownUtils, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetSeqMetadata, OffsetSeqMetadataBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AnyTimeType, DataType, DatetimeType, StringType, StructType, VariantType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Pushes the top-level columns and the filters that the optimized plan of a streaming query reads
 * from each Data Source V2 streaming source down to the scan of the source, as
 * `V2ScanRelationPushDown` does for the scans of batch queries, if the table of the source has the
 * capability `STREAMING_COLUMN_PRUNING` or `STREAMING_FILTER_PUSHDOWN`.
 *
 * The stream of a source is created from its scan, and has to exist before any micro-batch is
 * planned, so the scan is built when the query starts. The query is optimized once at that point,
 * by the optimizer of its micro-batches and with their configuration, and the scan of each source
 * gets what the planner takes from the optimized plan for the relation of the source (see
 * `DataSourceV2Strategy`): the projections and the filters over each occurrence of the relation.
 *
 * The plan of each micro-batch applies the filters that the scan builder got to the rows of the
 * relation, which the query applies anyway, so that the scan cannot drop a row that the plan keeps
 * (see `withPushedFilters`), and drops the copies of these filters that the query applies above
 * them (see `withoutRepeatedPushedFilters`). Otherwise, it is the plan without these pushdowns:
 * the optimizer keeps deciding
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
private[sql] object StreamingV2ScanPushDown extends Logging with PredicateHelper {

  /**
   * What the optimized plan of a streaming query reads from a streaming relation.
   *
   * @param requiredColumns the columns of the relation that the plan reads
   * @param occurrenceFilters for each occurrence of the relation, its filters in the order in which
   *                          the plan evaluates them, up to the first nondeterministic one: a
   *                          filter pushed before it would change how many times it is evaluated
   */
  case class Reads(requiredColumns: AttributeSet, occurrenceFilters: Seq[Seq[Expression]]) {

    /**
     * The filters that every occurrence evaluates first, in the same order: the longest common
     * prefix of the leading pushable filters of the occurrences. Each micro-batch evaluates the
     * pushed filters first, over the relation (see `withPushedFilters`), so a pushed filter that
     * may fail on some rows, as a cast or a division does under ANSI, or `raise_error`, must not
     * see a row that a filter written before it drops. These are the filters for a scan builder
     * whose filters may fail.
     */
    def filters: Seq[Expression] = {
      occurrenceFilters.map(_.takeWhile(isPushable)).reduceOption { (left, right) =>
        left.zip(right).takeWhile { case (l, r) => l.semanticEquals(r) }.map(_._1)
      }.getOrElse(Nil)
    }

    /**
     * The pushable filters that every occurrence applies, in the order of the first occurrence,
     * wherever they are. These are the filters for a scan builder whose filters cannot fail, which
     * may run before the filters written before them.
     */
    def filtersInAnyOrder: Seq[Expression] = {
      occurrenceFilters.map(_.filter(isPushable)) match {
        case Seq() => Nil
        case first +: others =>
          val sets = others.map(ExpressionSet(_))
          first.filter(filter => sets.forall(_.contains(filter)))
      }
    }
  }

  /**
   * Returns whether the scan of a relation may get `filter`, a deterministic filter over the
   * relation: if it has no subquery and no placeholder, whose value each micro-batch gets on its
   * own (see `forStartup`).
   */
  private def isPushable(filter: Expression): Boolean = {
    !SubqueryExpression.hasSubquery(filter) && !filter.exists(_.isInstanceOf[StartupValue])
  }

  /**
   * The scan of a streaming source, and its stream.
   *
   * @param scanOutput the columns of the relation that the scan reads, in the order of its read
   *                   schema, unless these are all its columns in their order
   * @param pushedFilters the filters that the scan builder got, which the scan may apply in any
   *                      form, so the plan of each micro-batch applies them over the relation
   */
  case class StreamingScan(
      scan: Scan,
      stream: SparkDataStream,
      scanOutput: Option[Seq[AttributeReference]],
      pushedFilters: Seq[Expression])

  /**
   * Returns the columns that each streaming relation of Data Source V2 needs to read, and the
   * filters that its scan may get, from `plan` optimized as the micro-batches are (see
   * `forStartup`), without the filters that the optimizer infers from the constraints of the plan
   * or from generators, so that a scan only gets the filters that the query applies to its own
   * source. A relation without an entry reads all its columns with no filter, as all do if the
   * optimization fails. The plan is not optimized when no source prunes its columns and the plan
   * has no condition, since nothing could be pushed down.
   *
   * @param session the session of the stream
   * @param offsetLogMetadata the metadata of the last entry of the offset log of the query, if any
   */
  def columnsAndFiltersOfSources(
      session: SparkSession,
      plan: LogicalPlan,
      offsetLogMetadata: Option[OffsetSeqMetadataBase]): Map[StreamingRelationV2, Reads] = {
    val conf = session.sessionState.conf.clone()
    offsetLogMetadata.foreach(OffsetSeqMetadata.setSessionConf(_, conf, logChanges = false))
    conf.setConf(SQLConf.CBO_ENABLED, false)
    conf.setConfString(SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
      (conf.optimizerExcludedRules.toSeq ++
        Seq(InferFiltersFromConstraints.ruleName, InferFiltersFromGenerate.ruleName)).mkString(","))
    val prunesColumns = conf.streamingV2ColumnPruningEnabled && plan.exists {
      case relation: StreamingRelationV2 =>
        relation.table.capabilities().contains(TableCapability.STREAMING_COLUMN_PRUNING)
      case _ => false
    }
    if (!prunesColumns && !mayHaveConditions(plan)) {
      return Map.empty
    }
    session.withActive {
      SQLConf.withExistingConf(conf) {
        try {
          val optimized = session.sessionState.optimizer.execute(forStartup(plan))
          // The planner of a micro-batch gets the values of the expressions that depend on when
          // they are evaluated, which are deterministic.
          val planned = optimized.transformUpWithSubqueries {
            case node => node.transformExpressions {
              case value: StartupValue => value.copy(planned = true)
            }
          }
          val occurrences = mutable.ArrayBuffer.empty[(StreamingRelationV2, Reads)]
          // Visits the nodes as the planner plans them: the projections and the filters over a
          // relation are planned with its scan, and the other nodes on their own.
          def visit(node: LogicalPlan): Unit = node match {
            case PhysicalOperation(project, filters, relation: StreamingRelationV2) =>
              occurrences += relation -> Reads(
                AttributeSet((project ++ filters).flatMap(_.references)),
                Seq(filters.takeWhile(_.deterministic)))
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
            logWarning(log"The optimized streaming query reads the streaming relations " +
              log"${MDC(LogKeys.STREAMING_TABLE, unknown.distinct.mkString(", "))} that its " +
              log"plan does not have, so the scans of its sources read all their columns with " +
              log"no filter.")
            Map.empty
          } else {
            // The scan of a relation reads the rows of all its occurrences, so it reads the
            // columns that any of them reads, and can only apply the filters that all of them
            // apply (see `Reads`).
            occurrences.toSeq.groupMap(_._1)(_._2).map { case (relation, reads) =>
              relation -> Reads(AttributeSet.fromAttributeSets(reads.map(_.requiredColumns)),
                reads.flatMap(_.occurrenceFilters))
            }
          }
        } catch {
          case NonFatal(e) =>
            logWarning(log"Could not optimize the streaming query to push down what it reads " +
              log"to the scans of its sources, which read all their columns with no filter.", e)
            Map.empty
        }
      }
    }
  }

  /**
   * Returns whether `plan` has a node with a condition that the optimizer could push down to a
   * relation: a filter, or a join, an as-of or a lateral join, or a set operation, which it may
   * turn into one.
   */
  private def mayHaveConditions(plan: LogicalPlan): Boolean = plan.exists {
    case _: Filter | _: Join | _: AsOfJoin | _: LateralJoin | _: SetOperation => true
    case _ => false
  }

  /**
   * Returns `plan` for the optimization when the query starts, so that the columns that it reads
   * from its streaming relations do not depend on when it is optimized:
   *  - The subtrees without a streaming relation, such as the static side of a join, and the
   *    relations of batch scans in subqueries, become placeholders of their outputs, so that
   *    nothing of them folds into the plan: a micro-batch applies the cache, which may change
   *    after the query starts, and their scans are neither built nor planned.
   *  - The expressions whose values may depend on when they are evaluated or on the state of the
   *    session, such as the current time or the conversions to dates and timestamps (see
   *    `makesDateOrTimestamp`), become placeholders that are not deterministic for the optimizer,
   *    as the current time of a micro-batch, and keep their children.
   * Visible for tests.
   */
  private[sql] def forStartup(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case node if !node.isStreaming => BatchRelation(node.output)
  }.transformUpWithSubqueries {
    case relation: DataSourceV2Relation => BatchRelation(relation.output)
    case relation: LogicalRelation if !relation.isStreaming => BatchRelation(relation.output)
    case relation: HiveTableRelation => BatchRelation(relation.output)
    // The rows of an inline table may have values of the current time, which the optimizer
    // evaluates.
    case table: ResolvedInlineTable => BatchRelation(table.output)
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
   * table of the source has the capability `STREAMING_COLUMN_PRUNING` or
   * `STREAMING_FILTER_PUSHDOWN`, its scan builder supports the pushdown, and `conf` enables it,
   * the scan only reads the columns of `output` that the query reads, or gets the filters that the
   * query applies to all the rows that it reads from the source (see `columnsAndFiltersOfSources`).
   * `reads` is only evaluated for such a scan. Otherwise, the scan reads all the columns of
   * `output` with no filter, as the scans of streaming queries did before these pushdowns.
   *
   * The streams of the scans of a table with `STREAMING_COLUMN_PRUNING` return the columns of the
   * read schemas of the scans, in their order, so Spark asks each scan of such a table for its read
   * schema before it creates the stream, whether the scan reads all the columns or not. A pruned
   * scan must read the required columns and only columns of `output`, each once and with its
   * type, as `SupportsPushDownRequiredColumns` requires. If the scan builder or the scan fails
   * with the pushdowns, or the read schema of a pruned scan cannot be read or does not follow
   * `SupportsPushDownRequiredColumns`, the scan is built again from a new scan builder: with the
   * column pruning alone if it had filters too, and then with all the columns and no filter, as
   * without these pushdowns. If the read schema of a scan of all the columns of such a table does
   * not follow the relation, the query fails with STREAMING_SCAN_INVALID_READ_SCHEMA.
   *
   * The stream is created once, from the scan that the source reads, outside of the fallback: a
   * stream can start to use the checkpoint of the source, or other resources, before it fails, so
   * a failure of `createStream` fails the query, as it did without these pushdowns.
   *
   * @param output the output of the relation of the source, with the metadata columns that the
   *               query reads, which the analyzer added to it, so that connectors implementing
   *               `SupportsMetadataColumns` include them in `readSchema()`
   * @param conf the configuration of the query
   * @param reads what the query reads from the relation, or None to read all its columns with no
   *              filter
   */
  def buildScan(
      table: SupportsRead,
      options: CaseInsensitiveStringMap,
      output: Seq[AttributeReference],
      conf: SQLConf,
      reads: => Option[Reads])(
      createStream: Scan => SparkDataStream): StreamingScan = {
    val capabilities = table.capabilities()
    val returnsReadSchema = capabilities.contains(TableCapability.STREAMING_COLUMN_PRUNING)
    val builder = table.newScanBuilder(options)
    val prunesColumns = returnsReadSchema && conf.streamingV2ColumnPruningEnabled &&
      builder.isInstanceOf[SupportsPushDownRequiredColumns]
    val pushesFilters = conf.streamingV2FilterPushdownEnabled &&
      capabilities.contains(TableCapability.STREAMING_FILTER_PUSHDOWN) &&
      PushDownUtils.supportsFilters(builder)
    val known = if (prunesColumns || pushesFilters) reads else None
    val requiredOutput = known.filter(_ => prunesColumns)
      .map(r => output.filter(r.requiredColumns.contains))
      .getOrElse(output)
    // The filters that the scan builder gets in some form, which each micro-batch applies first:
    // the others, such as a filter with a UDF that cannot be translated to a filter of the source,
    // are evaluated after them, as for the scans of batch queries, rather than on all the rows of
    // the source. The filters of a `SupportsPushDownFilters` builder compare columns with values,
    // which cannot fail, so it gets those that all the reads apply, wherever they are. The
    // predicates of a `SupportsPushDownV2Filters` builder and the filters of the others may fail
    // on some rows, as a cast does with ANSI, so they are the leading filters of the reads, up to
    // the first that the builder does not get: none runs before a filter written before it. The
    // builders are matched in the order of `PushDownUtils.pushFilters`.
    def gets(filter: Expression): Boolean =
      PushDownUtils.filtersOfScanBuilder(builder, Seq(filter)).nonEmpty
    val filters = known.filter(_ => pushesFilters)
      .map { r =>
        builder match {
          case _: SupportsPushDownFilters =>
            DataSourceStrategy.normalizeExprs(r.filtersInAnyOrder, output).filter(gets)
          case _ =>
            DataSourceStrategy.normalizeExprs(r.filters, output).takeWhile(gets)
        }
      }
      .getOrElse(Nil)

    // Builds the scan of `columns` with `builder`, once it pushed `filters` down to it.
    def build(
        builder: ScanBuilder,
        columns: Seq[AttributeReference],
        filters: Seq[Expression]): Scan = {
      if (filters.nonEmpty) {
        // The plan of each micro-batch applies these filters to the rows of the source, see
        // `withPushedFilters`, so the scan may apply any of them, in any form.
        val (pushed, _) = PushDownUtils.pushFilters(builder, filters, partitionFields = None)
        val taken = pushed.fold(_.mkString(", "), _.mkString(", "))
        logInfo(log"The scan builder of the streaming table " +
          log"${MDC(LogKeys.STREAMING_TABLE, table)} took the filters " +
          log"${MDC(LogKeys.PUSHED_FILTERS, taken)}.")
      }
      builder match {
        case pruning: SupportsPushDownRequiredColumns =>
          pruning.pruneColumns(DataTypeUtils.fromAttributes(columns))
        case _ =>
      }
      builder.build()
    }
    // The scan of all the columns with no filter, as without these pushdowns, and the columns that
    // its scan node outputs.
    def scanOfAllColumns(builder: ScanBuilder): (Scan, Seq[AttributeReference]) = {
      val scan = build(builder, output, Nil)
      if (returnsReadSchema) {
        // The readers of the stream return the columns of the read schema, so a read schema that
        // does not follow the relation fails the query when it starts: the scan node cannot
        // output the columns of the relation in its place.
        Try(readOutput(scan.readSchema(), output, output)) match {
          case Success(Right(columns)) => scan -> columns
          case Success(Left(reason)) =>
            throw QueryExecutionErrors.streamingScanInvalidReadSchemaError(table.name(), reason)
          case Failure(e) =>
            throw QueryExecutionErrors.streamingScanInvalidReadSchemaError(
              table.name(), s"could not be read: $e", e)
        }
      } else {
        scan -> output
      }
    }
    // The scan of `columns` with `filters` and the columns that its scan node outputs, or why its
    // read schema does not follow `SupportsPushDownRequiredColumns`.
    def scanWith(
        builder: ScanBuilder,
        columns: Seq[AttributeReference],
        filters: Seq[Expression]): Either[String, (Scan, Seq[AttributeReference])] = {
      val scan = build(builder, columns, filters)
      if (returnsReadSchema) {
        readOutput(scan.readSchema(), output, columns).map(scan -> _)
      } else {
        Right(scan -> output)
      }
    }
    // Tries the scans of `attempts`, the columns and the filters of each, in order, each from a
    // new scan builder after the first, and then the scan of all the columns with no filter.
    def firstScan(
        attempts: Seq[(Seq[AttributeReference], Seq[Expression])],
        builder: ScanBuilder): ((Scan, Seq[AttributeReference]), Seq[Expression]) = {
      attempts match {
        case Seq() => scanOfAllColumns(builder) -> Nil
        case (columns, filters) +: rest =>
          val (reason, cause) = Try(scanWith(builder, columns, filters)) match {
            case Success(Right(built)) => return built -> filters
            case Success(Left(reason)) => s"its read schema $reason" -> None
            case Failure(e) => s"it failed with $e" -> Some(e)
          }
          val next = rest.headOption.map { case (columns, _) =>
            log"only the columns ${MDC(LogKeys.COLUMN_NAMES, columns.map(_.name).mkString(", "))}"
          }.getOrElse(log"all the columns")
          val message = if (filters.isEmpty) {
            log"The scan of the streaming table ${MDC(LogKeys.STREAMING_TABLE, table)} could " +
              log"not be built to read only the columns " +
              log"${MDC(LogKeys.COLUMN_NAMES, columns.map(_.name).mkString(", "))}: " +
              log"${MDC(LogKeys.REASON, reason)}. It reads " + next + log" instead."
          } else {
            val names = if (columns.length < output.length) {
              columns.map(_.name).mkString(", ")
            } else {
              "all"
            }
            log"The scan of the streaming table ${MDC(LogKeys.STREAMING_TABLE, table)} could " +
              log"not be built with the pushdowns (columns: ${MDC(LogKeys.COLUMN_NAMES, names)}, " +
              log"filters: ${MDC(LogKeys.FILTER, filters.map(_.sql).mkString(", "))}): " +
              log"${MDC(LogKeys.REASON, reason)}. It reads " + next + log" with no filter instead."
          }
          cause match {
            case Some(e) => logWarning(message, e)
            case None => logWarning(message)
          }
          firstScan(rest, table.newScanBuilder(options))
      }
    }

    val ((scan, scanOutput), pushedFilters) =
      if (requiredOutput.length == output.length && filters.isEmpty) {
        scanOfAllColumns(builder) -> Nil
      } else {
        // When the filters fail, the column pruning alone may still work.
        val prunedAlone = if (filters.nonEmpty && requiredOutput.length < output.length) {
          Seq(requiredOutput -> Nil)
        } else {
          Nil
        }
        firstScan((requiredOutput -> filters) +: prunedAlone, builder)
      }
    StreamingScan(scan, createStream(scan),
      Some(scanOutput).filterNot(_.map(_.exprId) == output.map(_.exprId)), pushedFilters)
  }

  /**
   * Returns `relation`, the relation of a streaming source in the plan of the query, with the
   * filters that its scan builder got applied to its rows. The query applies them anyway, as they
   * are the filters that it applies to all the rows that it reads from the source, so the plan of
   * each micro-batch has the same result with them. With them, it also applies all the filters
   * that the scan may apply, in any form, however the plan of the micro-batch is optimized, so the
   * scan cannot drop a row that the plan keeps.
   */
  def withPushedFilters(relation: StreamingDataSourceV2ScanRelation): LogicalPlan = {
    relation.pushedFilters.reduceOption(And).map(Filter(_, relation)).getOrElse(relation)
  }

  /**
   * Returns `plan`, the plan of a streaming query whose relations have their pushed filters (see
   * `withPushedFilters`), without the copies of these filters that the query applies above them:
   * the conjuncts of the filters over a relation, through projections that keep the columns of
   * the filter and aliases of subqueries, that are equal to its pushed filters. Every row above
   * the pushed filters passes them already, so each micro-batch evaluates them once, whether its
   * optimizer could merge the filters or not.
   */
  def withoutRepeatedPushedFilters(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case filter @ Filter(condition, child) =>
      val pushed = pushedFiltersBelow(child)
      if (pushed.isEmpty) {
        filter
      } else {
        splitConjunctivePredicates(condition).filterNot(pushed.contains)
          .reduceOption(And).map(Filter(_, child)).getOrElse(child)
      }
  }

  /**
   * Returns the pushed filters of the relation below `plan`, through filters, projections and
   * aliases of subqueries, that hold above `plan`: those whose columns `plan` outputs as they are.
   */
  private def pushedFiltersBelow(plan: LogicalPlan): ExpressionSet = plan match {
    case Filter(_, relation: StreamingDataSourceV2ScanRelation) =>
      ExpressionSet(relation.pushedFilters)
    case Filter(_, child) => pushedFiltersBelow(child)
    case SubqueryAlias(_, child) => pushedFiltersBelow(child)
    case project @ Project(_, child) =>
      val kept = AttributeSet(project.projectList.collect { case a: AttributeReference => a })
      ExpressionSet(pushedFiltersBelow(child).filter(_.references.subsetOf(kept)))
    case _ => ExpressionSet(Nil)
  }

  /**
   * Returns the columns of `output` that a scan built to read `requiredOutput` reads, in the
   * order of its read schema, or why the read schema does not follow
   * `SupportsPushDownRequiredColumns`: each of its fields must be a column of `output`, once, with
   * the type of the column, and it must have the columns of `requiredOutput`. The columns keep
   * their nullability, as the plans of the micro-batches are optimized with them.
   */
  private def readOutput(
      readSchema: StructType,
      output: Seq[AttributeReference],
      requiredOutput: Seq[AttributeReference]): Either[String, Seq[AttributeReference]] = {
    val columns = output.map(column => column.name -> column).toMap
    val fields = CharVarcharUtils.replaceCharVarcharWithStringInSchema(readSchema).fields.toSeq
    val names = fields.map(_.name)
    val unknown = names.filterNot(columns.contains)
    val repeated = names.diff(names.distinct).distinct
    lazy val retyped = fields.filterNot { field =>
      DataType.equalsIgnoreNullability(field.dataType, columns(field.name).dataType)
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
   * Stands for the relation of a batch scan in the plan that is optimized when the query starts,
   * which is never executed.
   */
  private case class BatchRelation(output: Seq[Attribute])
    extends LeafNode with MultiInstanceRelation {
    override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))
    override def computeStats(): Statistics = Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }

  /**
   * Stands for `expression`, whose value depends on when it is evaluated, in the plan that is
   * optimized when the query starts, which is never executed. It keeps the children of the
   * expression, and the expression itself, which is not one of its children, so that the
   * placeholders of different expressions, such as `current_user()` and `current_database()`,
   * differ. It is not deterministic for the optimizer, as the current time of a micro-batch is
   * not, so that the optimizer neither folds it nor moves operators around it as it could with a
   * value. It is deterministic once `planned`, as the values that the planner of a micro-batch
   * gets are.
   */
  private case class StartupValue(
      expression: Expression,
      children: Seq[Expression],
      planned: Boolean = false)
    extends Expression with Unevaluable {
    override val dataType: DataType = expression.dataType
    override val nullable: Boolean = expression.nullable
    override lazy val deterministic: Boolean = planned && children.forall(_.deterministic)
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
  }
}
