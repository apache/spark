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

import com.google.common.cache.{Cache, CacheBuilder}

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{AliasHelper, And, Attribute, AttributeMap, AttributeReference, AttributeSet, Cast, EqualNullSafe, EqualTo, Expression, ExpressionSet, Generator, Literal, Or, OuterReference, PredicateHelper, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.optimizer.{InferFiltersFromConstraints, InferFiltersFromGenerate, ReplaceExpressions}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, CTERelationDef, Filter, Join, LateralJoin, LeafNode, LogicalPlan, Project, SetOperation, Statistics}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.TreePattern.{CURRENT_LIKE, RUNTIME_REPLACEABLE}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{truncatedString, ArrayData, CharVarcharUtils}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.{PushDownUtils, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetSeqMetadata, OffsetSeqMetadataBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AnyTimestampNanoType, AnyTimeType, ArrayType, BinaryType, BooleanType, DataType, DatetimeType, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegralType, StringType, StructType, TimestampNTZType, TimestampType, TimeType, VariantType, YearMonthIntervalType}
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
 * The scan may drop the rows that fail the filters that its builder got, so the optimized plan of
 * each micro-batch must apply them to all the rows that it reads from the source, which it checks
 * before it reads them (see `checkPushedFilters`). Otherwise, it is the plan without these
 * pushdowns: the optimizer keeps deciding which columns every operator gets, and in which order,
 * including the stateful ones. Only the scan node of the physical plan outputs the columns that
 * the scan reads, and the planner checks that the micro-batch reads no other column of the
 * relation. A scan cannot read other columns once its stream exists, so a micro-batch that does
 * fails with STREAMING_SCAN_MISSING_COLUMNS, as when the configuration of the optimizer changes in
 * the session of the query after it starts so that its plan reads other columns of a source.
 *
 * The other operations that the scans of batch queries can apply, such as aggregates and limits,
 * apply to the data of all the micro-batches of a streaming query, which the scan of a micro-batch
 * does not read, so they are not pushed down.
 */
private[sql] object StreamingV2ScanPushDown
  extends Logging with PredicateHelper with AliasHelper {

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
     * prefix of the leading pushable filters of the occurrences. The scan may evaluate the pushed
     * filters first, on the rows of the source, so a pushed filter that may fail on some rows, as
     * a cast or a division does under ANSI, or `raise_error`, must not see a row that a filter
     * written before it drops. These are the filters for a scan builder whose filters may fail.
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
   *                      form, so the plan of each micro-batch must apply them too (see
   *                      `checkPushedFilters`)
   */
  case class StreamingScan(
      scan: Scan,
      stream: SparkDataStream,
      scanOutput: Option[Seq[AttributeReference]],
      pushedFilters: Seq[Expression])

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
   * Returns the columns that each streaming relation of Data Source V2 needs to read, and the
   * filters that its scan may get, from `plan` optimized as the micro-batches are (see
   * `forStartup`), without the filters that the optimizer infers from the constraints of the plan
   * or from generators, so that a scan only gets the filters that the query applies to its own
   * source. A relation without an entry reads all its columns with no filter, as all do if the
   * optimization fails, unless `spark.sql.streaming.v2ScanPushDown.failOnOptimizationError` makes
   * the failure fail the query, as in the tests. The plan is not optimized when no source prunes
   * its columns and the plan has no condition, since nothing could be pushed down.
   *
   * @param session the session of the stream
   * @param offsetLogMetadata the metadata of the last entry of the offset log of the query, if any
   */
  def columnsAndFiltersOfSources(
      session: SparkSession,
      plan: LogicalPlan,
      offsetLogMetadata: Option[OffsetSeqMetadataBase]): Map[StreamingRelationV2, Reads] = {
    try {
      readsOfSources(session, plan, offsetLogMetadata)
    } catch {
      case NonFatal(e)
          if !session.sessionState.conf.getConf(SQLConf.STREAMING_V2_SCAN_PUSHDOWN_FAIL_ON_ERROR) =>
        logWarning(log"Could not optimize the streaming query to push down what it reads " +
          log"to the scans of its sources, which read all their columns with no filter.", e)
        Map.empty
    }
  }

  // What the relations of `plan` read, see `columnsAndFiltersOfSources`, or the failure of the
  // optimization.
  private def readsOfSources(
      session: SparkSession,
      plan: LogicalPlan,
      offsetLogMetadata: Option[OffsetSeqMetadataBase]): Map[StreamingRelationV2, Reads] = {
    val conf = session.sessionState.conf.clone()
    offsetLogMetadata.foreach(OffsetSeqMetadata.setConfWithoutLogging(_, conf))
    conf.setConf(SQLConf.CBO_ENABLED, false)
    withoutInferredFilters(conf)
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
        // The plan of a micro-batch is normalized before it is optimized, see `QueryExecution`.
        val normalized = QueryExecution.normalize(session, plan)
        val optimized = session.sessionState.optimizer.execute(forStartup(normalized))
        val planned = forPlanning(optimized)
        val occurrences = mutable.ArrayBuffer.empty[(StreamingRelationV2, Reads)]
        // Visits the nodes as the planner plans them: the projections and the filters over a
        // relation are planned with its scan, and the other nodes on their own. This must match
        // the nodes that `DataSourceV2Strategy` plans the scans of streaming relations with.
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
          throw SparkException.internalError("The optimized streaming query reads the " +
            s"streaming relations ${unknown.distinct.mkString(", ")} that its plan does not have.")
        } else {
          // The scan of a relation reads the rows of all its occurrences, so it reads the
          // columns that any of them reads, and can only apply the filters that all of them
          // apply (see `Reads`).
          occurrences.toSeq.groupMap(_._1)(_._2).map { case (relation, reads) =>
            relation -> Reads(AttributeSet.fromAttributeSets(reads.map(_.requiredColumns)),
              reads.flatMap(_.occurrenceFilters))
          }
        }
      }
    }
  }

  // `conf` without the rules of the optimizer that infer filters from the constraints of a plan or
  // from its generators.
  private def withoutInferredFilters(conf: SQLConf): SQLConf = {
    conf.setConfString(SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
      (conf.optimizerExcludedRules.toSeq ++
        Seq(InferFiltersFromConstraints.ruleName, InferFiltersFromGenerate.ruleName)).mkString(","))
    conf
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
   * column pruning alone and then with the filters alone if it had both, and then with all the
   * columns and no filter, as without these pushdowns. If the read schema of a scan of all the
   * columns of such a table, with or without filters, does not follow the relation, the query
   * fails with STREAMING_SCAN_INVALID_READ_SCHEMA, unless `conf` disables the column pruning, with
   * which the scan node outputs the columns of the relation, as it did before the capability.
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
   * @param source the name of the source in the errors of its scan, see `sourceName`
   */
  def buildScan(
      table: SupportsRead,
      options: CaseInsensitiveStringMap,
      output: Seq[AttributeReference],
      conf: SQLConf,
      reads: => Option[Reads],
      source: Seq[String])(
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
    // the first that the builder does not get, in their order, which the streams evaluate them in
    // (see `TableCapability.STREAMING_FILTER_PUSHDOWN`): none runs before a filter written before
    // it. `PushDownUtils.pushFilters` decides which filters the builder gets in the same way.
    def gets(filter: Expression): Boolean = PushDownUtils.getsFilter(builder, filter)
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
        // Each micro-batch checks that its plan applies these filters to all the rows that it
        // reads from the source, see `checkPushedFilters`, so the scan may apply any of them, in
        // any form.
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
    // The columns that the scan node of `scan`, a scan of all the columns, outputs.
    def outputOfAllColumns(scan: Scan): Seq[AttributeReference] = {
      if (returnsReadSchema) {
        // The readers of the stream return the columns of the read schema, so a read schema that
        // does not follow the relation fails the query when it starts: the scan node cannot
        // output the columns of the relation in its place. Without the column pruning, the scan
        // node outputs them instead, as it did before the capability.
        def invalid(reason: String, cause: Throwable): Seq[AttributeReference] = {
          if (conf.streamingV2ColumnPruningEnabled) {
            throw QueryExecutionErrors.streamingScanInvalidReadSchemaError(source, reason, cause)
          }
          logWarning(log"The read schema of the scan of all the columns of the streaming table " +
            log"${MDC(LogKeys.STREAMING_TABLE, table)} ${MDC(LogKeys.REASON, reason)}. Its scan " +
            log"node outputs the columns of the relation, as the column pruning is disabled.")
          output
        }
        Try(readOutput(scan.readSchema(), output, output)) match {
          case Success(Right(columns)) => columns
          case Success(Left(reason)) => invalid(reason, null)
          case Failure(e) => invalid(s"could not be read: $e", e)
        }
      } else {
        output
      }
    }
    // The scan of all the columns with no filter, as without these pushdowns, and the columns that
    // its scan node outputs.
    def scanOfAllColumns(builder: ScanBuilder): (Scan, Seq[AttributeReference]) = {
      val scan = build(builder, output, Nil)
      scan -> outputOfAllColumns(scan)
    }
    // The scan of `columns` with `filters` and the columns that its scan node outputs, or why its
    // read schema does not follow `SupportsPushDownRequiredColumns`. A scan of all the columns
    // with filters is checked as one with no filter, outside of the fallback (see `firstScan`), as
    // its read schema fails the query in the same way: the columns of such a scan are None.
    def scanWith(
        builder: ScanBuilder,
        columns: Seq[AttributeReference],
        filters: Seq[Expression]): Either[String, (Scan, Option[Seq[AttributeReference]])] = {
      val scan = build(builder, columns, filters)
      if (!returnsReadSchema) {
        Right(scan -> Some(output))
      } else if (columns.length == output.length) {
        Right(scan -> None)
      } else {
        readOutput(scan.readSchema(), output, columns).map(columns => scan -> Some(columns))
      }
    }
    // The columns and the filters of a scan, in the warnings.
    def pushdowns(columns: Seq[AttributeReference], filters: Seq[Expression]): String = {
      val names = if (columns.length < output.length) columns.map(_.name).mkString(", ") else "all"
      val sqls = if (filters.isEmpty) "none" else filters.map(_.sql).mkString(", ")
      s"(columns: $names, filters: $sqls)"
    }
    // Tries the scans of `attempts`, the columns and the filters of each, in order, each from a
    // new scan builder after the first, and then the scan of all the columns with no filter.
    def firstScan(
        attempts: Seq[(Seq[AttributeReference], Seq[Expression])],
        builder: ScanBuilder): ((Scan, Seq[AttributeReference]), Seq[Expression]) = {
      attempts match {
        case Seq() => scanOfAllColumns(builder) -> Nil
        case (columns, filters) +: rest =>
          val (reason, cause, next) = Try(scanWith(builder, columns, filters)) match {
            case Success(Right((scan, scanOutput))) =>
              return (scan -> scanOutput.getOrElse(outputOfAllColumns(scan))) -> filters
            // The read schema of the next scans of these columns would not follow them either.
            case Success(Left(reason)) =>
              (s"its read schema $reason", None, rest.filterNot(_._1 == columns))
            case Failure(e) => (s"it failed with $e", Some(e), rest)
          }
          val (nextColumns, nextFilters) = next.headOption.getOrElse(output -> Nil)
          val message = log"The scan of the streaming table " +
            log"${MDC(LogKeys.STREAMING_TABLE, table)} could not be built with " +
            log"${MDC(LogKeys.OLD_VALUE, pushdowns(columns, filters))}: " +
            log"${MDC(LogKeys.REASON, reason)}. Trying " +
            log"${MDC(LogKeys.NEW_VALUE, pushdowns(nextColumns, nextFilters))} next."
          cause match {
            case Some(e) => logWarning(message, e)
            case None => logWarning(message)
          }
          firstScan(next, table.newScanBuilder(options))
      }
    }

    val ((scan, scanOutput), pushedFilters) =
      if (requiredOutput.length == output.length && filters.isEmpty) {
        scanOfAllColumns(builder) -> Nil
      } else {
        // When the filters fail, the column pruning alone may still work, and when the column
        // pruning fails, the filters alone.
        val fewerPushdowns = if (filters.nonEmpty && requiredOutput.length < output.length) {
          Seq(requiredOutput -> Nil, output -> filters)
        } else {
          Nil
        }
        firstScan((requiredOutput -> filters) +: fewerPushdowns, builder)
      }
    StreamingScan(scan, createStream(scan),
      Some(scanOutput).filterNot(_.map(_.exprId) == output.map(_.exprId)), pushedFilters)
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
   * Checks that `plan`, the optimized plan of a micro-batch in `session`, applies the filters that
   * the scans of its sources got when the query started to all the rows that it reads from them:
   * a scan may drop the rows that fail its filters, so the plan must drop them too. The conditions
   * that the plan applies to all the rows of each occurrence of a relation whose scan got filters
   * must imply these filters (see `impliedBy`). Otherwise, the value of an expression in a pushed
   * filter changed after the query started, as a function of the time that `forStartup` does not
   * replace would, or the configuration of the optimizer changed in the session of the query, or
   * the optimizer of the micro-batch rewrote a pushed filter in a way that `impliedBy` does not
   * follow, and the micro-batch fails before it reads its data, with the failure of the optimizer
   * as its cause if there is one.
   */
  def checkPushedFilters(
      session: SparkSession,
      plan: LogicalPlan,
      rewrites: PushedFilterRewrites = new PushedFilterRewrites): Unit = {
    val hasPushedFilters = plan.exists {
      case relation: StreamingDataSourceV2ScanRelation => relation.pushedFilters.nonEmpty
      case _ => false
    }
    if (hasPushedFilters) {
      // The configuration of the micro-batch, without the rules that infer filters, which would
      // add conditions to those that `impliedBy` gets from the optimizer.
      lazy val optimizerConf = withoutInferredFilters(session.sessionState.conf.clone())
      // The conjuncts of `condition` once the optimizer of the micro-batch optimizes a filter of
      // `relation` with it, none if it folds into true, which the next micro-batches reuse.
      def optimized(relation: StreamingDataSourceV2ScanRelation)(
          condition: Expression): Option[Seq[Expression]] = rewrites(condition) {
        SQLConf.withExistingConf(optimizerConf) {
          session.sessionState.optimizer.execute(
            Filter(condition, relation.copy(pushedFilters = Nil)))
        } match {
          case Filter(optimizedCondition, _: StreamingDataSourceV2ScanRelation) =>
            Some(splitConjunctivePredicates(optimizedCondition))
          case _: StreamingDataSourceV2ScanRelation => Some(Nil)
          case _ => None
        }
      }
      conditionsOverRelations(plan, Nil).foreach { case (relation, conditions) =>
        val implied = impliedBy(conditions, optimized(relation))
        val results = relation.pushedFilters.map(filter => filter -> Try(implied(filter)))
        val unapplied = results.collect {
          case (filter, result) if !result.getOrElse(false) => filter
        }
        if (unapplied.nonEmpty) {
          throw QueryExecutionErrors.streamingScanFiltersChangedError(
            sourceName(relation.relation.table, relation.catalog, relation.identifier),
            unapplied.map(_.sql),
            results.collectFirst { case (_, Failure(e)) => e }.orNull)
        }
      }
    }
  }

  /**
   * The conditions that the optimizer of a micro-batch rewrote for `checkPushedFilters`, by the
   * condition canonicalized, which a streaming query keeps for its micro-batches: they check the
   * same filters with the same values, which its optimizer rewrites in the same way. A query that
   * restarts gets a new one, with the configuration that it restarts with.
   */
  class PushedFilterRewrites {
    private val rewrites: Cache[Expression, Option[Seq[Expression]]] =
      CacheBuilder.newBuilder().maximumSize(100).build[Expression, Option[Seq[Expression]]]()

    def apply(condition: Expression)(
        rewrite: => Option[Seq[Expression]]): Option[Seq[Expression]] = {
      val key = condition.canonicalized
      Option(rewrites.getIfPresent(key)).getOrElse {
        val rewritten = rewrite
        rewrites.put(key, rewritten)
        rewritten
      }
    }
  }

  // Each occurrence of a streaming relation in `plan`, with the deterministic conditions that the
  // plan applies to all its rows, given `above`, those that it applies to all the rows of `plan`:
  // the conditions of the filters over it, through the projections, with the aliases of these in
  // place of their names, as `PhysicalOperation` finds the filters of a relation. The conditions
  // over a filter or a projection that is not deterministic do not hold for the rows that it gets,
  // nor do the conjuncts of such a filter after its first nondeterministic one, which sees the rows
  // that these drop: only the leading deterministic ones, as when the query starts.
  private def conditionsOverRelations(
      plan: LogicalPlan,
      above: Seq[Expression]): Seq[(StreamingDataSourceV2ScanRelation, Seq[Expression])] = {
    plan match {
      case relation: StreamingDataSourceV2ScanRelation => Seq(relation -> above)
      case Filter(condition, child) =>
        val conditions = splitConjunctivePredicates(condition)
        val applied = if (condition.deterministic) {
          conditions ++ above
        } else {
          conditions.takeWhile(_.deterministic)
        }
        conditionsOverRelations(child, applied)
      case Project(projectList, child) if projectList.forall(_.deterministic) =>
        val aliases = getAliasMap(projectList)
        conditionsOverRelations(child, above.map(replaceAlias(_, aliases)))
      case other => other.children.flatMap(conditionsOverRelations(_, Nil))
    }
  }

  /**
   * Returns whether a filter is implied by `facts`, the deterministic conditions that hold for all
   * the rows that a plan keeps from a relation. No row passes a fact that is false or null, which
   * implies any filter. Otherwise, the facts imply a filter if they have it, or its conjuncts, or
   * one of its disjuncts, also once the columns that the facts make equal to literals have these
   * values in it, and `optimize`, the optimizer of the micro-batch, rewrote it. The optimizer of a
   * micro-batch puts these values in place of the columns in its filters (see
   * `ConstantPropagation`), e.g. with an equality that it infers through a join, and rewrites the
   * filters with the other rules of the same batch, so that `id > 1` becomes true with `id = 5`,
   * and `CAST(extra AS BIGINT) < id` becomes `extra < 5`. The values replace their columns for the
   * types whose equal values are the same (see `hasExactEquality`), and for the floating types,
   * whose equal values only differ for 0.0 and -0.0, and for NaN values, which Spark evaluates
   * alike: a column equal to zero is 0.0 or -0.0, so the filter must hold with each, as
   * `floor(d) >= 0` does, unlike `CAST(d AS STRING) = '0.0'`. A struct or an array with floating
   * fields or elements gets its value if it has no 0.0 or -0.0 in them. A string of a collation
   * that is not binary gets no value, also where `ConstantPropagation` puts it in a comparison of
   * two columns of this collation. This may miss that the facts imply a filter, which fails the
   * micro-batch, but never finds that they imply a filter that they do not imply, as long as the
   * rules of the optimizer keep what the filters mean.
   */
  private def impliedBy(
      facts: Seq[Expression],
      optimize: Expression => Option[Seq[Expression]]): Expression => Boolean = {
    val noRow = facts.exists {
      case Literal(false, BooleanType) | Literal(null, _) => true
      case _ => false
    }
    val known = ExpressionSet(facts)
    def holds(condition: Expression): Boolean = known.contains(condition) || (condition match {
      case TrueLiteral => true
      case And(left, right) => holds(left) && holds(right)
      case Or(left, right) => holds(left) || holds(right)
      case _ => false
    })
    val values = AttributeMap(facts.collect {
      case EqualTo(column: Attribute, value: Literal) => column -> value
      case EqualTo(value: Literal, column: Attribute) => column -> value
      case EqualNullSafe(column: Attribute, value: Literal) => column -> value
      case EqualNullSafe(value: Literal, column: Attribute) => column -> value
    }.filter { case (column, value) =>
      DataType.equalsIgnoreNullability(column.dataType, value.dataType) &&
        (hasExactEquality(column.dataType) || isFloating(column.dataType) ||
          (hasExactEquality(column.dataType, floating = true) &&
            hasNoFloatingZero(value.value, value.dataType)))
    })
    filter => noRow || holds(filter) || {
      val columns = filter.references.filter(values.contains)
      // The columns of the filter that are equal to zero, which is 0.0 or -0.0 in each row.
      val zeros = columns.toSeq.filter(column => isFloatingZero(values(column))).toSet
      val zeroColumns = AttributeSet(zeros)
      columns.nonEmpty && zeros.size <= 3 && zeros.subsets().forall { negative =>
        val negativeZeros = AttributeSet(negative)
        val withValues = filter.transform {
          case column: Attribute if zeroColumns.contains(column) =>
            floatingZero(column.dataType, negativeZeros.contains(column))
          case column: Attribute if values.contains(column) => values(column)
        }
        optimize(withValues).exists(_.forall(holds))
      }
    }
  }

  // Whether the values of `dataType` that are equal are the same, so that a column that is equal
  // to a value has this value in any expression: not 0.0 and -0.0, which are equal, nor the
  // strings that a collation finds equal, nor the structs and arrays that have such values.
  // With `floating`, the floating types count as such too, for a value that has no 0.0 or -0.0 in
  // them (see `hasNoFloatingZero`): the other equal floating values are the same, or NaN values,
  // which Spark evaluates alike.
  private def hasExactEquality(dataType: DataType, floating: Boolean = false): Boolean =
    dataType match {
      case _: IntegralType | _: DecimalType | BooleanType | DateType | TimestampType |
           TimestampNTZType | _: AnyTimestampNanoType | _: TimeType | _: DayTimeIntervalType |
           _: YearMonthIntervalType | BinaryType => true
      case FloatType | DoubleType => floating
      case string: StringType => string.supportsBinaryEquality
      case struct: StructType =>
        struct.fields.forall(field => hasExactEquality(field.dataType, floating))
      case array: ArrayType => hasExactEquality(array.elementType, floating)
      case _ => false
    }

  // Whether `value`, of `dataType`, has no 0.0 or -0.0 in its fields and elements.
  private def hasNoFloatingZero(value: Any, dataType: DataType): Boolean = (value, dataType) match {
    case (double: Double, DoubleType) => double != 0.0
    case (float: Float, FloatType) => float != 0.0f
    case (row: InternalRow, struct: StructType) =>
      struct.fields.indices.forall { i =>
        row.isNullAt(i) || hasNoFloatingZero(row.get(i, struct(i).dataType), struct(i).dataType)
      }
    case (array: ArrayData, ArrayType(elementType, _)) =>
      (0 until array.numElements()).forall { i =>
        array.isNullAt(i) || hasNoFloatingZero(array.get(i, elementType), elementType)
      }
    case _ => true
  }

  private def isFloating(dataType: DataType): Boolean =
    dataType == FloatType || dataType == DoubleType

  private def isFloatingZero(value: Literal): Boolean = value.value match {
    case double: Double => double == 0.0
    case float: Float => float == 0.0f
    case _ => false
  }

  // 0.0 or -0.0, of a floating type.
  private def floatingZero(dataType: DataType, negative: Boolean): Literal = dataType match {
    case FloatType => Literal(if (negative) -0.0f else 0.0f)
    case _ => Literal(if (negative) -0.0 else 0.0)
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
