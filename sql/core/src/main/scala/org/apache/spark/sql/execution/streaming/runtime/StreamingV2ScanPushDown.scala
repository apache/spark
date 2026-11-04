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
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, AttributeSet, Cast, Expression, ExpressionSet, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.optimizer.{ComputeCurrentTime, InferFiltersFromConstraints, ReplaceExpressions, SpecialDatetimeValues}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.TreePattern.{CAST, CURRENT_LIKE, RUNTIME_REPLACEABLE}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, PushDownUtils, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetSeqMetadata, OffsetSeqMetadataBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters
import org.apache.spark.sql.types.{DataType, StructType}
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
 * The plan of each micro-batch is the one without these pushdowns, but for a filter of the rows
 * of the relation with the filters that the scan builder got, which the query applies anyway, so
 * that the scan cannot drop a row that the plan keeps (see `withPushedFilters`). The optimizer
 * keeps deciding which columns every operator gets, and in which order, including the stateful
 * ones, and Spark still evaluates all the filters over the relation. Only the scan node of the
 * physical plan outputs the columns that the scan reads, and the planner checks that the
 * micro-batch reads no other column of the relation.
 *
 * The other operations that the scans of batch queries can apply, such as aggregates and limits,
 * apply to the data of all the micro-batches of a streaming query, which the scan of a micro-batch
 * does not read, so they are not pushed down.
 */
private[sql] object StreamingV2ScanPushDown extends Logging {

  /**
   * What the optimized plan of a streaming query reads from a streaming relation.
   *
   * @param requiredColumns the columns of the relation that the plan reads
   * @param filters the deterministic filters without subqueries that the plan applies over every
   *                occurrence of the relation, which its scan may apply too
   */
  case class Reads(requiredColumns: AttributeSet, filters: Seq[Expression])

  /**
   * The scan of a streaming source, and its stream.
   *
   * @param prunedOutput the columns of the relation that the scan reads, in the order of its read
   *                     schema, unless these are all its columns
   * @param pushedFilters the filters that the scan builder got, which the scan may apply in any
   *                      form, so the plan of each micro-batch applies them over the relation
   */
  case class StreamingScan(
      scan: Scan,
      stream: SparkDataStream,
      prunedOutput: Option[Seq[AttributeReference]],
      pushedFilters: Seq[Expression])

  /**
   * Returns what the optimized `plan` reads from each of its streaming relations of Data Source
   * V2: what the planner takes from the projections and the filters over each occurrence of the
   * relation, or all its columns and no filter where nothing is projected or filtered over it. A
   * relation that the optimized plan does not have has no entry, so that its scan reads all its
   * columns and gets no filter, as no relation has if the optimizer fails, or if the optimized
   * plan has a streaming relation that `plan` does not have.
   *
   * The plan is optimized as the micro-batches are: with the optimizer and the configuration of
   * the stream, the values of the configurations that the offset log of a restarted query pins, and
   * without the cost-based reordering of joins, which `StreamExecution` disables for its queries.
   * The plan does not depend on when it is optimized, and the scan of a relation only gets what the
   * query reads from that relation:
   *  - The expressions whose values depend on when they are evaluated or on the state of the
   *    session, such as the current time, the casts of special strings such as 'today' or 'now'
   *    to dates and timestamps, or the current database, which each micro-batch gets the values
   *    of, become placeholders that the optimizer does not take as deterministic, as the current
   *    time of a micro-batch, so neither its filters nor its columns depend on them, and no filter
   *    on them is pushed down.
   *  - The filters that the optimizer infers from the constraints of the plan, such as those of the
   *    filters over another relation joined with the relation, are not inferred, so that the scan
   *    only gets the filters that the query applies to the relation itself.
   *  - The relations of batch scans become placeholders, so that their scans are neither built
   *    nor planned: they do not change what the plan reads from its streaming relations.
   *
   * @param session the session of the stream
   * @param offsetLogMetadata the metadata of the last entry of the offset log of the query, if any
   */
  def reads(
      session: SparkSession,
      plan: LogicalPlan,
      offsetLogMetadata: Option[OffsetSeqMetadataBase]): Map[StreamingRelationV2, Reads] = {
    val conf = session.sessionState.conf.clone()
    offsetLogMetadata.foreach(OffsetSeqMetadata.setSessionConf(_, conf, logChanges = false))
    conf.setConf(SQLConf.CBO_ENABLED, false)
    conf.setConfString(SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
      (conf.optimizerExcludedRules.toSeq :+ InferFiltersFromConstraints.ruleName).mkString(","))
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
                pushableFilters(filters))
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
            combine(occurrences.toSeq)
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
   * Returns what the occurrences of each relation read, combined: the scan of a relation reads the
   * rows of all its occurrences, so it reads the columns that any of them reads, and can only
   * apply the filters that all of them apply. With several occurrences, the filters that may throw
   * are left out: each occurrence evaluates the pushed filters before its own, in the order of the
   * first occurrence, so such a filter could see the rows that another occurrence drops with a
   * filter written before it, which is not pushed down.
   */
  private def combine(
      occurrences: Seq[(StreamingRelationV2, Reads)]): Map[StreamingRelationV2, Reads] = {
    occurrences.groupBy(_._1).map { case (relation, reads) =>
      val filters = reads.map(r => ExpressionSet(r._2.filters)).reduce { (left, right) =>
        ExpressionSet(left.filter(right.contains))
      }.toSeq
      relation -> Reads(
        AttributeSet.fromAttributeSets(reads.map(_._2.requiredColumns)),
        if (reads.length == 1) filters else filters.filterNot(_.throwable))
    }
  }

  /**
   * Returns the filters that the scan of a relation may get of `filters`, the filters over an
   * occurrence of the relation in the order in which the plan evaluates them: the deterministic
   * ones without subqueries or placeholders. The plan of each micro-batch evaluates the pushed
   * filters first, over the relation (see `withPushedFilters`), so once a filter is not pushed
   * down, only the filters after it that cannot throw are, as `PushPredicateThroughNonJoin` splits
   * the filters that it pushes: a filter that throws, such as one with `raise_error`, does not see
   * the rows that a filter written before it drops.
   */
  private def pushableFilters(filters: Seq[Expression]): Seq[Expression] = {
    def canPush(filter: Expression): Boolean = filter.deterministic &&
      !SubqueryExpression.hasSubquery(filter) && !filter.exists(_.isInstanceOf[StartupValue])
    val (leading, rest) = filters.span(canPush)
    // The head of `rest`, if any, is the first filter that is not pushed down.
    leading ++ rest.drop(1).filter(filter => canPush(filter) && !filter.throwable)
  }

  /**
   * Returns `plan` for the optimization when the query starts, see `reads`, with its subqueries.
   * The expressions that are replaced keep their children, so that the plan still reads the same
   * columns. Visible for tests.
   */
  private[sql] def forStartup(plan: LogicalPlan): LogicalPlan = plan.transformUpWithSubqueries {
    case relation: DataSourceV2Relation => BatchRelation(relation.output)
    case relation: LogicalRelation if !relation.isStreaming => BatchRelation(relation.output)
    case relation: HiveTableRelation => BatchRelation(relation.output)
    // The rows of an inline table may have values of the current time, which the optimizer
    // evaluates.
    case table: ResolvedInlineTable => BatchRelation(table.output)
    case node =>
      // The expressions that ReplaceExpressions replaces with others when the optimization starts
      // are replaced first, as the optimizer does, so that the casts that they stand for, such as
      // the one of to_date('today'), are replaced below.
      val replaced = if (node.containsPattern(RUNTIME_REPLACEABLE)) {
        node.mapExpressions(ReplaceExpressions.replace)
      } else {
        node
      }
      replaced.transformExpressionsUpWithPruning(
          bits => bits.containsPattern(CURRENT_LIKE) || bits.containsPattern(CAST)) {
        // The children of an expression are replaced before it, as are the subqueries, so an
        // expression that still has the pattern CURRENT_LIKE has it itself: its value is the
        // current time, or comes from the state of the session, as the current database does.
        case e if e.containsPattern(CURRENT_LIKE) => StartupValue(e, e.children)
        // So does a cast that ComputeCurrentTime or SpecialDatetimeValues rewrites: a cast of a
        // time to a timestamp, which takes the current date, or a cast of a special string, such
        // as 'today' or 'now', to a date or a timestamp. These rules only rewrite casts besides
        // the expressions with the pattern CURRENT_LIKE.
        case cast: Cast if ComputeCurrentTime.applyForExpression(cast) != cast ||
            SpecialDatetimeValues.applyForExpression(cast) != cast =>
          StartupValue(cast, cast.children)
      }
  }

  /**
   * Builds the scan of a streaming source, and creates its stream with `createStream`. If the
   * table of the source has the capability `STREAMING_COLUMN_PRUNING` or
   * `STREAMING_FILTER_PUSHDOWN`, its scan builder supports the pushdown, and `conf` enables it,
   * the scan only reads the columns of `output` that the query reads, or gets the filters that
   * the query applies to all the rows that it reads from the source (see `reads`). `reads` is only
   * evaluated for such a scan. Otherwise, the scan reads all the columns of `output` with no
   * filter, as the scans of streaming queries did before these pushdowns.
   *
   * A pruned scan must read the required columns and only columns of `output`, each once, as
   * `SupportsPushDownRequiredColumns` requires. Spark asks a scan for its read schema before it
   * creates its stream, as for a batch. If the scan builder, the scan or its stream fails with the
   * pushdowns, or the read schema of a pruned scan cannot be read or does not follow
   * `SupportsPushDownRequiredColumns`, a scan of all the columns with no filter is built instead,
   * from a new scan builder, as without these pushdowns.
   *
   * @param output the output of the relation of the source, with the metadata columns that the
   *               query reads, which the analyzer added to it, so that connectors implementing
   *               `SupportsMetadataColumns` include them in `readSchema()`
   * @param conf the configuration of the query
   * @param reads what the query reads from the relation, or None to read all its columns
   */
  def buildScan(
      table: SupportsRead,
      options: CaseInsensitiveStringMap,
      output: Seq[AttributeReference],
      conf: SQLConf,
      reads: => Option[Reads])(
      createStream: Scan => SparkDataStream): StreamingScan = {
    val builder = table.newScanBuilder(options)
    val capabilities = table.capabilities()
    val prunesColumns = conf.streamingV2ColumnPruningEnabled &&
      capabilities.contains(TableCapability.STREAMING_COLUMN_PRUNING) &&
      builder.isInstanceOf[SupportsPushDownRequiredColumns]
    val pushesFilters = conf.streamingV2FilterPushdownEnabled &&
      capabilities.contains(TableCapability.STREAMING_FILTER_PUSHDOWN) && supportsFilters(builder)
    val known = if (prunesColumns || pushesFilters) reads else None
    val requiredOutput = known.filter(_ => prunesColumns)
      .map(r => output.filter(r.requiredColumns.contains))
      .getOrElse(output)
    val filters = known.filter(_ => pushesFilters)
      .map(r => DataSourceStrategy.normalizeExprs(r.filters, output))
      .getOrElse(Nil)

    // Builds the scan of `columns` with `builder`, once it pushed `filters` down to it.
    def build(
        builder: ScanBuilder,
        columns: Seq[AttributeReference],
        filters: Seq[Expression]): Scan = {
      if (filters.nonEmpty) {
        // The plan of each micro-batch applies all the filters to the rows of the source, see
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
    // The scan of all the columns with no filter, as without these pushdowns.
    def scanOfAllColumns(builder: ScanBuilder): StreamingScan = {
      val scan = build(builder, output, Nil)
      StreamingScan(scan, createStream(scan), None, Nil)
    }

    if (requiredOutput.length == output.length && filters.isEmpty) {
      scanOfAllColumns(builder)
    } else {
      // Logs why the scan with the pushdowns could not be built, and builds the scan of all the
      // columns with no filter instead.
      def fallBack(reason: String, cause: Option[Throwable]): StreamingScan = {
        val columns = if (requiredOutput.length < output.length) {
          requiredOutput.map(_.name).mkString(", ")
        } else {
          "all"
        }
        val filterList = if (filters.nonEmpty) filters.map(_.sql).mkString(", ") else "none"
        val message = log"The scan of the streaming table " +
          log"${MDC(LogKeys.STREAMING_TABLE, table)} could not be built with the pushdowns " +
          log"(columns: ${MDC(LogKeys.COLUMN_NAMES, columns)}, filters: " +
          log"${MDC(LogKeys.FILTER, filterList)}): ${MDC(LogKeys.REASON, reason)}. It reads all " +
          log"the columns with no filter instead."
        cause match {
          case Some(e) => logWarning(message, e)
          case None => logWarning(message)
        }
        scanOfAllColumns(table.newScanBuilder(options))
      }
      // The scan with the pushdowns and its stream, or why the read schema of the pruned scan
      // does not follow `SupportsPushDownRequiredColumns`.
      val pushedDown = Try {
        val scan = build(builder, requiredOutput, filters)
        val prunedOutput = if (requiredOutput.length == output.length) {
          Right(None)
        } else {
          prunedScanOutput(scan.readSchema(), output, requiredOutput).map { scanOutput =>
            if (scanOutput.map(_.exprId) == output.map(_.exprId)) None else Some(scanOutput)
          }
        }
        prunedOutput.map(StreamingScan(scan, createStream(scan), _, filters))
      }
      pushedDown match {
        case Success(Right(streamingScan)) => streamingScan
        case Success(Left(reason)) => fallBack(s"its read schema $reason", None)
        case Failure(e) => fallBack(s"it failed with $e", Some(e))
      }
    }
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

  /** Returns whether the scan builder of a streaming source takes filters. */
  private def supportsFilters(builder: ScanBuilder): Boolean = builder match {
    case _: SupportsPushDownFilters | _: SupportsPushDownV2Filters |
        _: SupportsPushDownCatalystFilters => true
    case _ => false
  }

  /**
   * Returns the columns of `output` that a scan pruned to `requiredOutput` reads, as batch scans
   * map them with `PushDownUtils.toOutputAttrs`, or why the read schema does not follow
   * `SupportsPushDownRequiredColumns`.
   */
  private def prunedScanOutput(
      readSchema: StructType,
      output: Seq[AttributeReference],
      requiredOutput: Seq[AttributeReference]): Either[String, Seq[AttributeReference]] = {
    val names = readSchema.fieldNames.toSeq
    val outputNames = output.map(_.name).toSet
    val unknown = names.filterNot(outputNames.contains)
    val repeated = names.diff(names.distinct).distinct
    if (unknown.nonEmpty) {
      Left(s"${readSchema.simpleString} has the columns ${unknown.mkString(", ")}, which the " +
        "relation does not output")
    } else if (repeated.nonEmpty) {
      Left(s"${readSchema.simpleString} has the columns ${repeated.mkString(", ")} more than once")
    } else {
      val scanOutput = PushDownUtils.toOutputAttrs(readSchema, output)
      val missing = requiredOutput.filterNot(AttributeSet(scanOutput).contains)
      if (missing.nonEmpty) {
        Left(s"${readSchema.simpleString} does not have the columns " +
          s"${missing.map(_.name).mkString(", ")}")
      } else {
        Right(scanOutput)
      }
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
