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
import scala.util.control.NonFatal

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Cast, Expression, ExpressionSet, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.TreePattern.{CAST_TO_TIMESTAMP, CURRENT_LIKE}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, PushDownUtils}
import org.apache.spark.sql.execution.datasources.v2.python.PythonScanBuilder
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetSeqMetadata, OffsetSeqMetadataBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Pushes the top-level columns and the filters that the optimized plan of a streaming query reads
 * from each Data Source V2 streaming source down to the scan of the source, as
 * `V2ScanRelationPushDown` does for the scans of batch queries.
 *
 * The stream of a source is created from its scan, and has to exist before any micro-batch is
 * planned, so the scan is built when the query starts. The query is optimized once at that point,
 * by the optimizer of its micro-batches and with their configuration, and the scan of each source
 * gets what the planner takes from the optimized plan for the relation of the source (see
 * `DataSourceV2Strategy`): the projections and the filters over each occurrence of the relation.
 *
 * The plan of the query does not change, so each micro-batch is optimized as without these
 * pushdowns: the optimizer keeps deciding which columns every operator gets, and in which order,
 * including the stateful ones, and Spark still evaluates all the filters over the relation. Only
 * the scan node of the physical plan outputs the columns that the scan reads, and the planner
 * checks that the micro-batch reads no other column of the relation, and still applies the filters
 * that the scan got.
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
   * @param pushedFilters the filters that the scan got, which the plan of each micro-batch must
   *                      still apply over the relation
   */
  case class StreamingScan[S <: SparkDataStream](
      scan: Scan,
      stream: S,
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
   * The plan does not depend on when it is optimized, nor on the data of the other relations:
   *  - The expressions whose values depend on when they are evaluated or on the state of the
   *    session, such as the current time or the current database, which each micro-batch gets the
   *    values of, become placeholders that the optimizer does not take as deterministic, as the
   *    current time of a micro-batch, and no filter on them is pushed down.
   *  - The filters that the optimizer infers from the constraints of the plan, such as those of the
   *    filters over another relation joined with the relation, are not inferred. The optimizer of a
   *    micro-batch may not infer them, for example when the other relation is a source without new
   *    data in the micro-batch, which the optimizer removes with its filters.
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
                filters.filter { filter =>
                  filter.deterministic && !SubqueryExpression.hasSubquery(filter) &&
                    !filter.exists(_.isInstanceOf[StartupValue])
                })
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
   * apply the filters that all of them apply.
   */
  private def combine(
      occurrences: Seq[(StreamingRelationV2, Reads)]): Map[StreamingRelationV2, Reads] = {
    occurrences.groupBy(_._1).map { case (relation, reads) =>
      val filters = reads.map(r => ExpressionSet(r._2.filters)).reduce { (left, right) =>
        ExpressionSet(left.filter(right.contains))
      }
      relation -> Reads(
        AttributeSet.fromAttributeSets(reads.map(_._2.requiredColumns)), filters.toSeq)
    }
  }

  /**
   * Returns `plan` for the optimization when the query starts, see `reads`, with its subqueries.
   * The expressions that are replaced keep their children, so that the plan still reads the same
   * columns.
   */
  private def forStartup(plan: LogicalPlan): LogicalPlan = plan.transformUpWithSubqueries {
    case relation: DataSourceV2Relation => BatchRelation(relation.output)
    case relation: LogicalRelation if !relation.isStreaming => BatchRelation(relation.output)
    case relation: HiveTableRelation => BatchRelation(relation.output)
    case node =>
      node.transformExpressionsUpWithPruning(
          bits => bits.containsPattern(CURRENT_LIKE) || bits.containsPattern(CAST_TO_TIMESTAMP)) {
        // The children of an expression are replaced before it, as are the subqueries, so an
        // expression that still has the pattern CURRENT_LIKE has it itself: its value is the
        // current time, or comes from the state of the session, as the current database does.
        case e if e.containsPattern(CURRENT_LIKE) || isTimeToTimestamp(e) =>
          StartupValue(e.children, e.dataType, e.nullable)
      }
  }

  /**
   * Returns whether `e` is a cast of a time to a timestamp, which takes the current date, see
   * `ComputeCurrentTime`.
   */
  private def isTimeToTimestamp(e: Expression): Boolean = e match {
    case cast: Cast =>
      Cast.isTimeToTimestampNTZ(cast.child.dataType, cast.dataType) ||
        Cast.isTimeToTimestampLTZ(cast.child.dataType, cast.dataType)
    case _ => false
  }

  /**
   * Builds the scan of a streaming source, and creates its stream with `createStream`. If its
   * scan builder supports it, and `conf` enables it, the scan only reads the columns of `output`
   * that the query reads, and gets the filters that the query applies to all the rows that it reads
   * from the source (see `reads`). `reads` is only evaluated for such a scan builder.
   *
   * A pruned scan must read the required columns and only columns of `output`, each once, as
   * `SupportsPushDownRequiredColumns` requires. A scan that does not, or whose read schema cannot
   * be read, is replaced by a scan of all the columns, as without pruning, before its stream is
   * created. Spark asks a scan for its read schema before it creates its stream, as for a batch.
   *
   * @param output the output of the relation of the source, with the metadata columns that the
   *               query reads, which the analyzer added to it, so that connectors implementing
   *               `SupportsMetadataColumns` include them in `readSchema()`
   * @param conf the configuration of the query
   * @param reads what the query reads from the relation, or None to read all its columns
   */
  def buildScan[S <: SparkDataStream](
      table: SupportsRead,
      options: CaseInsensitiveStringMap,
      output: Seq[AttributeReference],
      conf: SQLConf,
      reads: => Option[Reads])(
      createStream: Scan => S): StreamingScan[S] = {
    val builder = table.newScanBuilder(options)
    val prunesColumns = conf.streamingV2ColumnPruningEnabled &&
      builder.isInstanceOf[SupportsPushDownRequiredColumns]
    val pushesFilters = conf.streamingV2FilterPushdownEnabled && supportsFilters(builder)
    val known = if (prunesColumns || pushesFilters) reads else None
    val requiredOutput = known.filter(_ => prunesColumns)
      .map(r => output.filter(r.requiredColumns.contains))
      .getOrElse(output)
    val filters = known.filter(_ => pushesFilters)
      .map(r => DataSourceStrategy.normalizeExprs(r.filters, output))
      .getOrElse(Nil)

    def build(builder: ScanBuilder, columns: Seq[AttributeReference]): Scan = {
      if (filters.nonEmpty) {
        // The plan of the query keeps the filters, which Spark evaluates after the scan, so the
        // scan may apply any of them.
        PushDownUtils.pushFilters(builder, filters, partitionFields = None)
      }
      builder match {
        case pruning: SupportsPushDownRequiredColumns =>
          pruning.pruneColumns(DataTypeUtils.fromAttributes(columns))
        case _ =>
      }
      builder.build()
    }
    def withStream(scan: Scan, prunedOutput: Option[Seq[AttributeReference]]): StreamingScan[S] = {
      StreamingScan(scan, createStream(scan), prunedOutput, filters)
    }

    if (requiredOutput.length == output.length) {
      withStream(build(builder, output), None)
    } else {
      val scan = build(builder, requiredOutput)
      val scanOutput = try {
        prunedScanOutput(scan.readSchema(), output, requiredOutput)
      } catch {
        case NonFatal(e) => Left(s"cannot be read: $e")
      }
      scanOutput match {
        case Right(scanOutput) =>
          val readsOutput = scanOutput.map(_.exprId) == output.map(_.exprId)
          withStream(scan, if (readsOutput) None else Some(scanOutput))
        case Left(reason) =>
          val columns = requiredOutput.map(_.name).mkString(", ")
          logWarning(log"The scan of the streaming table " +
            log"${MDC(LogKeys.STREAMING_TABLE, table)} was asked to read the columns " +
            log"${MDC(LogKeys.COLUMN_NAMES, columns)}, but its read schema " +
            log"${MDC(LogKeys.REASON, reason)}. It reads all the columns instead.")
          withStream(build(table.newScanBuilder(options), output), None)
      }
    }
  }

  /**
   * Returns whether the scan builder of a streaming source takes filters. A Python data source
   * pushes filters down with the reader of its batch scans, which a streaming read does not use,
   * and may not have at all.
   */
  private def supportsFilters(builder: ScanBuilder): Boolean = builder match {
    case _: PythonScanBuilder => false
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
   * Stands for an expression whose value depends on when it is evaluated, in the plan that is
   * optimized when the query starts, which is never executed. It keeps the children of the
   * expression. It is not deterministic for the optimizer, as the current time of a micro-batch
   * is not, so that the optimizer neither folds it nor moves operators around it as it could
   * with a value. It is deterministic once `planned`, as the values that the planner of a
   * micro-batch gets are.
   */
  private case class StartupValue(
      children: Seq[Expression],
      dataType: DataType,
      nullable: Boolean,
      planned: Boolean = false)
    extends Expression with Unevaluable {
    override lazy val deterministic: Boolean = planned && children.forall(_.deterministic)
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
  }
}
