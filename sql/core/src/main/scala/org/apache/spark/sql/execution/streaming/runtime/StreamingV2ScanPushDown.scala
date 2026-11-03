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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, ExpressionSet, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.PushDownUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters
import org.apache.spark.sql.types.StructType
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
 * checks that the micro-batch reads no other column of the relation.
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
   * Returns what the optimized `plan` reads from each of its streaming relations of Data Source
   * V2: what the planner takes from the projections and the filters over each occurrence of the
   * relation, or all its columns and no filter where nothing is projected or filtered over it. A
   * relation that the optimized plan does not have, or any relation if the optimizer fails, has no
   * entry, so that its scan reads all its columns and gets no filter.
   *
   * @param optimizer the optimizer of the micro-batches of the query
   * @param conf the configuration that the micro-batches of the query run with
   * @param plan the plan of the query, with the current time of a micro-batch as the micro-batches
   *             have it, which the optimizer does not fold, so that no filter on it is pushed down
   */
  def reads(
      optimizer: RuleExecutor[LogicalPlan],
      conf: SQLConf,
      plan: LogicalPlan): Map[StreamingRelationV2, Reads] = {
    SQLConf.withExistingConf(conf) {
      try {
        val occurrences = mutable.HashMap.empty[StreamingRelationV2, mutable.Buffer[Reads]]
        // Visits the nodes as the planner plans them: the projections and the filters over a
        // relation are planned with its scan, and the other nodes on their own.
        def visit(node: LogicalPlan): Unit = node match {
          case PhysicalOperation(project, filters, relation: StreamingRelationV2) =>
            occurrences.getOrElseUpdate(relation, mutable.Buffer.empty) += Reads(
              AttributeSet((project ++ filters).flatMap(_.references)),
              filters.filter(f => f.deterministic && !SubqueryExpression.hasSubquery(f)))
          case _ =>
            node.children.foreach(visit)
        }
        visit(optimizer.execute(plan))
        occurrences.map { case (relation, reads) =>
          // The scan of a relation reads the rows of all its occurrences, so it can only apply the
          // filters that all of them apply.
          val filters = reads.map(r => ExpressionSet(r.filters)).reduce { (left, right) =>
            ExpressionSet(left.filter(right.contains))
          }
          relation -> Reads(
            AttributeSet.fromAttributeSets(reads.map(_.requiredColumns)), filters.toSeq)
        }.toMap
      } catch {
        case NonFatal(e) =>
          logWarning(log"Could not optimize the streaming query to push down what it reads to " +
            log"the scans of its sources, which read all their columns with no filter.", e)
          Map.empty
      }
    }
  }

  /**
   * Builds the scan of a streaming source, and creates its stream with `createStream`. If its
   * scan builder supports it, and `conf` enables it, the scan only reads the columns of `output`
   * that the query reads, and gets the filters that the query applies to all the rows that it reads
   * from the source (see `reads`). `reads` is only evaluated for such a scan builder. Returns the
   * scan, its stream, and the columns of `output` that the scan reads, in the order of its read
   * schema, unless these are `output` itself.
   *
   * A pruned scan must read the required columns and only columns of `output`, each once, as
   * `SupportsPushDownRequiredColumns` requires. A scan that does not is replaced by a scan of all
   * the columns, as without pruning, and its stream is stopped. Spark asks a scan for its read
   * schema once the stream is created, since a scan may only know then what its stream reads.
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
      createStream: Scan => S): (Scan, S, Option[Seq[AttributeReference]]) = {
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
    def buildFullScan(builder: ScanBuilder): (Scan, S, Option[Seq[AttributeReference]]) = {
      val scan = build(builder, output)
      (scan, createStream(scan), None)
    }

    if (requiredOutput.length == output.length) {
      buildFullScan(builder)
    } else {
      val scan = build(builder, requiredOutput)
      val stream = createStream(scan)
      val (readSchema, scanOutput) = try {
        val readSchema = scan.readSchema()
        (readSchema, prunedScanOutput(readSchema, output, requiredOutput))
      } catch {
        case e: Throwable =>
          try stream.stop() catch {
            case NonFatal(suppressed) => e.addSuppressed(suppressed)
          }
          throw e
      }
      scanOutput match {
        case Right(scanOutput) =>
          val readsOutput = scanOutput.map(_.exprId) == output.map(_.exprId)
          (scan, stream, if (readsOutput) None else Some(scanOutput))
        case Left(reason) =>
          val columns = requiredOutput.map(_.name).mkString(", ")
          logWarning(log"The scan of the streaming table " +
            log"${MDC(LogKeys.STREAMING_TABLE, table)} was asked to read the columns " +
            log"${MDC(LogKeys.COLUMN_NAMES, columns)}, but its read schema " +
            log"${MDC(LogKeys.SCHEMA, readSchema.simpleString)} " +
            log"${MDC(LogKeys.REASON, reason)}. It reads all the columns instead.")
          stream.stop()
          buildFullScan(table.newScanBuilder(options))
      }
    }
  }

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
      Left(s"has the columns ${unknown.mkString(", ")}, which the relation does not output")
    } else if (repeated.nonEmpty) {
      Left(s"has the columns ${repeated.mkString(", ")} more than once")
    } else {
      val scanOutput = PushDownUtils.toOutputAttrs(readSchema, output)
      val missing = requiredOutput.filterNot(AttributeSet(scanOutput).contains)
      if (missing.nonEmpty) {
        Left(s"does not have the columns ${missing.map(_.name).mkString(", ")}")
      } else {
        Right(scanOutput)
      }
    }
  }
}
