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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.datasources.v2.PushDownUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Prunes the top-level columns that the scans of Data Source V2 streaming sources read to those
 * that the optimized plan of the query reads from them, as `V2ScanRelationPushDown` does for the
 * scans of batch queries.
 *
 * The stream of a source is created from its scan, and has to exist before any micro-batch is
 * planned, so the scan is built when the query starts. The query is optimized once at that point,
 * by the optimizer of its micro-batches and with their configuration, and the scan of each source
 * reads the columns that the planner takes from the optimized plan for the relation of the source
 * (see `DataSourceV2Strategy`).
 *
 * The relations in the plan of the query keep all their columns, so each micro-batch is optimized
 * as it is without this pruning: the optimizer keeps deciding which columns every operator gets,
 * and in which order, including the stateful ones. Only the scan node of the physical plan outputs
 * the columns that the scan reads, and the planner checks that the micro-batch reads no other
 * column of the relation.
 */
private[sql] object StreamingV2ColumnPruning extends Logging {

  /**
   * Returns the columns that the optimized `plan` reads from each of its streaming relations of
   * Data Source V2: what the planner takes from the projections and the filters over each
   * occurrence of the relation, or all its columns where nothing is projected over it. A relation
   * that the optimized plan does not have, or any relation if the optimizer fails, has no entry,
   * so that its scan reads all its columns.
   *
   * @param optimizer the optimizer of the micro-batches of the query
   * @param conf the configuration that the micro-batches of the query run with
   */
  def requiredColumns(
      optimizer: RuleExecutor[LogicalPlan],
      conf: SQLConf,
      plan: LogicalPlan): Map[StreamingRelationV2, AttributeSet] = {
    SQLConf.withExistingConf(conf) {
      try {
        val required = mutable.HashMap.empty[StreamingRelationV2, mutable.Buffer[AttributeSet]]
        // Visits the nodes as the planner plans them: the projections and the filters over a
        // relation are planned with its scan, and the other nodes on their own.
        def visit(node: LogicalPlan): Unit = node match {
          case PhysicalOperation(project, filters, relation: StreamingRelationV2) =>
            required.getOrElseUpdate(relation, mutable.Buffer.empty) +=
              AttributeSet((project ++ filters).flatMap(_.references))
          case _ =>
            node.children.foreach(visit)
        }
        visit(optimizer.execute(plan))
        required.map { case (relation, columns) =>
          relation -> AttributeSet.fromAttributeSets(columns)
        }.toMap
      } catch {
        case NonFatal(e) =>
          logWarning(log"Could not optimize the streaming query to prune the columns of the " +
            log"scans of its sources, which read all their columns.", e)
          Map.empty
      }
    }
  }

  /**
   * Builds the scan of a streaming source, which reads the columns of `output` in `required` when
   * its scan builder supports column pruning, and creates its stream with `createStream`. Returns
   * the scan, its stream, and the columns of `output` that the scan reads, in the order of its
   * read schema, unless these are `output` itself. `required` is only evaluated for a scan builder
   * that supports column pruning.
   *
   * A pruned scan must read the required columns and only columns of `output`, each once, as
   * `SupportsPushDownRequiredColumns` requires. A scan that does not is replaced by a scan of all
   * the columns, as without pruning, and its stream is stopped. Spark asks a scan for its read
   * schema once the stream is created, since a scan may only know then what its stream reads.
   *
   * @param output the output of the relation of the source, with the metadata columns that the
   *               query reads, which the analyzer added to it, so that connectors implementing
   *               `SupportsMetadataColumns` include them in `readSchema()`
   * @param required the columns of `output` that the query reads, or None to read all of them
   */
  def buildScan[S <: SparkDataStream](
      table: SupportsRead,
      options: CaseInsensitiveStringMap,
      output: Seq[AttributeReference],
      required: => Option[AttributeSet])(
      createStream: Scan => S): (Scan, S, Option[Seq[AttributeReference]]) = {
    def build(builder: ScanBuilder, columns: Seq[AttributeReference]): Scan = {
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

    val builder = table.newScanBuilder(options)
    val requiredOutput = builder match {
      case _: SupportsPushDownRequiredColumns =>
        required.map(columns => output.filter(columns.contains)).getOrElse(output)
      case _ =>
        output
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
