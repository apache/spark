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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, Distinct, LogicalPlan, SetOperation, Union}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Prunes the top-level columns that the scans of Data Source V2 streaming sources read to those
 * that a streaming query needs, as `V2ScanRelationPushDown` does for batch scans. The scan of a
 * streaming source is built once, when the query starts, from its analyzed plan, so the required
 * columns are taken from that plan: the optimizer prunes the columns of each micro-batch further,
 * so stateful operators keep getting the same columns as without this pruning.
 */
private[sql] object StreamingV2ColumnPruning {

  /**
   * Returns the attributes of its sources that a query needs: those in its output, those that its
   * operators reference, and every column of the children of the operators that need all of them,
   * such as a distinct, or that map the columns of a child to their output by position rather
   * than by attribute, such as a union or the definition of a common table expression.
   */
  def requiredAttributes(plan: LogicalPlan): AttributeSet = {
    var required = plan.outputSet
    plan.foreach { node =>
      required ++= node.references
      val needsAllColumns = node match {
        case _: Union | _: SetOperation | _: Distinct | _: CTERelationDef => true
        case _ => mapsColumnsByPosition(node)
      }
      if (needsAllColumns) {
        node.children.foreach(child => required ++= child.outputSet)
      }
    }
    required
  }

  /**
   * Returns whether a node outputs attributes that are neither of its children, nor produced or
   * defined by its expressions, which it then maps from the columns of its children by position.
   */
  private def mapsColumnsByPosition(node: LogicalPlan): Boolean = node.children.nonEmpty && {
    val known = AttributeSet(node.children.flatMap(_.output)) ++ node.producedAttributes ++
      AttributeSet(node.expressions.collect { case named: NamedExpression => named.toAttribute })
    node.output.exists(attribute => !known.contains(attribute))
  }

  /**
   * Builds the scan of a streaming source that reads the columns of `output` in `required`, when
   * its scan builder supports column pruning, and creates its stream with `createStream`.
   * Returns the scan, its stream and its output, the columns of `output` that it reads, in the
   * order of its read schema. A scan that does not read the required columns, as named in
   * `output`, is replaced by one of all the columns, as without pruning.
   *
   * The read schema of the scan is asked once its stream is created, since a scan may only know
   * then what it reads for a stream.
   *
   * @param output the output of the source, with the metadata columns that the query reads, which
   *               the analyzer added to it
   * @param required the attributes that the query needs, or None to read all the columns
   */
  def buildScan[S <: SparkDataStream](
      table: SupportsRead,
      options: CaseInsensitiveStringMap,
      output: Seq[AttributeReference],
      required: Option[AttributeSet])(
      createStream: Scan => S): (Scan, S, Seq[AttributeReference]) = {
    def build(columns: Seq[AttributeReference]): (Scan, Boolean) = {
      val builder = table.newScanBuilder(options)
      val prunes = builder match {
        case pruning: SupportsPushDownRequiredColumns =>
          pruning.pruneColumns(DataTypeUtils.fromAttributes(columns))
          true
        case _ => false
      }
      (builder.build(), prunes)
    }
    val requiredOutput = required.map(r => output.filter(r.contains)).getOrElse(output)
    val (scan, prunes) = build(requiredOutput)
    val stream = createStream(scan)
    if (!prunes || requiredOutput.length == output.length) {
      (scan, stream, output)
    } else {
      val byName = output.map(attribute => attribute.name -> attribute).toMap
      val readSchema = scan.readSchema()
      val scanOutput = readSchema.fieldNames.toSeq.flatMap(byName.get)
      if (scanOutput.length == readSchema.length && requiredOutput.forall(scanOutput.contains)) {
        (scan, stream, scanOutput)
      } else {
        stream.stop()
        val (fullScan, _) = build(output)
        (fullScan, createStream(fullScan), output)
      }
    }
  }
}
