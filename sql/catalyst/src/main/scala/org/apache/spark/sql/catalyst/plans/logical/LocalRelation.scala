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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.trees.TreePattern.{LOCAL_RELATION, TreePattern}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.Utils

object LocalRelation {
  def apply(output: Attribute*): LocalRelation = new LocalRelation(output)

  def apply(output1: StructField, output: StructField*): LocalRelation = {
    apply(StructType(output1 +: output))
  }

  def apply(schema: StructType): LocalRelation = {
    new LocalRelation(DataTypeUtils.toAttributes(schema))
  }

  def fromExternalRows(
      output: Seq[Attribute],
      data: Seq[Row],
      isSqlScript: Boolean = false): LocalRelation = {
    val schema = DataTypeUtils.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val rel = LocalRelation(output, data.map(converter(_).asInstanceOf[InternalRow]))
    rel.isSqlScript = true
    rel
  }

  def fromProduct(
      output: Seq[Attribute],
      data: Seq[Product],
      isSqlScript: Boolean = false): LocalRelation = {
    val schema = DataTypeUtils.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val rel = LocalRelation(output, data.map(converter(_).asInstanceOf[InternalRow]))
    rel.isSqlScript = true
    rel
  }
}

/**
 * Logical plan node for scanning data from a local collection.
 *
 * @param data The local collection holding the data. It doesn't need to be sent to executors
 *             and then doesn't need to be serializable.
 */
case class LocalRelation(
    output: Seq[Attribute],
    data: Seq[InternalRow] = Nil,
    // Indicates whether this relation has data from a streaming source.
    override val isStreaming: Boolean = false)
  extends LeafNode with analysis.MultiInstanceRelation {

  // A local relation must have resolved output.
  require(output.forall(_.resolved), "Unresolved attributes found when constructing LocalRelation.")

  var isSqlScript: Boolean = false

  /**
   * Returns an identical copy of this relation with new exprIds for all attributes.  Different
   * attributes are required when a relation is going to be included multiple times in the same
   * query.
   */
  override final def newInstance(): this.type = {
    LocalRelation(output.map(_.newInstance()), data, isStreaming).asInstanceOf[this.type]
  }

  override protected def stringArgs: Iterator[Any] = {
    if (data.isEmpty) {
      Iterator("<empty>", output)
    } else {
      Iterator(output)
    }
  }

  override def computeStats(): Statistics = {
    val rowCount: Option[BigInt] = if (Utils.isTesting &&
      conf.getConfString("spark.sql.test.localRelationRowCount", "false") != "true") {
      // LocalRelation is heavily used in tests and we should not report row count by default in
      // tests to keep the test coverage, in case the plan is overly optimized.
      None
    } else {
      Some(data.length)
    }
    Statistics(
      sizeInBytes = EstimationUtils.getSizePerRow(output) * data.length, rowCount = rowCount)
  }

  def toSQL(inlineTableName: String): String = {
    require(data.nonEmpty)
    val types = output.map(_.dataType)
    val rows = data.map { row =>
      val cells = row.toSeq(types).zip(types).map { case (v, tpe) => Literal(v, tpe).sql }
      cells.mkString("(", ", ", ")")
    }
    "VALUES " + rows.mkString(", ") +
      " AS " + inlineTableName +
      output.map(_.name).mkString("(", ", ", ")")
  }

  override def maxRows: Option[Long] = Some(data.length.toLong)

  override val nodePatterns: Seq[TreePattern] = Seq(LOCAL_RELATION)
}
