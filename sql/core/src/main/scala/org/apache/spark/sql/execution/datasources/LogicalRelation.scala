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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NormalizeableRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{ExposesMetadataColumns, LeafNode, LogicalPlan, Statistics, StreamSourceAwareLogicalPlan}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.{truncatedString, CharVarcharUtils}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.sources.BaseRelation

/**
 * Used to link a [[BaseRelation]] in to a logical query plan.
 *
 * This class is widely used for pattern matching. We encourage developers to avoid using the
 * default pattern if possible (except all parameters are needed). We provide convenient pattern
 * objects to help avoiding the default pattern.
 *
 * Here's the list of pattern objects:
 * - [[LogicalRelationWithTable]]
 */
case class LogicalRelation(
    relation: BaseRelation,
    output: Seq[AttributeReference],
    catalogTable: Option[CatalogTable],
    override val isStreaming: Boolean,
    @transient stream: Option[SparkDataStream])
  extends LeafNode
  with StreamSourceAwareLogicalPlan
  with MultiInstanceRelation
  with ExposesMetadataColumns
  with NormalizeableRelation {

  // Only care about relation when canonicalizing.
  override def doCanonicalize(): LogicalPlan = copy(
    output = output.map(QueryPlan.normalizeExpressions(_, output)),
    catalogTable = None)

  override def computeStats(): Statistics = {
    catalogTable
      .flatMap(_.stats.map(_.toPlanStats(output, conf.cboEnabled || conf.planStatsEnabled)))
      .getOrElse(Statistics(sizeInBytes = relation.sizeInBytes))
  }

  /** Used to lookup original attribute capitalization */
  val attributeMap: AttributeMap[AttributeReference] = AttributeMap(output.map(o => (o, o)))

  /**
   * Returns a new instance of this LogicalRelation. According to the semantics of
   * MultiInstanceRelation, this method returns a copy of this object with
   * unique expression ids. We respect the `expectedOutputAttributes` and create
   * new instances of attributes in it.
   */
  override def newInstance(): LogicalRelation = {
    this.copy(output = output.map(_.newInstance()))
  }

  override def refresh(): Unit = relation match {
    case fs: HadoopFsRelation => fs.location.refresh()
    case _ =>  // Do nothing.
  }

  override def simpleString(maxFields: Int): String = {
    s"Relation ${catalogTable.map(_.identifier.unquotedString).getOrElse("")}" +
      s"[${truncatedString(output, ",", maxFields)}] $relation"
  }

  override lazy val metadataOutput: Seq[AttributeReference] = relation match {
    case relation: HadoopFsRelation =>
      metadataOutputWithOutConflicts(Seq(relation.fileFormat.createFileMetadataCol()))
    case _ => Nil
  }

  override def withMetadataColumns(): LogicalRelation = {
    val newMetadata = metadataOutput.filterNot(outputSet.contains)
    if (newMetadata.nonEmpty) {
      val newRelation = this.copy(output = output ++ newMetadata)
      newRelation.copyTagsFrom(this)
      newRelation
    } else {
      this
    }
  }

  override def withStream(stream: SparkDataStream): LogicalRelation = copy(stream = Some(stream))

  override def getStream: Option[SparkDataStream] = stream

  /**
   * Minimally normalizes this [[LogicalRelation]] to make it comparable in [[NormalizePlan]].
   */
  override def normalize(): LogicalPlan = {
    copy(catalogTable = catalogTable.map(CatalogTable.normalize))
  }
}

object LogicalRelation {
  def apply(relation: BaseRelation, isStreaming: Boolean = false): LogicalRelation = {
    // The v1 source may return schema containing char/varchar type. We replace char/varchar
    // with "annotated" string type here as the query engine doesn't support char/varchar yet.
    val schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(relation.schema)
    LogicalRelation(relation, toAttributes(schema), None, isStreaming, None)
  }

  def apply(relation: BaseRelation, table: CatalogTable): LogicalRelation = {
    // The v1 source may return schema containing char/varchar type. We replace char/varchar
    // with "annotated" string type here as the query engine doesn't support char/varchar yet.
    val schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(relation.schema)
    LogicalRelation(relation, toAttributes(schema), Some(table), false, None)
  }
}

/**
 * Extract the [[BaseRelation]] and [[CatalogTable]] from [[LogicalRelation]]. You can also
 * retrieve the instance of LogicalRelation like following:
 *
 * case l @ LogicalRelationWithTable(relation, catalogTable) => ...
 */
object LogicalRelationWithTable {
  def unapply(plan: LogicalRelation): Option[(BaseRelation, Option[CatalogTable])] = {
    Some(plan.relation, plan.catalogTable)
  }
}
