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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{ExposesMetadataColumns, LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

object VersionUnresolvedRelation {
  def apply(
      dataSource: DataSource,
      isStreaming: Boolean)(session: SparkSession): VersionUnresolvedRelation = {
    VersionUnresolvedRelation(
      dataSource = dataSource,
      output = dataSource.sourceInfo.schema.toAttributes,
      isStreaming = isStreaming)(session)
  }
}

case class VersionUnresolvedRelation (
    dataSource: DataSource,
    output: Seq[Attribute],
    override val isStreaming: Boolean)(session: SparkSession)
  extends LeafNode with MultiInstanceRelation {

  val source: String = dataSource.sourceInfo.name
  val options: Map[String, String] = dataSource.options
  val userSpecifiedSchema: Option[StructType] = dataSource.userSpecifiedSchema

  override def newInstance(): LogicalPlan = this.copy(output = output.map(_.newInstance()))(session)

  override def computeStats(): Statistics = Statistics(
    sizeInBytes = BigInt(conf.defaultSizeInBytes)
  )

  override def toString: String = s"[Version Unresolved] $source"

}
