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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.types.StructType

object VersionUnresolvedRelation {
  def apply(
      dataSource: DataSource,
      isStreaming: Boolean)(session: SparkSession): VersionUnresolvedRelation = {
    VersionUnresolvedRelation(
      source = dataSource.className,
      dataSource = Some(dataSource),
      options = dataSource.options,
      userSpecifiedSchema = dataSource.userSpecifiedSchema,
      output = dataSource.sourceInfo.schema.toAttributes,
      isStreaming = isStreaming)(session)
  }
}

case class VersionUnresolvedRelation (
    source: String,
    dataSource: Option[DataSource],
    options: Map[String, String] = Map.empty,
    userSpecifiedSchema: Option[StructType] = None,
    paths: Seq[String] = Seq.empty,
    output: Seq[Attribute] = Seq.empty,
    override val isStreaming: Boolean = false)(session: SparkSession)
  extends LeafNode with MultiInstanceRelation {

  val sparkSession: SparkSession = session

  override def newInstance(): LogicalPlan = this.copy(output = output.map(_.newInstance()))(session)

  override def computeStats(): Statistics = Statistics(
    sizeInBytes = BigInt(conf.defaultSizeInBytes)
  )

  override def toString: String = s"[Version Unresolved] $source"
}
