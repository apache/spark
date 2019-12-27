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

package org.apache.spark.sql.connector.catalog

import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A logical plan representing a data source v1 table.
 */
case class DataSourceV1Relation(
    table: V1Table,
    output: Seq[AttributeReference],
    options: CaseInsensitiveStringMap)
  extends LeafNode with MultiInstanceRelation with NamedRelation {

  override def name: String = table.name

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  override def simpleString(maxFields: Int): String = {
    s"RelationV1${truncatedString(output, "[", ", ", "]", maxFields)} $name"
  }

  override def computeStats(): Statistics = {
    table.v1Table.stats.map(_.toPlanStats(output, conf.cboEnabled || conf.planStatsEnabled))
      .getOrElse(Statistics(sizeInBytes = conf.defaultSizeInBytes))
  }
}

object DataSourceV1Relation {
  def create(table: V1Table, options: CaseInsensitiveStringMap): DataSourceV1Relation = {
    val output = table.schema.toAttributes
    DataSourceV1Relation(table, output, options)
  }

  def create(table: V1Table): DataSourceV1Relation = create(table, CaseInsensitiveStringMap.empty)
}
