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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, ResolvedTempView}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2Table, FileTable}

/**
 * Replace File source V2 targets in [[InsertIntoStatement]] with V1 [[FileFormat]] relations.
 * For example, inserting into a temporary view or persistent table backed by
 * [[org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2]] otherwise fails because there
 * is no corresponding physical plan.
 * This is a temporary hack for making current data source V2 work. It should be
 * removed when Catalog support of file data source v2 is finished.
 */
class FallBackFileSourceV2(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private object FileTableTarget {
    def unapply(insert: InsertIntoStatement)
        : Option[(LogicalPlan, DataSourceV2Relation, FileTable)] = insert.table match {
      case view: ResolvedTempView =>
        view.viewRelation.plan.map(SubqueryAlias.stripLeadingAliases).collect {
          case d @ ExtractV2Table(table: FileTable) => (view, d, table)
        }
      case target @ ResolvedTable(catalog, identifier, table: FileTable, _) =>
        val relation = DataSourceV2Relation.create(
          table, Some(catalog), Some(identifier), insert.tableOptions)
        Some((target, relation, table))
      case _ => None
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case i @ FileTableTarget(originalTarget, d, table) =>
      val v1FileFormat = table.fallbackFileFormat.getDeclaredConstructor().newInstance()
      val relation = HadoopFsRelation(
        table.fileIndex,
        table.fileIndex.partitionSchema,
        table.schema,
        None,
        v1FileFormat,
        d.options.asScala.toMap)(sparkSession)
      val logicalRelation = LogicalRelation(relation)
      val target = originalTarget match {
        case view: ResolvedTempView =>
          ResolvedTempView(
            view.identifier,
            view.viewRelation.copy(plan = Some(logicalRelation)))
        case _ => logicalRelation
      }
      target.copyTagsFrom(originalTarget)
      i.copy(table = target)
  }
}
