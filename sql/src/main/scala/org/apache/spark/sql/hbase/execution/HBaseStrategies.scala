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

package org.apache.spark.sql.hbase.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.execution.{Project, SparkPlan}
import org.apache.spark.sql.hbase.{HBaseRelation, execution}
import org.apache.spark.sql.sources.LogicalRelation
import org.apache.spark.sql.{SQLContext, Strategy}

/**
 * Retrieves data using a HBaseTableScan.  Partition pruning predicates are also detected and
 * applied.
 */
private[hbase] trait HBaseStrategies {
  self: SQLContext#SparkPlanner =>

  private[hbase] object HBaseDataSource extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, inPredicates,
      l@LogicalRelation(relation: HBaseRelation)) =>
        pruneFilterProjectHBase(
          l,
          projectList,
          inPredicates,
          (a, f) => relation.buildScan(a, f)) :: Nil

      case InsertIntoTable(l@LogicalRelation(t: HBaseRelation), partition, child, _) =>
        execution.InsertIntoHBaseTable(t, planLater(child)) :: Nil
      case _ => Nil
    }

    // Based on Catalyst expressions.
    // Almost identical to pruneFilterProjectRaw
    protected def pruneFilterProjectHBase(
                                  relation: LogicalRelation,
                                  projectList: Seq[NamedExpression],
                                  filterPredicates: Seq[Expression],
                                  scanBuilder: (Seq[Attribute], Seq[Expression]) => RDD[Row]) = {

      val projectSet = AttributeSet(projectList.flatMap(_.references))
      val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

      val pushedFilters = if (filterPredicates.nonEmpty) {
        Seq(filterPredicates.map {
          _ transform {
            // Match original case of attributes.
            case a: AttributeReference => relation.attributeMap(a)
            // We will do HBase-specific predicate pushdown so just use the original predicate here
          }
        }.reduceLeft(And))
      } else {
        filterPredicates
      }

      val hbaseRelation = relation.relation.asInstanceOf[HBaseRelation]
      if (projectList.map(_.toAttribute) == projectList &&
        projectSet.size == projectList.size &&
        filterSet.subsetOf(projectSet)) {
        // When it is possible to just use column pruning to get the right projection and
        // when the columns of this projection are enough to evaluate all filter conditions,
        // just do a scan followed by a filter, with no extra project.
        val requestedColumns =
          projectList.asInstanceOf[Seq[Attribute]] // Safe due to if above.
            .map(relation.attributeMap) // Match original case of attributes.

        // We have to use a HBase-specific scanner here while maintain as much compatibility
        // with the data source API as possible, primarily because
        // 1) We need to set up the outputPartitioning field to HBase-specific partitions
        // 2) Future use of HBase co-processor
        // 3) We will do partition-specific predicate pushdown
        // The above two *now* are absent from the PhysicalRDD class.

        HBaseSQLTableScan(hbaseRelation, projectList.map(_.toAttribute),
          scanBuilder(requestedColumns, pushedFilters))
      } else {
        val requestedColumns = (projectSet ++ filterSet).map(relation.attributeMap).toSeq
        val scan = HBaseSQLTableScan(hbaseRelation, requestedColumns,
          scanBuilder(requestedColumns, pushedFilters))
        Project(projectList, scan)
      }
    }
  }
}
