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
package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.CheckInvariant
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, V2WriteCommand, WriteDelta}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.write.RowLevelOperation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * @param analyze Analyzes a plan under the active SQLConf. The check of a generated column goes
 *                through it, so that the settings its stored text is read under cover its analysis
 *                without applying to the query of the write.
 */
class ResolveTableConstraints(
    val catalogManager: CatalogManager,
    analyze: LogicalPlan => LogicalPlan) extends Rule[LogicalPlan] {

  private val tableConstraintResolution = new TableConstraintResolution(
    this.catalogManager, analyze, conf.sessionLocalTimeZone)

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(COMMAND), ruleId) {
    // Deleting a delta of rows from an existing table doesn't produce any new rows, thus enforcing
    // check constraints is unnecessary.
    case w: WriteDelta if w.operation.command() == RowLevelOperation.Command.DELETE =>
      w
    case v2Write: V2WriteCommand
      if v2Write.table.resolved && v2Write.query.resolved &&
        !containsCheckInvariant(v2Write.query) && v2Write.outputResolved =>
      v2Write.table match {
        case r: DataSourceV2Relation =>
          tableConstraintResolution.resolveTableConstraints(v2Write, r)
        case _ =>
          v2Write
      }
  }

  private def containsCheckInvariant(plan: LogicalPlan): Boolean = {
    plan match {
      case Filter(condition, _) =>
        condition.exists(_.isInstanceOf[CheckInvariant])

      case _ => false
    }
  }
}
