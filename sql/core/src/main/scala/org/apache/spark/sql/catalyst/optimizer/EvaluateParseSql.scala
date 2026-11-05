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

package org.apache.spark.sql.catalyst.optimizer

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, ParseSql}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.PARSE_SQL
import org.apache.spark.sql.types.StringType

/**
 * Evaluates constant ANALYZE `parse_sql` calls on the driver, where the session catalog is
 * available. ANALYZE must run here even when `ConstantFolding` is excluded. PARSE calls are
 * left to `ConstantFolding`, which keeps its tolerance for failing constants in untaken
 * branches.
 *
 * A constant ANALYZE call whose SQL argument fails to evaluate is left in place. The error
 * then surfaces when the expression runs, so a failure in an untaken branch stays unobserved.
 */
object EvaluateParseSql extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformDownWithSubqueriesAndPruning(_.containsPattern(PARSE_SQL)) {
      case subQuery =>
        subQuery.transformAllExpressionsWithPruning(_.containsPattern(PARSE_SQL)) {
          case p: ParseSql if p.isConstantAnalyze => evaluate(p)
        }
    }

  private def evaluate(p: ParseSql): Expression =
    try {
      Literal.create(p.eval(), StringType)
    } catch {
      case NonFatal(_) => p
    }
}
