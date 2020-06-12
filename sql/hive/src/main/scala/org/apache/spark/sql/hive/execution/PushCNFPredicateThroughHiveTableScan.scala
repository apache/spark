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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{And, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Try converting join condition to conjunctive normal form expression so that more predicates may
 * be able to be pushed down.
 * To avoid expanding the join condition, the join condition will be kept in the original form even
 * when predicate pushdown happens.
 */
object PushCNFPredicateThroughHiveTableScan extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    var resolved = false
    plan resolveOperatorsDown {
      case op @ ScanOperation(projectList, conditions, relation: HiveTableRelation)
        if conditions.nonEmpty && !resolved =>
        resolved = true
        val predicates = conjunctiveNormalFormAndGroupExpsByReference(conditions.reduceLeft(And))
        if (predicates.isEmpty) {
          op
        } else {
          Project(projectList, Filter(predicates.reduceLeft(And), relation))
        }
    }
  }
}
