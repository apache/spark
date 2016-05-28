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

package org.apache.spark.sql.hive

import org.apache.spark.sql.{ExperimentalMethods, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

class HiveOptimizer (
  sparkSession: SparkSession,
  catalog: HiveSessionCatalog,
  conf: SQLConf,
  experimentalMethods: ExperimentalMethods)
  extends Optimizer(catalog, conf) {

  override def batches: Seq[Batch] = super.batches :+
    Batch("Partition Pruner", Once, PartitionPruner(conf)) :+
      Batch("User Provided Optimizers", fixedPoint, experimentalMethods.extraOptimizations: _*)
}

case class PartitionPruner(conf: SQLConf) extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.hivePartitionPrunerForStats) {
      return plan
    }
    plan.transform {
      case filter@Filter(condition, relation: MetastoreRelation)
        if relation.partitionKeys.nonEmpty && condition.deterministic =>
        val partitionKeyIds = AttributeSet(relation.partitionKeys)
        val predicates = splitConjunctivePredicates(condition)
        val pruningPredicates = predicates.filter { predicate =>
          !predicate.references.isEmpty &&
            predicate.references.subsetOf(partitionKeyIds)
        }
        relation.partitionPruningPred = pruningPredicates
        filter
        //filter.withNewChildren(relation.newInstance(pruningPredicates) :: Nil)
    }
  }
}
