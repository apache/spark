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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A trait that should be mixed into query operators where an single instance might appear multiple
 * times in a logical query plan.  It is invalid to have multiple copies of the same attribute
 * produced by distinct operators in a query tree as this breaks the guarantee that expression
 * ids, which are used to differentiate attributes, are unique.
 *
 * Before analysis, all operators that include this trait will be asked to produce a new version
 * of itself with globally unique expression ids.
 */
trait MultiInstanceRelation {
  def newInstance(): this.type
}

/**
 * If any MultiInstanceRelation appears more than once in the query plan then the plan is updated so
 * that each instance has unique expression ids for the attributes produced.
 */
object NewRelationInstances extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val localRelations = plan collect { case l: MultiInstanceRelation => l}
    val multiAppearance = localRelations
      .groupBy(identity[MultiInstanceRelation])
      .filter { case (_, ls) => ls.size > 1 }
      .map(_._1)
      .toSet

    plan transform {
      case l: MultiInstanceRelation if multiAppearance.contains(l) => l.newInstance()
    }
  }
}
