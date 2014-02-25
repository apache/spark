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

package org.apache.spark.sql
package catalyst
package plans
package logical

import expressions._
import rules._

object LocalRelation {
  def apply(output: Attribute*) =
    new LocalRelation(output)
}

case class LocalRelation(output: Seq[Attribute], data: Seq[Product] = Nil) extends LeafNode {
  // TODO: Validate schema compliance.
  def loadData(newData: Seq[Product]) = new LocalRelation(output, data ++ newData)

  /**
   * Returns an identical copy of this relation with new exprIds for all attributes.  Different
   * attributes are required when a relation is going to be included multiple times in the same
   * query.
   */
  def newInstance: LocalRelation = {
    LocalRelation(output.map(_.newInstance), data)
  }

  override protected def stringArgs = Iterator(output)
}

/**
 * If any local relation appears more than once in the query plan then the plan is updated so that
 * each instance has unique expression ids for the attributes produced.
 */
object NewLocalRelationInstances extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val localRelations = plan collect { case l: LocalRelation => l}
    val multiAppearance = localRelations
      .groupBy(identity[LocalRelation])
      .filter { case (_, ls) => ls.size > 1 }
      .map(_._1)
      .toSet

    plan transform {
      case l: LocalRelation if multiAppearance contains l => l.newInstance
    }
  }
}
