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

// import scala.collection.mutable

// import org.apache.spark.sql.catalyst.expressions.{DistributedSequenceID, Expression}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.DISTRIBUTED_SEQUENCE_ID
import org.apache.spark.sql.types.LongType

// object ReplaceDistributedSequenceID extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan match {
//    case _ if plan.containsPattern(DISTRIBUTED_SEQUENCE_ID) =>
//      replace(plan)
//    case _ => plan
//  }
//
//  private def replace(plan: LogicalPlan): LogicalPlan = {
//    val attributeMap = mutable.HashMap[Expression, Expression]()
//
//    // val newChildren = plan.children.map { child =>
//    plan.children.map { child =>
//      val validExprs = plan.expressions.filter { expr =>
//        expr.references.subsetOf(child.outputSet)
//      }
//      val resultAttrs = validExprs.zipWithIndex.map { case (u, i) =>
//        AttributeReference(s"distributed_sequence_id$i", u.dataType)()
//      }
//      attributeMap ++= validExprs.zip(resultAttrs)
//    }
//
//    val newPlan = plan.transformExpressions {
//      case p: DistributedSequenceID => attributeMap.getOrElse(p, p)
//    }
//
//    if (newPlan.output != plan.output) {
//      // Trim away the new UDF value if it was only used for filtering or something.
//      Project(plan.output, newPlan)
//    } else {
//      newPlan
//    }
//  }
// }

object ReplaceDistributedSequenceID extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case _ if plan.containsPattern(DISTRIBUTED_SEQUENCE_ID) =>
      plan.withNewChildren(
        plan.children.map(c =>
          AttachDistributedSequence(
            AttributeReference("distributed_sequence_id", LongType, nullable = false)(),
            c))
      )
    case _ => plan
  }
}
