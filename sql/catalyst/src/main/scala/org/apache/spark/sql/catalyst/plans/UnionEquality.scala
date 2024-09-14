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

package org.apache.spark.sql.catalyst.plans

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId}

trait UnionEquality [PlanType <: QueryPlan[PlanType]] {
  self: PlanType =>

  private lazy val positionAgnosticHash = this.children.toSet.hashCode()

  // for now should be used only for unionAll. Union distinct may not have size check
  def positionAgnosticEquals(that: PlanType): Boolean = {
    val thisChildren = this.children
    val thatChildren = that.children
    if (thisChildren.length == thatChildren.length &&
      normalizeOutputAttributes(this.asInstanceOf[PlanType]) == normalizeOutputAttributes(that)) {
      // check for elements equality disregarding order
      val buff = mutable.ArrayBuffer[PlanType](thatChildren: _*)
      thisChildren.forall(p => {
        val i = buff.indexOf(p)
        if (i == -1) {
          false
        } else {
          buff.remove(i)
          true
        }
      } )
    } else {
      false
    }
  }

  def positionAgnosticHashCode: Int = this.positionAgnosticHash

  protected def isCanonicalizedPlan: Boolean

  // if the canonicalized plan's output is taken , and if the top plan 's project  has  Alias,
  // then the attribute ref created out of it will have name = "", so we need to force it to none
  private def normalizeOutputAttributes(plan: PlanType): Seq[Attribute] =
    plan.output.zipWithIndex.map {
      case (attr, i) =>
        val conditioned = attr.withExprId(ExprId.apply(i))
        if (plan.asInstanceOf[UnionEquality[PlanType]].isCanonicalizedPlan) {
          conditioned.withName("none")
        } else {
          conditioned
        }
    }
}
