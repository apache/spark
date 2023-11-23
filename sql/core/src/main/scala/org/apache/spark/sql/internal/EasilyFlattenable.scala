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

package org.apache.spark.sql.internal

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, AttributeSet, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}

private[sql] object EasilyFlattenable {
  def unapply(tuple: (LogicalPlan, Seq[NamedExpression])): Option[LogicalPlan] = {
    val (logicalPlan, newProjList) = tuple
    logicalPlan match {
      case p@Project(projList, child) =>

        // In the new column list identify those Named Expressions which are just attributes and
        // hence pass thru
        val (passThruAttribs, tinkeredOrNewNamedExprs) = newProjList.partition {
          case _: AttributeReference => true
          case _ => false
        }
        val currentOutputAttribs = AttributeSet(p.output)

        if (passThruAttribs.size == currentOutputAttribs.size && passThruAttribs.forall(
          currentOutputAttribs.contains) && tinkeredOrNewNamedExprs.nonEmpty) {

          val attribsReassignedInProj = AttributeSet(projList.filter(ne => ne match {
            case _: AttributeReference => false
            case _ => true
          }).map(_.toAttribute)).intersect(AttributeSet(child.output))
          if (tinkeredOrNewNamedExprs.exists(ne => ne.references.exists {
            case attr: AttributeReference => attribsReassignedInProj.contains(attr)
            case u: UnresolvedAttribute => attribsReassignedInProj.exists(_.name == u.name)
          } || ne.collectFirst{case u: UnresolvedFunction => u}.nonEmpty)) {
            None
          } else {
            val remappedNewProjList = newProjList.map(ne => ne match {
              case attr: AttributeReference => projList.find(
                _.toAttribute.canonicalized == attr.canonicalized).getOrElse(attr)
              case anyOtherExpr => (anyOtherExpr transformUp {
                case attr: AttributeReference => projList.find(
                  _.toAttribute.canonicalized == attr.canonicalized).map(x => x match {
                  case al: Alias => al.child
                  case _ => x
                }).getOrElse(attr)

                case u: UnresolvedAttribute => projList.find(
                  _.toAttribute.name == u.name).map(x => x match {
                  case al: Alias => al.child
                  case _ => x
                }).getOrElse(u)

              }).asInstanceOf[NamedExpression]
            })
            Option(p.copy(projectList = remappedNewProjList))
          }
        } else {
          // for now None
          None
        }

      case _ => None
    }
  }
}
