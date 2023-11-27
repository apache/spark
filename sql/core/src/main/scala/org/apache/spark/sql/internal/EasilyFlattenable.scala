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

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, AttributeSet, NamedExpression, UserDefinedExpression}
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
              case u: UnresolvedAttribute => if (u.nameParts.size > 1) {
                   true
                 } else {
                   attribsReassignedInProj.exists(attr => attr.name.equalsIgnoreCase(u.name))
                 }
          } || ne.collectFirst{
            case ex if !ex.deterministic => ex
            case ex if ex.isInstanceOf[UserDefinedExpression] => ex
            case u: UnresolvedAttribute if u.nameParts.size != 1 => u
            case u: UnresolvedAlias => u
            case u : UnresolvedFunction if u.nameParts.size == 1 & u.nameParts.head == "struct" => u
          }.nonEmpty)) {
            None
          } else {
            val remappedNewProjListResult = Try {
              newProjList.map {
                case attr: AttributeReference => projList.find(
                  _.toAttribute.canonicalized == attr.canonicalized).getOrElse(attr)
                case anyOtherExpr =>
                  (anyOtherExpr transformUp {
                    case attr: AttributeReference => projList.find(
                      _.toAttribute.canonicalized == attr.canonicalized).map {
                      case al: Alias => al.child
                      case x => x
                    }.getOrElse(attr)

                    case u: UnresolvedAttribute => projList.find(
                      _.toAttribute.name.equalsIgnoreCase(u.name)).map(x => x match {
                      case al: Alias => al.child
                      case u: UnresolvedAttribute =>
                        throw new UnsupportedOperationException("Not able to flatten" +
                        s"  unresolved attribute $u")
                      case _ => x
                    }).getOrElse(throw new UnsupportedOperationException("Not able to flatten" +
                      s"  unresolved attribute $u"))
                  }).asInstanceOf[NamedExpression]
              }
            }
            remappedNewProjListResult match {
              case Success(remappedNewProjList) => val pidOpt = p.getTagValue(
                LogicalPlan.PLAN_ID_TAG)
                val newProj = p.copy(projectList = remappedNewProjList)
                pidOpt.foreach(id => newProj.setTagValue[Long](LogicalPlan.PLAN_ID_TAG, id))
                Option(newProj)
              case Failure(_) => None
            }
          }
        } else {
          // for now None
          None
        }

      case _ => None
    }
  }
}
