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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Expression, NamedExpression, UserDefinedExpression, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}

private[sql] object EarlyCollapseProject {
  object OpType extends Enumeration {
    type OpType = Value
    val AddNewColumnsOnly, RemapOnly, Unknown = Value
  }

  def unapply(logicalPlan: LogicalPlan): Option[LogicalPlan] = {
    logicalPlan match {
      case newP @ Project(newProjList, p @ Project(projList, child)) if
        !p.getTagValue(LogicalPlan.SKIP_EARLY_PROJECT_COLLAPSE).getOrElse(false) &&
        !newP.getTagValue(LogicalPlan.SKIP_EARLY_PROJECT_COLLAPSE).getOrElse(false) &&
        p.getTagValue(LogicalPlan.PLAN_ID_TAG).isEmpty &&
        newP.getTagValue(LogicalPlan.PLAN_ID_TAG).isEmpty
         =>
        val currentOutputAttribs = AttributeSet(p.output)

        // In the new column list identify those Named Expressions which are just attributes and
        // hence pass thru
        val (passThruAttribs, tinkeredOrNewNamedExprs) = newProjList.partition {
          case _: Attribute => true
          case _ => false
        }

        val passThruAttribsContainedInCurrentOutput = passThruAttribs.forall(attribute =>
          currentOutputAttribs.contains(attribute) ||
            currentOutputAttribs.exists(_.name == attribute.name))
        val opType = identifyOp(passThruAttribs, currentOutputAttribs, tinkeredOrNewNamedExprs,
          passThruAttribsContainedInCurrentOutput)
        opType match {
          case OpType.AddNewColumnsOnly =>
            // case of new columns being added only
            val childOutput = child.output.map(_.name).toSet
            val attribsRemappedInProj = projList.flatMap(ne => ne match {
              case _: AttributeReference => Seq.empty[(String, Expression)]

              case Alias(expr, name) => if (childOutput.contains(name)) {
                Seq(name -> expr)
              } else {
                Seq.empty[(String, Expression)]
              }

              case _ => Seq.empty[(String, Expression)]
            }).toMap

            if (tinkeredOrNewNamedExprs.exists(_.collectFirst {
              // we will not flatten if expressions contain windows or aggregate as if they
              // are collapsed it can cause recalculation of functions and inefficiency with
              // separate group by clauses
              case ex if !ex.deterministic => ex
              case ex: AggregateExpression => ex
              case ex: WindowExpression => ex
              case ex: UserDefinedExpression => ex
            }.nonEmpty)) {
              None
            } else {
              val remappedNewProjListResult = Try {
                newProjList.map {
                  case attr: AttributeReference => projList.find(
                    _.toAttribute.canonicalized == attr.canonicalized).getOrElse(attr)

                  case anyOtherExpr =>
                    (anyOtherExpr transformUp {
                      case attr: AttributeReference =>
                        attribsRemappedInProj.get(attr.name).orElse(projList.find(
                          _.toAttribute.canonicalized == attr.canonicalized).map {
                          case al: Alias => al.child
                          case x => x
                        }).getOrElse(attr)
                    }).asInstanceOf[NamedExpression]
                }
              }
              remappedNewProjListResult match {
                case Success(remappedNewProjList) => Option(Project(remappedNewProjList, child))

                case Failure(_) => None
              }
            }

          case OpType.RemapOnly =>
            // case of renaming of columns
            val remappedNewProjListResult = Try {
              newProjList.map {
                case attr: AttributeReference => projList.find(
                  _.toAttribute.canonicalized == attr.canonicalized).get


                case al@Alias(ar: AttributeReference, name) =>
                  projList.find(_.toAttribute.canonicalized == ar.canonicalized).map {

                    case alx: Alias => Alias(alx.child, name)(al.exprId, al.qualifier,
                      al.explicitMetadata, al.nonInheritableMetadataKeys)

                    case _: AttributeReference => al
                  }.get
              }
            }
            remappedNewProjListResult match {
              case Success(remappedNewProjList) => Option(Project(remappedNewProjList, child))

              case Failure(_) => None
            }

          case _ => None
        }

      case _ => None
    }
  }

  private def identifyOp(
      passThruAttribs: Seq[NamedExpression],
      currentOutputAttribs: AttributeSet,
      tinkeredOrNewNamedExprs: Seq[NamedExpression],
      passThruAttribsContainedInCurrentOutput: Boolean): OpType.OpType = {

    if (passThruAttribs.size == currentOutputAttribs.size &&
          passThruAttribsContainedInCurrentOutput && tinkeredOrNewNamedExprs.nonEmpty) {
      OpType.AddNewColumnsOnly
    } else if (passThruAttribs.size + tinkeredOrNewNamedExprs.size == currentOutputAttribs.size
      && passThruAttribsContainedInCurrentOutput && tinkeredOrNewNamedExprs.forall {
        case Alias(_: AttributeReference, _) => true
        case _ => false
    }) {
      OpType.RemapOnly
    } else {
      OpType.Unknown
    }
  }
}
