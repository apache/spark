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
  object OpType extends Enumeration {
    type OpType = Value
    val AddNewColumnsOnly, RemapOnly, Unknown = Value
  }

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
        val passThruAttribsContainedInCurrentOutput = passThruAttribs.forall(
          currentOutputAttribs.contains)
        val opType = identifyOp(passThruAttribs, currentOutputAttribs, tinkeredOrNewNamedExprs,
          passThruAttribsContainedInCurrentOutput)

        opType match {
          case OpType.AddNewColumnsOnly =>
            // case of new columns being added only
            val attribsReassignedInProj = projList.filter(ne => ne match {
              case _: AttributeReference => false
              case _ => true
            }).map(_.name).toSet.intersect(child.output.map(_.name).toSet)

            if (tinkeredOrNewNamedExprs.exists(ne => ne.references.exists {
              case attr: AttributeReference => attribsReassignedInProj.contains(attr.name)
              case u: UnresolvedAttribute => if (u.nameParts.size > 1) {
                true
              } else {
                attribsReassignedInProj.contains(u.name)
              }
            } || ne.collectFirst {
              case ex if !ex.deterministic => ex
              case ex if ex.isInstanceOf[UserDefinedExpression] => ex
              case u: UnresolvedAttribute if u.nameParts.size != 1 => u
              case u: UnresolvedAlias => u
              case u: UnresolvedFunction if u.nameParts.size == 1 & u.nameParts.head == "struct" =>
                u
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
                        _.toAttribute.name.equalsIgnoreCase(u.name)).map {
                        case al: Alias => al.child
                        case u: UnresolvedAttribute =>
                          throw new UnsupportedOperationException("Not able to flatten" +
                            s"  unresolved attribute $u")
                        case x => x
                      }.getOrElse(throw new UnsupportedOperationException("Not able to flatten" +
                        s"  unresolved attribute $u"))
                    }).asInstanceOf[NamedExpression]
                }
              }
              remappedNewProjListResult match {
                case Success(remappedNewProjList) =>
                  val newProj = p.copy(projectList = remappedNewProjList)
                  Option(newProj)
                case Failure(_) => None
              }
            }

          case OpType.RemapOnly =>
            // case of renaming of columns
            val remappedNewProjListResult = newProjList.map {
              case attr: AttributeReference => projList.find(
                _.toAttribute.canonicalized == attr.canonicalized).get

              case al@Alias(ar: AttributeReference, name) =>
                projList.find(
                  _.toAttribute.canonicalized == ar.canonicalized).map {
                  case alx: Alias => alx.copy(name = name)(exprId = al.exprId,
                    qualifier = al.qualifier, explicitMetadata = al.explicitMetadata,
                    nonInheritableMetadataKeys = al.nonInheritableMetadataKeys)

                  case _: AttributeReference => al
                }.get
            }

            Option(p.copy(projectList = remappedNewProjListResult))

          case _ => None
        }

      case _ => None
    }
  }

  private def identifyOp(
      passThruAttribs: Seq[NamedExpression],
      currentOutputAttribs: AttributeSet,
      tinkeredOrNewNamedExprs: Seq[NamedExpression],
      passThruAttribsContainedInCurrentOutput: Boolean
      ): OpType.OpType = {

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
