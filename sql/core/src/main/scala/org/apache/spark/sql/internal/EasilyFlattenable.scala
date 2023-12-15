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

import org.apache.spark.sql.{Dataset, RuntimeConfig}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, NamedExpression, UserDefinedExpression, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.types.MetadataBuilder



private[sql] object EasilyFlattenable {
  object OpType extends Enumeration {
    type OpType = Value
    val AddNewColumnsOnly, RemapOnly, Unknown = Value
  }

  def unapply(tuple: (LogicalPlan, Seq[NamedExpression], RuntimeConfig)): Option[LogicalPlan]
  = {
    val (logicalPlan, newProjList, conf) = tuple

    logicalPlan match {
      case p @ Project(projList, child: LogicalPlan) =>
        val currentOutputAttribs = AttributeSet(p.output)
        val ambiguiousAttribs = p.output.groupBy(_.name).filter(_._2.size > 1).keySet

        val currentDatasetIdOpt = p.getTagValue(Dataset.DATASET_ID_TAG).get.toSet.headOption
        val childDatasetIdOpt = child.getTagValue(Dataset.DATASET_ID_TAG).flatMap(
          _.toSet.headOption)
        // In the new column list identify those Named Expressions which are just attributes and
        // hence pass thru
        val (passThruAttribs, tinkeredOrNewNamedExprs) = newProjList.partition {
          case _: Attribute => true
          case _ => false
        }

        val passThruAttribsContainedInCurrentOutput = passThruAttribs.forall( attribute =>
          currentOutputAttribs.contains(attribute) ||
            currentOutputAttribs.exists(_.name == attribute.name))
        val opType = identifyOp(passThruAttribs, currentOutputAttribs, tinkeredOrNewNamedExprs,
          passThruAttribsContainedInCurrentOutput)
        opType match {
          case OpType.AddNewColumnsOnly =>
            // case of new columns being added only
            val childOutput = child.output.map(_.name).toSet
            val attribsRemappedInProj = projList.flatMap(ne => ne match {
              case _: AttributeReference => Seq.empty[(String, Alias)]

              case al @ Alias(_, name) => if (childOutput.contains(name)) {
                Seq(name -> al)
              } else {
                Seq.empty[(String, Alias)]
              }

              case _ => Seq.empty[(String, Alias)]
            }).toMap

            if (tinkeredOrNewNamedExprs.exists(_.collectFirst {
              // we will not flatten if expressions contain windows or aggregate as if they
              // are collapsed it can cause recalculation of functions and inefficiency with
              // separate group by clauses
              case ex if !ex.deterministic => ex
              case ex: AggregateExpression => ex
              case ex: WindowExpression => ex
              case ex: UserDefinedExpression => ex
              case u: UnresolvedAttribute if u.nameParts.size != 1 |
                ambiguiousAttribs.contains(u.name) => u
              case u: UnresolvedFunction if u.nameParts.size == 1 & u.nameParts.head == "struct" =>
                u
            }.nonEmpty)) {
              None
            } else {
              val remappedNewProjListResult = Try {
                newProjList.map {

                  case attr: AttributeReference =>
                    val ne = projList.find(
                    _.toAttribute.canonicalized == attr.canonicalized).getOrElse(attr)
                    if (attr.metadata.contains(Dataset.DATASET_ID_KEY) &&
                      currentDatasetIdOpt.contains(attr.metadata.getLong(
                        Dataset.DATASET_ID_KEY))) {
                      addDataFrameIdToCol(conf, ne, child, childDatasetIdOpt)
                    } else {
                      ne
                    }

                  case ua: UnresolvedAttribute =>
                    projList.find(_.toAttribute.name.equalsIgnoreCase(ua.name)).
                      getOrElse(throw new UnsupportedOperationException("Not able to flatten" +
                    s"  unresolved attribute $ua"))

                  case anyOtherExpr =>
                    (anyOtherExpr transformUp {
                      case attr: AttributeReference => val ne =
                        attribsRemappedInProj.get(attr.name).orElse(
                        projList.find(
                        _.toAttribute.canonicalized == attr.canonicalized).map {
                        case al: Alias => al
                        case x => x
                      }).getOrElse(attr)
                      if (attr.metadata.contains(Dataset.DATASET_ID_KEY) &&
                        currentDatasetIdOpt.contains(attr.metadata.getLong(
                          Dataset.DATASET_ID_KEY))) {
                        addDataFrameIdToCol(conf, ne, child, childDatasetIdOpt)
                      } else {
                          ne
                      }

                      case u: UnresolvedAttribute => attribsRemappedInProj.get(u.name).orElse(
                        projList.find( _.toAttribute.name.equalsIgnoreCase(u.name)).map {
                        case al: Alias => al.child
                        case u: UnresolvedAttribute =>
                          throw new UnsupportedOperationException("Not able to flatten" +
                            s"  unresolved attribute $u")
                        case x => x
                      }).getOrElse(throw new UnsupportedOperationException("Not able to flatten" +
                        s"  unresolved attribute $u"))
                    }).asInstanceOf[NamedExpression]

                }
              }
              remappedNewProjListResult match {
                case Success(remappedNewProjList) =>
                  Option(p.copy(projectList = remappedNewProjList))

                case Failure(_) => None
              }
            }

          case OpType.RemapOnly =>
            // case of renaming of columns
            val remappedNewProjListResult = Try {
              newProjList.map {
                case attr: AttributeReference => val ne = projList.find(
                  _.toAttribute.canonicalized == attr.canonicalized).get
                  if (attr.metadata.contains(Dataset.DATASET_ID_KEY) &&
                    currentDatasetIdOpt.contains(attr.metadata.getLong(
                      Dataset.DATASET_ID_KEY))) {
                    addDataFrameIdToCol(conf, ne, child, childDatasetIdOpt)
                  } else {
                    ne
                  }

                case ua: UnresolvedAttribute if ua.nameParts.size == 1 &
                  !ambiguiousAttribs.contains(ua.name) => projList.find(
                  _.toAttribute.name.equalsIgnoreCase(ua.name)).
                  getOrElse(throw new UnsupportedOperationException("Not able to flatten" +
                    s"  unresolved attribute $ua"))

                case al@Alias(ar: AttributeReference, name) =>
                  val ne = projList.find(_.toAttribute.canonicalized == ar.canonicalized).map {

                    case alx : Alias => Alias(alx.child, name)(al.exprId, al.qualifier,
                      al.explicitMetadata, al.nonInheritableMetadataKeys)

                    case _: AttributeReference => al
                  }.get
                  if (ar.metadata.contains(Dataset.DATASET_ID_KEY) &&
                    currentDatasetIdOpt.contains(ar.metadata.getLong(
                      Dataset.DATASET_ID_KEY))) {
                    addDataFrameIdToCol(conf, ne, child, childDatasetIdOpt)
                  } else {
                    ne
                  }
                case x => throw new UnsupportedOperationException("Not able to flatten" +
                  s"  unresolved attribute $x")
              }
            }
            remappedNewProjListResult match {
              case Success(remappedNewProjList) =>

                val newProj = p.copy(projectList = remappedNewProjList)

                Option(newProj)

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

  private def addDataFrameIdToCol(
      conf: RuntimeConfig,
      expr: NamedExpression,
      logicalPlan: LogicalPlan,
      childDatasetId: Option[Long]): NamedExpression =
    if (conf.get(SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED) && childDatasetId.nonEmpty) {
      val newExpr = expr transform {
      case a: AttributeReference
         =>
        val metadata = new MetadataBuilder()
          .withMetadata(a.metadata)
          .putLong(Dataset.DATASET_ID_KEY, childDatasetId.get)
          .putLong(Dataset.COL_POS_KEY, logicalPlan.output.indexWhere(a.semanticEquals))
          .build()
        a.withMetadata(metadata)
    }
    newExpr.asInstanceOf[NamedExpression]
  } else {
      expr
    }
}
