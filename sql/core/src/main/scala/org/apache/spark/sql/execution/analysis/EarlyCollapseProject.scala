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

package org.apache.spark.sql.execution.analysis

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Expression, NamedExpression, UserDefinedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
import org.apache.spark.util.Utils


private[sql] object EarlyCollapseProject extends Rule[LogicalPlan] {
  val expressionRemapper: (Expression, AttributeMap[(NamedExpression, Expression)]) => Expression =
    (expr, mappings) => {
      expr transformUp {
        case attr: AttributeReference => mappings.get(attr).map {
          case (_, expr) => expr
        }.getOrElse(attr)
      }
    }
  def apply(logicalPlan: LogicalPlan): LogicalPlan =
    logicalPlan match {
      case newP @ Project(_, p : Project) if checkEarlyCollapsePossible(newP, p) =>
        collapseProjectEarly(newP, p) getOrElse newP

      case newP@Project(_, f@Filter(_, filterChild: UnaryNode)) =>
        // check if its case of nested filters followed by project
        val filterNodes = mutable.ListBuffer(f)
        var projectAtEnd: Option[Project] = None
        var keepGoing = true
        var currentChild = filterChild
        while (keepGoing) {
          currentChild match {
            case p: Project => projectAtEnd = Option(p)
              keepGoing = false
            case filter @ Filter(expr, u: UnaryNode) if expr.deterministic =>
              filterNodes += filter
              currentChild = u
            case _ => keepGoing = false
          }
        }
        if (projectAtEnd.isDefined) {
          val p = projectAtEnd.get
          if (checkEarlyCollapsePossible(newP, p)) {
            val newProjOpt = collapseProjectEarly(newP, p)
            val mappingFilterExpr = AttributeMap(p.projectList.flatMap(ne => ne match {
              case _: Attribute => Seq.empty[(Attribute, (NamedExpression, Expression))]
              case al: Alias => Seq(al.toAttribute -> (al, al.child))
            }))
            newProjOpt.map(collapsedProj => {
              val lastFilterNode = filterNodes.last
              val lastFilterMod = lastFilterNode.copy(
                  condition = expressionRemapper(lastFilterNode.condition, mappingFilterExpr),
                  child = collapsedProj.child)
              val filterChain = filterNodes.dropRight(1).foldRight(lastFilterMod)((f, c) =>
                  f.copy(condition = expressionRemapper(f.condition, mappingFilterExpr), child = c))
              collapsedProj.copy(child = filterChain)
            }).getOrElse {
              newP
            }
          } else {
            newP
          }
        } else {
          newP
        }

      case _ => logicalPlan
    }

  private def checkEarlyCollapsePossible(newP: Project, p: Project): Boolean =
    newP.getTagValue(LogicalPlan.PLAN_ID_TAG).isEmpty &&
    p.getTagValue(LogicalPlan.PLAN_ID_TAG).isEmpty &&
   !p.child.isInstanceOf[Window] &&
    p.projectList.forall(_.collectFirst {
      case ex if !ex.deterministic => ex
      case ex: UserDefinedExpression => ex
    }.isEmpty)

  private def transferMetadata(from: Attribute, to: NamedExpression): NamedExpression =
    if (from.metadata == Metadata.empty) {
      to
    } else {
      to match {
        case al: Alias =>
          val newMdBuilder = new MetadataBuilder().withMetadata(from.metadata)
          val newMd = newMdBuilder.build()
          al.copy()(exprId = al.exprId, qualifier = from.qualifier,
            nonInheritableMetadataKeys = al.nonInheritableMetadataKeys,
            explicitMetadata = Option(newMd))

        case attr: AttributeReference => attr.copy(metadata = from.metadata)(
          exprId = attr.exprId, qualifier = from.qualifier)
      }
    }



  def collapseProjectEarly(newP: Project, p: Project): Option[Project] = {
    val child = p.child
    val newProjList = newP.projectList
    val projList = p.projectList
    val childOutput = child.outputSet
    val attribsToExprInProj = AttributeMap(
      projList.flatMap(ne => ne match {
        case al@Alias(child, _) => child match {
             case attr: Attribute if childOutput.contains(attr) =>
               Seq(al.toAttribute -> (al, transferMetadata(al.toAttribute, attr)))

             case _ => Seq(al.toAttribute -> (al, child))
          }

        case _ => Seq.empty[(Attribute, (NamedExpression, Expression))]
      }))

    val remappedNewProjListResult = Try {
      newProjList.map {
        case attr: AttributeReference => attribsToExprInProj.get(attr).map {
          case (al : Alias, _) => if (attr.name == al.name) {
            transferMetadata(attr, al)
          } else {
            // To Handle the case of change of (Caps/lowercase) via toSchema resulting
            // in rename
            transferMetadata(attr, al.copy(name = attr.name)(
              exprId = al.exprId, qualifier = al.qualifier,
              explicitMetadata = al.explicitMetadata,
              nonInheritableMetadataKeys = al.nonInheritableMetadataKeys))
          }
        }.getOrElse(attr)

        case ne => expressionRemapper(ne, attribsToExprInProj).asInstanceOf[NamedExpression]
      }
    }

      remappedNewProjListResult match {
        case Success(remappedNewProjList) =>
          val newProject = Project(remappedNewProjList, child)
          val droppedNamedExprs = projList.filter(ne =>
            remappedNewProjList.forall(_.toAttribute != ne.toAttribute))
          val prevDroppedColsPart1 = p.getTagValue(LogicalPlan.DROPPED_NAMED_EXPRESSIONS).
            getOrElse(Seq.empty)
          // remove any attribs which have been added back in the new project list
          val prevDroppedColsPart2 = prevDroppedColsPart1.filterNot(x =>
            remappedNewProjList.exists(y => y.toAttribute == x.toAttribute || y.name == x.name))
          val prevDroppedColsFinal = prevDroppedColsPart2.filterNot(x =>
            droppedNamedExprs.exists(y => y == x || y.name == x.name))
          val newDroppedList = droppedNamedExprs ++ prevDroppedColsFinal
          newProject.copyTagsFrom(p)
          // remove the datasetId copied from current P due to above copy
          newProject.unsetTagValue(Dataset.DATASET_ID_TAG)
          // use the dataset id of the incoming new project
          newP.getTagValue(Dataset.DATASET_ID_TAG).foreach(map =>
            newProject.setTagValue(Dataset.DATASET_ID_TAG, map.clone()))
          newProject.unsetTagValue(LogicalPlan.DROPPED_NAMED_EXPRESSIONS)
          if (newDroppedList.nonEmpty) {
            newProject.setTagValue(LogicalPlan.DROPPED_NAMED_EXPRESSIONS, newDroppedList)
          }
          Option(newProject)

        case Failure(x) => if (Utils.isTesting) {
          throw x
        } else {
          None
        }
      }

  }
}

