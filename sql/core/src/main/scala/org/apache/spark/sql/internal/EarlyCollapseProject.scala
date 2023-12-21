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

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, Expression, NamedExpression, UserDefinedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, UnaryNode, Window}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
import org.apache.spark.util.Utils


private[sql] object EarlyCollapseProject {
  def unapply(logicalPlan: LogicalPlan): Option[LogicalPlan] =
    logicalPlan match {
      case newP @ Project(newProjList, p @ Project(projList, child))
        if checkEarlyCollapsePossible(p, newP, child) =>
        collapseProjectEarly(newP, newProjList, p, projList, child)

      case newP @ Project(newProjList, f @ Filter(_, filterChild: UnaryNode)) =>
        // check if its case of nested filters followed by project
        val filterNodes = mutable.ListBuffer(f)
        var projectAtEnd: Option[Project] = None
        var keepGoing = true
        var currentChild = filterChild
        while(keepGoing) {
          currentChild match {
            case p: Project => projectAtEnd = Option(p)
              keepGoing = false
            case filter @ Filter(_, u: UnaryNode) =>
              filterNodes += filter
              currentChild = u
            case _ => keepGoing = false
          }
        }
        if (projectAtEnd.isDefined &&
          filterNodes.map(_.condition.references).reduce(_ ++ _).
            subsetOf(
              AttributeSet(newProjList.filter(_.isInstanceOf[AttributeReference])
                .map(_.toAttribute)))) {
          val p = projectAtEnd.get
          val child = p.child
          if (checkEarlyCollapsePossible(p, newP, child)) {
            val newProjOpt = collapseProjectEarly(newP, newProjList, p, p.projectList, child)
            newProjOpt.map(collapsedProj => {
              val lastFilterMod = filterNodes.last.copy(child = collapsedProj)
              filterNodes.dropRight(1).foldRight(lastFilterMod)((f, c) => f.copy(child = c))
            })
          } else {
            None
          }
        } else {
          None
        }

      case _ => None
    }

  private def checkEarlyCollapsePossible(p: Project, newP: Project, child: LogicalPlan): Boolean =
    p.getTagValue(LogicalPlan.PLAN_ID_TAG).isEmpty &&
      newP.getTagValue(LogicalPlan.PLAN_ID_TAG).isEmpty &&
      !child.isInstanceOf[Window]

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

  def collapseProjectEarly(
      newP: Project,
      newProjList: Seq[NamedExpression],
      p: Project,
      projList: Seq[NamedExpression],
      child: LogicalPlan): Option[Project] = {
    // In the new column list identify those Named Expressions which are just attributes and
    // hence pass thru
    val (_, tinkeredOrNewNamedExprs) = newProjList.partition {
      case _: Attribute => true
      case _ => false
    }

    val childOutput = child.outputSet
    val attribsRemappedInProj = AttributeMap(
      projList.flatMap(ne => ne match {
        case _: AttributeReference => Seq.empty[(Attribute, Expression)]

        case al@Alias(attr: AttributeReference, _) =>
          if (childOutput.contains(attr)) {
            Seq(al.toAttribute -> transferMetadata(al.toAttribute, attr))
          } else {
            Seq.empty[(Attribute, Expression)]
          }

        case _ => Seq.empty[(Attribute, Expression)]
      }))

    if ((tinkeredOrNewNamedExprs ++ p.projectList).exists(_.collectFirst {
      // we will not flatten if expressions contain windows or aggregate as if they
      // are collapsed it can cause recalculation of functions and inefficiency with
      // separate group by clauses
      case ex if !ex.deterministic => ex
      case ex: UserDefinedExpression => ex
    }.nonEmpty)) {
      None
    } else {
      val remappedNewProjListResult = Try {
        newProjList.map {
          case attr: AttributeReference => projList.find(
            _.toAttribute.canonicalized == attr.canonicalized).map {
            case al: Alias =>
              if (attr.name == al.name) {
                transferMetadata(attr, al)
              } else {
                // To Handle the case of change of (Caps/lowercase) via toSchema resulting
                // in rename
                transferMetadata(attr, al.copy(name = attr.name)(
                  exprId = al.exprId, qualifier = al.qualifier,
                  explicitMetadata = al.explicitMetadata,
                  nonInheritableMetadataKeys = al.nonInheritableMetadataKeys))
              }

            case _: AttributeReference => attr
          }.getOrElse(attr)

          case anyOtherExpr =>
            (anyOtherExpr transformUp {
              case attr: AttributeReference =>
                attribsRemappedInProj.get(attr).orElse(projList.find(
                  _.toAttribute.canonicalized == attr.canonicalized).map {
                  case al: Alias => al.child
                  case _ => attr
                }).getOrElse(attr)
            }).asInstanceOf[NamedExpression]
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
}
