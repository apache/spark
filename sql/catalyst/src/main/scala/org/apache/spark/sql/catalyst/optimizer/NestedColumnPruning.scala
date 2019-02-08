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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.StructType

/**
 * This aims to handle nested column pruning pattern inside `ColumnPruning` optimizer rule.
 * If a project or its child references to nested fields, and not all the fields
 * in a nested attribute are used, we can create a new [[CreateNamedStruct]] containing only
 * subset of the fields as a new project in the child.
 */
object NestedColumnPruning {

  type ExprMap = Map[Expression, Expression]
  type RequiredFieldMap = Map[Attribute, Set[GetStructField]]

  def unapply(plan: LogicalPlan): Option[(Map[ExprId, Alias], ExprMap)] = plan match {
    case p @ Project(_, child) if canProjectPushThrough(child) =>
      getPrunedNestedAttrsAndProjects(getRequiredFieldMap(p, child))
    case _ => None
  }

  /**
   * Add projects to prune unused nested columns.
   */
  def prune(
      plan: LogicalPlan,
      nestedAttrs: Map[ExprId, Alias],
      projectsOnNestedAttrs: ExprMap): LogicalPlan = plan match {
    case Project(projectList, g @ GlobalLimit(_, grandChild: LocalLimit)) =>
      Project(
        getNewProjectList(projectList, projectsOnNestedAttrs),
        g.copy(child = addPrunedProjectToChildren(grandChild, nestedAttrs)))
    case Project(projectList, child) =>
      Project(
        getNewProjectList(projectList, projectsOnNestedAttrs),
        addPrunedProjectToChildren(child, nestedAttrs))
    case _ => plan
  }

  /**
   * Get a map from a referenced attribute to its set of required fields.
   */
  private def getRequiredFieldMap(plans: LogicalPlan*): RequiredFieldMap = {
    val rootReferenceAttrSet = plans.map(getRootReferences).reduce(_ ++ _)
    val nestedFieldReferences = plans.map(getNestedFieldReferences).reduce(_ ++ _)

    nestedFieldReferences
      .filter(!_.references.subsetOf(rootReferenceAttrSet))
      .groupBy(_.references)
      .filter(_._1.size == 1)
      .map(x => (x._1.head, x._2.toSet))
  }

  /**
   * Return a replaced project list.
   */
  private def getNewProjectList(projectList: Seq[NamedExpression], projectsOnNestedAttrs: ExprMap) =
    projectList.map(_.transform {
      case e if projectsOnNestedAttrs.contains(e) => projectsOnNestedAttrs(e)
    }.asInstanceOf[NamedExpression])

  /**
   * Return a plan with new childen pruned with `Project`.
   */
  private def addPrunedProjectToChildren(plan: LogicalPlan, nestedAttrs: Map[ExprId, Alias]) =
    plan.withNewChildren(plan.children.map { child =>
      Project(child.output.map(a => nestedAttrs.getOrElse(a.exprId, a)), child)
    })

  /**
   * Returns true for those operators that project can be pushed through.
   */
  private def canProjectPushThrough(plan: LogicalPlan) = plan match {
    case _: GlobalLimit => true
    case _: LocalLimit => true
    case _: Repartition => true
    case _: Sample => true
    case _ => false
  }

  /**
   * Similar to [[QueryPlan.references]], but this only returns all attributes
   * that are explicitly referenced on the root levels in a [[LogicalPlan]].
   */
  private def getRootReferences(plan: LogicalPlan) = {
    def helper(e: Expression): AttributeSet = e match {
      case attr: AttributeReference => AttributeSet(attr)
      case _: GetStructField => AttributeSet.empty
      case es if es.children.nonEmpty => AttributeSet(es.children.flatMap(helper))
      case _ => AttributeSet.empty
    }
    AttributeSet.fromAttributeSets(plan.expressions.map(helper))
  }

  /**
   * Returns all the nested fields that are explicitly referenced as [[Expression]]
   * in a [[LogicalPlan]]. Currently, we only support having [[GetStructField]] in the chain
   * of the expressions. If the chain contains GetArrayStructFields, GetMapValue, or
   * GetArrayItem, the nested field substitution will not be performed.
   */
  private def getNestedFieldReferences(plan: LogicalPlan): Seq[GetStructField] = {
    def helper(e: Expression): Seq[GetStructField] = e match {
      case f @ GetStructField(child, _, _) if isGetStructFieldOrAttr(child) => Seq(f)
      case es if es.children.nonEmpty => es.children.flatMap(e => helper(e))
      case _ => Seq.empty
    }

    def isGetStructFieldOrAttr(e: Expression): Boolean = e match {
      case GetStructField(child, _, _) => isGetStructFieldOrAttr(child)
      case _: AttributeReference => true
      case _ => false
    }

    plan.expressions.flatMap(helper)
  }

  /**
   * Return `CreateNamedStruct` iff the given expression is a struct type consisting of
   * some of the given nested fields. Otherwise, this returns None.
   */
  private def getReducedCreateNamedStruct(expression: Expression, nestedFields: Set[GetStructField])
    : Option[CreateNamedStruct] = {
    expression.dataType match {
      case st: StructType =>
        val pairs = st.zipWithIndex.flatMap { case (structField, ordinal) =>
          val newField = GetStructField(expression, ordinal, Some(structField.name))
          if (nestedFields.map(_.canonicalized).contains(newField.canonicalized)) {
            Seq(Literal(structField.name), newField)
          } else {
            None
          }
        }
        if (pairs.nonEmpty && pairs.length / 2 < st.length) {
          Some(CreateNamedStruct(pairs))
        } else {
          None
        }
      case _ => None
    }
  }

  /**
   * Return two maps if they exists.
   * First map is used to replace with the pruned nested attributes.
   * Second map is used to replace the projects with the new pruned nested attributes.
   */
  private def getPrunedNestedAttrsAndProjects(requiredFieldMap: RequiredFieldMap)
    : Option[(Map[ExprId, Alias], ExprMap)] = {
    val projectsOnNestedAttrs = mutable.Map.empty[Expression, Expression]

    val nestedAttrMap = requiredFieldMap.flatMap {
      case (attr: Attribute, nestedFields: Set[GetStructField]) =>
        getReducedCreateNamedStruct(attr, nestedFields) match {
          case Some(createNamedStruct) =>
            val alias = Alias(createNamedStruct, attr.name)()
            nestedFields.foreach { s: GetStructField =>
              val ordinal = nestedFields.zipWithIndex.find(_._1.name.equals(s.name)).head._2
              projectsOnNestedAttrs.put(s, s.copy(child = alias.toAttribute, ordinal = ordinal))
            }
            Some(attr.exprId -> alias)
          case None => None
        }
    }

    if (nestedAttrMap.nonEmpty) {
      Some((nestedAttrMap, projectsOnNestedAttrs.toMap))
    } else {
      None
    }
  }
}
