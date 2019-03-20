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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * This aims to handle a nested column aliasing pattern inside the `ColumnPruning` optimizer rule.
 * If a project or its child references to nested fields, and not all the fields
 * in a nested attribute are used, we can substitute them by alias attributes; then a project
 * of the nested fields as aliases on the children of the child will be created.
 */
object NestedColumnAliasing {

  def unapply(plan: LogicalPlan)
    : Option[(Map[GetStructField, Alias], Map[ExprId, Seq[Alias]])] = plan match {
    case Project(projectList, child)
        if SQLConf.get.nestedSchemaPruningEnabled && canProjectPushThrough(child) =>
      getAliasSubMap(projectList)
    case _ => None
  }

  /**
   * Replace nested columns to prune unused nested columns later.
   */
  def replaceToAliases(
      plan: LogicalPlan,
      nestedFieldToAlias: Map[GetStructField, Alias],
      attrToAliases: Map[ExprId, Seq[Alias]]): LogicalPlan = plan match {
    case Project(projectList, child) =>
      Project(
        getNewProjectList(projectList, nestedFieldToAlias),
        replaceChildrenWithAliases(child, attrToAliases))
  }

  /**
   * Return a replaced project list.
   */
  private def getNewProjectList(
      projectList: Seq[NamedExpression],
      nestedFieldToAlias: Map[GetStructField, Alias]): Seq[NamedExpression] = {
    projectList.map(_.transform {
      case f: GetStructField if nestedFieldToAlias.contains(f) =>
        nestedFieldToAlias(f).toAttribute
    }.asInstanceOf[NamedExpression])
  }

  /**
   * Return a plan with new children replaced with aliases.
   */
  private def replaceChildrenWithAliases(
      plan: LogicalPlan,
      attrToAliases: Map[ExprId, Seq[Alias]]): LogicalPlan = {
    plan.withNewChildren(plan.children.map { plan =>
      Project(plan.output.flatMap(a => attrToAliases.getOrElse(a.exprId, Seq(a))), plan)
    })
  }

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
   * Return root references that are individually accessed as a whole, and `GetStructField`s.
   */
  private def collectRootReferenceAndGetStructField(e: Expression): Seq[Expression] = e match {
    case _: AttributeReference | _: GetStructField => Seq(e)
    case es if es.children.nonEmpty => es.children.flatMap(collectRootReferenceAndGetStructField)
    case _ => Seq.empty
  }

  /**
   * Return two maps in order to replace nested fields to aliases.
   *
   * 1. GetStructField -> Alias: A new alias is created for each nested field.
   * 2. ExprId -> Seq[Alias]: A reference attribute has multiple aliases pointing it.
   */
  private def getAliasSubMap(projectList: Seq[NamedExpression])
    : Option[(Map[GetStructField, Alias], Map[ExprId, Seq[Alias]])] = {
    val (nestedFieldReferences, otherRootReferences) =
      projectList.flatMap(collectRootReferenceAndGetStructField).partition {
        case _: GetStructField => true
        case _ => false
      }

    val aliasSub = nestedFieldReferences.asInstanceOf[Seq[GetStructField]]
      .filter(!_.references.subsetOf(AttributeSet(otherRootReferences)))
      .groupBy(_.references.head)
      .flatMap { case (attr, nestedFields: Seq[GetStructField]) =>
        // Each expression can contain multiple nested fields.
        // Note that we keep the original names to deliver to parquet in a case-sensitive way.
        val nestedFieldToAlias = nestedFields.distinct.map { f =>
          val exprId = NamedExpression.newExprId
          (f, Alias(f, s"_gen_alias_${exprId.id}")(exprId, Seq.empty, None))
        }

        // If all nested fields of `attr` are used, we don't need to introduce new aliases.
        // By default, ColumnPruning rule uses `attr` already.
        if (nestedFieldToAlias.nonEmpty &&
            nestedFieldToAlias.length < totalFieldNum(attr.dataType)) {
          Some(attr.exprId -> nestedFieldToAlias)
        } else {
          None
        }
      }

    if (aliasSub.isEmpty) {
      None
    } else {
      Some((aliasSub.values.flatten.toMap, aliasSub.map(x => (x._1, x._2.map(_._2)))))
    }
  }

  /**
   * Return total number of fields of this type. This is used as a threshold to use nested column
   * pruning. It's okay to underestimate. If the number of reference is bigger than this, the parent
   * reference is used instead of nested field references.
   */
  private def totalFieldNum(dataType: DataType): Int = dataType match {
    case _: AtomicType => 1
    case StructType(fields) => fields.map(f => totalFieldNum(f.dataType)).sum
    case ArrayType(elementType, _) => totalFieldNum(elementType)
    case MapType(keyType, valueType, _) => totalFieldNum(keyType) + totalFieldNum(valueType)
    case _ => 1 // UDT and others
  }
}
