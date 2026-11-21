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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, CTERelationRef, Except, Intersect, LogicalPlan, Project, SetOperation, Union, UnionLoop, UnionLoopRef}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * The SQL standard gives a set operation of CHAR(n) and CHAR(m) the length max(n, m). When the
 * children of a Union, Intersect or Except have CHAR columns or fields of different lengths at
 * the same position, this pads the values of the shorter children to the widest length, in a
 * Project over each child that needs it, as [[TypeCoercionBase.WidenSetOperationTypes]] casts
 * them to the widest type. The output of the set operation, which comes from its first child,
 * then has the widest length. Like read-side padding, the padding does not truncate longer values.
 *
 * During analysis, such a set operation is not resolved until its children are padded, see
 * [[isAligned]], so that nothing above it is resolved with the shorter length of its first child.
 * The children are padded once the set operation is resolved otherwise, after the widening of
 * their types and the deduplication of their output: in [[PadSetOperationCharLengths]] by the
 * fixed-point analyzer, and in `SetOperationLikeResolver` by the single-pass resolver, both with
 * [[padChildren]].
 */
object SetOperationCharPadding {

  /**
   * Whether the CHAR lengths of the children of a set operation are the same at each position, so
   * that it needs no padding. Only analysis pads the children, so outside of it, see
   * [[AnalysisHelper.isInAnalyzer]], this is true: an optimizer rule can make the metadata of the
   * children differ, such as one that simplifies an expression into a column, whose alias then
   * has the column's raw type. True if the children are not resolved yet.
   */
  def isAligned(children: Seq[LogicalPlan]): Boolean =
    !AnalysisHelper.isInAnalyzer || !needsPadding(children)

  /** Whether the resolved children of a set operation have CHAR lengths to pad. */
  def needsPadding(children: Seq[LogicalPlan]): Boolean = {
    children.forall(_.resolved) &&
      widerCharRawTypes(children, children.map(_.output)).exists(_.isDefined)
  }

  /**
   * Pads the columns of the children of a set operation that have CHAR lengths shorter than the
   * widest at their position, see [[CharVarcharUtils.widerCharRawTypes]]. Without read-side
   * padding, the values of the widest children can be shorter too, so they are padded as well.
   * `newAlias` builds the alias of a padded column from the padded value, the column, and the
   * raw type of the padded value. Returns the children and their outputs, which are the given
   * ones if no column needs padding.
   */
  def padChildren(
      children: Seq[LogicalPlan],
      childOutputs: Seq[Seq[Attribute]],
      newAlias: (Expression, Attribute, DataType) => Alias)
      : (Seq[LogicalPlan], Seq[Seq[Attribute]]) = {
    val rawTypes = widerCharRawTypes(children, childOutputs)
    if (rawTypes.forall(_.isEmpty)) {
      (children, childOutputs)
    } else {
      val alwaysPad = !SQLConf.get.readSideCharPadding
      children.zip(childOutputs).map { case (child, output) =>
        // A column at more than one position has one raw type, and gets one padded column.
        val paddedColumns = mutable.HashMap.empty[ExprId, NamedExpression]
        val projectList = output.zip(rawTypes).map {
          case (attr, Some(rawType)) =>
            paddedColumns.getOrElseUpdate(attr.exprId,
              CharVarcharUtils.padCharToWiderLength(attr, rawType, alwaysPad).map {
                case (padded, paddedRawType) => newAlias(padded, attr, paddedRawType)
              }.getOrElse(attr))
          case (attr, None) => attr
        }
        if (projectList == output) {
          (child, output)
        } else {
          (Project(projectList, child), projectList.map(_.toAttribute))
        }
      }.unzip
    }
  }

  /** The alias of a padded column for the fixed-point analyzer. */
  def newAlias(padded: Expression, attr: Attribute, rawType: DataType): Alias = {
    Alias(padded, attr.name)(
      qualifier = attr.qualifier,
      explicitMetadata = Some(CharVarcharUtils.withRawType(attr.metadata, rawType)))
  }

  // The raw types with the widest CHAR lengths at each position, or None for every position of a
  // set operation that reads a recursive reference.
  private def widerCharRawTypes(
      children: Seq[LogicalPlan],
      childOutputs: Seq[Seq[Attribute]]): Seq[Option[DataType]] = {
    val conf = SQLConf.get
    // As in ApplyCharTypePadding, CHAR is a STRING in the legacy mode, and nothing is padded.
    if ((conf.charVarcharAsString && !conf.charVarcharStandardSemantics) ||
        children.exists(readsRecursiveReference)) {
      Seq.fill(childOutputs.head.length)(None)
    } else {
      CharVarcharUtils.widerCharRawTypes(childOutputs)
    }
  }

  /**
   * Whether the plan reads the recursive reference of a recursive CTE that it is not part of,
   * such as the union of the anchor and the recursive term of a recursive CTE before it becomes a
   * [[UnionLoop]]. Such a set operation is not padded: the columns of a recursive CTE have the
   * CHAR lengths of its anchor, and ApplyCharTypePadding narrows the values of the recursive term
   * to them.
   */
  private def readsRecursiveReference(plan: LogicalPlan): Boolean = plan match {
    case _: UnionLoop => false
    case r: CTERelationRef => r.recursive
    case _: UnionLoopRef => true
    case p => p.children.exists(readsRecursiveReference)
  }
}

/**
 * Pads the children of set operations whose CHAR lengths differ, see [[SetOperationCharPadding]].
 */
object PadSetOperationCharLengths extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUpWithNewOutput {
    // Once the types are widened and the output is deduplicated, as in the single-pass resolver.
    case s @ (_: Union | _: Intersect | _: Except)
        if resolvedExceptCharLengths(s) && SetOperationCharPadding.needsPadding(s.children) =>
      val (newChildren, _) = SetOperationCharPadding.padChildren(
        s.children, s.children.map(_.output), SetOperationCharPadding.newAlias)
      if (newChildren == s.children) {
        s -> Nil
      } else {
        // The output of a set operation is the output of its first child.
        val attrMapping = s.children.head.output.zip(newChildren.head.output)
          .distinctBy { case (a, _) => a.exprId }
          .filterNot { case (a, b) => a.exprId == b.exprId }
        s.withNewChildren(newChildren) -> attrMapping
      }
  }

  private def resolvedExceptCharLengths(plan: LogicalPlan): Boolean = plan match {
    case u: Union => u.resolvedExceptCharLengths
    case s: SetOperation => s.resolvedExceptCharLengths
    case _ => false
  }
}
