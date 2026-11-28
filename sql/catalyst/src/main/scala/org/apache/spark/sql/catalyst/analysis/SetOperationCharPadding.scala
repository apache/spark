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
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, CTERelationRef, Except, Intersect, LogicalPlan, Project, SetOperation, Union, UnionLoop}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
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
 * their types and the deduplication of their output. In the fixed-point analyzer, the rules that
 * do these rewrites pad the set operation in the same step with [[padResolved]], so that a chain
 * of set operations is resolved in one traversal, and [[PadSetOperationCharLengths]] pads the
 * others. The single-pass resolver pads them in `SetOperationLikeResolver`, with [[padChildren]].
 */
object SetOperationCharPadding {

  /**
   * Marks a set operation of a plan that is analyzed again after it was optimized, such as an
   * iteration of a recursive CTE, which recursive CTE execution builds from the optimized plan of
   * the recursive term, so that it is not padded again: it was padded when the query was analyzed.
   */
  val SKIP_CHAR_PADDING = TreeNodeTag[Unit]("skipCharPadding")

  /**
   * Marks a set operation whose children analysis padded, so that a streaming query restored from
   * a checkpoint can tell whether the padding changed its plan, see [[changesStreamingPlan]].
   */
  val CHAR_PADDED = TreeNodeTag[Unit]("charPadded")

  /**
   * Whether the CHAR lengths of the children of a set operation are the same at each position, so
   * that it needs no padding. Only analysis pads the children, so outside of it, see
   * [[AnalysisHelper.isInAnalyzer]], this is true: an optimizer rule can make the metadata of the
   * children differ, such as one that simplifies an expression into a column, whose alias then
   * has the column's raw type. So it is for a set operation marked with [[SKIP_CHAR_PADDING]].
   * True if the children are not resolved yet.
   */
  def isAligned(setOperation: LogicalPlan): Boolean =
    !AnalysisHelper.isInAnalyzer || setOperation.getTagValue(SKIP_CHAR_PADDING).isDefined ||
      !needsPadding(setOperation.children)

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
        // A column at more than one position is padded at each to the raw type there, once for
        // each raw type. Nothing above the set operation refers to it yet, see isAligned.
        val paddedColumns = mutable.HashMap.empty[(ExprId, DataType), NamedExpression]
        val projectList = output.zip(rawTypes).map {
          case (attr, Some(rawType)) =>
            paddedColumns.getOrElseUpdate((attr.exprId, rawType),
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

  /**
   * Pads the children of a set operation that is resolved but for its CHAR lengths, during
   * analysis, see [[padChildren]]. Returns the set operation and the mapping from its output to
   * the new output, which is empty if it needs no padding. Rules of the fixed-point analyzer that
   * rewrite a set operation into such a one call this on the result, so that it is resolved before
   * the traversal visits its parent.
   */
  def padResolved(plan: LogicalPlan): (LogicalPlan, Seq[(Attribute, Attribute)]) = plan match {
    case s @ (_: Union | _: Intersect | _: Except)
        if resolvedExceptCharLengths(s) && !isAligned(s) =>
      val (newChildren, _) = padChildren(s.children, s.children.map(_.output), newAlias)
      if (newChildren == s.children) {
        s -> Nil
      } else {
        // The output of a set operation is the output of its first child.
        val attrMapping = s.children.head.output.zip(newChildren.head.output)
          .distinctBy { case (a, _) => a.exprId }
          .filterNot { case (a, b) => a.exprId == b.exprId }
        val padded = s.withNewChildren(newChildren)
        padded.setTagValue(CHAR_PADDED, ())
        padded -> attrMapping
      }
    case _ => plan -> Nil
  }

  /**
   * Whether the CHAR padding of set operations, see [[isEnabled]], changes the analyzed plan of a
   * streaming query: whether a streaming set operation in it was padded, or has children whose
   * CHAR lengths differ, which the padding would pad. Its stateful operators then key on values
   * that the padding changes, so a query restored from a checkpoint keeps the padding that the
   * checkpoint records.
   */
  def changesStreamingPlan(plan: LogicalPlan): Boolean = plan.exists {
    case s @ (_: Union | _: Intersect | _: Except) if s.isStreaming =>
      s.getTagValue(CHAR_PADDED).isDefined ||
        CharVarcharUtils.widerCharRawTypes(s.children.map(_.output)).exists(_.isDefined)
    case _ => false
  }

  /**
   * [[padResolved]] for a set operation that a rule rewrote with the given mapping of its output,
   * such as the widening of its types. Returns both mappings as one.
   */
  def padResolved(
      plan: LogicalPlan,
      attrMapping: Seq[(Attribute, Attribute)]): (LogicalPlan, Seq[(Attribute, Attribute)]) = {
    val (padded, paddingMapping) = padResolved(plan)
    if (paddingMapping.isEmpty) {
      padded -> attrMapping
    } else {
      val paddedById = paddingMapping.map { case (a, b) => a.exprId -> b }.toMap
      val remapped = attrMapping.map { case (a, b) => a -> paddedById.getOrElse(b.exprId, b) }
      val remappedIds = attrMapping.map(_._2.exprId).toSet
      padded -> (remapped ++ paddingMapping.filterNot { case (a, _) => remappedIds(a.exprId) })
    }
  }

  private def resolvedExceptCharLengths(plan: LogicalPlan): Boolean = plan match {
    case u: Union => u.resolvedExceptCharLengths
    case s: SetOperation => s.resolvedExceptCharLengths
    case _ => false
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
    def none = Seq.fill(childOutputs.head.length)(None)
    if (!isEnabled) {
      none
    } else {
      val rawTypes = CharVarcharUtils.widerCharRawTypes(childOutputs)
      // Most set operations need no padding, so the plan is only walked when one does.
      if (rawTypes.exists(_.isDefined) && children.exists(readsRecursiveReference)) {
        none
      } else {
        rawTypes
      }
    }
  }

  /**
   * Whether set operations of CHAR columns of different lengths have the widest length. Not in the
   * legacy mode where CHAR is a STRING, in which ApplyCharTypePadding pads nothing either, or
   * with [[SQLConf.LEGACY_NO_CHAR_WIDENING_IN_SET_OPERATIONS]].
   */
  def isEnabled: Boolean = {
    val conf = SQLConf.get
    !(conf.charVarcharAsString && !conf.charVarcharStandardSemantics) &&
      !conf.getConf(SQLConf.LEGACY_NO_CHAR_WIDENING_IN_SET_OPERATIONS)
  }

  /**
   * Whether the plan reads a recursive CTE whose definition it is part of, before the reference is
   * resolved to a `UnionLoopRef`: the union of the anchor and the recursive term of a recursive
   * CTE, before `ResolveWithCTE` turns it into a [[UnionLoop]]. Such a union is not padded: the
   * columns of a recursive CTE have the CHAR lengths of its anchor, and ApplyCharTypePadding brings
   * the values of the recursive term to them. A set operation in the recursive term also reads the
   * reference until then, and is padded once it reads a `UnionLoopRef`, like any other.
   */
  private def readsRecursiveReference(plan: LogicalPlan): Boolean = plan match {
    case _: UnionLoop => false
    case r: CTERelationRef => r.recursive
    case p => p.children.exists(readsRecursiveReference)
  }
}

/**
 * Pads the children of set operations whose CHAR lengths differ, see [[SetOperationCharPadding]],
 * that no other rule padded when it rewrote them.
 *
 * TODO: a SequentialStreamingUnion is not padded, so it keeps the CHAR lengths of its first child.
 */
object PadSetOperationCharLengths extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUpWithNewOutput {
    case s @ (_: Union | _: Intersect | _: Except) => SetOperationCharPadding.padResolved(s)
  }
}
