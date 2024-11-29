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

import org.apache.spark.SparkException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Cast, Equality, Expression, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.MetadataBuilder

/**
 * Detects ambiguous self-joins, so that we can fail the query instead of returning confusing
 * results.
 *
 * Dataset column reference is simply an [[AttributeReference]] that is returned by `Dataset#col`.
 * Most of time we don't need to do anything special, as [[AttributeReference]] can point to
 * the column precisely. However, in case of self-join, the analyzer generates
 * [[AttributeReference]] with new expr IDs for the right side plan of the join. If the Dataset
 * column reference points to a column in the right side plan of a self-join, users will get
 * unexpected result because the column reference can't match the newly generated
 * [[AttributeReference]].
 *
 * Note that, this rule removes all the Dataset id related metadata from `AttributeReference`, so
 * that they don't exist after analyzer.
 */
object DetectAmbiguousSelfJoin extends Rule[LogicalPlan] {

  // Dataset column reference is an `AttributeReference` with 2 special metadata.
  private def isColumnReference(a: AttributeReference): Boolean = {
    a.metadata.contains(Dataset.DATASET_ID_KEY) && a.metadata.contains(Dataset.COL_POS_KEY)
  }

  private case class ColumnReference(datasetId: Long, colPos: Int, exprId: ExprId)

  private def toColumnReference(a: AttributeReference): ColumnReference = {
    ColumnReference(
      a.metadata.getLong(Dataset.DATASET_ID_KEY),
      a.metadata.getLong(Dataset.COL_POS_KEY).toInt,
      a.exprId)
  }

  object LogicalPlanWithDatasetId {
    def unapply(p: LogicalPlan): Option[(LogicalPlan, mutable.HashSet[Long])] = {
      p.getTagValue(Dataset.DATASET_ID_TAG).map(ids => p -> ids)
    }
  }

  object AttrWithCast {
    @scala.annotation.tailrec
    def unapply(expr: Expression): Option[AttributeReference] = expr match {
      case Cast(child, _, _, _) => unapply(child)
      case a: AttributeReference => Some(a)
      case _ => None
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED)) return plan

    // We always remove the special metadata from `AttributeReference` at the end of this rule, so
    // Dataset column reference only exists in the root node via Dataset transformations like
    // `Dataset#select`.
    if (!plan.exists(_.isInstanceOf[Join])) return stripColumnReferenceMetadataInPlan(plan)

    val colRefAttrs = plan.expressions.flatMap(_.collect {
      case a: AttributeReference if isColumnReference(a) => a
    })

    if (colRefAttrs.nonEmpty) {
      val colRefs = colRefAttrs.map(toColumnReference).distinct
      val ambiguousColRefs = mutable.HashSet.empty[ColumnReference]
      val dsIdSet = colRefs.map(_.datasetId).toSet
      val inputAttrs = AttributeSet(plan.children.flatMap(_.output))

      plan.foreach {
        case LogicalPlanWithDatasetId(p, ids) if dsIdSet.intersect(ids).nonEmpty =>
          colRefs.foreach { ref =>
            if (ids.contains(ref.datasetId)) {
              if (ref.colPos < 0 || ref.colPos >= p.output.length) {
                throw SparkException.internalError(
                  "Hit an invalid Dataset column reference: " +
                  s"$ref. Please open a JIRA ticket to report it.")
              } else {
                // When self-join happens, the analyzer asks the right side plan to generate
                // attributes with new exprIds. If a plan of a Dataset outputs an attribute which
                // is referred by a column reference, and this attribute has different exprId than
                // the attribute of column reference, then the column reference is ambiguous, as it
                // refers to a column that gets regenerated by self-join.
                val actualAttr = p.output(ref.colPos).asInstanceOf[AttributeReference]
                // We should only count ambiguous column references if the attribute is available as
                // the input attributes of the root node. For example:
                //   Join(b#1 = 3)
                //     TableScan(t, [a#0, b#1])
                //     Project(a#2)
                //       TableScan(t, [a#2, b#3])
                // This query is a self-join. The column 'b' in the join condition is not ambiguous,
                // as it can't come from the right side, which only has column 'a'.
                if (actualAttr.exprId != ref.exprId && inputAttrs.contains(actualAttr)) {
                  ambiguousColRefs += ref
                }
              }
            }
          }

        case _ =>
      }

      val ambiguousAttrs: Seq[AttributeReference] = plan match {
        case Join(
            LogicalPlanWithDatasetId(_, leftId),
            LogicalPlanWithDatasetId(_, rightId),
            _, condition, _) =>
          // If we are dealing with root join node, we need to take care of SPARK-6231:
          //  1. We can de-ambiguous `df("col") === df("col")` in the join condition.
          //  2. There is no ambiguity in direct self join like
          //     `df.join(df, df("col") === 1)`, because it doesn't matter which side the
          //     column comes from.
          def getAmbiguousAttrs(expr: Expression): Seq[AttributeReference] = expr match {
            case Equality(AttrWithCast(a), AttrWithCast(b)) if a.sameRef(b) =>
              Nil
            case Equality(AttrWithCast(a), b) if leftId == rightId && b.foldable =>
              Nil
            case Equality(a, AttrWithCast(b)) if leftId == rightId && a.foldable =>
              Nil
            case a: AttributeReference =>
              if (isColumnReference(a)) {
                val colRef = toColumnReference(a)
                if (ambiguousColRefs.contains(colRef)) Seq(a) else Nil
              } else {
                Nil
              }
            case _ => expr.children.flatMap(getAmbiguousAttrs)
          }
          condition.toSeq.flatMap(getAmbiguousAttrs)

        case _ => ambiguousColRefs.toSeq.map { ref =>
          colRefAttrs.find(attr => toColumnReference(attr) == ref).get
        }
      }

      if (ambiguousAttrs.nonEmpty) {
        throw QueryCompilationErrors.ambiguousAttributesInSelfJoinError(ambiguousAttrs)
      }
    }

    stripColumnReferenceMetadataInPlan(plan)
  }

  private def stripColumnReferenceMetadataInPlan(plan: LogicalPlan): LogicalPlan = {
    plan.transformExpressions {
      case a: AttributeReference if isColumnReference(a) =>
        // Remove the special metadata from this `AttributeReference`, as the detection is done.
        stripColumnReferenceMetadata(a)
    }
  }

  private[sql] def stripColumnReferenceMetadata(a: AttributeReference): AttributeReference = {
    val metadataWithoutId = new MetadataBuilder()
      .withMetadata(a.metadata)
      .remove(Dataset.DATASET_ID_KEY)
      .remove(Dataset.COL_POS_KEY)
      .build()
    a.withMetadata(metadataWithoutId)
  }
}
