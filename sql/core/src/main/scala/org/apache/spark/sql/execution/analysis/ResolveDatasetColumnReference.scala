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
import scala.util.Try

import org.apache.spark.sql.{AnalysisException, Column, Dataset}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Equality, EqualNullSafe, EqualTo}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolves the Dataset column reference by traversing the query plan and finding the plan subtree
 * of the Dataset that the column reference belongs to.
 *
 * Dataset column reference is simply an [[AttributeReference]] that is returned by `Dataset#col`.
 * Most of time we don't need to do anything special, as [[AttributeReference]] can point to
 * the column precisely. However, in case of self-join, the analyzer generates
 * [[AttributeReference]] with new expr IDs for the right side plan of the join. If the Dataset
 * column reference points to a column in the right side plan of a self-join, we need to replace it
 * with the corresponding newly generated [[AttributeReference]].
 */
class ResolveDatasetColumnReference(conf: SQLConf) extends Rule[LogicalPlan] {

  // Dataset column reference is an `AttributeReference` with 2 special metadata.
  private def isColumnReference(a: AttributeReference): Boolean = {
    a.metadata.contains(Dataset.ID_PREFIX) && a.metadata.contains(Dataset.COL_POS_PREFIX)
  }

  private case class ColumnReference(datasetId: Long, colPos: Int)

  private def toColumnReference(a: AttributeReference): ColumnReference = {
    ColumnReference(
      a.metadata.getLong(Dataset.ID_PREFIX),
      a.metadata.getLong(Dataset.COL_POS_PREFIX).toInt)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.RESOLVE_DATASET_COLUMN_REFERENCE)) return plan

    // We always remove the special metadata from `AttributeReference` at the end of this rule, so
    // Dataset column reference only exists in the root node via Dataset transformations like
    // `Dataset#select`.
    val colRefAttrs = plan.expressions.flatMap(_.collect {
      case a: AttributeReference if isColumnReference(a) => a
    })

    if (colRefAttrs.isEmpty) {
      plan
    } else {
      val colRefs = colRefAttrs.map(toColumnReference).distinct
      // Keeps the mapping between the column reference and the actual attribute it points to. This
      // will be used to replace the column references with actual attributes later.
      val colRefToActualAttr = new mutable.HashMap[ColumnReference, AttributeReference]()
      // Keeps the column references that points to more than one actual attributes.
      val ambiguousColRefs = new mutable.HashMap[ColumnReference, Seq[AttributeReference]]()
      val dsIdSet = colRefs.map(_.datasetId).toSet

      plan.foreach {
        // We only add the special `SubqueryAlias` to attach the dataset id for self-join. After
        // self-join resolving, the child of `SubqueryAlias` should have generated new
        // `AttributeReference`, and we need to resolve column reference with them.
        case SubqueryAlias(DatasetIdAlias(id), child) if dsIdSet.contains(id) =>
          colRefs.foreach { case ref =>
            if (id == ref.datasetId) {
              if (ref.colPos < 0 || ref.colPos >= child.output.length) {
                throw new IllegalStateException("[BUG] Hit an invalid Dataset column reference: " +
                  s"$ref. Please open a JIRA ticket to report it.")
              } else {
                val actualAttr = child.output(ref.colPos).asInstanceOf[AttributeReference]
                // Record the ambiguous column references. We will deal with them later.
                if (ambiguousColRefs.contains(ref)) {
                  assert(!colRefToActualAttr.contains(ref))
                  ambiguousColRefs(ref) = ambiguousColRefs(ref) :+ actualAttr
                } else if (colRefToActualAttr.contains(ref)) {
                  ambiguousColRefs(ref) = Seq(colRefToActualAttr.remove(ref).get, actualAttr)
                } else {
                  colRefToActualAttr(ref) = actualAttr
                }
              }
            }
          }

        case _ =>
      }

      val inputSet = plan.inputSet
      val deAmbiguousColsRefs = new mutable.HashSet[ColumnReference]()
      val newPlan = plan.transformExpressions {
        case e @ Equality(a: AttributeReference, b: AttributeReference)
            if isColumnReference(a) && isColumnReference(b) && a.sameRef(b) =>
          val colRefA = toColumnReference(a)
          val colRefB = toColumnReference(a)
          val maybeActualAttrs = ambiguousColRefs.get(colRefA)
          val areValidAttrs = maybeActualAttrs.exists { attrs =>
            attrs.length == 2 && attrs.forall(inputSet.contains)
          }
          if (colRefA == colRefB && areValidAttrs) {
            deAmbiguousColsRefs += colRefA
            if (e.isInstanceOf[EqualTo]) {
              EqualTo(maybeActualAttrs.get.head, maybeActualAttrs.get.last)
            } else {
              EqualNullSafe(maybeActualAttrs.get.head, maybeActualAttrs.get.last)
            }
          } else {
            e
          }

        case a: AttributeReference if isColumnReference(a) =>
          val actualAttr = colRefToActualAttr.get(toColumnReference(a)).filter { attr =>
            // Make sure the attribute is valid.
            inputSet.contains(attr)
          }.getOrElse(a)

          // Remove the special metadata from this `AttributeReference`, as the column reference
          // resolving is done.
          Column.stripColumnReferenceMetadata(actualAttr)
      }

      ambiguousColRefs.filterKeys(!deAmbiguousColsRefs.contains(_)).foreach { case (ref, _) =>
        val originalAttr = colRefAttrs.find(attr => toColumnReference(attr) == ref).get
        throw new AnalysisException(s"Column $originalAttr is ambiguous. It's probably " +
          "because you joined several Datasets together, and some of these Datasets are the " +
          "same. This column points to one of the Datasets but Spark is unable to figure out " +
          "which Datasset. Please alias the Datasets with different names via `Dataset.as` " +
          "before joining them, and specify the column using qualified name, e.g. " +
          """`df.as("a").join(df.as("b"), $"a.id" > $"b.id")`.""")
      }

      newPlan
    }
  }

  object DatasetIdAlias {
    def unapply(alias: AliasIdentifier): Option[Long] = {
      val expectedPrefix = SubqueryAlias.HIDDEN_ALIAS_PREFIX + Dataset.ID_PREFIX
      if (alias.database.isEmpty && alias.identifier.startsWith(expectedPrefix)) {
        Try(alias.identifier.drop(expectedPrefix.length + 1).toLong).toOption
      } else {
        None
      }
    }
  }
}
