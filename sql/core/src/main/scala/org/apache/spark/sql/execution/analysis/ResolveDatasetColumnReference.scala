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

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.MetadataBuilder

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

  private def stripColumnReferenceMetadata(a: AttributeReference): AttributeReference = {
    val metadataWithoutId = new MetadataBuilder()
      .withMetadata(a.metadata)
      .remove(Dataset.ID_PREFIX)
      .remove(Dataset.COL_POS_PREFIX)
      .build()
    a.withMetadata(metadataWithoutId)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.RESOLVE_DATASET_COLUMN_REFERENCE)) return plan

    // We always remove the special metadata from `AttributeReference` at the end of this rule, so
    // Dataset column reference only exists in the root node via Dataset transformations like
    // `Dataset#select`.
    val colRefs = plan.expressions.flatMap(_.collect {
      case a: AttributeReference if isColumnReference(a) => toColumnReference(a)
    })

    if (colRefs.isEmpty) {
      plan
    } else {
      // Keeps the mapping between the column reference and the actual column it points to. This
      // will be used to replace the column references with actual columns in the root node later.
      val colRefToActualCol = new mutable.HashMap[ColumnReference, AttributeReference]()
      // Keeps the column references that points to more than one actual columns. We will not
      // replace these ambiguous column references and leave them as they were.
      val ambiguousColRefs = new mutable.HashSet[ColumnReference]()

      val dsIdSet = colRefs.map(_.datasetId).toSet
      plan.foreach {
        // We only add the special `SubqueryAlias` to attach the dataset id for self-join. After
        // self-join resolving, the child of `SubqueryAlias` should have generated new
        // `AttributeReference`, and we need to resolve column reference with them.
        case SubqueryAlias(DatasetIdAlias(id), child) if dsIdSet.contains(id) =>
          colRefs.filterNot(ambiguousColRefs.contains).foreach { ref =>
            if (id == ref.datasetId) {
              if (ref.colPos < 0 || ref.colPos >= child.output.length) {
                logWarning("[BUG] Hit an invalid Dataset column reference: " + ref)
              } else if (colRefToActualCol.contains(ref)) {
                ambiguousColRefs += ref
                colRefToActualCol.remove(ref)
              } else {
                val actualCol = child.output(ref.colPos).asInstanceOf[AttributeReference]
                colRefToActualCol(ref) = actualCol
              }
            }
          }

        case _ =>
      }

      val inputSet = plan.inputSet
      plan.transformExpressions {
        case a: AttributeReference if isColumnReference(a) =>
          val actualCol = colRefToActualCol.get(toColumnReference(a)).filter { actualCol =>
            // Make sure the actual column is valid.
            inputSet.contains(actualCol)
          }.getOrElse(a)

          // Remove the special metadata from this `AttributeReference`, as the column reference
          // resolving is done.
          stripColumnReferenceMetadata(actualCol)
      }
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
