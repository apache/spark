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

import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{Project, Union}
import org.apache.spark.sql.types.MetadataBuilder

/**
 * Deduplicates columns with same [[ExprId]]s in single [[Union]] child output, by placing aliases
 * on non-first duplicates.
 */
object DeduplicateUnionChildOutput {

  /**
   * Deduplicate expression IDs at the scope of each individual child output. This is necessary to
   * handle the following case:
   *
   * {{{
   * -- The correct answer is (1, 1), (1, 2). Without deduplication it would be (1, 1), because
   * -- aggregation would be done only based on the first column.
   * SELECT
   *   a, a
   * FROM
   *   VALUES (1, 1), (1, 2) AS t1 (a, b)
   * UNION
   * SELECT
   *  a, b
   * FROM
   *   VALUES (1, 1), (1, 2) AS t2 (a, b)
   * }}}
   *
   * Putting [[Alias]] introduces a new expression ID for the attribute duplicates in the output. We
   * also add `__is_duplicate` metadata so that [[AttributeSeq.getCandidatesForResolution]] doesn't
   * produce conflicting candidates when resolving names in the upper [[Project]] - this is
   * technically still the same attribute.
   *
   * See SPARK-37865 for more details.
   */
  def deduplicateOutputPerChild(union: Union): Union = {
    val newChildren = union.children.map { c =>
      if (c.output.map(_.exprId).distinct.length < c.output.length) {
        val existingExprIds = mutable.HashSet[ExprId]()
        val projectList = c.output.map { attr =>
          if (existingExprIds.contains(attr.exprId)) {
            // replace non-first duplicates with aliases and tag them
            val newMetadata = new MetadataBuilder()
              .withMetadata(attr.metadata)
              .putNull("__is_duplicate")
              .build()
            Alias(attr, attr.name)(explicitMetadata = Some(newMetadata))
          } else {
            // leave first duplicate alone
            existingExprIds.add(attr.exprId)
            attr
          }
        }
        Project(projectList, c)
      } else {
        c
      }
    }
    union.withNewChildren(newChildren).asInstanceOf[Union]
  }
}
