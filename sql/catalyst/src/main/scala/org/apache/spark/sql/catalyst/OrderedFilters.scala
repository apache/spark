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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

/**
 * An instance of the class compiles filters to predicates and sorts them in
 * the order which allows to apply the predicates to an internal row with partially
 * initialized values, for instance converted from parsed CSV fields.
 *
 * @param filters The filters pushed down to a datasource.
 * @param requiredSchema The schema with only fields requested by the upper layer.
 */
class OrderedFilters(filters: Seq[sources.Filter], requiredSchema: StructType)
  extends StructFilters(filters, requiredSchema) {
  /**
   * Converted filters to predicates and grouped by maximum field index
   * in the read schema. For example, if an filter refers to 2 attributes
   * attrA with field index 5 and attrB with field index 10 in the read schema:
   *   0 === $"attrA" or $"attrB" < 100
   * the filter is compiled to a predicate, and placed to the `predicates`
   * array at the position 10. In this way, if there is a row with initialized
   * fields from the 0 to 10 index, the predicate can be applied to the row
   * to check that the row should be skipped or not.
   * Multiple predicates with the same maximum reference index are combined
   * by the `And` expression.
   */
  private val predicates: Array[BasePredicate] = {
    val len = requiredSchema.fields.length
    val groupedPredicates = Array.fill[BasePredicate](len)(null)
    val groupedFilters = Array.fill(len)(Seq.empty[sources.Filter])
    for (filter <- filters) {
      val refs = filter.references
      val index = if (refs.isEmpty) {
        // For example, `AlwaysTrue` and `AlwaysFalse` doesn't have any references
        // Filters w/o refs always return the same result. Taking into account
        // that predicates are combined via `And`, we can apply such filters only
        // once at the position 0.
        0
      } else {
        // readSchema must contain attributes of all filters.
        // Accordingly, `fieldIndex()` returns a valid index always.
        refs.map(requiredSchema.fieldIndex).max
      }
      groupedFilters(index) :+= filter
    }
    if (len > 0 && groupedFilters(0).nonEmpty) {
      // We assume that filters w/o refs like `AlwaysTrue` and `AlwaysFalse`
      // can be evaluated faster that others. We put them in front of others.
      val (literals, others) = groupedFilters(0).partition(_.references.isEmpty)
      groupedFilters(0) = literals ++ others
    }
    for (i <- 0 until len) {
      if (groupedFilters(i).nonEmpty) {
        groupedPredicates(i) = toPredicate(groupedFilters(i))
      }
    }
    groupedPredicates
  }

  /**
   * Applies all filters that refer to row fields at the positions from 0 to `index`.
   * @param row The internal row to check.
   * @param index Maximum field index. The function assumes that all fields
   *              from 0 to `index` position are set.
   * @return false` iff row fields at the position from 0 to `index` pass filters
   *         or there are no applicable filters
   *         otherwise `false` if at least one of the filters returns `false`.
   */
  def skipRow(row: InternalRow, index: Int): Boolean = {
    assert(0 <= index && index < requiredSchema.fields.length,
      "Index is out of the valid range: it must point out to a field of the required schema.")
    val predicate = predicates(index)
    predicate != null && !predicate.eval(row)
  }

  // The filters are applied sequentially, and no need to track which filter references
  // point out to already set row values. The `reset()` method is trivial because
  // the filters don't have any states.
  def reset(): Unit = {}
}
