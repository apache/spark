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

package org.apache.spark.sql.catalyst.json

import org.apache.spark.sql.catalyst.{InternalRow, StructFilters}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

/**
 * The class provides API for applying pushed down source filters to rows with
 * struct schema parsed from JSON records. It assumes that:
 *   1. reset() is called before any skipRow() calls for new row.
 *   2. skipRow() can be called for any valid index of the struct fields,
 *      and in any order.
 *
 * @param pushedFilters The pushed down source filters. The filters should refer to
 *                      the fields of the provided schema.
 * @param schema The required schema of records from datasource files.
 */
class JsonFilters(pushedFilters: Seq[sources.Filter], schema: StructType)
  extends StructFilters(pushedFilters, schema) {

  /**
   * Stateful JSON predicate that keeps track of its dependent references in the
   * current row via `refCount`.
   *
   * @param predicate The predicate compiled from pushed down source filters.
   * @param totalRefs The total amount of all filters references which the predicate
   *                  compiled from.
   * @param refCount The current number of predicate references in the row that have
   *                 been not set yet. When `refCount` reaches zero, the predicate
   *                 has all dependencies are set, and can be applied to the row.
   */
  case class JsonPredicate(predicate: BasePredicate, totalRefs: Int, var refCount: Int) {
    def reset(): Unit = {
      refCount = totalRefs
    }
  }

  // Predicates compiled from the pushed down filters, and indexed by their references
  // in the given schema. A predicate can occur in the array more than once if it
  // has many references. For example:
  //  schema: i INTEGER, s STRING
  //  filters: IsNotNull("i"), AlwaysTrue, And(EqualTo("i", 0), StringStartsWith("s", "abc"))
  //  predicates:
  //    0: Array(IsNotNull("i"), AlwaysTrue, And(EqualTo("i", 0), StringStartsWith("s", "abc")))
  //    1: Array(AlwaysTrue, And(EqualTo("i", 0), StringStartsWith("s", "abc")))
  private val predicates: Array[Array[JsonPredicate]] = {
    val groupedPredicates = Array.fill(schema.length)(Array.empty[JsonPredicate])
    if (SQLConf.get.jsonFilterPushDown) {
      val groupedByRefSet = filters
        // Group filters that have the same set of references. For example:
        //   IsNotNull("i") -> Set("i"), AlwaysTrue -> Set(),
        //   And(EqualTo("i", 0), StringStartsWith("s", "abc")) -> Set("i", "s")
        // By grouping filters we could avoid tracking their state of references in the
        // current row separately.
        .groupBy(_.references.toSet)
        // Combine all filters from the same group by `And` because all filters should
        // return `true` to do not skip a row. The result is compiled to a predicate.
        .map { case (refSet, refsFilters) =>
          (refSet, JsonPredicate(toPredicate(refsFilters), refSet.size, 0))
        }
      // Apply predicates w/o references like AlwaysTrue and AlwaysFalse to all fields.
      // We cannot set such predicates to a particular position because skipRow() can
      // be invoked for any index due to unpredictable order of JSON fields in JSON records.
      val withLiterals = groupedByRefSet.map { case (refSet, pred) =>
        if (refSet.isEmpty) {
          (schema.fields.map(_.name).toSet, pred.copy(totalRefs = 1))
        } else {
          (refSet, pred)
        }
      }
      // Build a map where key is only one field and value is seq of predicates refer to the field
      // "i" -> Seq(AlwaysTrue, IsNotNull("i"), And(EqualTo("i", 0), StringStartsWith("s", "abc")))
      // "s" -> Seq(AlwaysTrue, And(EqualTo("i", 0), StringStartsWith("s", "abc")))
      val groupedByFields = withLiterals.toSeq
        .flatMap { case (refSet, pred) => refSet.map((_, pred)) }
        .groupBy(_._1)
      // Build the final array by converting keys of `groupedByFields` to their
      // indexes in the provided schema.
      groupedByFields.foreach { case (fieldName, fieldPredicates) =>
        val fieldIndex = schema.fieldIndex(fieldName)
        groupedPredicates(fieldIndex) = fieldPredicates.map(_._2).toArray
      }
    }
    groupedPredicates
  }

  /**
   * Applies predicates (compiled filters) associated with the row field value
   * at the position `index` only if other predicates dependencies are already
   * set in the given row.
   *
   * @param row The row with fully or partially set values.
   * @param index The index of already set value.
   * @return true if at least one of applicable predicates (all dependent row values are set)
   *         return `false`. It returns `false` if all predicates return `true`.
   */
  def skipRow(row: InternalRow, index: Int): Boolean = {
    var skip = false
    predicates(index).foreach { pred =>
      pred.refCount -= 1
      if (!skip && pred.refCount == 0) {
        skip = !pred.predicate.eval(row)
      }
    }
    skip
  }

  /**
   * Reset states of all predicates by re-initializing reference counters.
   */
  override def reset(): Unit = predicates.foreach(_.foreach(_.reset))
}
