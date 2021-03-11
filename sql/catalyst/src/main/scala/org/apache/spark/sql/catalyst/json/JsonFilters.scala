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
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

/**
 * The class provides API for applying pushed down source filters to rows with
 * a struct schema parsed from JSON records. The class should be used in this way:
 * 1. Before processing of the next row, `JacksonParser` (parser for short) resets the internal
 *    state of `JsonFilters` by calling the `reset()` method.
 * 2. The parser reads JSON fields one-by-one in streaming fashion. It converts an incoming
 *    field value to the desired type from the schema. After that, it sets the value to an instance
 *    of `InternalRow` at the position according to the schema. Order of parsed JSON fields can
 *    be different from the order in the schema.
 * 3. Per every JSON field of the top-level JSON object, the parser calls `skipRow` by passing
 *    an `InternalRow` in which some of fields can be already set, and the position of the JSON
 *    field according to the schema.
 *    3.1 `skipRow` finds a group of predicates that refers to this JSON field.
 *    3.2 Per each predicate from the group, `skipRow` decrements its reference counter.
 *    3.2.1 If predicate reference counter becomes 0, it means that all predicate attributes have
 *          been already set in the internal row, and the predicate can be applied to it. `skipRow`
 *          invokes the predicate for the row.
 *    3.3 `skipRow` applies predicates until one of them returns `false`. In that case, the method
 *        returns `true` to the parser.
 *    3.4 If all predicates with zero reference counter return `true`, the final result of
 *        the method is `false` which tells the parser to not skip the row.
 * 4. If the parser gets `true` from `JsonFilters.skipRow`, it must not call the method anymore
 *    for this internal row, and should go the step 1.
 *
 * Besides of `StructFilters` assumptions, `JsonFilters` assumes that:
 *   - `skipRow()` can be called for any valid index of the struct fields,
 *      and in any order.
 *   - After `skipRow()` returns `true`, the internal state of `JsonFilters` can be inconsistent,
 *     so, `skipRow()` must not be called for the current row anymore without `reset()`.
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
   */
  case class JsonPredicate(predicate: BasePredicate, totalRefs: Int) {
    // The current number of predicate references in the row that have been not set yet.
    // When `refCount` reaches zero, the predicate has all dependencies are set, and can
    // be applied to the row.
    var refCount: Int = totalRefs

    def reset(): Unit = {
      refCount = totalRefs
    }
  }

  // Predicates compiled from the pushed down filters. The predicates are grouped by their
  // attributes. The i-th group contains predicates that refer to the i-th field of the given
  // schema. A predicates can be placed to many groups if it has many attributes. For example:
  //   schema: i INTEGER, s STRING
  //   filters: IsNotNull("i"), AlwaysTrue, Or(EqualTo("i", 0), StringStartsWith("s", "abc"))
  //   predicates:
  //     0: Array(IsNotNull("i"), AlwaysTrue, Or(EqualTo("i", 0), StringStartsWith("s", "abc")))
  //     1: Array(AlwaysTrue, Or(EqualTo("i", 0), StringStartsWith("s", "abc")))
  private val predicates: Array[Array[JsonPredicate]] = {
    val groupedPredicates = Array.fill(schema.length)(Array.empty[JsonPredicate])
    val groupedByRefSet: Map[Set[String], JsonPredicate] = filters
      // Group filters that have the same set of references. For example:
      //   IsNotNull("i") -> Set("i"), AlwaysTrue -> Set(),
      //   Or(EqualTo("i", 0), StringStartsWith("s", "abc")) -> Set("i", "s")
      // By grouping filters we could avoid tracking their state of references in the
      // current row separately.
      .groupBy(_.references.toSet)
      // Combine all filters from the same group by `And` because all filters should
      // return `true` to do not skip a row. The result is compiled to a predicate.
      .map { case (refSet, refsFilters) =>
        (refSet, JsonPredicate(toPredicate(refsFilters), refSet.size))
      }
    // Apply predicates w/o references like `AlwaysTrue` and `AlwaysFalse` to all fields.
    // We cannot set such predicates to a particular position because skipRow() can
    // be invoked for any index due to unpredictable order of JSON fields in JSON records.
    val withLiterals: Map[Set[String], JsonPredicate] = groupedByRefSet.map {
      case (refSet, pred) if refSet.isEmpty =>
        (schema.fields.map(_.name).toSet, pred.copy(totalRefs = 1))
      case others => others
    }
    // Build a map where key is only one field and value is seq of predicates refer to the field
    //   "i" -> Seq(AlwaysTrue, IsNotNull("i"), Or(EqualTo("i", 0), StringStartsWith("s", "abc")))
    //   "s" -> Seq(AlwaysTrue, Or(EqualTo("i", 0), StringStartsWith("s", "abc")))
    val groupedByFields: Map[String, Seq[(String, JsonPredicate)]] = withLiterals.toSeq
      .flatMap { case (refSet, pred) => refSet.map((_, pred)) }
      .groupBy(_._1)
    // Build the final array by converting keys of `groupedByFields` to their
    // indexes in the provided schema.
    groupedByFields.foreach { case (fieldName, fieldPredicates) =>
      val fieldIndex = schema.fieldIndex(fieldName)
      groupedPredicates(fieldIndex) = fieldPredicates.map(_._2).toArray
    }
    groupedPredicates
  }

  /**
   * Applies predicates (compiled filters) associated with the row field value
   * at the position `index` only if other predicates dependencies are already
   * set in the given row.
   *
   * Note: If the function returns `true`, `refCount` of some predicates can be not decremented.
   *
   * @param row The row with fully or partially set values.
   * @param index The index of already set value.
   * @return `true` if at least one of applicable predicates (all dependent row values are set)
   *         return `false`. It returns `false` if all predicates return `true`.
   */
  def skipRow(row: InternalRow, index: Int): Boolean = {
    assert(0 <= index && index < schema.fields.length,
      s"The index $index is out of the valid range [0, ${schema.fields.length}). " +
      s"It must point out to a field of the schema: ${schema.catalogString}.")
    var skip = false
    for (pred <- predicates(index) if !skip) {
      pred.refCount -= 1
      assert(pred.refCount >= 0,
        s"Predicate reference counter cannot be negative but got ${pred.refCount}.")
      skip = pred.refCount == 0 && !pred.predicate.eval(row)
    }
    skip
  }

  /**
   * Reset states of all predicates by re-initializing reference counters.
   */
  override def reset(): Unit = predicates.foreach(_.foreach(_.reset))
}
