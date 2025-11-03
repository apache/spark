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

import scala.util.Try

import org.apache.spark.sql.catalyst.StructFilters._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{BooleanType, StructType}
import org.apache.spark.util.ArrayImplicits._

/**
 * The class provides API for applying pushed down filters to partially or
 * fully set internal rows that have the struct schema.
 *
 * `StructFilters` assumes that:
 *   - `reset()` is called before any `skipRow()` calls for new row.
 *
 * @param pushedFilters The pushed down source filters. The filters should refer to
 *                      the fields of the provided schema.
 * @param schema The required schema of records from datasource files.
 */
abstract class StructFilters(pushedFilters: Seq[sources.Filter], schema: StructType) {

  protected val filters = StructFilters.pushedFilters(pushedFilters.toArray, schema)

  /**
   * Applies pushed down source filters to the given row assuming that
   * value at `index` has been already set.
   *
   * @param row The row with fully or partially set values.
   * @param index The index of already set value.
   * @return `true` if currently processed row can be skipped otherwise false.
   */
  def skipRow(row: InternalRow, index: Int): Boolean

  /**
   * Resets states of pushed down filters. The method must be called before
   * processing any new row otherwise `skipRow()` may return wrong result.
   */
  def reset(): Unit

  /**
   * Compiles source filters to a predicate.
   */
  def toPredicate(filters: Seq[sources.Filter]): BasePredicate = {
    val reducedExpr = filters
      .sortBy(_.references.length)
      .flatMap(filterToExpression(_, toRef))
      .reduce(And)
    Predicate.create(reducedExpr)
  }

  // Finds a filter attribute in the schema and converts it to a `BoundReference`
  private def toRef(attr: String): Option[BoundReference] = {
    // The names have been normalized and case sensitivity is not a concern here.
    schema.getFieldIndex(attr).map { index =>
      val field = schema(index)
      BoundReference(index, field.dataType, field.nullable)
    }
  }
}

object StructFilters {
  private def checkFilterRefs(filter: sources.Filter, fieldNames: Set[String]): Boolean = {
    // The names have been normalized and case sensitivity is not a concern here.
    filter.references.forall(fieldNames.contains)
  }

  /**
   * Returns the filters currently supported by the datasource.
   * @param filters The filters pushed down to the datasource.
   * @param schema data schema of datasource files.
   * @return a sub-set of `filters` that can be handled by the datasource.
   */
  def pushedFilters(filters: Array[sources.Filter], schema: StructType): Array[sources.Filter] = {
    val fieldNames = schema.fieldNames.toSet
    filters.filter(checkFilterRefs(_, fieldNames))
  }

  private def zip[A, B](a: Option[A], b: Option[B]): Option[(A, B)] = {
    a.zip(b)
  }

  private def toLiteral(value: Any): Option[Literal] = {
    Try(Literal(value)).toOption
  }

  /**
   * Converts a filter to an expression and binds it to row positions.
   *
   * @param filter The filter to convert.
   * @param toRef The function converts a filter attribute to a bound reference.
   * @return some expression with resolved attributes or `None` if the conversion
   *         of the given filter to an expression is impossible.
   */
  def filterToExpression(
      filter: sources.Filter,
      toRef: String => Option[BoundReference]): Option[Expression] = {
    def zipAttributeAndValue(name: String, value: Any): Option[(BoundReference, Literal)] = {
      zip(toRef(name), toLiteral(value))
    }
    def translate(filter: sources.Filter): Option[Expression] = filter match {
      case sources.And(left, right) =>
        zip(translate(left), translate(right)).map(And.tupled)
      case sources.Or(left, right) =>
        zip(translate(left), translate(right)).map(Or.tupled)
      case sources.Not(child) =>
        translate(child).map(Not)
      case sources.EqualTo(attribute, value) =>
        zipAttributeAndValue(attribute, value).map(EqualTo.tupled)
      case sources.EqualNullSafe(attribute, value) =>
        zipAttributeAndValue(attribute, value).map(EqualNullSafe.tupled)
      case sources.IsNull(attribute) =>
        toRef(attribute).map(IsNull)
      case sources.IsNotNull(attribute) =>
        toRef(attribute).map(IsNotNull)
      case sources.In(attribute, values) =>
        val literals = values.toImmutableArraySeq.flatMap(toLiteral)
        if (literals.length == values.length) {
          toRef(attribute).map(In(_, literals))
        } else {
          None
        }
      case sources.GreaterThan(attribute, value) =>
        zipAttributeAndValue(attribute, value).map(GreaterThan.tupled)
      case sources.GreaterThanOrEqual(attribute, value) =>
        zipAttributeAndValue(attribute, value).map(GreaterThanOrEqual.tupled)
      case sources.LessThan(attribute, value) =>
        zipAttributeAndValue(attribute, value).map(LessThan.tupled)
      case sources.LessThanOrEqual(attribute, value) =>
        zipAttributeAndValue(attribute, value).map(LessThanOrEqual.tupled)
      case sources.StringContains(attribute, value) =>
        zipAttributeAndValue(attribute, value).map(Contains.tupled)
      case sources.StringStartsWith(attribute, value) =>
        zipAttributeAndValue(attribute, value).map(StartsWith.tupled)
      case sources.StringEndsWith(attribute, value) =>
        zipAttributeAndValue(attribute, value).map(EndsWith.tupled)
      case sources.AlwaysTrue() =>
        Some(Literal(true, BooleanType))
      case sources.AlwaysFalse() =>
        Some(Literal(false, BooleanType))
      case _: sources.CollatedFilter =>
        None
    }
    translate(filter)
  }
}

class NoopFilters extends StructFilters(Seq.empty, new StructType()) {
  override def skipRow(row: InternalRow, index: Int): Boolean = false
  override def reset(): Unit = {}
}
