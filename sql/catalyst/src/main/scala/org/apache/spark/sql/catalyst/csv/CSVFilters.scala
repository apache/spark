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

package org.apache.spark.sql.catalyst.csv

import scala.util.Try

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{BooleanType, StructType}

/**
 * An instance of the class compiles filters to predicates and allows to
 * apply the predicates to an internal row with partially initialized values
 * converted from parsed CSV fields.
 *
 * @param filters The filters pushed down to CSV datasource.
 * @param requiredSchema The schema with only fields requested by the upper layer.
 */
class CSVFilters(filters: Seq[sources.Filter], requiredSchema: StructType) {
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
    if (SQLConf.get.csvFilterPushDown) {
      val groupedFilters = Array.fill(len)(Seq.empty[sources.Filter])
      for (filter <- filters) {
        val refs = filter.references
        val index = if (refs.isEmpty) {
          // For example, AlwaysTrue and AlwaysFalse doesn't have any references
          // Filters w/o refs always return the same result. Taking into account
          // that predicates are combined via And, we can apply such filters only
          // once at the position 0.
          0
        } else {
          // readSchema must contain attributes of all filters.
          // Accordingly, fieldIndex() returns a valid index always.
          refs.map(requiredSchema.fieldIndex).max
        }
        groupedFilters(index) :+= filter
      }
      if (len > 0 && !groupedFilters(0).isEmpty) {
        // We assume that filters w/o refs like AlwaysTrue and AlwaysFalse
        // can be evaluated faster that others. We put them in front of others.
        val (literals, others) = groupedFilters(0).partition(_.references.isEmpty)
        groupedFilters(0) = literals ++ others
      }
      for (i <- 0 until len) {
        if (!groupedFilters(i).isEmpty) {
          val reducedExpr = groupedFilters(i)
            .flatMap(CSVFilters.filterToExpression(_, toRef))
            .reduce(And)
          groupedPredicates(i) = Predicate.create(reducedExpr)
        }
      }
    }
    groupedPredicates
  }

  /**
   * Applies all filters that refer to row fields at the positions from 0 to index.
   * @param row The internal row to check.
   * @param index Maximum field index. The function assumes that all fields
   *              from 0 to index position are set.
   * @return false iff row fields at the position from 0 to index pass filters
   *         or there are no applicable filters
   *         otherwise false if at least one of the filters returns false.
   */
  def skipRow(row: InternalRow, index: Int): Boolean = {
    val predicate = predicates(index)
    predicate != null && !predicate.eval(row)
  }

  // Finds a filter attribute in the read schema and converts it to a `BoundReference`
  private def toRef(attr: String): Option[BoundReference] = {
    requiredSchema.getFieldIndex(attr).map { index =>
      val field = requiredSchema(index)
      BoundReference(requiredSchema.fieldIndex(attr), field.dataType, field.nullable)
    }
  }
}

object CSVFilters {
  private def checkFilterRefs(filter: sources.Filter, schema: StructType): Boolean = {
    val fieldNames = schema.fields.map(_.name).toSet
    filter.references.forall(fieldNames.contains(_))
  }

  /**
   * Returns the filters currently supported by CSV datasource.
   * @param filters The filters pushed down to CSV datasource.
   * @param schema data schema of CSV files.
   * @return a sub-set of `filters` that can be handled by CSV datasource.
   */
  def pushedFilters(filters: Array[sources.Filter], schema: StructType): Array[sources.Filter] = {
    filters.filter(checkFilterRefs(_, schema))
  }

  private def zip[A, B](a: Option[A], b: Option[B]): Option[(A, B)] = {
    a.zip(b).headOption
  }

  private def toLiteral(value: Any): Option[Literal] = {
    Try(Literal(value)).toOption
  }

  /**
   * Converts a filter to an expression and binds it to row positions.
   *
   * @param filter The filter to convert.
   * @param toRef The function converts a filter attribute to a bound reference.
   * @return some expression with resolved attributes or None if the conversion
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
        val literals = values.toSeq.flatMap(toLiteral)
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
    }
    translate(filter)
  }
}
