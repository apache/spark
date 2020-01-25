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

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}

class JsonFilters(filters: Seq[sources.Filter], schema: DataType) {
  case class JsonPredicate(predicate: BasePredicate, totalRefs: Int, var refCount: Int) {
    def reset(): Unit = {
      refCount = totalRefs
    }
  }

  def buildPredicates(requiredSchema: StructType): Array[Array[JsonPredicate]] = {
    val groupedFilters = filters.map(filter => (filter.references.toSet, filter))
      .groupBy { case (refs, _) => refs }
      .mapValues(_.map { case (_, filter) => filter })
    val filterLiterals = filters.filter(_.references.isEmpty)
    val groupedPredicates = groupedFilters.map { case (refs, filters) =>
      val reducedExpr = (filterLiterals ++ filters)
        .flatMap(JsonFilters.filterToExpression(_, toRef))
        .reduce(And)
      val predicate = Predicate.create(reducedExpr)
      (refs, JsonPredicate(predicate, refs.size, 0))
    }
    val groupedByFields = groupedPredicates.toSeq.flatMap { case (refs, predicate) =>
      refs.map(ref => (ref, predicate))
    }.groupBy { case (ref, _) => ref }
    .map { case (ref, values) => (ref, values.map { case (_, predicate) => predicate})}

    val jsonPredicates = Array.fill[Array[JsonPredicate]](requiredSchema.length)(null)
    groupedByFields.foreach { case (fieldName, predicates) =>
      val fieldIndex = requiredSchema.fieldIndex(fieldName)
      jsonPredicates(fieldIndex) = predicates.toArray
    }
    jsonPredicates
  }

  private val indexedPredicates: Array[Array[JsonPredicate]] = schema match {
    case st: StructType => buildPredicates(st)
    case _ => null
  }

  def skipRow(row: InternalRow, index: Int): Boolean = {
    var skip = false
    assert(indexedPredicates != null, "skipRow() can be called only for structs")
    val predicates = indexedPredicates(index)
    if (predicates != null) {
      val len = predicates.length
      var i = 0

      while (i < len) {
        val pred = predicates(i)
        pred.refCount -= 1
        if (!skip && pred.refCount == 0) {
          skip = !pred.predicate.eval(row)
        }
        i += 1
      }
    }

    skip
  }

  private val allPredicates: Array[JsonPredicate] = {
    val predicates = new ArrayBuffer[JsonPredicate]()
    if (indexedPredicates != null) {
      for {
        groupedPredicates <- indexedPredicates
        predicate <- groupedPredicates if groupedPredicates != null
      } {
        predicates += predicate
      }
    }
    predicates.toArray
  }

  def reset(): Unit = {
    val len = allPredicates.length
    var i = 0
    while (i < len) {
      val pred = allPredicates(i)
      pred.refCount = pred.totalRefs
      i += 1
    }
  }

  // Finds a filter attribute in the read schema and converts it to a `BoundReference`
  private def toRef(attr: String): Option[BoundReference] = {
    None
  }
}

object JsonFilters {
  private def checkFilterRefs(filter: sources.Filter, schema: StructType): Boolean = {
    val fieldNames = schema.fields.map(_.name).toSet
    filter.references.forall(fieldNames.contains(_))
  }

  /**
   * Returns the filters currently supported by JSON datasource.
   * @param filters The filters pushed down to JSON datasource.
   * @param schema data schema of JSON files.
   * @return a sub-set of `filters` that can be handled by JSON datasource.
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
