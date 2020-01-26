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

import scala.util.Try

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{BooleanType, StructType}

class JsonFilters(filters: Seq[sources.Filter], schema: StructType) {
  case class JsonPredicate(predicate: BasePredicate, totalRefs: Int, var refCount: Int) {
    def reset(): Unit = {
      refCount = totalRefs
    }
  }

  private val predicates: Array[Array[JsonPredicate]] = {
    val literals = filters.filter(_.references.isEmpty)
    val groupedByRefSet = filters
      .groupBy(_.references.toSet)
      .map { case (refSet, refsFilters) =>
        val reducedExpr = (literals ++ refsFilters)
          .flatMap(JsonFilters.filterToExpression(_, toRef))
          .reduce(And)
        (refSet, JsonPredicate(Predicate.create(reducedExpr), refSet.size, 0))
      }
    val groupedByFields = groupedByRefSet.toSeq
      .flatMap { case (refSet, pred) => refSet.map((_, pred)) }
      .groupBy(_._1)
    val groupedPredicates = Array.fill(schema.length)(Array.empty[JsonPredicate])
    groupedByFields.foreach { case (fieldName, fieldPredicates) =>
      val fieldIndex = schema.fieldIndex(fieldName)
      groupedPredicates(fieldIndex) = fieldPredicates.map(_._2).toArray
    }
    groupedPredicates
  }

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

  def reset(): Unit = predicates.foreach(_.foreach(_.reset))

  // Finds a filter attribute in the read schema and converts it to a `BoundReference`
  private def toRef(attr: String): Option[BoundReference] = {
    schema.getFieldIndex(attr).map { index =>
      val field = schema(index)
      BoundReference(schema.fieldIndex(attr), field.dataType, field.nullable)
    }
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
