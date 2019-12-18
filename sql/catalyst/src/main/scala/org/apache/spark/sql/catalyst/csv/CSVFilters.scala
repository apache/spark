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
import org.apache.spark.sql.types.StructType

class CSVFilters(
    filters: Seq[sources.Filter],
    dataSchema: StructType,
    requiredSchema: StructType) {
  require(checkFilters(), "All filters must be applicable to dataSchema")

  /** The schema to read from the underlying CSV parser */
  val readSchema: StructType = {
    val refs = filters.flatMap(_.references).toSet
    val readFields = dataSchema.filter { field =>
      requiredSchema.contains(field) || refs.contains(field.name)
    }
    StructType(readFields)
  }

  private val predicates: Array[BasePredicate] = {
    val len = readSchema.fields.length
    val groupedPredicates = Array.fill[BasePredicate](len)(null)
    if (SQLConf.get.csvFilterPushDown) {
      val groupedExprs = Array.fill(len)(Seq.empty[Expression])
      for (filter <- filters) {
        val index = filter.references.map(readSchema.fieldIndex).max
        groupedExprs(index) ++= CSVFilters.filterToExpression(filter, toRef)
      }
      for (i <- 0 until len) {
        if (!groupedExprs(i).isEmpty) {
          val reducedExpr = groupedExprs(i).reduce(And)
          groupedPredicates(i) = Predicate.create(reducedExpr)
        }
      }
    }
    groupedPredicates
  }

  /**
   * Apply all filters that refer to row fields at the positions from 0 to index.
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
    readSchema.getFieldIndex(attr).map { index =>
      val field = readSchema(index)
      BoundReference(readSchema.fieldIndex(attr), field.dataType, field.nullable)
    }
  }

  // Checks that all filters refer to an field in the data schema
  private def checkFilters(): Boolean = {
    val refs = filters.flatMap(_.references).toSet
    val fieldNames = dataSchema.fields.map(_.name).toSet
    refs.forall(fieldNames.contains(_))
  }
}

object CSVFilters {

  def unsupportedFilters(filters: Array[sources.Filter]): Array[sources.Filter] = {
    filters.filter {
      case sources.AlwaysFalse | sources.AlwaysTrue => true
      case _ => !SQLConf.get.csvFilterPushDown
    }
  }

  private def zip[A, B](a: Option[A], b: Option[B]): Option[(A, B)] = {
    a.zip(b).headOption
  }

  private def toLiteral(value: Any): Option[Literal] = {
    Try(Literal(value)).toOption
  }

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
      case _ => None
    }
    translate(filter)
  }
}
