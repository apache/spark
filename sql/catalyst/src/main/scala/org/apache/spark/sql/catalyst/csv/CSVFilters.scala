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
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

class CSVFilters(filters: Seq[sources.Filter], schema: StructType) {

  private def toRef(attr: String): Option[BoundReference] = {
    schema.getFieldIndex(attr).map { index =>
      val field = schema(index)
      BoundReference(schema.fieldIndex(attr), field.dataType, field.nullable)
    }
  }

  private def toLiteral(value: Any): Option[Literal] = {
    Try(Literal(value)).toOption
  }

  private def zip[A, B](a: Option[A], b: Option[B]): Option[(A, B)] = a.zip(b).headOption

  private def zipAttributeAndValue(name: String, value: Any): Option[(BoundReference, Literal)] = {
    zip(toRef(name), toLiteral(value))
  }

  private def filterToExpression(filter: sources.Filter): Option[Expression] = {
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

  private val predicates: Array[Seq[BasePredicate]] = {
    val parr = Array.fill(schema.fields.length)(Seq.empty[BasePredicate])
    for (filter <- filters) {
      val index = filter.references.map(schema.fieldIndex).max
      for (expr <- filterToExpression(filter)) {
        val predicate = Predicate.create(expr)
        parr(index) :+= predicate
      }
    }
    parr
  }

  def skipRow(row: InternalRow, index: Int): Boolean = {
    predicates(index).exists(!_.eval(row))
  }
}
