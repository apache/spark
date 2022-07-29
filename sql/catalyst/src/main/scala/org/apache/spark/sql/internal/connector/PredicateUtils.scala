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

package org.apache.spark.sql.internal.connector

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.connector.expressions.{LiteralValue, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{And => V2And, Not => V2Not, Or => V2Or, Predicate}
import org.apache.spark.sql.sources.{AlwaysFalse, AlwaysTrue, And, EqualNullSafe, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.StringType

private[sql] object PredicateUtils {

  def toV1(predicate: Predicate): Option[Filter] = {

    def isValidBinaryPredicate(): Boolean = {
      if (predicate.children().length == 2 &&
        predicate.children()(0).isInstanceOf[NamedReference] &&
        predicate.children()(1).isInstanceOf[LiteralValue[_]]) {
        true
      } else {
        false
      }
    }

    predicate.name() match {
      case "IN" if predicate.children()(0).isInstanceOf[NamedReference] =>
        val attribute = predicate.children()(0).toString
        val values = predicate.children().drop(1)
        if (values.length > 0) {
          if (!values.forall(_.isInstanceOf[LiteralValue[_]])) return None
          val dataType = values(0).asInstanceOf[LiteralValue[_]].dataType
          if (!values.forall(_.asInstanceOf[LiteralValue[_]].dataType.sameType(dataType))) {
            return None
          }
          val inValues = values.map(v =>
            CatalystTypeConverters.convertToScala(v.asInstanceOf[LiteralValue[_]].value, dataType))
          Some(In(attribute, inValues))
        } else {
          Some(In(attribute, Array.empty[Any]))
        }

      case "=" | "<=>" | ">" | "<" | ">=" | "<=" if isValidBinaryPredicate =>
        val attribute = predicate.children()(0).toString
        val value = predicate.children()(1).asInstanceOf[LiteralValue[_]]
        predicate.name() match {
          case "=" =>
            Some(EqualTo(attribute,
              CatalystTypeConverters.convertToScala(value.value, value.dataType)))
          case "<=>" =>
            Some(EqualNullSafe(attribute,
              CatalystTypeConverters.convertToScala(value.value, value.dataType)))
          case ">" =>
            Some(GreaterThan(attribute,
              CatalystTypeConverters.convertToScala(value.value, value.dataType)))
          case ">=" =>
            Some(GreaterThanOrEqual(attribute,
              CatalystTypeConverters.convertToScala(value.value, value.dataType)))
          case "<" =>
            Some(LessThan(attribute,
              CatalystTypeConverters.convertToScala(value.value, value.dataType)))
          case "<=" =>
            Some(LessThanOrEqual(attribute,
              CatalystTypeConverters.convertToScala(value.value, value.dataType)))
        }

      case "IS_NULL" | "IS_NOT_NULL" if predicate.children().length == 1 &&
          predicate.children()(0).isInstanceOf[NamedReference] =>
        val attribute = predicate.children()(0).toString
        predicate.name() match {
          case "IS_NULL" => Some(IsNull(attribute))
          case "IS_NOT_NULL" => Some(IsNotNull(attribute))
        }

      case "STARTS_WITH" | "ENDS_WITH" | "CONTAINS" if isValidBinaryPredicate =>
        val attribute = predicate.children()(0).toString
        val value = predicate.children()(1).asInstanceOf[LiteralValue[_]]
        if (!value.dataType.sameType(StringType)) return None
        predicate.name() match {
          case "STARTS_WITH" =>
            Some(StringStartsWith(attribute, value.value.toString))
          case "ENDS_WITH" =>
            Some(StringEndsWith(attribute, value.value.toString))
          case "CONTAINS" =>
            Some(StringContains(attribute, value.value.toString))
        }

      case "ALWAYS_TRUE" | "ALWAYS_FALSE" if predicate.children().isEmpty =>
        predicate.name() match {
          case "ALWAYS_TRUE" => Some(AlwaysTrue())
          case "ALWAYS_FALSE" => Some(AlwaysFalse())
        }

      case "AND" =>
        val and = predicate.asInstanceOf[V2And]
        val left = toV1(and.left())
        val right = toV1(and.right())
        if (left.nonEmpty && right.nonEmpty) {
          Some(And(left.get, right.get))
        } else {
          None
        }

      case "OR" =>
        val or = predicate.asInstanceOf[V2Or]
        val left = toV1(or.left())
        val right = toV1(or.right())
        if (left.nonEmpty && right.nonEmpty) {
          Some(Or(left.get, right.get))
        } else {
          None
        }

      case "NOT" =>
        val not = predicate.asInstanceOf[V2Not]
        Some(Not(toV1(not.child()).get))

      case _ => None
    }
  }
}
