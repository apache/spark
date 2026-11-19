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
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, LiteralValue, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{And => V2And, Not => V2Not, Or => V2Or, Predicate}
import org.apache.spark.sql.sources.{AlwaysFalse, AlwaysTrue, And, EqualNullSafe, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

private[sql] object PredicateUtils {

  // The largest IN that a comparison of a CHAR column is converted into. It covers any single
  // value of a CHAR(255) column, the longest CHAR in Hive and MySQL, so that a wide CHAR column
  // cannot turn a comparison into a huge filter.
  private val MAX_CHAR_PADDING_IN_VALUES = 256

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
          if (!values.forall(e =>
            DataTypeUtils.sameType(e.asInstanceOf[LiteralValue[_]].dataType, dataType))) {
            return None
          }
          val inValues = values.map(v =>
            CatalystTypeConverters.convertToScala(v.asInstanceOf[LiteralValue[_]].value, dataType))
          Some(In(attribute, inValues))
        } else {
          Some(In(attribute, Array.empty[Any]))
        }

      case "=" | "<=>" | ">" | "<" | ">=" | "<=" if isValidBinaryPredicate() =>
        val attribute = predicate.children()(0).toString
        val value = predicate.children()(1).asInstanceOf[LiteralValue[_]]
        val v1Value = CatalystTypeConverters.convertToScala(value.value, value.dataType)
        val v1Filter = predicate.name() match {
          case "=" => EqualTo(attribute, v1Value)
          case "<=>" => EqualNullSafe(attribute, v1Value)
          case ">" => GreaterThan(attribute, v1Value)
          case ">=" => GreaterThanOrEqual(attribute, v1Value)
          case "<" => LessThan(attribute, v1Value)
          case "<=" => LessThanOrEqual(attribute, v1Value)
        }
        Some(v1Filter)

      case "=" | "<=>" | "<>" | "IN" => charReadSidePaddingToV1(predicate)

      case "IS_NULL" | "IS_NOT_NULL" if predicate.children().length == 1 &&
          predicate.children()(0).isInstanceOf[NamedReference] =>
        val attribute = predicate.children()(0).toString
        val v1Filter = predicate.name() match {
          case "IS_NULL" => IsNull(attribute)
          case "IS_NOT_NULL" => IsNotNull(attribute)
        }
        Some(v1Filter)

      case "STARTS_WITH" | "ENDS_WITH" | "CONTAINS" if isValidBinaryPredicate() =>
        val attribute = predicate.children()(0).toString
        val value = predicate.children()(1).asInstanceOf[LiteralValue[_]]
        if (!DataTypeUtils.sameType(value.dataType, StringType)) return None
        val v1Value = value.value.toString
        val v1Filter = predicate.name() match {
          case "STARTS_WITH" =>
            StringStartsWith(attribute, v1Value)
          case "ENDS_WITH" =>
            StringEndsWith(attribute, v1Value)
          case "CONTAINS" =>
            StringContains(attribute, v1Value)
        }
        Some(v1Filter)

      case "ALWAYS_TRUE" | "ALWAYS_FALSE" if predicate.children().isEmpty =>
        val v1Filter = predicate.name() match {
          case "ALWAYS_TRUE" => AlwaysTrue()
          case "ALWAYS_FALSE" => AlwaysFalse()
        }
        Some(v1Filter)

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
        // Converting only one side would select fewer rows than the OR.
        if (left.nonEmpty && right.nonEmpty) {
          Some(Or(left.get, right.get))
        } else {
          None
        }

      case "NOT" =>
        val child = toV1(predicate.asInstanceOf[V2Not].child())
        if (child.nonEmpty) {
          Some(Not(child.get))
        } else {
          None
        }

      case _ => None
    }
  }

  def toV1(predicates: Array[Predicate]): Array[Filter] = {
    predicates.flatMap(toV1(_))
  }

  // CHAR_READ_SIDE_PADDING(col, n, ' '), see V2ExpressionBuilder.
  private object CharReadSidePadding {
    def unapply(e: Expression): Option[(NamedReference, Int)] = e match {
      case f: GeneralScalarExpression if f.name() == CharVarcharUtils.V2_CHAR_READ_SIDE_PADDING =>
        f.children().toSeq match {
          case Seq(col: NamedReference, LiteralValue(n: Int, _), _) => Some((col, n))
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Converts a comparison of CHAR_READ_SIDE_PADDING(col, n, ' ') with string literals of n
   * characters, which is how Spark sends a comparison of a CHAR(n) column, into an IN on the
   * stored values of the column. The read-side padding of a value equals such a literal iff the
   * value is the literal without its trailing spaces, followed by none up to all of those spaces.
   * A value longer than n is not padded, so it does not match either way. A literal without
   * trailing spaces matches only itself, so it becomes an equality, as for any other column.
   */
  private def charReadSidePaddingToV1(predicate: Predicate): Option[Filter] = {
    predicate.children().toSeq match {
      case CharReadSidePadding(col, n) +: literals =>
        // UTF8_BINARY only: other collations do not compare the stored values byte by byte.
        val prefixes = literals.collect {
          case LiteralValue(s: UTF8String, t: StringType)
              if t.isUTF8BinaryCollation && s.numChars() == n => s.trimRight()
        }
        if (prefixes.length < literals.length ||
            prefixes.map(n - _.numChars() + 1L).sum > MAX_CHAR_PADDING_IN_VALUES) {
          return None
        }
        val attr = col.toString
        val values = prefixes.flatMap { p =>
          (0 to n - p.numChars()).map(p.toString + " " * _)
        }.toArray[Any]
        val matches = if (values.length == 1) EqualTo(attr, values.head) else In(attr, values)
        predicate.name() match {
          case "<=>" if values.length == 1 => Some(EqualNullSafe(attr, values.head))
          // NULL <=> literal is false, not NULL, which differs under a NOT
          case "<=>" => Some(And(IsNotNull(attr), matches))
          case "<>" => Some(Not(matches))
          case _ => Some(matches)
        }
      case _ => None
    }
  }
}
