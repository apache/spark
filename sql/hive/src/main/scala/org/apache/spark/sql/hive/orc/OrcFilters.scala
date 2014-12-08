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

package org.apache.spark.sql.hive.orc

import org.apache.spark.sql.catalyst.expressions._
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder
import org.apache.spark.Logging

private[sql] object OrcFilters extends Logging {

  def createFilter(expr: Seq[Expression]): Option[SearchArgument] = {
    if (expr == null || expr.size == 0) return None
    var sarg: Option[Builder] = Some(SearchArgument.FACTORY.newBuilder())
    sarg.get.startAnd()
    expr.foreach {
      x => {
        sarg match {
          case Some(s1) => sarg = createFilter(x, s1)
          case _ => None
        }
      }
    }
    sarg match {
      case Some(b) => Some(b.end.build)
      case _ => None
    }
  }

  def createFilter(expression: Expression, builder: Builder): Option[Builder] = {
    expression match {
      case p@And(left: Expression, right: Expression) => {
        val b1 = builder.startAnd()
        val b2 = createFilter(left, b1)
        b2 match {
          case Some(b) => val b3 = createFilter(right, b)
            if (b3.isDefined) {
              Some(b3.get.end)
            } else {
              None
            }
          case _ => None
        }
      }
      case p@Or(left: Expression, right: Expression) => {
        val b1 = builder.startOr()
        val b2 = createFilter(left, b1)
        b2 match {
          case Some(b) => val b3 = createFilter(right, b)
            if (b3.isDefined) {
              Some(b3.get.end)
            } else {
              None
            }
          case _ => None
        }
      }
      case p@EqualTo(left: Literal, right: NamedExpression) => {
        val b1 = builder.equals(right.name, left.value)
        Some(b1)
      }
      case p@EqualTo(left: NamedExpression, right: Literal) => {
        val b1 = builder.equals(left.name, right.value)
        Some(b1)
      }
      case p@LessThan(left: NamedExpression, right: Literal) => {
        val b1 = builder.lessThan(left.name, right.value)
        Some(b1)
      }
      case p@LessThan(left: Literal, right: NamedExpression) => {
        val b1 = builder.startNot().lessThanEquals(right.name, left.value).end()
        Some(b1)
      }
      case p@LessThanOrEqual(left: NamedExpression, right: Literal) => {
        val b1 = builder.lessThanEquals(left.name, right.value)
        Some(b1)
      }
      case p@LessThanOrEqual(left: Literal, right: NamedExpression) => {
        val b1 = builder.startNot().lessThan(right.name, left.value).end()
        Some(b1)
      }
      case p@GreaterThan(left: NamedExpression, right: Literal) => {
        val b1 = builder.startNot().lessThanEquals(left.name, right.value).end()
        Some(b1)
      }
      case p@GreaterThan(left: Literal, right: NamedExpression) => {
        val b1 = builder.lessThanEquals(right.name, left.value)
        Some(b1)
      }
      case p@GreaterThanOrEqual(left: NamedExpression, right: Literal) => {
        val b1 = builder.startNot().lessThan(left.name, right.value).end()
        Some(b1)
      }
      case p@GreaterThanOrEqual(left: Literal, right: NamedExpression) => {
        val b1 = builder.lessThan(right.name, left.value)
        Some(b1)
      }
      // TODO: test it
      case p@EqualNullSafe(left: NamedExpression, right: NamedExpression) => {
        val b1 = builder.nullSafeEquals(left.name, right.name)
        Some(b1)
      }
      case p@In(left: NamedExpression, list: Seq[Literal]) => {
        val b1 = builder.in(left.name, list.map(_.value).toArray)
        Some(b1)
      }
      case _ => None
    }
  }
}
