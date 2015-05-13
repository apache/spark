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

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder
import org.apache.spark.Logging
import org.apache.spark.sql.sources._

private[sql] object OrcFilters extends Logging {

  def createFilter(expr: Array[Filter]): Option[SearchArgument] = {
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

  def createFilter(expression: Filter, builder: Builder): Option[Builder] = {
    expression match {
      case p@And(left: Filter, right: Filter) => {
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
      case p@Or(left: Filter, right: Filter) => {
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
      case p@Not(child: Filter) => {
        val b1 = builder.startNot()
        val b2 = createFilter(child, b1)
        b2 match {
          case Some(b) => Some(b.end)
          case _ => None
        }
      }
      case p@EqualTo(attribute: String, value: Any) => {
        val b1 = builder.equals(attribute, value)
        Some(b1)
      }
      case p@LessThan(attribute: String, value: Any) => {
        val b1 = builder.lessThan(attribute ,value)
        Some(b1)
      }
      case p@LessThanOrEqual(attribute: String, value: Any) => {
        val b1 = builder.lessThanEquals(attribute, value)
        Some(b1)
      }
      case p@GreaterThan(attribute: String, value: Any) => {
        val b1 = builder.startNot().lessThanEquals(attribute, value).end()
        Some(b1)
      }
      case p@GreaterThanOrEqual(attribute: String, value: Any) => {
        val b1 = builder.startNot().lessThan(attribute, value).end()
        Some(b1)
      }
      case p@IsNull(attribute: String) => {
        val b1 = builder.isNull(attribute)
        Some(b1)
      }
      case p@IsNotNull(attribute: String) => {
        val b1 = builder.startNot().isNull(attribute).end()
        Some(b1)
      }
      case p@In(attribute: String, values: Array[Any]) => {
        val b1 = builder.in(attribute, values)
        Some(b1)
      }
      // not supported in filter
      // case p@EqualNullSafe(left: String, right: String) => {
      //  val b1 = builder.nullSafeEquals(left, right)
      //  Some(b1)
      // }
      case _ => None
    }
  }
}
