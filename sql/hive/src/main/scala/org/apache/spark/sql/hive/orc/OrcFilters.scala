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

/**
 * It may be optimized by push down partial filters. But we are conservative here.
 * Because if some filters fail to be parsed, the tree may be corrupted,
 * and cannot be used anymore.
 */
private[orc] object OrcFilters extends Logging {
  def createFilter(expr: Array[Filter]): Option[SearchArgument] = {
    if (expr.nonEmpty) {
      expr.foldLeft(Some(SearchArgument.FACTORY.newBuilder().startAnd()): Option[Builder]) {
        (maybeBuilder, e) => createFilter(e, maybeBuilder)
      }.map(_.end().build())
    } else {
      None
    }
  }

  private def createFilter(expression: Filter, maybeBuilder: Option[Builder]): Option[Builder] = {
    maybeBuilder.flatMap { builder =>
      expression match {
        case p@And(left, right) =>
          for {
            lhs <- createFilter(left, Some(builder.startAnd()))
            rhs <- createFilter(right, Some(lhs))
          } yield rhs.end()
        case p@Or(left, right) =>
          for {
            lhs <- createFilter(left, Some(builder.startOr()))
            rhs <- createFilter(right, Some(lhs))
          } yield rhs.end()
        case p@Not(child) =>
          createFilter(child, Some(builder.startNot())).map(_.end())
        case p@EqualTo(attribute, value) =>
          Some(builder.equals(attribute, value))
        case p@LessThan(attribute, value) =>
          Some(builder.lessThan(attribute, value))
        case p@LessThanOrEqual(attribute, value) =>
          Some(builder.lessThanEquals(attribute, value))
        case p@GreaterThan(attribute, value) =>
          Some(builder.startNot().lessThanEquals(attribute, value).end())
        case p@GreaterThanOrEqual(attribute, value) =>
          Some(builder.startNot().lessThan(attribute, value).end())
        case p@IsNull(attribute) =>
          Some(builder.isNull(attribute))
        case p@IsNotNull(attribute) =>
          Some(builder.startNot().isNull(attribute).end())
        case p@In(attribute, values) =>
          Some(builder.in(attribute, values))
        case _ => None
      }
    }
  }
}
