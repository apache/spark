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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.expressions.{CaseWhen, Expression, Literal}
import org.apache.spark.sql.connector.expressions.{GeneralSQLExpression, LiteralValue}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy

/**
 * The builder to build {@link GeneralSQLExpression}.
 */
class ExpressionSQLBuilder(e: Expression) {

  def build(): Option[GeneralSQLExpression] = e match {
    case CaseWhen(branches, elseValue) =>
      val newBranches = branches.collect {
        case (predicate: Expression, Literal(value, dataType)) =>
          val translated =
            DataSourceV2Strategy.translateFilterV2WithMapping(predicate, None, false)
          (translated, LiteralValue(value, dataType))
      }.filter(_._1.isDefined)
      if (newBranches.length == branches.length) {
        val branchSQL = newBranches.map { case (c, v) => s" WHEN ${c.get} THEN $v" }.mkString
        val elseSQL = elseValue.map {
          case Literal(value, dataType) => LiteralValue(value, dataType)
          case _ => return None
        }.map(l => s" ELSE $l").getOrElse("")
        val sql = s"CASE$branchSQL$elseSQL END"
        Some(new GeneralSQLExpression(sql))
      } else {
        None
      }
    case _ => None
  }
}
