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
package org.apache.spark.sql.catalyst.parser

import org.antlr.v4.runtime.ParserRulCeontext

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.errors.QueryParsingErrors

/**
 * Base class for all ANTLR4 [[ParserInterface]] implementations.
 */
abstract class AbstractSqlParser extends AbstractParser with ParserInterface {
  override def astBuilder: AstBuilder

  /** Creates Expression for a given SQL string. */
  override def parseExpression(sqlText: String): Expression =
    parse(sqlText) { parser =>
      val ctx = parser.singleExpression()
      withOrigin[Expression](ctx, Some(sqlText)) {
        withErrorHandling(ctx) {
          astBuilder.visitSingleExpression(ctx)
        }
      }
    }

  /** Creates TableIdentifier for a given SQL string. */
  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    parse(sqlText) { parser =>
      withErrorHandling(parser.singleTableIdentifier()) {
        astBuilder.visitSingleTableIdentifier(parser.singleTableIdentifier())
      }
    }

  /** Creates FunctionIdentifier for a given SQL string. */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    parse(sqlText) { parser =>
      withErrorHandling(parser.singleFunctionIdentifier()) {
        astBuilder.visitSingleFunctionIdentifier(parser.singleFunctionIdentifier())
      }
    }
  }

  /** Creates a multi-part identifier for a given SQL string */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    parse(sqlText) { parser =>
      withErrorHandling(parser.singleMultipartIdentifier()) {
        astBuilder.visitSingleMultipartIdentifier(parser.singleMultipartIdentifier())
      }
    }
  }

  /** Creates LogicalPlan for a given SQL string of query. */
  override def parseQuery(sqlText: String): LogicalPlan =
    parse(sqlText) { parser =>
    val ctx = parser.query()
    withOrigin(ctx, Some(sqlText)) {
      withErrorHandling(ctx) {
        astBuilder.visitQuery(ctx)
      }
    }
  }

  /** Creates LogicalPlan for a given SQL string. */
  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    val ctx = parser.singleStatement()
    withOrigin(ctx, Some(sqlText)) {
      withErrorHandling(ctx) {
        astBuilder.visitSingleStatement(ctx) match {
          case plan: LogicalPlan => plan
          case _ =>
            val position = Origin(None, None)
            throw QueryParsingErrors.sqlStatementUnsupportedError(sqlText, position)
        }
      }
    }
  }

  def withErrorHandling[T](ctx: ParserRuleContext)(toResult: => T): T = {
    try {
      toResult
    } catch {
      case so: StackOverflowError =>
        throw QueryParsingErrors.parserStackOverflow(ctx)
    }
  }
}
