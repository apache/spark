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

package org.apache.spark.sql.execution

import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.sql.catalyst.{ParserInterface, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * The top level Spark SQL parser. This parser recognizes syntaxes that are available for all SQL
 * dialects supported by Spark SQL, and delegates all the other syntaxes to the `fallback` parser.
 *
 * @param fallback A function that returns the next parser in the chain. This is a call-by-name
 *                 parameter because this allows us to return a different dialect if we
 *                 have to.
 */
class SparkSQLParser(fallback: => ParserInterface) extends ParserInterface {

  override def parseExpression(sql: String): Expression = fallback.parseExpression(sql)

  override def parseTableIdentifier(sql: String): TableIdentifier =
    fallback.parseTableIdentifier(sql)

  override def parsePlan(sqlText: String): LogicalPlan =
    SetCommandParser(sqlText).getOrElse(fallback.parsePlan(sqlText))

  // A parser for the key-value part of the "SET [key = [value ]]" syntax
  private object SetCommandParser extends RegexParsers {
    private val set: Parser[String] = "(?i)set".r

    private val key: Parser[String] = "(?m)[^=]+".r

    private val value: Parser[String] = "(?m).*$".r

    private val pair: Parser[LogicalPlan] =
      set ~> (key ~ ("=".r ~> value).?).? ^^ {
        case None => SetCommand(None)
        case Some(k ~ v) => SetCommand(Some(k.trim -> v.map(_.trim)))
      }

    def apply(input: String): Option[LogicalPlan] = parseAll(pair, input) match {
      case Success(plan, _) => Some(plan)
      case _ => None
    }
  }
}
