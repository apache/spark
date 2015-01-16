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

package org.apache.spark.sql

import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.sql.catalyst.{SqlLexical, AbstractSparkSQLParser}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{UncacheTableCommand, CacheTableCommand, SetCommand}
import org.apache.spark.sql.types.StringType


/**
 * The top level Spark SQL parser. This parser recognizes syntaxes that are available for all SQL
 * dialects supported by Spark SQL, and delegates all the other syntaxes to the `fallback` parser.
 *
 * @param fallback A function that parses an input string to a logical plan
 */
private[sql] class SparkSQLParser(fallback: String => LogicalPlan) extends AbstractSparkSQLParser {

  // A parser for the key-value part of the "SET [key = [value ]]" syntax
  private object SetCommandParser extends RegexParsers {
    private val key: Parser[String] = "(?m)[^=]+".r

    private val value: Parser[String] = "(?m).*$".r

    private val output: Seq[Attribute] = Seq(AttributeReference("", StringType, nullable = false)())

    private val pair: Parser[LogicalPlan] =
      (key ~ ("=".r ~> value).?).? ^^ {
        case None => SetCommand(None, output)
        case Some(k ~ v) => SetCommand(Some(k.trim -> v.map(_.trim)), output)
      }

    def apply(input: String): LogicalPlan = parseAll(pair, input) match {
      case Success(plan, _) => plan
      case x => sys.error(x.toString)
    }
  }

  protected val AS      = Keyword("AS")
  protected val CACHE   = Keyword("CACHE")
  protected val LAZY    = Keyword("LAZY")
  protected val SET     = Keyword("SET")
  protected val TABLE   = Keyword("TABLE")
  protected val UNCACHE = Keyword("UNCACHE")

  protected implicit def asParser(k: Keyword): Parser[String] =
    lexical.allCaseVersions(k.str).map(x => x : Parser[String]).reduce(_ | _)

  private val reservedWords: Seq[String] =
    this
      .getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = new SqlLexical(reservedWords)

  override protected lazy val start: Parser[LogicalPlan] = cache | uncache | set | others

  private lazy val cache: Parser[LogicalPlan] =
    CACHE ~> LAZY.? ~ (TABLE ~> ident) ~ (AS ~> restInput).? ^^ {
      case isLazy ~ tableName ~ plan =>
        CacheTableCommand(tableName, plan.map(fallback), isLazy.isDefined)
    }

  private lazy val uncache: Parser[LogicalPlan] =
    UNCACHE ~ TABLE ~> ident ^^ {
      case tableName => UncacheTableCommand(tableName)
    }

  private lazy val set: Parser[LogicalPlan] =
    SET ~> restInput ^^ {
      case input => SetCommandParser(input)
    }

  private lazy val others: Parser[LogicalPlan] =
    wholeInput ^^ {
      case input => fallback(input)
    }

}
