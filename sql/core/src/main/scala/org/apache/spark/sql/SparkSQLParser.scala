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

import org.apache.spark.sql.catalyst.AbstractSparkSQLParser
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{DescribeFunction, LogicalPlan, ShowFunctions}
import org.apache.spark.sql.execution._
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
        case None => SetCommand(None)
        case Some(k ~ v) => SetCommand(Some(k.trim -> v.map(_.trim)))
      }

    def apply(input: String): LogicalPlan = parseAll(pair, input) match {
      case Success(plan, _) => plan
      case x => sys.error(x.toString)
    }
  }

  protected val AS = Keyword("AS")
  protected val CACHE = Keyword("CACHE")
  protected val CLEAR = Keyword("CLEAR")
  protected val DESCRIBE = Keyword("DESCRIBE")
  protected val EXTENDED = Keyword("EXTENDED")
  protected val FUNCTION = Keyword("FUNCTION")
  protected val FUNCTIONS = Keyword("FUNCTIONS")
  protected val IN = Keyword("IN")
  protected val LAZY = Keyword("LAZY")
  protected val SET = Keyword("SET")
  protected val SHOW = Keyword("SHOW")
  protected val TABLE = Keyword("TABLE")
  protected val TABLES = Keyword("TABLES")
  protected val UNCACHE = Keyword("UNCACHE")

  override protected lazy val start: Parser[LogicalPlan] =
    cache | uncache | set | show | desc | others

  private lazy val cache: Parser[LogicalPlan] =
    CACHE ~> LAZY.? ~ (TABLE ~> ident) ~ (AS ~> restInput).? ^^ {
      case isLazy ~ tableName ~ plan =>
        CacheTableCommand(tableName, plan.map(fallback), isLazy.isDefined)
    }

  private lazy val uncache: Parser[LogicalPlan] =
    ( UNCACHE ~ TABLE ~> ident ^^ {
        case tableName => UncacheTableCommand(tableName)
      }
    | CLEAR ~ CACHE ^^^ ClearCacheCommand
    )

  private lazy val set: Parser[LogicalPlan] =
    SET ~> restInput ^^ {
      case input => SetCommandParser(input)
    }

  // It can be the following patterns:
  // SHOW FUNCTIONS;
  // SHOW FUNCTIONS mydb.func1;
  // SHOW FUNCTIONS func1;
  // SHOW FUNCTIONS `mydb.a`.`func1.aa`;
  private lazy val show: Parser[LogicalPlan] =
    ( SHOW ~> TABLES ~ (IN ~> ident).? ^^ {
        case _ ~ dbName => ShowTablesCommand(dbName)
      }
    | SHOW ~ FUNCTIONS ~> ((ident <~ ".").? ~ (ident | stringLit)).? ^^ {
        case Some(f) => ShowFunctions(f._1, Some(f._2))
        case None => ShowFunctions(None, None)
      }
    )

  private lazy val desc: Parser[LogicalPlan] =
    DESCRIBE ~ FUNCTION ~> EXTENDED.? ~ (ident | stringLit) ^^ {
      case isExtended ~ functionName => DescribeFunction(functionName, isExtended.isDefined)
    }

  private lazy val others: Parser[LogicalPlan] =
    wholeInput ^^ {
      case input => fallback(input)
    }

}
