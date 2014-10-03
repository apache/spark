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

package org.apache.spark.sql.hive

import scala.language.implicitConversions
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.PackratParsers
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.SqlLexical

/**
 * A parser that recognizes all HiveQL constructs together with several Spark SQL specific 
 * extensions like CACHE TABLE and UNCACHE TABLE.
 */
private[hive] class ExtendedHiveQlParser extends StandardTokenParsers with PackratParsers {  
  
  def apply(input: String): LogicalPlan = {
    // Special-case out set commands since the value fields can be
    // complex to handle without RegexParsers. Also this approach
    // is clearer for the several possible cases of set commands.
    if (input.trim.toLowerCase.startsWith("set")) {
      input.trim.drop(3).split("=", 2).map(_.trim) match {
        case Array("") => // "set"
          SetCommand(None, None)
        case Array(key) => // "set key"
          SetCommand(Some(key), None)
        case Array(key, value) => // "set key=value"
          SetCommand(Some(key), Some(value))
      }
    } else if (input.trim.startsWith("!")) {
      ShellCommand(input.drop(1))
    } else {
      phrase(query)(new lexical.Scanner(input)) match {
        case Success(r, x) => r
        case x => sys.error(x.toString)
      }
    }
  }

  protected case class Keyword(str: String)

  protected val CACHE = Keyword("CACHE")
  protected val SET = Keyword("SET")
  protected val ADD = Keyword("ADD")
  protected val JAR = Keyword("JAR")
  protected val TABLE = Keyword("TABLE")
  protected val AS = Keyword("AS")
  protected val UNCACHE = Keyword("UNCACHE")
  protected val FILE = Keyword("FILE")
  protected val DFS = Keyword("DFS")
  protected val SOURCE = Keyword("SOURCE")

  protected implicit def asParser(k: Keyword): Parser[String] =
    lexical.allCaseVersions(k.str).map(x => x : Parser[String]).reduce(_ | _)

  protected def allCaseConverse(k: String): Parser[String] =
    lexical.allCaseVersions(k).map(x => x : Parser[String]).reduce(_ | _)

  protected val reservedWords =
    this.getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = new SqlLexical(reservedWords)

  protected lazy val query: Parser[LogicalPlan] = 
    cache | uncache | addJar | addFile | dfs | source | hiveQl

  protected lazy val hiveQl: Parser[LogicalPlan] =
    remainingQuery ^^ {
      case r => HiveQl.createPlan(r.trim())
    }

  /** It returns all remaining query */
  protected lazy val remainingQuery: Parser[String] = new Parser[String] {
    def apply(in: Input) =
      Success(
        in.source.subSequence(in.offset, in.source.length).toString,
        in.drop(in.source.length()))
  }

  /** It returns all query */
  protected lazy val allQuery: Parser[String] = new Parser[String] {
    def apply(in: Input) =
      Success(in.source.toString, in.drop(in.source.length()))
  }

  protected lazy val cache: Parser[LogicalPlan] =
    CACHE ~ TABLE ~> ident ~ opt(AS ~> hiveQl) ^^ {
      case tableName ~ None => CacheCommand(tableName, true)
      case tableName ~ Some(plan) =>
        CacheTableAsSelectCommand(tableName, plan)
    }

  protected lazy val uncache: Parser[LogicalPlan] =
    UNCACHE ~ TABLE ~> ident ^^ {
      case tableName => CacheCommand(tableName, false)
    }

  protected lazy val addJar: Parser[LogicalPlan] =
    ADD ~ JAR ~> remainingQuery ^^ {
      case rq => AddJar(rq.trim())
    }

  protected lazy val addFile: Parser[LogicalPlan] =
    ADD ~ FILE ~> remainingQuery ^^ {
      case rq => AddFile(rq.trim())
    }

  protected lazy val dfs: Parser[LogicalPlan] =
    DFS ~> allQuery ^^ {
      case aq => NativeCommand(aq.trim())
    }

  protected lazy val source: Parser[LogicalPlan] =
    SOURCE ~> remainingQuery ^^ {
      case rq => SourceCommand(rq.trim())
    }
}
