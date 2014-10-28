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

package org.apache.spark.sql.catalyst

import scala.language.implicitConversions
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.{PackratParsers, RegexParsers}
import scala.util.parsing.input.CharArrayReader.EofCh

import org.apache.spark.sql.catalyst.plans.logical._

private[sql] abstract class AbstractSparkSQLParser
  extends StandardTokenParsers with PackratParsers {

  def apply(input: String): LogicalPlan = phrase(start)(new lexical.Scanner(input)) match {
    case Success(plan, _) => plan
    case failureOrError => sys.error(failureOrError.toString)
  }

  protected case class Keyword(str: String)

  protected def start: Parser[LogicalPlan]

  // Returns the whole input string
  protected lazy val wholeInput: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(in.source.toString, in.drop(in.source.length()))
  }

  // Returns the rest of the input string that are not parsed yet
  protected lazy val restInput: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(
        in.source.subSequence(in.offset, in.source.length()).toString,
        in.drop(in.source.length()))
  }
}

class SqlLexical(val keywords: Seq[String]) extends StdLexical {
  case class FloatLit(chars: String) extends Token {
    override def toString = chars
  }

  reserved ++= keywords.flatMap(w => allCaseVersions(w))

  delimiters += (
    "@", "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")",
    ",", ";", "%", "{", "}", ":", "[", "]", "."
  )

  override lazy val token: Parser[Token] =
    ( identChar ~ (identChar | digit).* ^^
      { case first ~ rest => processIdent((first :: rest).mkString) }
    | rep1(digit) ~ ('.' ~> digit.*).? ^^ {
        case i ~ None    => NumericLit(i.mkString)
        case i ~ Some(d) => FloatLit(i.mkString + "." + d.mkString)
      }
    | '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^
      { case chars => StringLit(chars mkString "") }
    | '"' ~> chrExcept('"', '\n', EofCh).* <~ '"' ^^
      { case chars => StringLit(chars mkString "") }
    | '`' ~> chrExcept('`', '\n', EofCh).* <~ '`' ^^
      { case chars => Identifier(chars mkString "") }
    | EofCh ^^^ EOF
    | '\'' ~> failure("unclosed string literal")
    | '"' ~> failure("unclosed string literal")
    | delim
    | failure("illegal character")
    )

  override def identChar = letter | elem('_')

  override def whitespace: Parser[Any] =
    ( whitespaceChar
    | '/' ~ '*' ~ comment
    | '/' ~ '/' ~ chrExcept(EofCh, '\n').*
    | '#' ~ chrExcept(EofCh, '\n').*
    | '-' ~ '-' ~ chrExcept(EofCh, '\n').*
    | '/' ~ '*' ~ failure("unclosed comment")
    ).*

  /** Generate all variations of upper and lower case of a given string */
  def allCaseVersions(s: String, prefix: String = ""): Stream[String] = {
    if (s == "") {
      Stream(prefix)
    } else {
      allCaseVersions(s.tail, prefix + s.head.toLower) ++
        allCaseVersions(s.tail, prefix + s.head.toUpper)
    }
  }
}

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

  protected val AS      = Keyword("AS")
  protected val CACHE   = Keyword("CACHE")
  protected val LAZY    = Keyword("LAZY")
  protected val SET     = Keyword("SET")
  protected val TABLE   = Keyword("TABLE")
  protected val SOURCE  = Keyword("SOURCE")
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

  override protected lazy val start: Parser[LogicalPlan] =
    cache | uncache | set | shell | source | others

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

  private lazy val shell: Parser[LogicalPlan] =
    "!" ~> restInput ^^ {
      case input => ShellCommand(input.trim)
    }

  private lazy val source: Parser[LogicalPlan] =
    SOURCE ~> restInput ^^ {
      case input => SourceCommand(input.trim)
    }

  private lazy val others: Parser[LogicalPlan] =
    wholeInput ^^ {
      case input => fallback(input)
    }
}
