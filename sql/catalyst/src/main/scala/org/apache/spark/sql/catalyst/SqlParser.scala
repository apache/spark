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
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.input.CharArrayReader.EofCh

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types._

/**
 * A very simple SQL parser.  Based loosely on:
 * https://github.com/stephentu/scala-sql-parser/blob/master/src/main/scala/parser.scala
 *
 * Limitations:
 *  - Only supports a very limited subset of SQL.
 *
 * This is currently included mostly for illustrative purposes.  Users wanting more complete support
 * for a SQL like language should checkout the HiveQL support in the sql/hive sub-project.
 */
class SqlParser extends StandardTokenParsers with PackratParsers {

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
    } else {
      phrase(query)(new lexical.Scanner(input)) match {
        case Success(r, x) => r
        case x => sys.error(x.toString)
      }
    }
  }

  protected case class Keyword(str: String)

  protected implicit def asParser(k: Keyword): Parser[String] =
    lexical.allCaseVersions(k.str).map(x => x : Parser[String]).reduce(_ | _)

  protected val ALL = Keyword("ALL")
  protected val AND = Keyword("AND")
  protected val AS = Keyword("AS")
  protected val ASC = Keyword("ASC")
  protected val APPROXIMATE = Keyword("APPROXIMATE")
  protected val AVG = Keyword("AVG")
  protected val BETWEEN = Keyword("BETWEEN")
  protected val BY = Keyword("BY")
  protected val CACHE = Keyword("CACHE")
  protected val CAST = Keyword("CAST")
  protected val COUNT = Keyword("COUNT")
  protected val DESC = Keyword("DESC")
  protected val DISTINCT = Keyword("DISTINCT")
  protected val FALSE = Keyword("FALSE")
  protected val FIRST = Keyword("FIRST")
  protected val LAST = Keyword("LAST")
  protected val FROM = Keyword("FROM")
  protected val FULL = Keyword("FULL")
  protected val GROUP = Keyword("GROUP")
  protected val HAVING = Keyword("HAVING")
  protected val IF = Keyword("IF")
  protected val IN = Keyword("IN")
  protected val INNER = Keyword("INNER")
  protected val INSERT = Keyword("INSERT")
  protected val INTO = Keyword("INTO")
  protected val IS = Keyword("IS")
  protected val JOIN = Keyword("JOIN")
  protected val LEFT = Keyword("LEFT")
  protected val LIMIT = Keyword("LIMIT")
  protected val MAX = Keyword("MAX")
  protected val MIN = Keyword("MIN")
  protected val NOT = Keyword("NOT")
  protected val NULL = Keyword("NULL")
  protected val ON = Keyword("ON")
  protected val OR = Keyword("OR")
  protected val OVERWRITE = Keyword("OVERWRITE")
  protected val LIKE = Keyword("LIKE")
  protected val RLIKE = Keyword("RLIKE")
  protected val UPPER = Keyword("UPPER")
  protected val LOWER = Keyword("LOWER")
  protected val REGEXP = Keyword("REGEXP")
  protected val ORDER = Keyword("ORDER")
  protected val OUTER = Keyword("OUTER")
  protected val RIGHT = Keyword("RIGHT")
  protected val SELECT = Keyword("SELECT")
  protected val SEMI = Keyword("SEMI")
  protected val STRING = Keyword("STRING")
  protected val SUM = Keyword("SUM")
  protected val TABLE = Keyword("TABLE")
  protected val TIMESTAMP = Keyword("TIMESTAMP")
  protected val TRUE = Keyword("TRUE")
  protected val UNCACHE = Keyword("UNCACHE")
  protected val UNION = Keyword("UNION")
  protected val WHERE = Keyword("WHERE")
  protected val INTERSECT = Keyword("INTERSECT")
  protected val EXCEPT = Keyword("EXCEPT")
  protected val SUBSTR = Keyword("SUBSTR")
  protected val SUBSTRING = Keyword("SUBSTRING")
  protected val SQRT = Keyword("SQRT")
  protected val ABS = Keyword("ABS")

  // Use reflection to find the reserved words defined in this class.
  protected val reservedWords =
    this.getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = new SqlLexical(reservedWords)

  protected def assignAliases(exprs: Seq[Expression]): Seq[NamedExpression] = {
    exprs.zipWithIndex.map {
      case (ne: NamedExpression, _) => ne
      case (e, i) => Alias(e, s"c$i")()
    }
  }

  protected lazy val query: Parser[LogicalPlan] = (
    select * (
        UNION ~ ALL ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Union(q1, q2) } |
        INTERSECT ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Intersect(q1, q2) } |
        EXCEPT ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Except(q1, q2)} |
        UNION ~ opt(DISTINCT) ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Distinct(Union(q1, q2)) }
      )
    | insert | cache | unCache
  )

  protected lazy val select: Parser[LogicalPlan] =
    SELECT ~> opt(DISTINCT) ~ projections ~
    opt(from) ~ opt(filter) ~
    opt(grouping) ~
    opt(having) ~
    opt(orderBy) ~
    opt(limit) <~ opt(";") ^^ {
      case d ~ p ~ r ~ f ~ g ~ h ~ o ~ l  =>
        val base = r.getOrElse(NoRelation)
        val withFilter = f.map(f => Filter(f, base)).getOrElse(base)
        val withProjection =
          g.map {g =>
            Aggregate(assignAliases(g), assignAliases(p), withFilter)
          }.getOrElse(Project(assignAliases(p), withFilter))
        val withDistinct = d.map(_ => Distinct(withProjection)).getOrElse(withProjection)
        val withHaving = h.map(h => Filter(h, withDistinct)).getOrElse(withDistinct)
        val withOrder = o.map(o => Sort(o, withHaving)).getOrElse(withHaving)
        val withLimit = l.map { l => Limit(l, withOrder) }.getOrElse(withOrder)
        withLimit
  }

  protected lazy val insert: Parser[LogicalPlan] =
    INSERT ~> opt(OVERWRITE) ~ inTo ~ select <~ opt(";") ^^ {
      case o ~ r ~ s =>
        val overwrite: Boolean = o.getOrElse("") == "OVERWRITE"
        InsertIntoTable(r, Map[String, Option[String]](), s, overwrite)
    }

  protected lazy val cache: Parser[LogicalPlan] =
    CACHE ~ TABLE ~> ident ~ opt(AS ~> select) <~ opt(";") ^^ {
      case tableName ~ None => 
        CacheCommand(tableName, true)
      case tableName ~ Some(plan) =>
        CacheTableAsSelectCommand(tableName, plan)
    }
    
  protected lazy val unCache: Parser[LogicalPlan] =
    UNCACHE ~ TABLE ~> ident <~ opt(";") ^^ {
      case tableName => CacheCommand(tableName, false)
    }    

  protected lazy val projections: Parser[Seq[Expression]] = repsep(projection, ",")

  protected lazy val projection: Parser[Expression] =
    expression ~ (opt(AS) ~> opt(ident)) ^^ {
      case e ~ None => e
      case e ~ Some(a) => Alias(e, a)()
    }

  protected lazy val from: Parser[LogicalPlan] = FROM ~> relations

  protected lazy val inTo: Parser[LogicalPlan] = INTO ~> relation

  // Based very loosely on the MySQL Grammar.
  // http://dev.mysql.com/doc/refman/5.0/en/join.html
  protected lazy val relations: Parser[LogicalPlan] =
    relation ~ "," ~ relation ^^ { case r1 ~ _ ~ r2 => Join(r1, r2, Inner, None) } |
    relation

  protected lazy val relation: Parser[LogicalPlan] =
    joinedRelation |
    relationFactor

  protected lazy val relationFactor: Parser[LogicalPlan] =
    ident ~ (opt(AS) ~> opt(ident)) ^^ {
      case tableName ~ alias => UnresolvedRelation(None, tableName, alias)
    } |
    "(" ~> query ~ ")" ~ opt(AS) ~ ident ^^ { case s ~ _ ~ _ ~ a => Subquery(a, s) }

  protected lazy val joinedRelation: Parser[LogicalPlan] =
    relationFactor ~ opt(joinType) ~ JOIN ~ relationFactor ~ opt(joinConditions) ^^ {
      case r1 ~ jt ~ _ ~ r2 ~ cond =>
        Join(r1, r2, joinType = jt.getOrElse(Inner), cond)
    }

  protected lazy val joinConditions: Parser[Expression] =
    ON ~> expression

  protected lazy val joinType: Parser[JoinType] =
    INNER ^^^ Inner |
    LEFT ~ SEMI ^^^ LeftSemi |
    LEFT ~ opt(OUTER) ^^^ LeftOuter |
    RIGHT ~ opt(OUTER) ^^^ RightOuter |
    FULL ~ opt(OUTER) ^^^ FullOuter

  protected lazy val filter: Parser[Expression] = WHERE ~ expression ^^ { case _ ~ e => e }

  protected lazy val orderBy: Parser[Seq[SortOrder]] =
    ORDER ~> BY ~> ordering

  protected lazy val ordering: Parser[Seq[SortOrder]] =
    rep1sep(singleOrder, ",") |
    rep1sep(expression, ",") ~ opt(direction) ^^ {
      case exps ~ None => exps.map(SortOrder(_, Ascending))
      case exps ~ Some(d) => exps.map(SortOrder(_, d))
    }

  protected lazy val singleOrder: Parser[SortOrder] =
    expression ~ direction ^^ { case e ~ o => SortOrder(e,o) }

  protected lazy val direction: Parser[SortDirection] =
    ASC ^^^ Ascending |
    DESC ^^^ Descending

  protected lazy val grouping: Parser[Seq[Expression]] =
    GROUP ~> BY ~> rep1sep(expression, ",")

  protected lazy val having: Parser[Expression] =
    HAVING ~> expression

  protected lazy val limit: Parser[Expression] =
    LIMIT ~> expression

  protected lazy val expression: Parser[Expression] = orExpression

  protected lazy val orExpression: Parser[Expression] =
    andExpression * (OR ^^^ { (e1: Expression, e2: Expression) => Or(e1,e2) })

  protected lazy val andExpression: Parser[Expression] =
    comparisonExpression * (AND ^^^ { (e1: Expression, e2: Expression) => And(e1,e2) })

  protected lazy val comparisonExpression: Parser[Expression] =
    termExpression ~ "=" ~ termExpression ^^ { case e1 ~ _ ~ e2 => EqualTo(e1, e2) } |
    termExpression ~ "<" ~ termExpression ^^ { case e1 ~ _ ~ e2 => LessThan(e1, e2) } |
    termExpression ~ "<=" ~ termExpression ^^ { case e1 ~ _ ~ e2 => LessThanOrEqual(e1, e2) } |
    termExpression ~ ">" ~ termExpression ^^ { case e1 ~ _ ~ e2 => GreaterThan(e1, e2) } |
    termExpression ~ ">=" ~ termExpression ^^ { case e1 ~ _ ~ e2 => GreaterThanOrEqual(e1, e2) } |
    termExpression ~ "!=" ~ termExpression ^^ { case e1 ~ _ ~ e2 => Not(EqualTo(e1, e2)) } |
    termExpression ~ "<>" ~ termExpression ^^ { case e1 ~ _ ~ e2 => Not(EqualTo(e1, e2)) } |
    termExpression ~ BETWEEN ~ termExpression ~ AND ~ termExpression ^^ { 
      case e ~ _ ~ el ~ _  ~ eu => And(GreaterThanOrEqual(e, el), LessThanOrEqual(e, eu))
    } |
    termExpression ~ RLIKE ~ termExpression ^^ { case e1 ~ _ ~ e2 => RLike(e1, e2) } |
    termExpression ~ REGEXP ~ termExpression ^^ { case e1 ~ _ ~ e2 => RLike(e1, e2) } |
    termExpression ~ LIKE ~ termExpression ^^ { case e1 ~ _ ~ e2 => Like(e1, e2) } |
    termExpression ~ IN ~ "(" ~ rep1sep(termExpression, ",") <~ ")" ^^ {
      case e1 ~ _ ~ _ ~ e2 => In(e1, e2)
    } |
    termExpression ~ NOT ~ IN ~ "(" ~ rep1sep(termExpression, ",") <~ ")" ^^ {
      case e1 ~ _ ~ _ ~ _ ~ e2 => Not(In(e1, e2))
    } |
    termExpression <~ IS ~ NULL ^^ { case e => IsNull(e) } |
    termExpression <~ IS ~ NOT ~ NULL ^^ { case e => IsNotNull(e) } |
    NOT ~> termExpression ^^ {e => Not(e)} |
    termExpression

  protected lazy val termExpression: Parser[Expression] =
    productExpression * (
      "+" ^^^ { (e1: Expression, e2: Expression) => Add(e1,e2) } |
      "-" ^^^ { (e1: Expression, e2: Expression) => Subtract(e1,e2) } )

  protected lazy val productExpression: Parser[Expression] =
    baseExpression * (
      "*" ^^^ { (e1: Expression, e2: Expression) => Multiply(e1,e2) } |
      "/" ^^^ { (e1: Expression, e2: Expression) => Divide(e1,e2) } |
      "%" ^^^ { (e1: Expression, e2: Expression) => Remainder(e1,e2) }
    )

  protected lazy val function: Parser[Expression] =
    SUM ~> "(" ~> expression <~ ")" ^^ { case exp => Sum(exp) } |
    SUM ~> "(" ~> DISTINCT ~> expression <~ ")" ^^ { case exp => SumDistinct(exp) } |
    COUNT ~> "(" ~ "*" <~ ")" ^^ { case _ => Count(Literal(1)) } |
    COUNT ~> "(" ~ expression <~ ")" ^^ { case dist ~ exp => Count(exp) } |
    COUNT ~> "(" ~> DISTINCT ~> expression <~ ")" ^^ { case exp => CountDistinct(exp :: Nil) } |
    APPROXIMATE ~> COUNT ~> "(" ~> DISTINCT ~> expression <~ ")" ^^ {
      case exp => ApproxCountDistinct(exp)
    } |
    APPROXIMATE ~> "(" ~> floatLit ~ ")" ~ COUNT ~ "(" ~ DISTINCT ~ expression <~ ")" ^^ {
      case s ~ _ ~ _ ~ _ ~ _ ~ e => ApproxCountDistinct(e, s.toDouble)
    } |
    FIRST ~> "(" ~> expression <~ ")" ^^ { case exp => First(exp) } |
    LAST ~> "(" ~> expression <~ ")" ^^ { case exp => Last(exp) } |
    AVG ~> "(" ~> expression <~ ")" ^^ { case exp => Average(exp) } |
    MIN ~> "(" ~> expression <~ ")" ^^ { case exp => Min(exp) } |
    MAX ~> "(" ~> expression <~ ")" ^^ { case exp => Max(exp) } |
    UPPER ~> "(" ~> expression <~ ")" ^^ { case exp => Upper(exp) } |
    LOWER ~> "(" ~> expression <~ ")" ^^ { case exp => Lower(exp) } |
    IF ~> "(" ~> expression ~ "," ~ expression ~ "," ~ expression <~ ")" ^^ {
      case c ~ "," ~ t ~ "," ~ f => If(c,t,f)
    } |
    (SUBSTR | SUBSTRING) ~> "(" ~> expression ~ "," ~ expression <~ ")" ^^ {
      case s ~ "," ~ p => Substring(s,p,Literal(Integer.MAX_VALUE))
    } |
    (SUBSTR | SUBSTRING) ~> "(" ~> expression ~ "," ~ expression ~ "," ~ expression <~ ")" ^^ {
      case s ~ "," ~ p ~ "," ~ l => Substring(s,p,l)
    } |
    SQRT ~> "(" ~> expression <~ ")" ^^ { case exp => Sqrt(exp) } |
    ABS ~> "(" ~> expression <~ ")" ^^ { case exp => Abs(exp) } |
    ident ~ "(" ~ repsep(expression, ",") <~ ")" ^^ {
      case udfName ~ _ ~ exprs => UnresolvedFunction(udfName, exprs)
    }

  protected lazy val cast: Parser[Expression] =
    CAST ~> "(" ~> expression ~ AS ~ dataType <~ ")" ^^ { case exp ~ _ ~ t => Cast(exp, t) }

  protected lazy val literal: Parser[Literal] =
    numericLit ^^ {
      case i if i.toLong > Int.MaxValue => Literal(i.toLong)
      case i => Literal(i.toInt)
    } |
    NULL ^^^ Literal(null, NullType) |
    floatLit ^^ {case f => Literal(f.toDouble) } |
    stringLit ^^ {case s => Literal(s, StringType) }

  protected lazy val floatLit: Parser[String] =
    elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  protected lazy val baseExpression: PackratParser[Expression] =
    expression ~ "[" ~ expression <~ "]" ^^ {
      case base ~ _ ~ ordinal => GetItem(base, ordinal)
    } |
    (expression <~ ".") ~ ident ^^ {
      case base ~ fieldName => GetField(base, fieldName)
    } |
    TRUE ^^^ Literal(true, BooleanType) |
    FALSE ^^^ Literal(false, BooleanType) |
    cast |
    "(" ~> expression <~ ")" |
    function |
    "-" ~> literal ^^ UnaryMinus |
    dotExpressionHeader |
    ident ^^ UnresolvedAttribute |
    "*" ^^^ Star(None) |
    literal

  protected lazy val dotExpressionHeader: Parser[Expression] =
    (ident <~ ".") ~ ident ~ rep("." ~> ident) ^^ {
      case i1 ~ i2 ~ rest => UnresolvedAttribute(i1 + "." + i2 + rest.mkString(".", ".", ""))
    }

  protected lazy val dataType: Parser[DataType] =
    STRING ^^^ StringType | TIMESTAMP ^^^ TimestampType
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

  override lazy val token: Parser[Token] = (
    identChar ~ rep( identChar | digit ) ^^
      { case first ~ rest => processIdent(first :: rest mkString "") }
      | rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
      case i ~ None    => NumericLit(i mkString "")
      case i ~ Some(d) => FloatLit(i.mkString("") + "." + d.mkString(""))
    }
      | '\'' ~ rep( chrExcept('\'', '\n', EofCh) ) ~ '\'' ^^
      { case '\'' ~ chars ~ '\'' => StringLit(chars mkString "") }
      | '\"' ~ rep( chrExcept('\"', '\n', EofCh) ) ~ '\"' ^^
      { case '\"' ~ chars ~ '\"' => StringLit(chars mkString "") }
      | EofCh ^^^ EOF
      | '\'' ~> failure("unclosed string literal")
      | '\"' ~> failure("unclosed string literal")
      | delim
      | failure("illegal character")
    )

  override def identChar = letter | elem('_')

  override def whitespace: Parser[Any] = rep(
    whitespaceChar
      | '/' ~ '*' ~ comment
      | '/' ~ '/' ~ rep( chrExcept(EofCh, '\n') )
      | '#' ~ rep( chrExcept(EofCh, '\n') )
      | '-' ~ '-' ~ rep( chrExcept(EofCh, '\n') )
      | '/' ~ '*' ~ failure("unclosed comment")
  )

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
