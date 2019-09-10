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

package org.apache.spark.ml.feature

import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.types._

/**
 * Represents a parsed R formula.
 */
private[ml] case class ParsedRFormula(label: Label, terms: Seq[Term]) {
  /**
   * Resolves formula terms into column names. A schema is necessary for inferring the meaning
   * of the special '.' term. Duplicate terms will be removed during resolution.
   */
  def resolve(schema: StructType): ResolvedRFormula = {
    val dotTerms = expandDot(schema)
    var includedTerms = Seq[Seq[String]]()
    var evalExprs = Seq[String]()
    val seen = mutable.Set[Set[String]]()

    label match {
      case EvalExpr(expr) => evalExprs :+= expr
      case _ =>
    }

    terms.foreach {
      case col: ColumnRef =>
        includedTerms :+= Seq(col.value)
      case EvalExpr(expr) =>
        includedTerms :+= Seq(expr)
        evalExprs :+= expr
      case ColumnInteraction(cols) =>
        expandInteraction(schema, cols) foreach { t =>
          // add equivalent interaction terms only once
          if (!seen.contains(t.toSet)) {
            includedTerms :+= t
            seen += t.toSet
          }
        }
      case Dot =>
        includedTerms ++= dotTerms.map(Seq(_))
      case Deletion(term: Term) =>
        term match {
          case inner: ColumnRef =>
            includedTerms = includedTerms.filter(_ != Seq(inner.value))
          case EvalExpr(expr) =>
            evalExprs = evalExprs.filter(_ != expr)
            includedTerms = includedTerms.filter(_ != Seq(expr))
          case ColumnInteraction(cols) =>
            val fromInteraction = expandInteraction(schema, cols).map(_.toSet)
            includedTerms = includedTerms.filter(t => !fromInteraction.contains(t.toSet))
          case Dot =>
            // e.g. "- .", which removes all first-order terms
            includedTerms = includedTerms.filter {
              case Seq(t) => !dotTerms.contains(t)
              case _ => true
            }
          case _: Deletion =>
            throw new RuntimeException("Deletion terms cannot be nested")
          case _: Intercept =>
          case _: Terms =>
          case EmptyTerm =>
        }
      case _: Intercept =>
      case _: Terms =>
      case EmptyTerm =>
    }
    ResolvedRFormula(label.value, includedTerms.distinct, hasIntercept, evalExprs.distinct)
  }

  /** Whether this formula specifies fitting with response variable. */
  def hasLabel: Boolean = label.value.nonEmpty

  /** Whether this formula specifies fitting with an intercept term. */
  def hasIntercept: Boolean = {
    var intercept = true
    terms.foreach {
      case Intercept(enabled) =>
        intercept = enabled
      case Deletion(Intercept(enabled)) =>
        intercept = !enabled
      case _ =>
    }
    intercept
  }

  // expands the Dot operators in interaction terms
  private def expandInteraction(
      schema: StructType, terms: Seq[InteractableTerm]): Seq[Seq[String]] = {
    if (terms.isEmpty) {
      return Seq(Nil)
    }

    val rest = expandInteraction(schema, terms.tail)
    val validInteractions = (terms.head match {
      case Dot =>
        expandDot(schema).flatMap { t =>
          rest.map { r =>
            Seq(t) ++ r
          }
        }
      case ColumnRef(value) =>
        rest.map(Seq(value) ++ _)
      case EvalExpr(value) =>
        rest.map(Seq(value) ++ _)
    }).map(_.distinct)

    // Deduplicates feature interactions, for example, a:b is the same as b:a.
    val seen = mutable.Set[Set[String]]()
    validInteractions.flatMap {
      case t if seen.contains(t.toSet) =>
        None
      case t =>
        seen += t.toSet
        Some(t)
    }.sortBy(_.length)
  }

  // the dot operator excludes complex column types
  private def expandDot(schema: StructType): Seq[String] = {
    schema.fields.filter(_.dataType match {
      case _: NumericType | StringType | BooleanType | _: VectorUDT => true
      case _ => false
    }).map(_.name).filter(_ != label.value)
  }
}

/**
 * Represents a fully evaluated and simplified R formula.
 * @param label the column name of the R formula label (response variable).
 * @param terms the simplified terms of the R formula. Interactions terms are represented as Seqs
 *              of column names; non-interaction terms as length 1 Seqs.
 * @param hasIntercept whether the formula specifies fitting with an intercept.
 * @param evalExprs expressions inside the formula to be evaluated with expr spark function
 */
private[ml] case class ResolvedRFormula(
  label: String, terms: Seq[Seq[String]], hasIntercept: Boolean, evalExprs: Seq[String]) {

  override def toString: String = {
    val ts = terms.map {
      case t if t.length > 1 =>
        s"${t.mkString("{", ",", "}")}"
      case t =>
        t.mkString
    }
    val termStr = ts.mkString("[", ",", "]")
    val exprs = evalExprs.mkString("[", ",", "]")
    s"ResolvedRFormula(label=$label, terms=$termStr, hasIntercept=$hasIntercept, evalExprs=$exprs)"
  }
}

/**
 * R formula terms. See the R formula docs here for more information:
 * http://stat.ethz.ch/R-manual/R-patched/library/stats/html/formula.html
 */
private[ml] sealed trait Term {

  /** Default representation of a single Term as a part of summed terms. */
  def asTerms: Terms = Terms(Seq(this))

  /** Creates a summation term by concatenation of terms. */
  def add(other: Term): Term = Terms(this.asTerms.terms ++ other.asTerms.terms)

  /**
   * Fold by adding deletion terms to the left. Double negation
   * doesn't cancel deletion in order not to add extra terms, e.g.
   * a - (b - c) = a - Deletion(b) - Deletion(c) = a
   */
  def subtract(other: Term): Term = {
    other.asTerms.terms.foldLeft(this) {
      case (left, right) =>
        right match {
          case t: Deletion => left.add(t)
          case t: Term => left.add(Deletion(t))
        }
    }
  }

  /** Default interactions of a Term */
  def interact(other: Term): Term = EmptyTerm
}

/** Placeholder term for the result of undefined interactions, e.g. '1:1' or 'a:1' */
private[ml] case object EmptyTerm extends Term

/** A term that may be part of an interaction, e.g. 'x' in 'x:y' */
private[ml] sealed trait InteractableTerm extends Term {

  /** Convert to ColumnInteraction to wrap all interactions. */
  def asInteraction: ColumnInteraction = ColumnInteraction(Seq(this))

  /** Interactions of interactable terms. */
  override def interact(other: Term): Term = other match {
    case t: InteractableTerm => this.asInteraction.interact(t.asInteraction)
    case t: ColumnInteraction => this.asInteraction.interact(t)
    case t: Terms => this.asTerms.interact(t)
    case t: Term => t.interact(this) // Deletion or non-interactable term
  }
}

/* R formula reference to all available columns, e.g. "." in a formula */
private[ml] case object Dot extends InteractableTerm

/* R formula base trait for terms that might be on LHS of the formula */
private[ml] sealed trait Label extends InteractableTerm {
  val value: String
}

/* R formula reference to a column, e.g. "+ Species" in a formula */
private[ml] case class ColumnRef(value: String) extends Label

/* R formula reference to an expression to be evaluated as is, e.g. `log(y)` in a formula */
private[ml] case class EvalExpr(value: String) extends Label

/* R formula interaction of several columns, e.g. "Sepal_Length:Species" in a formula */
private[ml] case class ColumnInteraction(terms: Seq[InteractableTerm]) extends Term {

  // Convert to ColumnInteraction and concat terms
  override def interact(other: Term): Term = other match {
    case t: InteractableTerm => this.interact(t.asInteraction)
    case t: ColumnInteraction => ColumnInteraction(terms ++ t.terms)
    case t: Terms => this.asTerms.interact(t)
    case t: Term => t.interact(this)
  }
}

/* R formula intercept toggle, e.g. "+ 0" in a formula */
private[ml] case class Intercept(enabled: Boolean) extends Term

/* R formula deletion of a variable, e.g. "- Species" in a formula */
private[ml] case class Deletion(term: Term) extends Term {

  // Unnest the deletion and interact
  override def interact(other: Term): Term = other match {
    case Deletion(t) => Deletion(term.interact(t))
    case t: Term => Deletion(term.interact(t))
  }
}

/* Wrapper for multiple terms in a formula. */
private[ml] case class Terms(terms: Seq[Term]) extends Term {

  override def asTerms: Terms = this

  override def interact(other: Term): Term = {
    val interactions = for {
      left <- terms
      right <- other.asTerms.terms
    } yield left.interact(right)
    Terms(interactions)
  }
}

/**
 * Limited implementation of R formula parsing. Currently supports: '~', '+', '-', '.', ':',
 * '*', '^', 'I()'.
 */
private[ml] object RFormulaParser extends RegexParsers with EvalExprParser {

  /**
   * We'd like to skip whitespace for all parsers except for evaluated expressions,
   * as whitespace could be valid part of the expression, such as `I(concat(a, ' ', b))`.
   * We do that by disabling whitespace skipping with class-wide skipWhitespace flag,
   * and let the individual parsers decide whether to skip whitespace using skipSpace function.
   */
  override def skipWhitespace: Boolean = false

  private val space = "[ \\n]*".r

  /* Utility function for skipping whitespace around a regex. */
  private def skipSpace(r: Regex): Parser[String] = {
    space ~> r <~ space
  }

  private def add(left: Term, right: Term) = left.add(right)

  private def subtract(left: Term, right: Term) = left.subtract(right)

  private def interact(left: Term, right: Term) = left.interact(right)

  private def cross(left: Term, right: Term) = left.add(right).add(left.interact(right))

  private def power(base: Term, degree: Int): Term = {
    val exprs = List.fill(degree)(base)
    exprs match {
      case Nil => EmptyTerm
      case x :: Nil => x
      case x :: xs => xs.foldLeft(x)(cross _)
    }
  }

  private val intercept: Parser[Term] =
    skipSpace("([01])".r) ^^ { case a => Intercept(a == "1") }

  private val columnRef: Parser[ColumnRef] =
    skipSpace("([a-zA-Z]|\\.[a-zA-Z_])[a-zA-Z0-9._]*".r) ^^ { case a => ColumnRef(a) }

  private val empty: Parser[ColumnRef] = skipSpace("".r) ^^ { case a => ColumnRef("") }

  private val label: Parser[Label] = evalExpr | columnRef | empty

  private val dot: Parser[Term] = skipSpace("\\.".r) ^^ { case _ => Dot }

  private val parens: Parser[Term] = skipSpace("\\(".r) ~> expr <~ skipSpace("\\)".r)

  private val term: Parser[Term] = evalExpr | parens | intercept | columnRef | dot

  private val pow: Parser[Term] = term ~ "^" ~ skipSpace("^[1-9]\\d*".r) ^^ {
    case base ~ "^" ~ degree => power(base, degree.toInt)
  } | term

  private val interaction: Parser[Term] = pow * (skipSpace("\\:".r) ^^^ { interact _ })

  private val factor = interaction * (skipSpace("\\*".r) ^^^ { cross _ })

  private val sum = factor * (skipSpace("\\+".r) ^^^ { add _ } |
    skipSpace("\\-".r) ^^^ { subtract _ })

  private val expr = (sum | term)

  private val formula: Parser[ParsedRFormula] = (label ~ skipSpace("\\~".r) ~ expr) ^^ {
    case r ~ "~" ~ t => ParsedRFormula(r, t.asTerms.terms) }

  def parse(value: String): ParsedRFormula = parseAll(formula, value) match {
    case Success(result, _) => result
    case failure: NoSuccess => throw new IllegalArgumentException(
      "Could not parse formula: " + value)
  }
}

/**
 * Parser for evaluated expressions in a formula. An evaluated expression is
 * any alphanumeric identifiers followed by (), e.g. `func123(a+b, func())`,
 * or anything inside `I()`. A valid expression is any string-parentheses product,
 * such that if there are any parentheses ('(' or ')') they're all balanced.
 */
private[ml] trait EvalExprParser extends RegexParsers {

  /* Any char except `(` or `)`. */
  private def char: Parser[Char] = """[^\(\)]""".r ^^ { _.head }

  /* Characters followed by any number of numeric characters. */
  private def functionIdentifier: Parser[String] = """[a-zA-Z_]\w*""".r

  private val space = "[ \\n]*".r

  private def string: Parser[String] = rep(char) ^^ {_.mkString}

  private def nonEmptyString: Parser[String] = rep1(char) ^^ {_.mkString}

  private def stringParenProduct: Parser[String] = ((string) ~ paren ~ (string)) ^^ {
    case l ~ m ~ r => l + m  + r }

  private def products: Parser[String] = rep(stringParenProduct) ^^ {_.mkString}

  private def nonEmptyProducts: Parser[String] = rep1(stringParenProduct) ^^ {_.mkString}

  private def paren: Parser[String] = ("(" ~
    (nonEmptyProducts | nonEmptyString | products) ~ ")") ^^ {
    case l ~ m ~ r => l + m + r }

  private def balancedParenProduct: Parser[String] = (products ||| string)

  private def parsedExpr: Parser[String] =
    "I" ~ ( space ~> "(") ~> balancedParenProduct <~ ")" | (
      functionIdentifier ~ ( space ~> "(") ~ balancedParenProduct ~ ")") ^^ {
    case f ~ l ~ m ~ r => f + l + m + r}

  def evalExpr: Parser[Label] = parsedExpr ^^ {case expr => EvalExpr(expr)}
}
