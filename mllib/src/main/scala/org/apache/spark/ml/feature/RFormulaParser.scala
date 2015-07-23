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

import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.sql.types.StructType

/**
 * Represents a parsed R formula.
 */
private[ml] case class ParsedRFormula(label: ColumnRef, terms: Seq[Term]) {
  def fit(schema: StructType): FittedRFormula = {
    var includedTerms = Seq[String]()
    terms.foreach {
      case Dot() =>
        includedTerms ++= schema.map(_.name).filter(_ != label.value)
      case ColumnRef(value) =>
        includedTerms :+= value
      case Deletion(term: Term) =>
        term match {
          case ColumnRef(value) =>
            includedTerms = includedTerms.filter(_ != value)
          case _: Dot =>
            val fromSchema = schema.map(_.name)
            includedTerms = includedTerms.filter(fromSchema.contains(_))
          case _: Deletion =>
            assert(false, "Recursive deletion of terms")
        }
    }
    FittedRFormula(label.value, includedTerms.distinct)
  }
}

/**
 * Represents a fully evaluated and simplified R formula.
 */
private[ml] case class FittedRFormula(label: String, terms: Seq[String])

/** R formula terms. */
private[ml] sealed trait Term
private[ml] case class Dot() extends Term
private[ml] case class ColumnRef(value: String) extends Term
private[ml] case class Deletion(term: Term) extends Term

/**
 * Limited implementation of R formula parsing. Currently supports: '~', '+'.
 */
private[ml] object RFormulaParser extends RegexParsers {
  def columnRef: Parser[ColumnRef] = 
    "([a-zA-Z]|\\.[a-zA-Z_])[a-zA-Z0-9._]*".r ^^ { case a => ColumnRef(a) }

  def term: Parser[Term] = columnRef | "\\.".r ^^ { case _ => Dot() }

  def terms: Parser[List[Term]] =
    (term ~ rep("+" ~> term)) ^^ { case a ~ list => a :: list } |
    (term ~ rep("-" ~> term)) ^^ { case a ~ list => Deletion(a) :: list }

  def formula: Parser[ParsedRFormula] =
    (columnRef ~ "~" ~ terms) ^^ { case r ~ "~" ~ t => ParsedRFormula(r, t) }

  def parse(value: String): ParsedRFormula = parseAll(formula, value) match {
    case Success(result, _) => result
    case failure: NoSuccess => throw new IllegalArgumentException(
      "Could not parse formula: " + value)
  }
}
