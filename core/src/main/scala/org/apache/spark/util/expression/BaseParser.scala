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
package org.apache.spark.util.expression

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * The base parser provides for basic arithmetic support
 */
private[spark] abstract class BaseParser extends JavaTokenParsers {

  // Stacked Trait extension point for more tokens/functions
  protected def stackedExtensions: Parser[Double] = failure("BaseParser has no extensions")

  private def expr: Parser[Double] = term ~ rep(add | subtract) ^^ {
    case h ~ t => h + t.sum
  }

  private def term: Parser[Double] = factor ~ rep(multi | div) ^^ {
    case h ~ t => h * t.foldLeft(1.0)((a, b) => a * b)
  }

  private def factor: Parser[Double] = stackedExtensions |
    floatingPointNumber ^^ (_.toDouble) |
    "(" ~ expr ~ ")" ^^ {
      case "(" ~ expr ~ ")" => expr
    }

  private def multi: Parser[Double] = "*" ~ factor ^^ { case "*" ~ num => num}

  private def div: Parser[Double] = "/" ~ factor ^^ { case "/" ~ num => 1.0 / num}

  private def add: Parser[Double] = "+" ~ term ^^ { case "+" ~ num => num}

  private def subtract: Parser[Double] = "-" ~ term ^^ { case "-" ~ num => -num}

  /**
   * A dollar sign indicates that the string is an expression, otherwise it
   * should be treated as a double and parsed appropriately
   * @param expression Expression to be parsed: Must start with a '$' character
   *                   to be considered a valid expression
   * @return Some[Double] if parsable as a double, otherwise None
   */
  private[spark] def parse(expression: String): Option[Double] =
    if (expression.head == BaseParser.EXP_TAG) parseAll(expr, expression.tail) match {
      case Success(result, _) => Some(result)
      case _ => None
    } else {
      try { Some(expression.toDouble) } catch { case _: Throwable => None }
    }
}

private[spark] object BaseParser {
  /**
   * Strings prepended with this tag indicate that they are expressions
   */
  var EXP_TAG = '!'
}
