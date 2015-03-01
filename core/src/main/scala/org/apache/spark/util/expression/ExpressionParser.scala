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
 * A simple parser of arithmetic expressions
 * Inspired by examples given in Ordesky, Spoon and Venners.
 */
trait ExpressionParser[T] extends JavaTokenParsers {

  def expr: Parser[Double] = term ~ rep(add | subtract) ^^ {
    case h ~ t => h + t.sum
  }

  def term: Parser[Double] = factor ~ rep(multi | div) ^^ {
    case h ~ t => h * t.foldLeft(1.0)((a, b) => a * b)
  }

  def factor: Parser[Double] = extensions | decimalNumber ^^ (_.toDouble)
    "(" ~ expr ~ ")" ^^ {
      case "(" ~ expr ~ ")" => expr
    }

  def extensions: Parser[Double]

  def multi: Parser[Double] = "*" ~ factor ^^ { case "*" ~ num => num }
  def div: Parser[Double] = "/" ~ factor ^^ { case "/" ~ num => 1 / num }
  def add: Parser[Double] = "+" ~ term ^^ { case "+" ~ num => num }
  def subtract: Parser[Double] = "-" ~ term ^^ { case "-" ~ num => -num }

  def parse(expression: String): Option[T]
}


