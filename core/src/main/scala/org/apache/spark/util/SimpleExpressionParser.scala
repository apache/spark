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
package org.apache.spark.util

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * A simple parser of arithmetic expressions with a special token
 * to represent the number of cores in the machine.
 * Inspired by examples given by Ordesky, Spoon and Venners.
 */
class SimpleExpressionParser extends JavaTokenParsers {
  def expr: Parser[Double] = term ~ rep(add | subtract) ^^ {
    case h ~ t => h + t.sum
  }
  def term: Parser[Double] = factor ~ rep(multi | div) ^^ {
    case h ~ t => h * t.foldLeft(1.0)((a, b) => a * b)
  }
  def factor: Parser[Double] = floatingPointNumber ^^ (_.toDouble) | "(" ~ expr ~ ")" ^^ {
    case "(" ~ expr ~ ")" => expr
  }
  def multi: Parser[Double] = "*" ~ factor ^^ { case "*" ~ num => num }
  def div: Parser[Double] = "/" ~ factor ^^ { case "/" ~ num => 1.0 / num }
  def add: Parser[Double] = "+" ~ term ^^ { case "+" ~ num => num }
  def subtract: Parser[Double] = "-" ~ term ^^ { case "-" ~ num => -num }
}


