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

package org.apache.spark.mllib.util

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

private[mllib] object NumericTokenizer {
  val NUMBER = -1
  val END = -2
}

import NumericTokenizer._

/**
 * Simple tokenizer for a numeric structure consisting of three types:
 *
 *  - number: a double in Java's floating number format
 *  - array: an array of numbers stored as `[v0,v1,...,vn]`
 *  - tuple: a list of numbers, arrays, or tuples stored as `(...)`
 *
 * @param s input string
 * @param start start index
 * @param end end index
 */
private[mllib] class NumericTokenizer(s: String, start: Int, end: Int) {

  /**
   * Creates a tokenizer for the entire input string.
   */
  def this(s: String) = this(s, 0, s.length)

  private var cur = start
  private var allowComma = false
  private var _value = Double.NaN

  /**
   * Returns the most recent parsed number.
   */
  def value: Double = _value

  /**
   * Returns the next token, which could be any of the following:
   *  - '[', ']', '(', or ')'.
   *  - [[org.apache.spark.mllib.util.NumericTokenizer#NUMBER]], call value() to get its value.
   *  - [[org.apache.spark.mllib.util.NumericTokenizer#END]].
   */
  def next(): Int = {
    if (cur < end) {
      val c = s(cur)
      c match {
        case '(' | '[' =>
          allowComma = false
          cur += 1
          c
        case ')' | ']' =>
          allowComma = true
          cur += 1
          c
        case ',' =>
          if (allowComma) {
            cur += 1
            allowComma = false
            next()
          } else {
            sys.error("Found a ',' at a wrong location.")
          }
        case other => // expecting a number
          var inNumber = true
          val sb = new StringBuilder()
          while (cur < end && inNumber) {
            val d = s(cur)
            if (d == ')' || d == ']' || d == ',') {
              inNumber = false
            } else {
              sb.append(d)
              cur += 1
            }
          }
          _value = sb.toString().toDouble
          allowComma = true
          NUMBER
      }
    } else {
      END
    }
  }
}

/**
 * Simple parser for tokens from [[org.apache.spark.mllib.util.NumericTokenizer]].
 */
private[mllib] object NumericParser {

  /** Parses a string into a Double, an Array[Double], or a Seq[Any]. */
  def parse(s: String): Any = parse(new NumericTokenizer(s))

  private def parse(tokenizer: NumericTokenizer): Any = {
    tokenizer.next() match {
      case '(' =>
        parseTuple(tokenizer)
      case '[' =>
        parseArray(tokenizer)
      case NUMBER =>
        tokenizer.value
      case END =>
        null
    }
  }

  private def parseArray(tokenizer: NumericTokenizer): Array[Double] = {
    val values = ArrayBuffer.empty[Double]
    var token = tokenizer.next()
    while (token == NUMBER) {
      values.append(tokenizer.value)
      token = tokenizer.next()
    }
    require(token == ']')
    values.toArray
  }

  private def parseTuple(tokenizer: NumericTokenizer): Seq[_] = {
    val items = ListBuffer.empty[Any]
    var token = tokenizer.next()
    while (token != ')' && token != END) {
      token match {
        case '(' =>
          items.append(parseTuple(tokenizer))
        case '[' =>
          items.append(parseArray(tokenizer))
        case NUMBER =>
          items.append(tokenizer.value)
      }
      token = tokenizer.next()
    }
    require(token == ')')
    items.toSeq
  }
}
