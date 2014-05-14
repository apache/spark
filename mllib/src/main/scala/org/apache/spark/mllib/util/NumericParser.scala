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

import org.apache.spark.SparkException

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
      val c = s.charAt(cur)
      if (c == '(' || c == '[') {
        allowComma = false
        cur += 1
        c
      } else if (c == ')' || c == ']') {
        allowComma = true
        cur += 1
        c
      } else if (c == ',') {
        if (allowComma) {
          cur += 1
          allowComma = false
          next()
        } else {
          throw new SparkException(s"Found a ',' at a wrong location: $cur.")
        }
      } else {
        // expecting a number
        var inNumber = true
        val beginAt = cur
        while (cur < end && inNumber) {
          val d = s.charAt(cur)
          if (d == ')' || d == ']' || d == ',') {
            inNumber = false
          } else {
            cur += 1
          }
        }
        try {
          _value = java.lang.Double.parseDouble(s.substring(beginAt, cur))
        } catch {
          case e: Throwable =>
            throw new SparkException("Error parsing a number", e)
        }
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
    val token = tokenizer.next()
    if (token == NUMBER) {
      tokenizer.value
    } else if (token == '(') {
      parseTuple(tokenizer)
    } else if (token == '[') {
      parseArray(tokenizer)
    } else if (token == END) {
      null
    } else {
      throw new SparkException(s"Cannot recgonize token type: $token.")
    }
  }

  private def parseArray(tokenizer: NumericTokenizer): Array[Double] = {
    val values = ArrayBuffer.empty[Double]
    var token = tokenizer.next()
    while (token == NUMBER) {
      values.append(tokenizer.value)
      token = tokenizer.next()
    }
    if (token != ']') {
      throw new SparkException(s"An array must end with ] but got $token.")
    }
    values.toArray
  }

  private def parseTuple(tokenizer: NumericTokenizer): Seq[_] = {
    val items = ListBuffer.empty[Any]
    var token = tokenizer.next()
    while (token != ')' && token != END) {
      if (token == NUMBER) {
        items.append(tokenizer.value)
      } else if (token == '(') {
        items.append(parseTuple(tokenizer))
      } else if (token == '[') {
        items.append(parseArray(tokenizer))
      } else {
        throw new SparkException(s"Cannot recognize token type: $token.")
      }
      token = tokenizer.next()
    }
    if (token != ')') {
      throw new SparkException(s"A tuple must end with ) but got $token.")
    }
    items.toSeq
  }
}
