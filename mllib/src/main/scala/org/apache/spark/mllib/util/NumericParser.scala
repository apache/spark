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

import java.util.StringTokenizer

import scala.collection.mutable.{ArrayBuilder, ListBuffer}

import org.apache.spark.SparkException

/**
 * Simple parser for a numeric structure consisting of three types:
 *
 *  - number: a double in Java's floating number format
 *  - array: an array of numbers stored as `[v0,v1,...,vn]`
 *  - tuple: a list of numbers, arrays, or tuples stored as `(...)`
 */
private[mllib] object NumericParser {

  /** Parses a string into a Double, an Array[Double], or a Seq[Any]. */
  def parse(s: String): Any = {
    val tokenizer = new StringTokenizer(s, "()[],", true)
    if (tokenizer.hasMoreTokens()) {
      val token = tokenizer.nextToken()
      if (token == "(") {
        parseTuple(tokenizer)
      } else if (token == "[") {
        parseArray(tokenizer)
      } else {
        // expecting a number
        parseDouble(token)
      }
    } else {
      throw new SparkException(s"Cannot find any token from the input string.")
    }
  }

  private def parseArray(tokenizer: StringTokenizer): Array[Double] = {
    val values = ArrayBuilder.make[Double]
    var parsing = true
    var allowComma = false
    var token: String = null
    while (parsing && tokenizer.hasMoreTokens()) {
      token = tokenizer.nextToken()
      if (token == "]") {
        parsing = false
      } else if (token == ",") {
        if (allowComma) {
          allowComma = false
        } else {
          throw new SparkException("Found a ',' at a wrong position.")
        }
      } else {
        // expecting a number
        values += parseDouble(token)
        allowComma = true
      }
    }
    if (parsing) {
      throw new SparkException(s"An array must end with ']'.")
    }
    values.result()
  }

  private def parseTuple(tokenizer: StringTokenizer): Seq[_] = {
    val items = ListBuffer.empty[Any]
    var parsing = true
    var allowComma = false
    var token: String = null
    while (parsing && tokenizer.hasMoreTokens()) {
      token = tokenizer.nextToken()
      if (token == "(") {
        items += parseTuple(tokenizer)
        allowComma = true
      } else if (token == "[") {
        items += parseArray(tokenizer)
        allowComma = true
      } else if (token == ",") {
        if (allowComma) {
          allowComma = false
        } else {
          throw new SparkException("Found a ',' at a wrong position.")
        }
      } else if (token == ")") {
        parsing = false
      } else if (token.trim.isEmpty) {
          // ignore whitespaces between delim chars, e.g. ", ["
      } else {
        // expecting a number
        items += parseDouble(token)
        allowComma = true
      }
    }
    if (parsing) {
      throw new SparkException(s"A tuple must end with ')'.")
    }
    items.toSeq
  }

  private def parseDouble(s: String): Double = {
    try {
      java.lang.Double.parseDouble(s)
    } catch {
      case e: NumberFormatException =>
        throw new SparkException(s"Cannot parse a double from: $s", e)
    }
  }
}
