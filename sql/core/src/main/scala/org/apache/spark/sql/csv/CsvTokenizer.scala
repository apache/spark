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

package org.apache.spark.sql.csv

import scala.collection.mutable.ArrayBuffer

/**
 * Tokenizer based on RFC 4180 for comma separated values.
 * It implements an iterator that returns each tokenized line as an Array[Any].
 */
private[sql] class CsvTokenizer(
    inputIter: Iterator[String],
    delimiter: String,
    quote: String) extends Iterator[Array[String]] {

  private  val DELIM = delimiter.charAt(0)
  private  val QUOTE = quote.charAt(0)
  private  val BACKSLASH = '\\'
  private  val NEWLINE = '\n'

  private val delimLength = delimiter.length
  private val quoteLength = quote.length

  private def isDelimAt(inStr: String, index: Int): Boolean = {
    inStr.substring(index, index + delimLength) == delimiter
  }

  private def isQuoteAt(inStr: String, index: Int): Boolean = {
    inStr.substring(index, index + quoteLength) == quote
  }

  private def stripQuotes(inStr: String): String =  if (inStr.startsWith(quote)) {
    inStr.stripPrefix(quote).stripSuffix(quote)
  } else {
    inStr
  }

  import QuoteState._

  def hasNext: Boolean = inputIter.hasNext

  def next(): Array[String] = {
    var curState = Unquoted
    var curPosition = 0
    var startPosition = 0
    var curChar: Char = '\0'
    var leftOver: String = ""             // Used to keep track of tokens that span multiple lines
    val tokens = new ArrayBuffer[String]()
    var line = inputIter.next() + '\n'

    while (curPosition < line.length) {
      curChar = line.charAt(curPosition)

      (curState, curChar) match {
        case (Quoted, QUOTE) =>
          if (isQuoteAt(line, curPosition)) {
            curState = Unquoted
            curPosition += quoteLength
          } else {
            curPosition += 1
          }
        case (Quoted, NEWLINE) if inputIter.hasNext =>
          leftOver = leftOver + line.substring(startPosition, curPosition + 1)
          line = inputIter.next() + '\n'
          curPosition = 0
          startPosition = 0
        case (Unquoted, DELIM) =>
          if (isDelimAt(line, curPosition) && curPosition > startPosition) {
            tokens.append(stripQuotes(leftOver + line.substring(startPosition, curPosition)))
            curPosition += delimLength
            startPosition = curPosition
            leftOver = ""
          } else {
            curPosition += 1
          }
        case (Unquoted, QUOTE) =>
          if (isQuoteAt(line, curPosition)) {
            curState = Quoted
            curPosition += quoteLength
          } else {
            curPosition += 1
          }
        case (Unquoted, NEWLINE) =>
          if (startPosition == curPosition) {
            tokens.append(null)
          } else {
            tokens.append(stripQuotes(leftOver + line.substring(startPosition, curPosition)))
          }
          curPosition += 1
        case (_, BACKSLASH) =>
          curPosition += 2
        case (_, _) =>
          curPosition += 1
      }
    }
    tokens.toArray
  }

}

object QuoteState extends Enumeration {
  type State = Value
  val Quoted, Unquoted = Value
}

