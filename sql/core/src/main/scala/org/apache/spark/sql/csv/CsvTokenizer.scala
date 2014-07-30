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
    quote: Char) extends Iterator[Array[String]] {

  private val DELIM = delimiter.charAt(0)
  private val QUOTE = quote
  private val DOUBLEQUOTE = quote.toString * 2
  private val BACKSLASH = '\\'
  private val NEWLINE = '\n'

  private val MAX_QUOTED_LINES = 10

  private val delimLength = delimiter.length

  private def isDelimAt(inStr: String, index: Int): Boolean = {
    inStr.substring(index, index + delimLength) == delimiter
  }

  private def isDoubleQuoteAt(inStr: String, index: Int): Boolean = {
    inStr.substring(index, index + 2) == DOUBLEQUOTE
  }

  private def stripQuotes(inStr: StringBuilder): String =  {
    val end = inStr.length - 1
    if (inStr.charAt(0) == QUOTE && inStr.charAt(end) == QUOTE) {
      inStr.deleteCharAt(end).deleteCharAt(0).toString()
    } else {
      inStr.toString()
    }
  }

  import QuoteState._

  def hasNext: Boolean = inputIter.hasNext

  def next(): Array[String] = {
    var curState = Unquoted
    var curPosition = 0
    var startPosition = 0
    var curChar: Char = '\0'
    var quotedLines = 0
    val leftOver = new StringBuilder()    // Used to keep track of tokens that span multiple lines
    val tokens = new ArrayBuffer[String]()
    var line = inputIter.next() + '\n'

    while (curPosition < line.length) {
      curChar = line.charAt(curPosition)

      if (curChar == QUOTE) {
        if (curState == Quoted) {
          if (isDoubleQuoteAt(line, curPosition)) {
            leftOver.append(line.substring(startPosition, curPosition + 1))
            curPosition += 2
            startPosition = curPosition
          } else {
            curState = Unquoted
            curPosition += 1
          }
        } else {
          curState = Quoted
          curPosition += 1
        }
      } else if (curChar == DELIM) {
        if (curState == Unquoted && isDelimAt(line, curPosition) && curPosition > startPosition) {
          leftOver.append(line.substring(startPosition, curPosition))
          tokens.append(stripQuotes(leftOver))
          leftOver.clear()
          quotedLines = 0
          curPosition += delimLength
          startPosition = curPosition
        } else {
          curPosition += 1
        }
      } else if (curChar == NEWLINE) {
        if (curState == Quoted && quotedLines < MAX_QUOTED_LINES) {
          leftOver.append(line.substring(startPosition, curPosition + 1))
          line = inputIter.next() + '\n'
          curPosition = 0
          startPosition = 0
          quotedLines += 1
        } else {
          if (curPosition == startPosition) {
            tokens.append(null)
          } else {
            leftOver.append(line.substring(startPosition, curPosition))
            tokens.append(stripQuotes(leftOver))
          }
          leftOver.clear()
          quotedLines = 0
          curPosition += 1
        }
      } else if (curChar == BACKSLASH) {
        curPosition += 2
      } else {
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

