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

package org.apache.spark.sql.catalyst.util

import scala.collection.mutable

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{Decimal, DecimalType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * This class represents a parser to implement the to_number SQL function.
 *
 * It works by consuming an input string and a format string. This class accepts the format string
 * as a field, and proceeds to iterate through the format string to generate a sequence of tokens
 * (or throw an exception if the format string is invalid). Then when the function is called with an
 * input string, this class steps through the sequence of tokens and compares them against the input
 * string, returning a Scala Decimal object if they match (or throwing an exception otherwise).
 *
 * @param originNumberFormat
 * @param isParse
 */
class ToNumberParser(originNumberFormat: String) extends Serializable {
  import ToNumberParser._

  // Consumes the format string and produce a sequence of input tokens expected from each input
  // string.
  private lazy val inputTokens: Seq[InputToken] = {
    var tokens = mutable.Seq.empty[InputToken]
    var i = 0
    val format = originNumberFormat
    val len = originNumberFormat.length
    while (i < len) {
      val char: Char = originNumberFormat(i)
      char match {
        case ZERO_DIGIT =>
          var prevI = i
          do {
            i += 1
          } while (i < len && format(i) == NINE_DIGIT)
          tokens :+= ExactlyAsManyDigits(i - prevI)
        case NINE_DIGIT =>
          val prevI = i
          do {
            i += 1
          } while (i < len && format(i) == NINE_DIGIT)
          tokens :+= AtMostAsManyDigits(i - prevI)
        case POINT_SIGN | POINT_LETTER =>
          tokens :+= DecimalPoint()
        case COMMA_SIGN | COMMA_LETTER =>
          tokens :+= ThousandsSeparator()
        case DOLLAR_LETTER | DOLLAR_SIGN =>
          tokens :+= DollarSign()
        case MINUS_LETTER =>
          tokens :+= PlusOrMinusSign()
        case OPTIONAL_MINUS_STRING_START if i < len &&
          OPTIONAL_MINUS_STRING_END == originNumberFormat(i + 1) =>
          i += 1
          tokens :+= MinusSign()
        case WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_START if i < len &&
          WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_END == originNumberFormat(i + 1) =>
          i += 1
          tokens :+= OpeningAngleBracket()
          tokens +:= ClosingAngleBracket()
        case c: Char =>
          tokens :+= InvalidUnrecognizedCharacter(c)
      }
      i += 1
    }
    tokens
  }

  // Precision is the number of digits in a number. Scale is the number of digits to the right of
  // the decimal point in a number. For example, the number 123.45 has a precision of 5 and a
  // scale of 2.
  private lazy val precision = {
    inputTokens.map {
      case ExactlyAsManyDigits(num) => num
      case AtMostAsManyDigits(num) => num
      case _ => 0
    }.sum
  }

  private lazy val scale = {
    val index = inputTokens.indexOf(DecimalPoint())
    if (index != -1) {
      inputTokens.drop(inputTokens.indexOf(DecimalPoint())).map {
        case ExactlyAsManyDigits(num) => num
        case AtMostAsManyDigits(num) => num
        case _ => 0
      }.sum
    } else {
      0
    }
  }

  def parsedDecimalType: DecimalType = DecimalType(precision, scale)

  def check(): TypeCheckResult = {
    def multipleSignInNumberFormatError(message: String): String = {
      s"At most one $message is allowed in the number format: '$originNumberFormat'"
    }

    def nonFirstOrLastCharInNumberFormatError(message: String): String = {
      s"$message must be the first or last char in the number format: '$originNumberFormat'"
    }

    def notAtEndOfNumberFormatError(message: String): String = {
      s"$message must be at the end of the number format: '$originNumberFormat'"
    }

    def unexpectedCharacterInFormatError(char: Char): String = {
      s"Encountered invalid character $char in the number format: '$originNumberFormat'"
    }

    def counts: Map[InputToken, Int] = inputTokens.groupBy(identity).mapValues(_.size)

    if (originNumberFormat.isEmpty) {
      TypeCheckResult.TypeCheckFailure("The format string cannot be empty")
    } else if (inputTokens.exists{_.isInstanceOf[InvalidUnrecognizedCharacter]})  {
      val char: Char = inputTokens.map { case i: InvalidUnrecognizedCharacter => i.char }.head
      TypeCheckResult.TypeCheckFailure(unexpectedCharacterInFormatError(char))
    } else if (!inputTokens.exists{
      token => token.isInstanceOf[ExactlyAsManyDigits] || token.isInstanceOf[AtMostAsManyDigits]}) {
      TypeCheckResult.TypeCheckFailure("The format string requires at least one number digit")
    } else if (counts.getOrElse(DecimalPoint(), 0) > 1) {
      TypeCheckResult.TypeCheckFailure(
        multipleSignInNumberFormatError(s"'$POINT_LETTER' or '$POINT_SIGN'"))
    } else if (counts.getOrElse(PlusOrMinusSign(), 0) > 1) {
      TypeCheckResult.TypeCheckFailure(multipleSignInNumberFormatError(s"'$MINUS_LETTER'"))
    } else if (counts.getOrElse(DollarSign(), 0) > 1) {
      TypeCheckResult.TypeCheckFailure(multipleSignInNumberFormatError(s"'$DOLLAR_SIGN'"))
    } else if (counts.getOrElse(MinusSign(), 0) > 1 ||
      (counts.getOrElse(MinusSign(), 0) == 1 && inputTokens.last != MinusSign())) {
      TypeCheckResult.TypeCheckFailure(notAtEndOfNumberFormatError(s"'$OPTIONAL_MINUS_STRING'"))
    } else if (counts.getOrElse(OpeningAngleBracket(), 0) > 1) {
      TypeCheckResult.TypeCheckFailure(
        notAtEndOfNumberFormatError(s"'$WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER'"))
    } else if (counts.getOrElse(DollarSign(), 0) == 1 &&
      (inputTokens.head != DollarSign() || inputTokens.last != DollarSign())) {
      TypeCheckResult.TypeCheckFailure(
        nonFirstOrLastCharInNumberFormatError(s"'$DOLLAR_SIGN'"))
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  /**
   * Convert string to numeric based on the given number format.
   *
   * Iterates through the [[inputTokens]] obtained from processing the format string, while also
   * keeping a parallel index into the [[input]] string. Throws an exception if the latter does not
   * contain expected characters at any point.
   *
   * @param input the string need to converted
   * @return decimal obtained from string parsing
   */
  def parse(input: UTF8String): Decimal = {
    val inputStr = input.toString.trim
    var reachedDecimalPoint: Boolean = false
    val beforeDecimalPoint = new StringBuilder()
    val afterDecimalPoint = new StringBuilder()
    var i: Int = 0
    for (token: InputToken <- inputTokens) {
      token match {
        case _: ExactlyAsManyDigits | _: AtMostAsManyDigits =>
          val prevI = i
          while (i < inputStr.length && inputStr(i) >= '0' && inputStr(i) <= '9') {
            if (reachedDecimalPoint) {
              afterDecimalPoint += inputStr(i)
            } else {
              beforeDecimalPoint += inputStr(i)
            }
            i += 1
          }
          val consumedDigits = i - prevI
          token match {
            case ExactlyAsManyDigits(num) if (consumedDigits != num) =>
              throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
            case AtMostAsManyDigits(num) if (consumedDigits > num) =>
              throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
            case _ =>
          }
        case DecimalPoint() =>
          if (inputStr(i) != POINT_LETTER) {
            throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
          }
          reachedDecimalPoint = true
        case ThousandsSeparator() =>
          if (inputStr(i) != COMMA_SIGN && inputStr(i) != COMMA_LETTER) {
            throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
          }
        case DollarSign() =>
          if (inputStr(i) != DOLLAR_LETTER && inputStr(i) != DOLLAR_SIGN) {
            throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
          }
        case PlusOrMinusSign() =>
          if (inputStr(i) != PLUS_SIGN && inputStr(i) != MINUS_SIGN) {
            throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
          }
        case MinusSign() =>
          if (inputStr(i) != MINUS_SIGN) {
            throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
          }
        case OpeningAngleBracket() =>
          if (inputStr(i) != ANGLE_BRACKET_OPEN) {
            throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
          }
        case ClosingAngleBracket() =>
          if (inputStr(i) != ANGLE_BRACKET_CLOSE) {
            throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
          }
      }
      i += 1
    }
    var unscaled: Long = 0
    if (beforeDecimalPoint.nonEmpty) {
      unscaled += beforeDecimalPoint.toLong
    }
    for (i <- 0 until afterDecimalPoint.length) {
      unscaled *= 10
    }
    if (afterDecimalPoint.nonEmpty) {
      unscaled += afterDecimalPoint.toLong
    }
    Decimal(unscaled, precision, scale)
  }
}

object ToNumberParser {
  final val ANGLE_BRACKET_CLOSE = '>'
  final val ANGLE_BRACKET_OPEN = '<'
  final val COMMA_LETTER = 'G'
  final val COMMA_SIGN = ','
  final val DOLLAR_LETTER = 'L'
  final val DOLLAR_SIGN = '$'
  final val MINUS_LETTER = 'S'
  final val MINUS_SIGN = '-'
  final val NINE_DIGIT = '9'
  final val PLUS_SIGN = '+'
  final val POINT_LETTER = 'D'
  final val POINT_SIGN = '.'
  final val POUND_SIGN = '#'
  final val ZERO_DIGIT = '0'

  final val OPTIONAL_MINUS_STRING = "MI"
  final val WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER = "PR"

  final val OPTIONAL_MINUS_STRING_START = 'M'
  final val OPTIONAL_MINUS_STRING_END = 'I'
  final val WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_START = 'R'
  final val WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_END = 'R'

  // This class represents one or more characters that we expect to be present in the input string
  // based on the format string.
  abstract class InputToken()
  case class ExactlyAsManyDigits(num: Int) extends InputToken
  case class AtMostAsManyDigits(num: Int) extends InputToken
  case class DecimalPoint() extends InputToken
  case class ThousandsSeparator() extends InputToken
  case class DollarSign() extends InputToken
  case class PlusOrMinusSign() extends InputToken
  case class MinusSign() extends InputToken
  case class OpeningAngleBracket() extends InputToken
  case class ClosingAngleBracket() extends InputToken
  case class InvalidUnrecognizedCharacter(char: Char) extends InputToken
}
