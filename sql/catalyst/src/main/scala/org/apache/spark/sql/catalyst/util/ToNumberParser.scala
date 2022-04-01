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

// This object contains some definitions of characters and tokens for the parser below.
object ToNumberParser {
  final val ANGLE_BRACKET_CLOSE = '>'
  final val ANGLE_BRACKET_OPEN = '<'
  final val COMMA_LETTER = 'G'
  final val COMMA_SIGN = ','
  final val DOLLAR_LETTER = 'L'
  final val DOLLAR_SIGN = '$'
  final val MINUS_SIGN = '-'
  final val NINE_DIGIT = '9'
  final val OPTIONAL_PLUS_OR_MINUS_LETTER = 'S'
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
  // Represents some number of digits (0-9).
  abstract class Digits extends InputToken
  // Represents exactly 'num' digits (0-9).
  case class ExactlyAsManyDigits(num: Int) extends Digits
  // Represents at most 'num' digits (0-9).
  case class AtMostAsManyDigits(num: Int) extends Digits
  // Represents one decimal point (.).
  case class DecimalPoint() extends InputToken
  // Represents one thousands separator (,).
  case class ThousandsSeparator() extends InputToken
  // Represents one or more groups of digits (0-9) with thousands separators (,) between each group.
  case class DigitGroups(tokens: Seq[InputToken]) extends InputToken
  // Represents one dollar sign ($).
  case class DollarSign() extends InputToken
  // Represents one optional plus sign (+) or minus sign (-).
  case class OptionalPlusOrMinusSign() extends InputToken
  // Represents one optional minus sign (-).
  case class OptionalMinusSign() extends InputToken
  // Represents one opening angle bracket (<).
  case class OpeningAngleBracket() extends InputToken
  // Represents one closing angle bracket (>).
  case class ClosingAngleBracket() extends InputToken
  // Represents any unrecognized character other than the above.
  case class InvalidUnrecognizedCharacter(char: Char) extends InputToken
}

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
    var reachedDecimalPoint = false
    val format = originNumberFormat
    val len = originNumberFormat.length
    while (i < len) {
      val char: Char = originNumberFormat(i)
      char match {
        case ZERO_DIGIT =>
          val prevI = i
          do {
            i += 1
          } while (i < len && (format(i) == ZERO_DIGIT || format(i) == NINE_DIGIT))
          if (reachedDecimalPoint) {
            tokens :+= AtMostAsManyDigits(i - prevI)
          } else {
            tokens :+= ExactlyAsManyDigits(i - prevI)
          }
        case NINE_DIGIT =>
          val prevI = i
          do {
            i += 1
          } while (i < len && (format(i) == ZERO_DIGIT || format(i) == NINE_DIGIT))
          tokens :+= AtMostAsManyDigits(i - prevI)
        case POINT_SIGN | POINT_LETTER =>
          tokens :+= DecimalPoint()
          reachedDecimalPoint = true
          i += 1
        case COMMA_SIGN | COMMA_LETTER =>
          tokens :+= ThousandsSeparator()
          i += 1
        case DOLLAR_LETTER | DOLLAR_SIGN =>
          tokens :+= DollarSign()
          i += 1
        case OPTIONAL_PLUS_OR_MINUS_LETTER =>
          tokens :+= OptionalPlusOrMinusSign()
          i += 1
        case OPTIONAL_MINUS_STRING_START if i < len &&
          OPTIONAL_MINUS_STRING_END == originNumberFormat(i + 1) =>
          tokens :+= OptionalMinusSign()
          i += 1
        case WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_START if i < len &&
          WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_END == originNumberFormat(i + 1) =>
          tokens :+= OpeningAngleBracket()
          tokens +:= ClosingAngleBracket()
          i += 1
        case c: Char =>
          tokens :+= InvalidUnrecognizedCharacter(c)
          i += 1
      }
    }

    // Combine each group of consecutive Digits and ThousandsSeparator tokens into a single group.
    i = 0
    var groupedTokens = mutable.Seq.empty[InputToken]
    while (i < tokens.length) {
      val digits = tokens.drop(i).takeWhile{
        case _: Digits | _: ThousandsSeparator => true
        case _ => false
      }
      if (digits.nonEmpty) {
        groupedTokens :+= DigitGroups(digits.filter(_.isInstanceOf[Digits]))
        i += digits.length
      } else {
        groupedTokens :+= tokens(i)
        i += 1
      }
    }
    groupedTokens
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
    def multipleSignInNumberFormatError(message: String) = {
      s"At most one $message is allowed in the number format: '$originNumberFormat'"
    }
    def currencyAppearsAfterDigit = {
      s"Currency characters must appear before digits in the number format: '$originNumberFormat'"
    }
    def currencyAppearsAfterDecimalPoint = {
      "Currency characters must appear before any decimal point in the " +
        s"number format: '$originNumberFormat'"
    }
    def notAtEndOfNumberFormatError(message: String) = {
      s"$message must be at the end of the number format: '$originNumberFormat'"
    }
    def unexpectedCharacterInFormatError(char: Char) = {
      s"Encountered invalid character $char in the number format: '$originNumberFormat'"
    }
    def noDigitToLeftOfRightmostThousandsSeparator() = {
      "There must be a digit (0 or 9) to the left of the rightmost thousands separator " +
        s"in the number format: '$originNumberFormat'"
    }

    def counts: Map[InputToken, Int] = inputTokens.groupBy(identity).mapValues(_.size)
    val firstDollarSignIndex: Int = inputTokens.indexOf(DollarSign())
    val firstDigitIndex: Int = inputTokens.indexWhere {
      case _: AtMostAsManyDigits | _: ExactlyAsManyDigits => true
      case _ => false
    }
    val firstDecimalPointIndex: Int = inputTokens.indexOf(DecimalPoint())
    val lastThousandsSeparatorIndex: Int = inputTokens.lastIndexOf(ThousandsSeparator())

    if (originNumberFormat.isEmpty) {
      TypeCheckResult.TypeCheckFailure("The format string cannot be empty")
    } else if (inputTokens.exists{_.isInstanceOf[InvalidUnrecognizedCharacter]})  {
      val char: Char = inputTokens.map { case i: InvalidUnrecognizedCharacter => i.char }.head
      TypeCheckResult.TypeCheckFailure(unexpectedCharacterInFormatError(char))
    } else if (!inputTokens.exists{
      token => token.isInstanceOf[ExactlyAsManyDigits] || token.isInstanceOf[AtMostAsManyDigits]}) {
      TypeCheckResult.TypeCheckFailure("The format string requires at least one number digit")
    } else if (counts.getOrElse(DecimalPoint(), 0) > 1) {
      TypeCheckResult.TypeCheckFailure(multipleSignInNumberFormatError(
        s"'$POINT_LETTER' or '$POINT_SIGN'"))
    } else if (counts.getOrElse(OptionalPlusOrMinusSign(), 0) > 1) {
      TypeCheckResult.TypeCheckFailure(multipleSignInNumberFormatError(
        s"'$OPTIONAL_PLUS_OR_MINUS_LETTER'"))
    } else if (counts.getOrElse(DollarSign(), 0) > 1) {
      TypeCheckResult.TypeCheckFailure(multipleSignInNumberFormatError(s"'$DOLLAR_SIGN'"))
    } else if (counts.getOrElse(OptionalMinusSign(), 0) > 1 ||
      (counts.getOrElse(OptionalMinusSign(), 0) == 1 && inputTokens.last != OptionalMinusSign())) {
      TypeCheckResult.TypeCheckFailure(notAtEndOfNumberFormatError(s"'$OPTIONAL_MINUS_STRING'"))
    } else if (counts.getOrElse(OpeningAngleBracket(), 0) > 1) {
      TypeCheckResult.TypeCheckFailure(notAtEndOfNumberFormatError(
        s"'$WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER'"))
    } else if (firstDigitIndex < firstDollarSignIndex) {
      TypeCheckResult.TypeCheckFailure(currencyAppearsAfterDigit)
    } else if (firstDecimalPointIndex != -1 && firstDecimalPointIndex < firstDollarSignIndex) {
      TypeCheckResult.TypeCheckFailure(currencyAppearsAfterDecimalPoint)
    } else if (lastThousandsSeparatorIndex > 0 &&
      (inputTokens(lastThousandsSeparatorIndex - 1) match {
        case _: AtMostAsManyDigits => false
        case _: ExactlyAsManyDigits => false
        case _ => true
      })) {
      TypeCheckResult.TypeCheckFailure(noDigitToLeftOfRightmostThousandsSeparator())
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
    val beforeDecimalPoint = new StringBuilder()
    val afterDecimalPoint = new StringBuilder()
    var reachedDecimalPoint = false
    // Number of times that the input specified a negative result, such as with a minus sign.
    var negateResult = 0
    // This is an index into the characters of the provided input string.
    var inputIndex = 0
    // This is an index into the tokens of the provided format string.
    var formatIndex = 0

    // Iterate through the tokens representing the provided format string, in order.
    while (formatIndex < inputTokens.size) {
      val token: InputToken = inputTokens(formatIndex)
      token match {
        case DigitGroups(expected: Seq[InputToken]) =>
          // Consume characters from the current input index forwards in the input string as long as
          // they are digits (0-9) or the thousands separator (,). Then split these characters into
          // groups by the thousands separator (,).
          // For example, string "123,456,789" becomes array ("789", "456", "123").
          val actual: Array[String] = inputStr.drop(inputIndex).takeWhile {
            char => (char >= ZERO_DIGIT && char <= NINE_DIGIT) || char == COMMA_SIGN
          }.split(COMMA_SIGN).reverse
          // For each expected group of digits (0-9), throw an exception if the corresponding
          // provided set of digits is not the right length.
          // For example, if the reversed expected input is
          // DigitGroups(ExactlyAsManyDigits(3), AtMostAsManyDigits(3)) as a result of parsing the
          // format string "999,099", then this matches the example above.
          for ((expected: InputToken, index: Int) <- expected.reverse.zipWithIndex) {
            if (index < actual.length) {
              expected match {
                case ExactlyAsManyDigits(num) if (actual(index).length != num) =>
                  throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
                case AtMostAsManyDigits(num) if (actual(index).length > num) =>
                  throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
                case _ =>
              }
            } else if (expected.isInstanceOf[ExactlyAsManyDigits]) {
              throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
            }
          }
          inputIndex += actual.map{_.length + 1}.sum - 1
        case DecimalPoint() =>
          if (inputStr(inputIndex) == POINT_SIGN || inputStr(inputIndex) == POINT_LETTER) {
            reachedDecimalPoint = true
            inputIndex += 1
          } else {
            // There is no decimal point. Consume the token and remain at the same character in the
            // input string.
          }
        case ThousandsSeparator() =>
          if (inputStr(inputIndex) != COMMA_SIGN && inputStr(inputIndex) != COMMA_LETTER) {
            throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
          }
          inputIndex += 1
        case DollarSign() =>
          if (inputStr(inputIndex) != DOLLAR_LETTER && inputStr(inputIndex) != DOLLAR_SIGN) {
            throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
          }
          inputIndex += 1
        case OptionalPlusOrMinusSign() =>
          if (inputStr(inputIndex) == PLUS_SIGN) {
            inputIndex += 1
          } else if (inputStr(inputIndex) == MINUS_SIGN) {
            negateResult += 1
            inputIndex += 1
          } else {
            // There is no plus or minus sign. Consume the token and remain at the same character in
            // the input string.
          }
        case OptionalMinusSign() =>
          if (inputStr(inputIndex) == MINUS_SIGN) {
            negateResult += 1
            inputIndex += 1
          } else {
            // There is no minus sign. Consume the token and remain at the same character in the
            // input string.
          }
        case OpeningAngleBracket() =>
          if (inputStr(inputIndex) != ANGLE_BRACKET_OPEN) {
            throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
          }
          inputIndex += 1
        case ClosingAngleBracket() =>
          if (inputStr(inputIndex) != ANGLE_BRACKET_CLOSE) {
            throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
          }
          negateResult += 1
          inputIndex += 1
      }
      formatIndex += 1
    }
    // If we have consumed all the tokens in the format string, but characters remain unconsumed in
    // the input string, then the input string does not match the format string.
    if (inputIndex < inputStr.length) {
      throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
    }
    // Consume all digits before the decimal point into the unscaled value.
    var unscaled: Long = 0
    if (beforeDecimalPoint.nonEmpty) {
      unscaled += beforeDecimalPoint.toLong
    }
    // Append zeros to the afterDecimalPoint until it comprises the same number of digits as the
    // scale. This is necessary because we must determine the scale from the format string alone but
    // each input string may include a variable number of digits after the decimal point.
    for (i <- afterDecimalPoint.length until scale) {
      afterDecimalPoint += '0'
    }
    // For all digits after the decimal point, multiply the unscaled value by ten and then add the
    // new digits in.
    if (afterDecimalPoint.nonEmpty) {
      for (i <- 0 until afterDecimalPoint.length) {
        unscaled *= 10
      }
      unscaled += afterDecimalPoint.toLong
    }
    // Negate the result if the input contained relevant patterns such as a negative sign.
    if (negateResult % 2 == 1) {
      unscaled *= -1
    }
    Decimal(unscaled, precision, scale)
  }
}
