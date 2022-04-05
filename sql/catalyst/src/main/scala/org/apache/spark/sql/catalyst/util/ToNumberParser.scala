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
  final val WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_START = 'P'
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
 * @param originNumberFormat the format string describing the expected inputs.
 * @param errorOnFail true if evaluation should throw an exception if the input string fails to
 *                    match the format string. Otherwise, returns NULL instead.
 */
class ToNumberParser(originNumberFormat: String, errorOnFail: Boolean) extends Serializable {
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
        case OPTIONAL_MINUS_STRING_START if i < len - 1 &&
          OPTIONAL_MINUS_STRING_END == originNumberFormat(i + 1) =>
          tokens :+= OptionalMinusSign()
          i += 2
        case WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_START if i < len - 1 &&
          WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_END == originNumberFormat(i + 1) =>
          tokens +:= OpeningAngleBracket()
          tokens :+= ClosingAngleBracket()
          i += 2
        case c: Char =>
          tokens :+= InvalidUnrecognizedCharacter(c)
          i += 1
      }
    }

    // Combine each group of consecutive Digits and ThousandsSeparator tokens into a DigitGroups.
    i = 0
    var groupedTokens = mutable.Seq.empty[InputToken]
    while (i < tokens.length) {
      val suffix = tokens.drop(i)
      val gatheredTokens = suffix.takeWhile {
        case _: Digits | _: ThousandsSeparator => true
        case _ => false
      }
      if (gatheredTokens.nonEmpty) {
        groupedTokens :+= DigitGroups(gatheredTokens.reverse)
        i += gatheredTokens.length
      } else {
        groupedTokens :+= tokens(i)
        i += 1
      }
    }
    groupedTokens
  }

  /**
   * Precision is the number of digits in a number. Scale is the number of digits to the right of
   * the decimal point in a number. For example, the number 123.45 has a precision of 5 and a
   * scale of 2.
   */
  private lazy val precision = {
    val lengths = inputTokens.map {
      case DigitGroups(tokens) => tokens.map {
        case ExactlyAsManyDigits(num) => num
        case AtMostAsManyDigits(num) => num
        case _: ThousandsSeparator => 0
      }.sum
      case _ => 0
    }
    lengths.sum
  }

  private lazy val scale = {
    val index = inputTokens.indexOf(DecimalPoint())
    if (index != -1) {
      val decimalPointIndex = inputTokens.indexOf(DecimalPoint())
      val suffix: Seq[InputToken] = inputTokens.drop(decimalPointIndex)
      val lengths: Seq[Int] = suffix.map {
        case DigitGroups(tokens) => tokens.map {
          case ExactlyAsManyDigits(num) => num
          case AtMostAsManyDigits(num) => num
        }.sum
        case _ => 0
      }
      lengths.sum
    } else {
      0
    }
  }

  /**
   * The result type of this parsing is a Decimal value with the appropriate precision and scale.
   */
  def parsedDecimalType: DecimalType = DecimalType(precision, scale)

  /**
   * Consumes the format string to check validity and computes an appropriate Decimal output type.
   */
  def check(): TypeCheckResult = {
    val validateResult: String = validateFormatString
    if (validateResult.nonEmpty) {
      TypeCheckResult.TypeCheckFailure(validateResult)
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  /**
   * This implementation of the [[check]] method returns any error, or the empty string on success.
   */
  private def validateFormatString: String = {
    def multipleSignInNumberFormatError(message: String) = {
      s"At most one $message is allowed in the number format: '$originNumberFormat'"
    }

    def notAtEndOfNumberFormatError(message: String) = {
      s"$message must be at the end of the number format: '$originNumberFormat'"
    }

    def inputTokenCounts = inputTokens.groupBy(identity).mapValues(_.size)

    val firstDollarSignIndex: Int = inputTokens.indexOf(DollarSign())
    val firstDigitIndex: Int = inputTokens.indexWhere {
      case _: DigitGroups => true
      case _ => false
    }
    val firstDecimalPointIndex: Int = inputTokens.indexOf(DecimalPoint())
    val digitGroupsBeforeDecimalPoint: Seq[DigitGroups] = inputTokens.zipWithIndex.flatMap {
      case (d@DigitGroups(_), i) if firstDecimalPointIndex == -1 || i < firstDecimalPointIndex =>
        Seq(d)
      case _ => Seq()
    }
    val digitGroupsAfterDecimalPoint: Seq[DigitGroups] = inputTokens.zipWithIndex.flatMap {
      case (d@DigitGroups(_), i) if firstDecimalPointIndex != -1 && i > firstDecimalPointIndex =>
        Seq(d)
      case _ => Seq()
    }

    // Make sure the format string contains at least one token.
    if (originNumberFormat.isEmpty) {
      "The format string cannot be empty"
    }
    // Make sure the format string does not contain any unrecognized characters.
    else if (inputTokens.exists(_.isInstanceOf[InvalidUnrecognizedCharacter])) {
      val unrecognizedChars = inputTokens.filter(_.isInstanceOf[InvalidUnrecognizedCharacter]).map {
        case i: InvalidUnrecognizedCharacter => i.char
      }
      val char: Char = unrecognizedChars.head
      s"Encountered invalid character $char in the number format: '$originNumberFormat'"
    }
    // Make sure the format string contains at least one digit.
    else if (!inputTokens.exists(token => token.isInstanceOf[DigitGroups])) {
      "The format string requires at least one number digit"
    }
    // Make sure the format string contains at most one decimal point.
    else if (inputTokenCounts.getOrElse(DecimalPoint(), 0) > 1) {
      multipleSignInNumberFormatError(s"'$POINT_LETTER' or '$POINT_SIGN'")
    }
    // Make sure the format string contains at most one plus or minus sign.
    else if (inputTokenCounts.getOrElse(OptionalPlusOrMinusSign(), 0) > 1) {
      multipleSignInNumberFormatError(s"'$OPTIONAL_PLUS_OR_MINUS_LETTER'")
    }
    // Make sure the format string contains at most one dollar sign.
    else if (inputTokenCounts.getOrElse(DollarSign(), 0) > 1) {
      multipleSignInNumberFormatError(s"'$DOLLAR_SIGN'")
    }
    // Make sure the format string contains at most one minus sign at the end.
    else if (inputTokenCounts.getOrElse(OptionalMinusSign(), 0) > 1 ||
      (inputTokenCounts.getOrElse(OptionalMinusSign(), 0) == 1 &&
        inputTokens.last != OptionalMinusSign())) {
      notAtEndOfNumberFormatError(s"'$OPTIONAL_MINUS_STRING'")
    }
    // Make sure the format string contains at most one closing angle bracket at the end.
    else if (inputTokenCounts.getOrElse(ClosingAngleBracket(), 0) > 1 ||
      (inputTokenCounts.getOrElse(ClosingAngleBracket(), 0) == 1 &&
        inputTokens.last != ClosingAngleBracket())) {
      notAtEndOfNumberFormatError(s"'$WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER'")
    }
    // Make sure that any dollar sign in the format string occurs before any digits.
    else if (firstDigitIndex < firstDollarSignIndex) {
      s"Currency characters must appear before digits in the number format: '$originNumberFormat'"
    }
    // Make sure that any dollar sign in the format string occurs before any decimal point.
    else if (firstDecimalPointIndex != -1 && firstDecimalPointIndex < firstDollarSignIndex) {
      "Currency characters must appear before any decimal point in the " +
        s"number format: '$originNumberFormat'"
    }
    // Make sure that any thousands separators in the format string have digits before and after.
    else if (digitGroupsBeforeDecimalPoint.exists {
      case DigitGroups(tokens) =>
        tokens.zipWithIndex.exists({
          case (_: ThousandsSeparator, j: Int) if j == 0 || j == tokens.length - 1 =>
            true
          case (_: ThousandsSeparator, j: Int) if tokens(j - 1).isInstanceOf[ThousandsSeparator] =>
            true
          case (_: ThousandsSeparator, j: Int) if tokens(j + 1).isInstanceOf[ThousandsSeparator] =>
            true
          case _ =>
            false
        })
    }) {
      "Thousands separators (,) must have digits in between them " +
        s"in the number format: '$originNumberFormat'"
    }
    // Thousands separators are not allowed after the decimal point, if any.
    else if (digitGroupsAfterDecimalPoint.exists {
      case DigitGroups(tokens) => tokens.exists(_.isInstanceOf[ThousandsSeparator])
    }) {
      "Thousands separators (,) may not appear after the decimal point " +
        s"in the number format: '$originNumberFormat'"
    }
    // Validation of the format string finished successfully.
    else {
      ""
    }
  }

  /**
   * Convert string to numeric based on the given number format.
   *
   * Iterates through the [[inputTokens]] obtained from processing the format string, while also
   * keeping a parallel index into the input string. Throws an exception if the latter does not
   * contain expected characters at any point.
   *
   * @param input the string that needs to converted
   * @return the result Decimal value obtained from string parsing
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
        case d@DigitGroups(_) =>
          val expectedTokens: Seq[InputToken] = d.tokens.filter(_.isInstanceOf[Digits])
          // Consume characters from the current input index forwards in the input string as long as
          // they are digits (0-9) or the thousands separator (,). Then split these characters into
          // groups by the thousands separator (,). For example, the input string
          // "456,789" becomes the actualTokens array ("789", "456").
          val actualTokens: Array[String] = inputStr.drop(inputIndex).takeWhile {
            char => (char >= ZERO_DIGIT && char <= NINE_DIGIT) || char == COMMA_SIGN
          }.split(COMMA_SIGN).reverse
          // For each expected group of digits (0-9), throw an exception if the corresponding
          // provided set of digits is not the right length.
          // For example, if the format string is "999,099" and the corresponding reversed expected
          // input is DigitGroups(ExactlyAsManyDigits(3), AtMostAsManyDigits(3)), then this matches
          // the example above.
          if (actualTokens.size > expectedTokens.size) {
            // The input contains more thousands separators than the format string.
            return formatMatchFailure(input, originNumberFormat)
          }
          for ((expectedToken, i) <- expectedTokens.zipWithIndex) {
            val actualTokenLength = if (i < actualTokens.length) actualTokens(i).length else 0
            expectedToken match {
              case ExactlyAsManyDigits(num) if actualTokenLength != num =>
                // The input contained more or fewer digits than required.
                return formatMatchFailure(input, originNumberFormat)
              case AtMostAsManyDigits(num) if actualTokenLength > num =>
                // The input contained more digits than allowed.
                return formatMatchFailure(input, originNumberFormat)
              case _ =>
            }
          }
          // Advance the input string index by the length of each group of digits we encountered in
          // the input string above (plus one for each thousands separator). During this process,
          // append each group of input digits to the appropriate before/afterDecimalPoint string
          // for later use in constructing the result Decimal value.
          for ((actualToken: String, i: Int) <- actualTokens.reverse.zipWithIndex) {
            inputIndex += actualToken.length
            if (i < actualTokens.size - 1) {
              inputIndex += 1
            }
            if (reachedDecimalPoint) {
              afterDecimalPoint ++= actualToken
            } else {
              beforeDecimalPoint ++= actualToken
            }
          }
        case DecimalPoint() =>
          if (inputIndex < inputStr.length &&
            (inputStr(inputIndex) == POINT_SIGN || inputStr(inputIndex) == POINT_LETTER)) {
            reachedDecimalPoint = true
            inputIndex += 1
          } else {
            // There is no decimal point. Consume the token and remain at the same character in the
            // input string.
          }
        case DollarSign() =>
          if (inputIndex >= inputStr.length ||
            (inputStr(inputIndex) != DOLLAR_LETTER && inputStr(inputIndex) != DOLLAR_SIGN)) {
            // The input string did not contain an expected dollar sign.
            return formatMatchFailure(input, originNumberFormat)
          }
          inputIndex += 1
        case OptionalPlusOrMinusSign() =>
          if (inputIndex < inputStr.length &&
            inputStr(inputIndex) == PLUS_SIGN) {
            inputIndex += 1
          } else if (inputIndex < inputStr.length &&
            inputStr(inputIndex) == MINUS_SIGN) {
            negateResult += 1
            inputIndex += 1
          } else {
            // There is no plus or minus sign. Consume the token and remain at the same character in
            // the input string.
          }
        case OptionalMinusSign() =>
          if (inputIndex < inputStr.length &&
            inputStr(inputIndex) == MINUS_SIGN) {
            negateResult += 1
            inputIndex += 1
          } else {
            // There is no minus sign. Consume the token and remain at the same character in the
            // input string.
          }
        case OpeningAngleBracket() =>
          if (inputIndex >= inputStr.length ||
            inputStr(inputIndex) != ANGLE_BRACKET_OPEN) {
            // The input string did not contain an expected opening angle bracket.
            return formatMatchFailure(input, originNumberFormat)
          }
          inputIndex += 1
        case ClosingAngleBracket() =>
          if (inputIndex >= inputStr.length ||
            inputStr(inputIndex) != ANGLE_BRACKET_CLOSE) {
            // The input string did not contain an expected closing angle bracket.
            return formatMatchFailure(input, originNumberFormat)
          }
          negateResult += 1
          inputIndex += 1
      }
      formatIndex += 1
    }
    if (inputIndex < inputStr.length) {
      // If we have consumed all the tokens in the format string, but characters remain unconsumed
      // in the input string, then the input string does not match the format string.
      formatMatchFailure(input, originNumberFormat)
    } else {
      getDecimal(beforeDecimalPoint.toString(), afterDecimalPoint.toString(), negateResult)
    }
  }

  /**
   * This method executes when the input string fails to match the format string. It throws an
   * exception if indicated on construction of this class, or returns NULL otherwise.
   */
  private def formatMatchFailure(input: UTF8String, originNumberFormat: String): Decimal = {
    if (errorOnFail) {
      throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
    }
    null
  }

  /**
   * Computes the final Decimal value as a result of parsing.
   *
   * @param beforeDecimalPoint digits (0-9) that appeared before any decimal point (.)
   * @param afterDecimalPoint digits (0-9) that appeared after the decimal point (.), if any
   * @param negateResult how many times the input string specified to negate the result
   * @return a Decimal value with the value indicated by the input string and the precision and
   *         scale indicated by the format string
   */
  private def getDecimal(
      beforeDecimalPoint: String,
      afterDecimalPoint: String,
      negateResult: Int): Decimal = {
    // Consume all digits before the decimal point into the unscaled value.
    var unscaled: Long = 0
    if (beforeDecimalPoint.nonEmpty) {
      unscaled += beforeDecimalPoint.toLong
    }
    // Append zeros to the afterDecimalPoint until it comprises the same number of digits as the
    // scale. This is necessary because we must determine the scale from the format string alone but
    // each input string may include a variable number of digits after the decimal point.
    val extraZeros = "0" * (scale - afterDecimalPoint.length)
    val afterDecimalPadded = afterDecimalPoint + extraZeros
    // For all digits after the decimal point, multiply the unscaled value by ten and then add the
    // new digits in.
    if (afterDecimalPadded.nonEmpty) {
      for (i <- 0 until afterDecimalPadded.length) {
        unscaled *= 10
      }
      unscaled += afterDecimalPadded.toLong
    }
    // Negate the result if the input contained relevant patterns such as a negative sign.
    if (negateResult % 2 == 1) {
      unscaled *= -1
    }
    Decimal(unscaled, precision, scale)
  }
}
