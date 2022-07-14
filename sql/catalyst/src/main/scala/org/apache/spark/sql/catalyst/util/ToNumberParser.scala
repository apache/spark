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
  final val DOLLAR_SIGN = '$'
  final val MINUS_SIGN = '-'
  final val NINE_DIGIT = '9'
  final val OPTIONAL_PLUS_OR_MINUS_LETTER = 'S'
  final val PLUS_SIGN = '+'
  final val POINT_LETTER = 'D'
  final val POINT_SIGN = '.'
  final val POUND_SIGN = '#'
  final val SPACE = ' '
  final val ZERO_DIGIT = '0'

  final val OPTIONAL_MINUS_STRING = "MI"
  final val WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER = "PR"

  final val OPTIONAL_MINUS_STRING_START = 'M'
  final val OPTIONAL_MINUS_STRING_END = 'I'

  final val WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_START = 'P'
  final val WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_END = 'R'

  // This class represents one or more characters that we expect to be present in the input string
  // based on the format string. The toString method returns a representation of each token suitable
  // for use in error messages.
  abstract class InputToken()
  // Represents some number of digits (0-9).
  abstract class Digits extends InputToken
  // Represents exactly 'num' digits (0-9).
  case class ExactlyAsManyDigits(num: Int) extends Digits {
    override def toString: String = "digit sequence"
  }
  // Represents at most 'num' digits (0-9).
  case class AtMostAsManyDigits(num: Int) extends Digits {
    override def toString: String = "digit sequence"
  }
  // Represents one decimal point (.).
  case class DecimalPoint() extends InputToken {
    override def toString: String = ". or D"
  }
  // Represents one thousands separator (,).
  case class ThousandsSeparator() extends InputToken {
    override def toString: String = ", or G"
  }
  // Represents one or more groups of Digits (0-9) with ThousandsSeparators (,) between each group.
  // The 'tokens' are the Digits and ThousandsSeparators in order; the 'digits' are just the Digits.
  case class DigitGroups(tokens: Seq[InputToken], digits: Seq[Digits]) extends InputToken {
    override def toString: String = "digit sequence"
  }
  // Represents one dollar sign ($).
  case class DollarSign() extends InputToken {
    override def toString: String = "$"
  }
  // Represents one optional plus sign (+) or minus sign (-).
  case class OptionalPlusOrMinusSign() extends InputToken {
    override def toString: String = "S"
  }
  // Represents one optional minus sign (-).
  case class OptionalMinusSign() extends InputToken {
    override def toString: String = "MI"
  }
  // Represents one opening angle bracket (<).
  case class OpeningAngleBracket() extends InputToken {
    override def toString: String = "PR"
  }
  // Represents one closing angle bracket (>).
  case class ClosingAngleBracket() extends InputToken {
    override def toString: String = "PR"
  }
  // Represents any unrecognized character other than the above.
  case class InvalidUnrecognizedCharacter(char: Char) extends InputToken {
    override def toString: String = s"character '$char''"
  }
}

/**
 * This class represents a parser to implement the to_number or try_to_number SQL functions.
 *
 * It works by consuming an input string and a format string. This class accepts the format string
 * as a field, and proceeds to iterate through the format string to generate a sequence of tokens
 * (or throw an exception if the format string is invalid). Then when the function is called with an
 * input string, this class steps through the sequence of tokens and compares them against the input
 * string, returning a Spark Decimal object if they match (or throwing an exception otherwise).
 *
 * @param numberFormat the format string describing the expected inputs.
 * @param errorOnFail true if evaluation should throw an exception if the input string fails to
 *                    match the format string. Otherwise, returns NULL instead.
 */
class ToNumberParser(numberFormat: String, errorOnFail: Boolean) extends Serializable {
  import ToNumberParser._

  // Consumes the format string and produce a sequence of input tokens expected from each input
  // string.
  private lazy val formatTokens: Seq[InputToken] = {
    val tokens = mutable.Buffer.empty[InputToken]
    var i = 0
    var reachedDecimalPoint = false
    val len = numberFormat.length
    while (i < len) {
      val char: Char = numberFormat(i)
      char match {
        case ZERO_DIGIT =>
          val prevI = i
          do {
            i += 1
          } while (i < len && (numberFormat(i) == ZERO_DIGIT || numberFormat(i) == NINE_DIGIT))
          if (reachedDecimalPoint) {
            tokens.append(AtMostAsManyDigits(i - prevI))
          } else {
            tokens.append(ExactlyAsManyDigits(i - prevI))
          }
        case NINE_DIGIT =>
          val prevI = i
          do {
            i += 1
          } while (i < len && (numberFormat(i) == ZERO_DIGIT || numberFormat(i) == NINE_DIGIT))
          tokens.append(AtMostAsManyDigits(i - prevI))
        case POINT_SIGN | POINT_LETTER =>
          tokens.append(DecimalPoint())
          reachedDecimalPoint = true
          i += 1
        case COMMA_SIGN | COMMA_LETTER =>
          tokens.append(ThousandsSeparator())
          i += 1
        case DOLLAR_SIGN =>
          tokens.append(DollarSign())
          i += 1
        case OPTIONAL_PLUS_OR_MINUS_LETTER =>
          tokens.append(OptionalPlusOrMinusSign())
          i += 1
        case OPTIONAL_MINUS_STRING_START if i < len - 1 &&
          OPTIONAL_MINUS_STRING_END == numberFormat(i + 1) =>
          tokens.append(OptionalMinusSign())
          i += 2
        case WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_START if i < len - 1 &&
          WRAPPING_ANGLE_BRACKETS_TO_NEGATIVE_NUMBER_END == numberFormat(i + 1) =>
          tokens.prepend(OpeningAngleBracket())
          tokens.append(ClosingAngleBracket())
          i += 2
        case c: Char =>
          tokens.append(InvalidUnrecognizedCharacter(c))
          i += 1
      }
    }

    // Combine each group of consecutive Digits and ThousandsSeparator tokens into a DigitGroups.
    val groupedTokens = mutable.Buffer.empty[InputToken]
    var currentGroup = mutable.Buffer.empty[InputToken]
    var currentDigits = mutable.Buffer.empty[Digits]
    for (token <- tokens) {
      token match {
        case digits: Digits =>
          currentGroup.append(token)
          currentDigits.append(digits)
        case _: ThousandsSeparator =>
          currentGroup.append(token)
        case other =>
          if (currentGroup.nonEmpty) {
            // We reverse the expected digit tokens in this new DigitGroups here, and we do the same
            // for actual groups of 0-9 characters in each input string. In this way, we can safely
            // ignore any leading optional groups of digits in the format string.
            groupedTokens.append(
              DigitGroups(currentGroup.reverse.toSeq, currentDigits.reverse.toSeq))
            currentGroup = mutable.Buffer.empty[InputToken]
            currentDigits = mutable.Buffer.empty[Digits]
          }
          groupedTokens.append(other)
      }
    }
    if (currentGroup.nonEmpty) {
      groupedTokens.append(DigitGroups(currentGroup.reverse.toSeq, currentDigits.reverse.toSeq))
    }
    groupedTokens.toSeq
  }

  /**
   * Precision is the number of digits in a number. Scale is the number of digits to the right of
   * the decimal point in a number. For example, the number 123.45 has a precision of 5 and a
   * scale of 2.
   */
  private lazy val precision: Int = {
    val lengths = formatTokens.map {
      case DigitGroups(_, digits) => digits.map {
        case ExactlyAsManyDigits(num) => num
        case AtMostAsManyDigits(num) => num
      }.sum
      case _ => 0
    }
    lengths.sum
  }

  private lazy val scale: Int = {
    val index = formatTokens.indexOf(DecimalPoint())
    if (index != -1) {
      val suffix: Seq[InputToken] = formatTokens.drop(index)
      val lengths: Seq[Int] = suffix.map {
        case DigitGroups(_, digits) => digits.map {
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

  // Holds all digits (0-9) before the decimal point (.) while parsing each input string.
  private lazy val parsedBeforeDecimalPoint = new StringBuilder(precision)
  // Holds all digits (0-9) after the decimal point (.) while parsing each input string.
  private lazy val parsedAfterDecimalPoint = new StringBuilder(scale)
  // Number of digits (0-9) in each group of the input string, split by thousands separators.
  private lazy val parsedDigitGroupSizes = mutable.Buffer.empty[Int]
  // Increments to count the number of digits (0-9) in the current group within the input string.
  private var parsedNumDigitsInCurrentGroup: Int = 0
  // These are indexes into the characters of the input string before and after the decimal point.
  private var formattingBeforeDecimalPointIndex = 0
  private var formattingAfterDecimalPointIndex = 0

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
    val firstDollarSignIndex: Int = formatTokens.indexOf(DollarSign())
    val firstDigitIndex: Int = formatTokens.indexWhere {
      case _: DigitGroups => true
      case _ => false
    }
    val firstDecimalPointIndex: Int = formatTokens.indexOf(DecimalPoint())
    val digitGroupsBeforeDecimalPoint: Seq[DigitGroups] =
      formatTokens.zipWithIndex.flatMap {
        case (d@DigitGroups(_, _), i)
          if firstDecimalPointIndex == -1 ||
            i < firstDecimalPointIndex =>
          Seq(d)
        case _ => Seq()
      }
    val digitGroupsAfterDecimalPoint: Seq[DigitGroups] =
      formatTokens.zipWithIndex.flatMap {
        case (d@DigitGroups(_, _), i)
          if firstDecimalPointIndex != -1 &&
            i > firstDecimalPointIndex =>
          Seq(d)
        case _ => Seq()
      }

    // Make sure the format string contains at least one token.
    if (numberFormat.isEmpty) {
      return "The format string cannot be empty"
    }
    // Make sure the format string contains at least one digit.
    if (!formatTokens.exists(
      token => token.isInstanceOf[DigitGroups])) {
      return "The format string requires at least one number digit"
    }
    // Make sure that any dollar sign in the format string occurs before any digits.
    if (firstDigitIndex < firstDollarSignIndex) {
      return s"Currency characters must appear before digits in the number format: '$numberFormat'"
    }
    // Make sure that any dollar sign in the format string occurs before any decimal point.
    if (firstDecimalPointIndex != -1 &&
      firstDecimalPointIndex < firstDollarSignIndex) {
      return "Currency characters must appear before any decimal point in the " +
        s"number format: '$numberFormat'"
    }
    // Make sure that any thousands separators in the format string have digits before and after.
    if (digitGroupsBeforeDecimalPoint.exists {
      case DigitGroups(tokens, _) =>
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
      return "Thousands separators (, or G) must have digits in between them " +
        s"in the number format: '$numberFormat'"
    }
    // Make sure that thousands separators does not appear after the decimal point, if any.
    if (digitGroupsAfterDecimalPoint.exists {
      case DigitGroups(tokens, digits) =>
        tokens.length > digits.length
    }) {
      return "Thousands separators (, or G) may not appear after the decimal point " +
        s"in the number format: '$numberFormat'"
    }
    // Make sure that the format string does not contain any prohibited duplicate tokens.
    val inputTokenCounts = formatTokens.groupBy(identity).mapValues(_.size)
    Seq(DecimalPoint(),
      OptionalPlusOrMinusSign(),
      OptionalMinusSign(),
      DollarSign(),
      ClosingAngleBracket()).foreach {
      token => if (inputTokenCounts.getOrElse(token, 0) > 1) {
        return s"At most one ${token.toString} is allowed in the number format: '$numberFormat'"
      }
    }
    // Enforce the ordering of tokens in the format string according to this specification:
    // [ MI | S ] [ $ ]
    // [ 0 | 9 | G | , ] [...]
    // [ . | D ]
    // [ 0 | 9 ] [...]
    // [ $ ] [ PR | MI | S ]
    val allowedFormatTokens: Seq[Seq[InputToken]] = Seq(
      Seq(OpeningAngleBracket()),
      Seq(OptionalMinusSign(), OptionalPlusOrMinusSign()),
      Seq(DollarSign()),
      Seq(DigitGroups(Seq(), Seq())),
      Seq(DecimalPoint()),
      Seq(DigitGroups(Seq(), Seq())),
      Seq(DollarSign()),
      Seq(OptionalMinusSign(), OptionalPlusOrMinusSign(), ClosingAngleBracket())
    )
    var formatTokenIndex = 0
    for (allowedTokens: Seq[InputToken] <- allowedFormatTokens) {
      def tokensMatch(lhs: InputToken, rhs: InputToken): Boolean = {
        lhs match {
          case _: DigitGroups => rhs.isInstanceOf[DigitGroups]
          case _ => lhs == rhs
        }
      }
      if (formatTokenIndex < formatTokens.length &&
        allowedTokens.exists(tokensMatch(_, formatTokens(formatTokenIndex)))) {
        formatTokenIndex += 1
      }
    }
    if (formatTokenIndex < formatTokens.length) {
      return s"Unexpected ${formatTokens(formatTokenIndex).toString} found in the format string " +
        s"'$numberFormat'; the structure of the format string must match: " +
        "[MI|S] [$] [0|9|G|,]* [.|D] [0|9]* [$] [PR|MI|S]"
    }
    // Validation of the format string finished successfully.
    ""
  }

  /**
   * Convert string to numeric based on the given number format.
   *
   * Iterates through the [[formatTokens]] obtained from processing the format string, while also
   * keeping a parallel index into the input string. Throws an exception if the latter does not
   * contain expected characters at any point.
   *
   * @param input the string that needs to converted
   * @return the result Decimal value obtained from string parsing
   */
  def parse(input: UTF8String): Decimal = {
    val inputString = input.toString
    val inputLength = inputString.length
    // Build strings representing all digits before and after the decimal point, respectively.
    parsedBeforeDecimalPoint.clear()
    parsedAfterDecimalPoint.clear()
    // Tracks whether we've reached the decimal point yet in either parsing or formatting.
    var reachedDecimalPoint = false
    // Record whether we have consumed opening angle bracket characters in the input string.
    var reachedOpeningAngleBracket = false
    var reachedClosingAngleBracket = false
    // Record whether the input specified a negative result, such as with a minus sign.
    var negateResult = false
    // This is an index into the characters of the provided input string.
    var inputIndex = 0
    // This is an index into the tokens of the provided format string.
    var formatIndex = 0

    // Iterate through the tokens representing the provided format string, in order.
    while (formatIndex < formatTokens.size) {
      val token: InputToken = formatTokens(formatIndex)
      val inputChar: Option[Char] =
        if (inputIndex < inputLength) {
          Some(inputString(inputIndex))
        } else {
          Option.empty[Char]
        }
      token match {
        case d: DigitGroups =>
          inputIndex = parseDigitGroups(d, inputString, inputIndex, reachedDecimalPoint).getOrElse(
            return formatMatchFailure(input, numberFormat))
        case DecimalPoint() =>
          inputChar.foreach {
            case POINT_SIGN =>
              reachedDecimalPoint = true
              inputIndex += 1
            case _ =>
              // There is no decimal point. Consume the token and remain at the same character in
              // the input string.
          }
        case DollarSign() =>
          inputChar.foreach {
            case DOLLAR_SIGN =>
              inputIndex += 1
            case _ =>
              // The input string did not contain an expected dollar sign.
              return formatMatchFailure(input, numberFormat)
          }
        case OptionalPlusOrMinusSign() =>
          inputChar.foreach {
            case PLUS_SIGN =>
              inputIndex += 1
            case MINUS_SIGN =>
              negateResult = !negateResult
              inputIndex += 1
            case _ =>
              // There is no plus or minus sign. Consume the token and remain at the same character
              // in the input string.
          }
        case OptionalMinusSign() =>
          inputChar.foreach {
            case MINUS_SIGN =>
              negateResult = !negateResult
              inputIndex += 1
            case _ =>
              // There is no minus sign. Consume the token and remain at the same character in the
              // input string.
          }
        case OpeningAngleBracket() =>
          inputChar.foreach {
            case ANGLE_BRACKET_OPEN =>
              if (reachedOpeningAngleBracket) {
                return formatMatchFailure(input, numberFormat)
              }
              reachedOpeningAngleBracket = true
              inputIndex += 1
            case _ =>
          }
        case ClosingAngleBracket() =>
          inputChar.foreach {
            case ANGLE_BRACKET_CLOSE =>
              if (!reachedOpeningAngleBracket) {
                return formatMatchFailure(input, numberFormat)
              }
              reachedClosingAngleBracket = true
              negateResult = !negateResult
              inputIndex += 1
            case _ =>
          }
      }
      formatIndex += 1
    }
    if (inputIndex < inputLength ||
      reachedOpeningAngleBracket != reachedClosingAngleBracket) {
      // If we have consumed all the tokens in the format string, but characters remain unconsumed
      // in the input string, then the input string does not match the format string.
      formatMatchFailure(input, numberFormat)
    } else {
      parseResultToDecimalValue(negateResult)
    }
  }

  /**
   * Handle parsing the input string for the given expected DigitGroups from the format string.
   *
   * @param digitGroups the expected DigitGroups from the format string
   * @param inputString the input string provided to the original parsing method
   * @param startingInputIndex the input index within the input string to begin parsing here
   * @param reachedDecimalPoint true if we have already parsed past the decimal point
   * @return the new updated index within the input string to resume parsing, or None on error
   */
  private def parseDigitGroups(
      digitGroups: DigitGroups,
      inputString: String,
      startingInputIndex: Int,
      reachedDecimalPoint: Boolean): Option[Int] = {
    val expectedDigits: Seq[Digits] = digitGroups.digits
    val inputLength = inputString.length
    // Consume characters from the current input index forwards in the input string as long as
    // they are digits (0-9) or the thousands separator (,).
    parsedNumDigitsInCurrentGroup = 0
    var inputIndex = startingInputIndex
    parsedDigitGroupSizes.clear()

    while (inputIndex < inputLength &&
      parsedCharMatchesDigitOrComma(inputString(inputIndex), reachedDecimalPoint)) {
      inputIndex += 1
    }
    if (inputIndex == inputLength) {
      parsedDigitGroupSizes.prepend(parsedNumDigitsInCurrentGroup)
    }
    // Compare the number of digits encountered in each group (separated by thousands
    // separators) with the expected numbers from the format string.
    if (parsedDigitGroupSizes.length > expectedDigits.length) {
      // The input contains more thousands separators than the format string.
      return None
    }
    for (i <- expectedDigits.indices) {
      val expectedToken: Digits = expectedDigits(i)
      val actualNumDigits: Int =
        if (i < parsedDigitGroupSizes.length) {
          parsedDigitGroupSizes(i)
        } else {
          0
        }
      expectedToken match {
        case ExactlyAsManyDigits(expectedNumDigits)
          if actualNumDigits != expectedNumDigits =>
          // The input contained more or fewer digits than required.
          return None
        case AtMostAsManyDigits(expectedMaxDigits)
          if actualNumDigits > expectedMaxDigits =>
          // The input contained more digits than allowed.
          return None
        case _ =>
      }
    }
    Some(inputIndex)
  }

  /**
   * Returns true if the given character matches a digit (0-9) or a comma, updating fields of
   * this class related to parsing during the process.
   */
  private def parsedCharMatchesDigitOrComma(char: Char, reachedDecimalPoint: Boolean): Boolean = {
    char match {
      case _ if char.isWhitespace =>
        // Ignore whitespace and keep advancing through the input string.
        true
      case _ if char.isDigit =>
        parsedNumDigitsInCurrentGroup += 1
        // Append each group of input digits to the appropriate before/parsedAfterDecimalPoint
        // string for later use in constructing the result Decimal value.
        if (reachedDecimalPoint) {
          parsedAfterDecimalPoint.append(char)
        } else {
          parsedBeforeDecimalPoint.append(char)
        }
        true
      case COMMA_SIGN =>
        parsedDigitGroupSizes.prepend(parsedNumDigitsInCurrentGroup)
        parsedNumDigitsInCurrentGroup = 0
        true
      case _ =>
        parsedDigitGroupSizes.prepend(parsedNumDigitsInCurrentGroup)
        false
    }
  }

  /**
   * This method executes when the input string fails to match the format string. It throws an
   * exception if indicated on construction of this class, or returns NULL otherwise.
   */
  private def formatMatchFailure(input: UTF8String, originNumberFormat: String): Decimal = {
    if (errorOnFail) {
      throw QueryExecutionErrors.invalidNumberFormatError(
        "string", input.toString, originNumberFormat)
    }
    null
  }
  private def formatMatchFailure(input: Decimal, originNumberFormat: String): UTF8String = {
    if (errorOnFail) {
      throw QueryExecutionErrors.invalidNumberFormatError(
        "Decimal value", input.toString, originNumberFormat)
    }
    null
  }

  /**
   * Computes the final Decimal value from the parsedBeforeDecimalPoint and parsedAfterDecimalPoint
   * fields of this class, as a result of parsing.
   *
   * @param negateResult whether the input string specified to negate the result
   * @return a Decimal value with the value indicated by the input string and the precision and
   *         scale indicated by the format string
   */
  private def parseResultToDecimalValue(negateResult: Boolean): Decimal = {
    // Append zeros to the parsedAfterDecimalPoint string until it comprises the same number of
    // digits as the scale. This is necessary because we must determine the scale from the format
    // string alone but each input string may include a variable number of digits after the decimal
    // point.
    val extraZeros = "0" * (scale - parsedAfterDecimalPoint.length)
    val afterDecimalPadded = parsedAfterDecimalPoint.toString + extraZeros
    val prefix = if (negateResult) "-" else ""
    val suffix = if (afterDecimalPadded.nonEmpty) "." + afterDecimalPadded else ""
    val numStr = s"$prefix$parsedBeforeDecimalPoint$suffix"
    val javaDecimal = new java.math.BigDecimal(numStr)
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      // Constructs a `Decimal` with an unscaled `Long` value if possible.
      Decimal(javaDecimal.unscaledValue().longValue(), precision, scale)
    } else {
      // Otherwise, resorts to an unscaled `BigInteger` instead.
      Decimal(javaDecimal, precision, scale)
    }
  }

  /**
   * Converts a decimal value to a string based on the given number format.
   *
   * Iterates through the [[formatTokens]] obtained from processing the format string, while also
   * inspecting the input decimal value.
   *
   * @param input the decimal value that needs to be converted
   * @return the result String value obtained from string formatting
   */
  def format(input: Decimal): UTF8String = {
    val result = new StringBuilder()
    // These are string representations of the input Decimal value.
    val (inputBeforeDecimalPoint: String,
      inputAfterDecimalPoint: String) =
      formatSplitInputBeforeAndAfterDecimalPoint(input)
    // These are indexes into the characters of the input string before and after the decimal point.
    formattingBeforeDecimalPointIndex = 0
    formattingAfterDecimalPointIndex = 0
    var reachedDecimalPoint = false

    // Iterate through the tokens representing the provided format string, in order.
    for (formatToken: InputToken <- formatTokens) {
      formatToken match {
        case groups: DigitGroups =>
          formatDigitGroups(
            groups, inputBeforeDecimalPoint, inputAfterDecimalPoint, reachedDecimalPoint, result)
        case DecimalPoint() =>
          // If the last character so far is a space, change it to a zero. This means the input
          // decimal does not have an integer part.
          if (result.nonEmpty && result.last == SPACE) {
            result(result.length - 1) = ZERO_DIGIT
          }
          result.append(POINT_SIGN)
          reachedDecimalPoint = true
        case DollarSign() =>
          result.append(DOLLAR_SIGN)
        case _: OptionalPlusOrMinusSign =>
          stripTrailingLoneDecimalPoint(result)
          if (input < Decimal.ZERO) {
            addCharacterCheckingTrailingSpaces(result, MINUS_SIGN)
          } else {
            addCharacterCheckingTrailingSpaces(result, PLUS_SIGN)
          }
        case _: OptionalMinusSign =>
          if (input < Decimal.ZERO) {
            stripTrailingLoneDecimalPoint(result)
            addCharacterCheckingTrailingSpaces(result, MINUS_SIGN)
          } else {
            result.append(SPACE)
          }
        case OpeningAngleBracket() =>
          if (input < Decimal.ZERO) {
            result.append(ANGLE_BRACKET_OPEN)
          }
        case ClosingAngleBracket() =>
          stripTrailingLoneDecimalPoint(result)
          if (input < Decimal.ZERO) {
            addCharacterCheckingTrailingSpaces(result, ANGLE_BRACKET_CLOSE)
          } else {
            result.append(SPACE)
            result.append(SPACE)
          }
      }
    }

    if (formattingBeforeDecimalPointIndex < inputBeforeDecimalPoint.length ||
      formattingAfterDecimalPointIndex < inputAfterDecimalPoint.length) {
      // Remaining digits before or after the decimal point exist in the decimal value but not in
      // the format string.
      formatMatchFailure(input, numberFormat)
    } else {
      stripTrailingLoneDecimalPoint(result)
      val str = result.toString
      if (result.isEmpty || str == "+" || str == "-") {
        UTF8String.fromString("0")
      } else {
        UTF8String.fromString(str)
      }
    }
  }

  /**
   * Splits the provided Decimal value's string representation by the decimal point, if any.
   * @param input the Decimal value to consume
   * @return two strings representing the contents before and after the decimal point (if any)
   */
  private def formatSplitInputBeforeAndAfterDecimalPoint(input: Decimal): (String, String) = {
    // Convert the input Decimal value to a string (without exponent notation).
    val inputString = input.toJavaBigDecimal.toPlainString
    // Split the digits before and after the decimal point.
    val tokens: Array[String] = inputString.split(POINT_SIGN)
    var beforeDecimalPoint: String = tokens(0)
    var afterDecimalPoint: String = if (tokens.length > 1) tokens(1) else ""
    // Strip any leading minus sign to consider the digits only.
    // Strip leading and trailing zeros to match cases when the format string begins with a decimal
    // point.
    beforeDecimalPoint = beforeDecimalPoint.dropWhile(c => c == MINUS_SIGN || c == ZERO_DIGIT)
    afterDecimalPoint = afterDecimalPoint.reverse.dropWhile(_ == ZERO_DIGIT).reverse

    // If the format string specifies more digits than the 'beforeDecimalPoint', prepend leading
    // spaces to make them the same length. Likewise, if the format string specifies more digits
    // than the 'afterDecimalPoint', append trailing spaces to make them the same length. This step
    // simplifies logic consuming the format tokens later.
    var reachedDecimalPoint = false
    var numFormatDigitsBeforeDecimalPoint = 0
    var numFormatDigitsAfterDecimalPoint = 0
    formatTokens.foreach {
      case digitGroups: DigitGroups =>
        digitGroups.digits.foreach { digits =>
          val numDigits = digits match {
            case ExactlyAsManyDigits(num) => num
            case AtMostAsManyDigits(num) => num
          }
          for (_ <- 0 until numDigits) {
            if (!reachedDecimalPoint) {
              numFormatDigitsBeforeDecimalPoint += 1
            } else {
              numFormatDigitsAfterDecimalPoint += 1
            }
          }
        }
      case _: DecimalPoint =>
        reachedDecimalPoint = true
      case _ =>
    }
    // If there were more digits in the provided input string (before or after the decimal point)
    // than specified in the format string, this is an overflow.
    if (numFormatDigitsBeforeDecimalPoint < beforeDecimalPoint.length ||
      numFormatDigitsAfterDecimalPoint < afterDecimalPoint.length) {
      beforeDecimalPoint = "#" * numFormatDigitsBeforeDecimalPoint
      afterDecimalPoint = "#" * numFormatDigitsAfterDecimalPoint
    }
    val leadingSpaces = " " * (numFormatDigitsBeforeDecimalPoint - beforeDecimalPoint.length)
    val trailingZeros = "0" * (numFormatDigitsAfterDecimalPoint - afterDecimalPoint.length)
    (leadingSpaces + beforeDecimalPoint, afterDecimalPoint + trailingZeros)
  }

  /**
   * Performs format processing on the digits in [[groups]], updating [[result]].
   *
   * @param groups the token representing a group of digits from the format string
   * @param inputBeforeDecimalPoint string representation of the input decimal value before the
   *                                decimal point
   * @param inputAfterDecimalPoint string representation of the input decimal value after the
   *                               decimal point
   * @param reachedDecimalPoint true if we have reached the decimal point so far during processing
   * @param result the result of formatting is built here as a string during iteration
   */
  private def formatDigitGroups(
      groups: DigitGroups,
      inputBeforeDecimalPoint: String,
      inputAfterDecimalPoint: String,
      reachedDecimalPoint: Boolean,
      result: StringBuilder): Unit = {
    // Iterate through the tokens in the DigitGroups. Reverse the order of the tokens so we
    // consume them in the left-to-right order that they originally appeared in the format
    // string.
    for (digitGroupToken <- groups.tokens.reverse) {
      digitGroupToken match {
        case digits: Digits if !reachedDecimalPoint =>
          val numDigits = digits match {
            case ExactlyAsManyDigits(num) => num
            case AtMostAsManyDigits(num) => num
          }
          for (_ <- 0 until numDigits) {
            inputBeforeDecimalPoint(formattingBeforeDecimalPointIndex) match {
              case SPACE if digits.isInstanceOf[ExactlyAsManyDigits] =>
                // The format string started with a zero and had more digits than the provided
                // input string, so we prepend a zero to the result. Note that there is no need to
                // check for the presence of any previous positive or minus sign in the result
                // because we are adding zeros here and we want them to go directly after such a
                // sign, such as "-00000123.45".
                result.append(ZERO_DIGIT)
              case SPACE =>
                addSpaceCheckingTrailingCharacters(result)
              case c: Char =>
                result.append(c)
            }
            formattingBeforeDecimalPointIndex += 1
          }
        case digits: Digits if reachedDecimalPoint =>
          val numDigits = digits match {
            case ExactlyAsManyDigits(num) => num
            case AtMostAsManyDigits(num) => num
          }
          for (_ <- 0 until numDigits) {
            result.append(inputAfterDecimalPoint(formattingAfterDecimalPointIndex))
            formattingAfterDecimalPointIndex += 1
          }
        case _: ThousandsSeparator =>
          if (result.nonEmpty && result.last.isDigit) {
            result.append(COMMA_SIGN)
          } else {
            addSpaceCheckingTrailingCharacters(result)
          }
      }
    }
  }

  /**
   * Adds a character to the end of the string builder. After doing so, if we just added the
   * character after a space, swap the characters.
   */
  private def addCharacterCheckingTrailingSpaces(result: StringBuilder, char: Char): Unit = {
    result.append(char)
    var i = result.size - 1
    while (i >= 1 &&
      result(i - 1) == SPACE &&
      result(i) == char) {
      result(i) = SPACE
      result(i - 1) = char
      i -= 1
    }
  }

  /**
   * Adds a character to the end of the string builder. After doing so, if we just added the
   * character after cases like unary plus or minus, swap the characters.
   */
  private def addSpaceCheckingTrailingCharacters(result: StringBuilder): Unit = {
    result.append(SPACE)
    var i = result.size - 1
    while (i >= 1 &&
      (result(i - 1) == PLUS_SIGN ||
        result(i - 1) == MINUS_SIGN ||
        result(i - 1) == ANGLE_BRACKET_OPEN) &&
      result(i) == SPACE) {
      result(i) = result(i - 1)
      result(i - 1) = SPACE
      i -= 1
    }
  }

  /**
   * If the result string ends with a decimal point, strip it.
   */
  private def stripTrailingLoneDecimalPoint(result: StringBuilder): Unit = {
    val i = result.indexOf(POINT_SIGN)
    if (i != -1 &&
      (i == result.length - 1 ||
        result(i + 1) == SPACE)) {
      result(i) = SPACE
    }
  }
}
