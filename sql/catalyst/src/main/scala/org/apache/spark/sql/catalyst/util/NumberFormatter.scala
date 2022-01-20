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

import java.math.BigDecimal
import java.text.{DecimalFormat, ParsePosition}
import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{Decimal, DecimalType}
import org.apache.spark.unsafe.types.UTF8String

object NumberFormatter {
  final val POINT_SIGN = '.'
  final val POINT_LETTER = 'D'
  final val COMMA_SIGN = ','
  final val COMMA_LETTER = 'G'
  final val MINUS_SIGN = '-'
  final val MINUS_LETTER = 'S'
  final val DOLLAR_SIGN = '$'
  final val NINE_DIGIT = '9'
  final val ZERO_DIGIT = '0'
  final val POUND_SIGN = '#'

  final val COMMA_SIGN_STRING = COMMA_SIGN.toString
  final val POUND_SIGN_STRING = POUND_SIGN.toString

  final val SIGN_SET = Set(POINT_SIGN, COMMA_SIGN, MINUS_SIGN, DOLLAR_SIGN)
}

class NumberFormatter(originNumberFormat: String, isParse: Boolean = true) extends Serializable {
  import NumberFormatter._

  protected val normalizedNumberFormat = normalize(originNumberFormat)

  private val transformedFormat = transform(normalizedNumberFormat)

  private lazy val numberDecimalFormat = {
    val decimalFormat = new DecimalFormat(transformedFormat)
    decimalFormat.setParseBigDecimal(true)
    decimalFormat
  }

  private lazy val (precision, scale) = {
    val formatSplits = normalizedNumberFormat.split(POINT_SIGN).map(_.filterNot(isSign))
    assert(formatSplits.length <= 2)
    val precision = formatSplits.map(_.length).sum
    val scale = if (formatSplits.length == 2) formatSplits.last.length else 0
    (precision, scale)
  }

  def parsedDecimalType: DecimalType = DecimalType(precision, scale)

  /**
   * DecimalFormat provides '#' and '0' as placeholder of digit, ',' as grouping separator,
   * '.' as decimal separator, '-' as minus, '$' as dollar, but not '9', 'G', 'D', 'S'. So we need
   * replace them show below:
   * 1. '9' -> '#'
   * 2. 'G' -> ','
   * 3. 'D' -> '.'
   * 4. 'S' -> '-'
   *
   * Note: When calling format, we must preserve the digits after decimal point, so the digits
   * after decimal point should be replaced as '0'. For example: '999.9' will be normalized as
   * '###.0' and '999.99' will be normalized as '###.00', so if the input is 454, the format
   * output will be 454.0 and 454.00 respectively.
   *
   * @param format number format string
   * @return normalized number format string
   */
  private def normalize(format: String): String = {
    var notFindDecimalPoint = true
    val normalizedFormat = format.toUpperCase(Locale.ROOT).map {
      case NINE_DIGIT if notFindDecimalPoint => POUND_SIGN
      case ZERO_DIGIT if isParse && notFindDecimalPoint => POUND_SIGN
      case NINE_DIGIT if !notFindDecimalPoint => ZERO_DIGIT
      case COMMA_LETTER => COMMA_SIGN
      case POINT_LETTER | POINT_SIGN =>
        notFindDecimalPoint = false
        POINT_SIGN
      case MINUS_LETTER => MINUS_SIGN
      case other => other
    }
    // If the comma is at the beginning or end of number format, then DecimalFormat will be
    // invalid. For example, "##,###," or ",###,###" for DecimalFormat is invalid, so we must use
    // "##,###" or "###,###".
    normalizedFormat.stripPrefix(COMMA_SIGN_STRING).stripSuffix(COMMA_SIGN_STRING)
  }

  private def isSign(c: Char): Boolean = {
    SIGN_SET.contains(c)
  }

  private def transform(format: String): String = {
    if (format.contains(MINUS_SIGN)) {
      // For example: '#.######' represents a positive number,
      // but '#.######;#.######-' represents a negative number.
      val positiveFormatString = format.replaceAll("-", "")
      s"$positiveFormatString;$format"
    } else {
      format
    }
  }

  def check(): TypeCheckResult = {
    def invalidSignPosition(c: Char): Boolean = {
      val signIndex = normalizedNumberFormat.indexOf(c)
      signIndex > 0 && signIndex < normalizedNumberFormat.length - 1
    }

    def multipleSignInNumberFormatError(message: String): String = {
      s"At most one $message is allowed in the number format: '$originNumberFormat'"
    }

    def nonFistOrLastCharInNumberFormatError(message: String): String = {
      s"$message must be the first or last char in the number format: '$originNumberFormat'"
    }

    if (normalizedNumberFormat.length == 0) {
      TypeCheckResult.TypeCheckFailure("Number format cannot be empty")
    } else if (normalizedNumberFormat.count(_ == POINT_SIGN) > 1) {
      TypeCheckResult.TypeCheckFailure(
        multipleSignInNumberFormatError(s"'$POINT_LETTER' or '$POINT_SIGN'"))
    } else if (normalizedNumberFormat.count(_ == MINUS_SIGN) > 1) {
      TypeCheckResult.TypeCheckFailure(
        multipleSignInNumberFormatError(s"'$MINUS_LETTER' or '$MINUS_SIGN'"))
    } else if (normalizedNumberFormat.count(_ == DOLLAR_SIGN) > 1) {
      TypeCheckResult.TypeCheckFailure(multipleSignInNumberFormatError(s"'$DOLLAR_SIGN'"))
    } else if (invalidSignPosition(MINUS_SIGN)) {
      TypeCheckResult.TypeCheckFailure(
        nonFistOrLastCharInNumberFormatError(s"'$MINUS_LETTER' or '$MINUS_SIGN'"))
    } else if (invalidSignPosition(DOLLAR_SIGN)) {
      TypeCheckResult.TypeCheckFailure(
        nonFistOrLastCharInNumberFormatError(s"'$DOLLAR_SIGN'"))
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  /**
   * Convert string to numeric based on the given number format.
   * The format can consist of the following characters:
   * '0' or '9': digit position
   * '.' or 'D': decimal point (only allowed once)
   * ',' or 'G': group (thousands) separator
   * '-' or 'S': sign anchored to number (only allowed once)
   * '$': value with a leading dollar sign (only allowed once)
   *
   * @param input the string need to converted
   * @return decimal obtained from string parsing
   */
  def parse(input: UTF8String): Decimal = {
    val inputStr = input.toString.trim
    val inputSplits = inputStr.split(POINT_SIGN)
    assert(inputSplits.length <= 2)
    if (inputSplits.length == 1) {
      if (inputStr.filterNot(isSign).length > precision - scale) {
        throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
      }
    } else if (inputSplits(0).filterNot(isSign).length > precision - scale ||
      inputSplits(1).filterNot(isSign).length > scale) {
      throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
    }

    try {
      val number = numberDecimalFormat.parse(inputStr, new ParsePosition(0))
      assert(number.isInstanceOf[BigDecimal])
      Decimal(number.asInstanceOf[BigDecimal])
    } catch {
      case _: IllegalArgumentException =>
        throw QueryExecutionErrors.invalidNumberFormatError(input, originNumberFormat)
    }
  }

  /**
   * Convert numeric to string based on the given number format.
   * The format can consist of the following characters:
   * '9': digit position (can be dropped if insignificant)
   * '0': digit position (will not be dropped, even if insignificant)
   * '.' or 'D': decimal point (only allowed once)
   * ',' or 'G': group (thousands) separator
   * '-' or 'S': sign anchored to number (only allowed once)
   * '$': value with a leading dollar sign (only allowed once)
   *
   * @param input the decimal to format
   * @param numberFormat the format string
   * @return The string after formatting input decimal
   */
  def format(input: Decimal): String = {
    val bigDecimal = input.toJavaBigDecimal
    val decimalPlainStr = bigDecimal.toPlainString
    if (decimalPlainStr.length > transformedFormat.length) {
      transformedFormat.replaceAll("0", POUND_SIGN_STRING)
    } else {
      var resultStr = numberDecimalFormat.format(bigDecimal)
      // Since we trimmed the comma at the beginning or end of number format in function
      // `normalize`, we restore the comma to the result here.
      // For example, if the specified number format is "99,999," or ",999,999", function
      // `normalize` normalize them to "##,###" or "###,###".
      // new DecimalFormat("##,###").parse(12454) and new DecimalFormat("###,###").parse(124546)
      // will return "12,454" and "124,546" respectively. So we add ',' at the end and head of
      // the result, then the final output are "12,454," or ",124,546".
      if (originNumberFormat.last == COMMA_SIGN || originNumberFormat.last == COMMA_LETTER) {
        resultStr = resultStr + COMMA_SIGN
      }
      if (originNumberFormat.charAt(0) == COMMA_SIGN ||
        originNumberFormat.charAt(0) == COMMA_LETTER) {
        resultStr = COMMA_SIGN + resultStr
      }

      resultStr
    }
  }
}

// Visible for testing
class TestNumberFormatter(originNumberFormat: String, isParse: Boolean = true)
  extends NumberFormatter(originNumberFormat, isParse) {
  def checkWithException(): Unit = {
    check() match {
      case TypeCheckResult.TypeCheckFailure(message) =>
        throw new AnalysisException(message)
      case _ =>
    }
  }
}
