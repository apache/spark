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
import java.text.{DecimalFormat, NumberFormat, ParsePosition}
import java.util.Locale

import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

object NumberUtils {

  private val pointSign = '.'
  private val letterPointSign = 'D'
  private val commaSign = ','
  private val letterCommaSign = 'G'
  private val minusSign = '-'
  private val letterMinusSign = 'S'
  private val dollarSign = '$'

  private val commaSignStr = commaSign.toString

  private def normalize(format: String): String = {
    var notFindDecimalPoint = true
    val normalizedFormat = format.toUpperCase(Locale.ROOT).map {
      case '9' if notFindDecimalPoint => '#'
      case '9' if !notFindDecimalPoint => '0'
      case `letterPointSign` =>
        notFindDecimalPoint = false
        pointSign
      case `letterCommaSign` => commaSign
      case `letterMinusSign` => minusSign
      case `pointSign` =>
        notFindDecimalPoint = false
        pointSign
      case other => other
    }
    // If the comma is at the beginning or end of number format, then DecimalFormat will be invalid.
    // For example, "##,###," or ",###,###" for DecimalFormat is invalid, so we must use "##,###"
    // or "###,###".
    normalizedFormat.stripPrefix(commaSignStr).stripSuffix(commaSignStr)
  }

  private def isSign(c: Char): Boolean = {
    Set(pointSign, commaSign, minusSign, dollarSign).contains(c)
  }

  private def transform(format: String): String = {
    if (format.contains(minusSign)) {
      val positiveFormatString = format.replaceAll("-", "")
      s"$positiveFormatString;$format"
    } else {
      format
    }
  }

  private def check(normalizedFormat: String, numberFormat: String) = {
    def invalidSignPosition(format: String, c: Char): Boolean = {
      val signIndex = format.indexOf(c)
      signIndex > 0 && signIndex < format.length - 1
    }

    if (normalizedFormat.count(_ == pointSign) > 1) {
      throw QueryCompilationErrors.multipleSignInNumberFormatError(
        s"'$letterPointSign' or '$pointSign'", numberFormat)
    } else if (normalizedFormat.count(_ == minusSign) > 1) {
      throw QueryCompilationErrors.multipleSignInNumberFormatError(
        s"'$letterMinusSign' or '$minusSign'", numberFormat)
    } else if (normalizedFormat.count(_ == dollarSign) > 1) {
      throw QueryCompilationErrors.multipleSignInNumberFormatError(s"'$dollarSign'", numberFormat)
    } else if (invalidSignPosition(normalizedFormat, minusSign)) {
      throw QueryCompilationErrors.nonFistOrLastCharInNumberFormatError(
        s"'$letterMinusSign' or '$minusSign'", numberFormat)
    } else if (invalidSignPosition(normalizedFormat, dollarSign)) {
      throw QueryCompilationErrors.nonFistOrLastCharInNumberFormatError(
        s"'$dollarSign'", numberFormat)
    }
  }

  /**
   * Convert string to numeric based on the given number format.
   * The format can consist of the following characters:
   * '9':  digit position (can be dropped if insignificant)
   * '0':  digit position (will not be dropped, even if insignificant)
   * '.':  decimal point (only allowed once)
   * ',':  group (thousands) separator
   * 'S':  sign anchored to number (uses locale)
   * 'D':  decimal point (uses locale)
   * 'G':  group separator (uses locale)
   * '$':  specifies that the input value has a leading $ (Dollar) sign.
   *
   * @param input the string need to converted
   * @param numberFormat the given number format
   * @return decimal obtained from string parsing
   */
  def parse(input: UTF8String, numberFormat: String): Decimal = {
    val normalizedFormat = normalize(numberFormat)
    check(normalizedFormat, numberFormat)

    val precision = normalizedFormat.filterNot(isSign).length
    val formatSplits = normalizedFormat.split(pointSign)
    val scale = if (formatSplits.length == 1) {
      0
    } else {
      formatSplits(1).filterNot(isSign).length
    }
    val transformedFormat = transform(normalizedFormat)
    val numberFormatInstance = NumberFormat.getInstance()
    val numberDecimalFormat = numberFormatInstance.asInstanceOf[DecimalFormat]
    numberDecimalFormat.setParseBigDecimal(true)
    numberDecimalFormat.applyPattern(transformedFormat)
    val inputStr = input.toString.trim
    val inputSplits = inputStr.split(pointSign)
    if (inputSplits.length == 1) {
      if (inputStr.filterNot(isSign).length > precision - scale) {
        throw QueryExecutionErrors.invalidNumberFormatError(numberFormat)
      }
    } else if (inputSplits(0).filterNot(isSign).length > precision - scale ||
      inputSplits(1).filterNot(isSign).length > scale) {
      throw QueryExecutionErrors.invalidNumberFormatError(numberFormat)
    }
    val number = numberDecimalFormat.parse(inputStr, new ParsePosition(0))
    Decimal(number.asInstanceOf[BigDecimal])
  }

  /**
   * Convert numeric to string based on the given number format.
   * The format can consist of the following characters:
   * '9':  digit position (can be dropped if insignificant)
   * '0':  digit position (will not be dropped, even if insignificant)
   * '.':  decimal point (only allowed once)
   * ',':  group (thousands) separator
   * 'S':  sign anchored to number (uses locale)
   * 'D':  decimal point (uses locale)
   * 'G':  group separator (uses locale)
   * '$':  specifies that the input value has a leading $ (Dollar) sign.
   *
   * @param input the decimal to format
   * @param numberFormat the format string
   * @return The string after formatting input decimal
   */
  def format(input: Decimal, numberFormat: String): String = {
    val normalizedFormat = normalize(numberFormat)
    check(normalizedFormat, numberFormat)

    val transformedFormat = transform(normalizedFormat)
    val bigDecimal = input.toJavaBigDecimal
    val decimalPlainStr = bigDecimal.toPlainString
    if (decimalPlainStr.length > transformedFormat.length) {
      transformedFormat.replaceAll("0", "#")
    } else {
      val decimalFormat = new DecimalFormat(transformedFormat)
      var resultStr = decimalFormat.format(bigDecimal)
      // Since we trimmed the comma at the beginning or end of number format in function
      // `normalize`, we restore the comma to the result here.
      // For example, if the specified number format is "99,999," or ",999,999", function
      // `normalize` normalize them to "##,###" or "###,###".
      // new DecimalFormat("##,###").parse(12454) and new DecimalFormat("###,###").parse(124546)
      // will return "12,454" and "124,546" respectively. So we add ',' at the end and head of
      // the result, then the final output are "12,454," or ",124,546".
      if (numberFormat.last == commaSign || numberFormat.last == letterCommaSign) {
        resultStr = resultStr + commaSign
      }
      if (numberFormat.charAt(0) == commaSign || numberFormat.charAt(0) == letterCommaSign) {
        resultStr = commaSign + resultStr
      }

      resultStr
    }
  }

}
