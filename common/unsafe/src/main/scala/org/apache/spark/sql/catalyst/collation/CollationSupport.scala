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

package org.apache.spark.sql.catalyst.collation

import java.util.regex.Pattern

import com.ibm.icu.text.SearchIterator

import org.apache.spark.sql.catalyst.collation.CollationFactory.Collation
import org.apache.spark.unsafe.types.UTF8String

trait CollationAwareStringExpressionBase {
  val expressionName: String
  def methodPath: String = s"CollationSupport.$expressionName.exec"
  def getCollation(collationId: Int): Collation = CollationFactory.fetchCollation(collationId)
}

trait UnaryCollationAwareStringExpression[T1, R] extends CollationAwareStringExpressionBase {
  def exec(input: T1, collationId: Int): R = {
    val collation = getCollation(collationId)
    if (collation.supportsBinaryEquality) {
      execBinary(input)
    } else if (collation.supportsLowercaseEquality) {
      execLowercase(input)
    } else {
      execICU(input, collationId)
    }
  }

  def genCode(input: String, collationId: Int): String = {
    val collation = getCollation(collationId)
    if (collation.supportsBinaryEquality) {
      s"${methodPath}Binary($input)"
    } else if (collation.supportsLowercaseEquality) {
      s"${methodPath}Lowercase($input)"
    } else {
      s"${methodPath}ICU($input, $collationId)"
    }
  }

  def execBinary(input: T1): R
  def execLowercase(input: T1): R
  def execICU(input: T1, collationId: Int): R
}

trait BinaryCollationAwareStringExpression[T1, T2, R] extends CollationAwareStringExpressionBase {
  def exec(input1: T1, input2: T2, collationId: Int): R = {
    val collation = getCollation(collationId)
    if (collation.supportsBinaryEquality) {
      execBinary(input1, input2)
    } else if (collation.supportsLowercaseEquality) {
      execLowercase(input1, input2)
    } else {
      execICU(input1, input2, collationId)
    }
  }

  def genCode(input1: String, input2: String, collationId: Int): String = {
    val collation = getCollation(collationId)
    if (collation.supportsBinaryEquality) {
      s"${methodPath}Binary($input1, $input2)"
    } else if (collation.supportsLowercaseEquality) {
      s"${methodPath}Lowercase($input1, $input2)"
    } else {
      s"${methodPath}ICU($input1, $input2, $collationId)"
    }
  }

  def execBinary(input1: T1, input2: T2): R
  def execLowercase(input1: T1, input2: T2): R
  def execICU(input1: T1, input2: T2, collationId: Int): R
}

trait TernaryCollationAwareStringExpression[T1, T2, T3, R]
  extends CollationAwareStringExpressionBase {
  def exec(input1: T1, input2: T2, input3: T3, collationId: Int): R = {
    val collation = getCollation(collationId)
    if (collation.supportsBinaryEquality) {
      execBinary(input1, input2, input3)
    } else if (collation.supportsLowercaseEquality) {
      execLowercase(input1, input2, input3)
    } else {
      execICU(input1, input2, input3, collationId)
    }
  }

  def genCode(input1: String, input2: String, input3: String, collationId: Int): String = {
    val collation = getCollation(collationId)
    if (collation.supportsBinaryEquality) {
      s"${methodPath}Binary($input1, $input2, $input3)"
    } else if (collation.supportsLowercaseEquality) {
      s"${methodPath}Lowercase($input1, $input2, $input3)"
    } else {
      s"${methodPath}ICU($input1, $input2, $input3, $collationId)"
    }
  }

  def execBinary(input1: T1, input2: T2, input3: T3): R
  def execLowercase(input1: T1, input2: T2, input3: T3): R
  def execICU(input1: T1, input2: T2, input3: T3, collationId: Int): R
}

object CollationSupport {
  object StringSplitSQL
    extends BinaryCollationAwareStringExpression[UTF8String, UTF8String, Array[UTF8String]] {
    val expressionName: String = "StringSplitSQL"

    override def execBinary(string: UTF8String, delimiter: UTF8String): Array[UTF8String] =
      string.splitSQL(delimiter, -1)

    override def execLowercase(string: UTF8String, input2: UTF8String): Array[UTF8String] =
      CollationAwareUTF8String.lowercaseSplitSQL(string, input2, -1)

    override def execICU(
                          string: UTF8String,
                          delimiter: UTF8String,
                          collationId: Int): Array[UTF8String] =
      CollationAwareUTF8String.icuSplitSQL(string, delimiter, -1, collationId)
  }

  object Contains
    extends BinaryCollationAwareStringExpression[UTF8String, UTF8String, Boolean] {
    val expressionName: String = "Contains"

    override def execBinary(l: UTF8String, r: UTF8String): Boolean =
      l.contains(r)

    override def execLowercase(l: UTF8String, r: UTF8String): Boolean =
      CollationAwareUTF8String.lowercaseContains(l, r)

    override def execICU(
                          l: UTF8String,
                          r: UTF8String,
                          collationId: Int): Boolean = {
      if (r.numBytes == 0) return true
      if (l.numBytes == 0) return false
      val stringSearch = CollationFactory.getStringSearch(l, r, collationId)
      stringSearch.first != SearchIterator.DONE
    }
  }

  object StartsWith
    extends BinaryCollationAwareStringExpression[UTF8String, UTF8String, Boolean] {
    val expressionName: String = "StartsWith"

    override def execBinary(l: UTF8String, r: UTF8String): Boolean =
      l.startsWith(r)

    override def execLowercase(l: UTF8String, r: UTF8String): Boolean =
      CollationAwareUTF8String.lowercaseStartsWith(l, r)

    override def execICU(
                          l: UTF8String,
                          r: UTF8String,
                          collationId: Int): Boolean = {
      if (r.numBytes == 0) return true
      if (l.numBytes == 0) return false
      val stringSearch = CollationFactory.getStringSearch(l, r, collationId)
      stringSearch.first == 0
    }
  }

  object EndsWith
    extends BinaryCollationAwareStringExpression[UTF8String, UTF8String, Boolean] {
    val expressionName: String = "EndsWith"

    override def execBinary(l: UTF8String, r: UTF8String): Boolean =
      l.endsWith(r)

    override def execLowercase(l: UTF8String, r: UTF8String): Boolean =
      CollationAwareUTF8String.lowercaseEndsWith(l, r)

    override def execICU(
                          l: UTF8String,
                          r: UTF8String,
                          collationId: Int): Boolean = {
      if (r.numBytes == 0) return true
      if (l.numBytes == 0) return false
      val stringSearch = CollationFactory.getStringSearch(l, r, collationId)
      val endIndex = stringSearch.getTarget.getEndIndex
      stringSearch.last == endIndex - stringSearch.getMatchLength
    }
  }

  object Upper extends BinaryCollationAwareStringExpression[UTF8String, Boolean, UTF8String] {
    val expressionName: String = "Upper"

    override def exec(v: UTF8String, useICU: Boolean, collationId: Int): UTF8String = {
      if (getCollation(collationId).supportsBinaryOrdering && useICU) {
        execBinaryICU(v)
      } else {
        super.exec(v, useICU, collationId)
      }
    }

    override def execBinary(v: UTF8String, useICU: Boolean): UTF8String =
      // scalastyle:off
      v.toUpperCase
    // scalastyle:on

    def execBinaryICU(v: UTF8String): UTF8String =
      CollationAwareUTF8String.toUpperCase(v)

    override def execLowercase(v: UTF8String, useICU: Boolean): UTF8String =
      CollationAwareUTF8String.toUpperCase(v)

    override def execICU(v: UTF8String, useICU: Boolean, collationId: Int): UTF8String =
      CollationAwareUTF8String.toUpperCase(v, collationId)
  }

  object Lower extends BinaryCollationAwareStringExpression[UTF8String, Boolean, UTF8String] {
    val expressionName: String = "Lower"

    override def exec(v: UTF8String, useICU: Boolean, collationId: Int): UTF8String = {
      if (getCollation(collationId).supportsBinaryOrdering && useICU) {
        execBinaryICU(v)
      } else {
        super.exec(v, useICU, collationId)
      }
    }

    override def execBinary(v: UTF8String, useICU: Boolean): UTF8String =
      // scalastyle:off
      v.toLowerCase
    // scalastyle:on

    def execBinaryICU(v: UTF8String): UTF8String =
      CollationAwareUTF8String.toLowerCase(v)

    override def execLowercase(v: UTF8String, useICU: Boolean): UTF8String =
      CollationAwareUTF8String.toLowerCase(v)

    override def execICU(v: UTF8String, useICU: Boolean, collationId: Int): UTF8String =
      CollationAwareUTF8String.toLowerCase(v, collationId)
  }

  object InitCap extends BinaryCollationAwareStringExpression[UTF8String, Boolean, UTF8String] {
    val expressionName: String = "InitCap"

    override def exec(v: UTF8String, useICU: Boolean, collationId: Int): UTF8String = {
      if (getCollation(collationId).supportsBinaryOrdering && useICU) {
        execBinaryICU(v)
      } else {
        super.exec(v, useICU, collationId)
      }
    }

    override def execBinary(v: UTF8String, useICU: Boolean): UTF8String =
      // scalastyle:off
      v.toLowerCase.toTitleCase
    // scalastyle:on

    def execBinaryICU(v: UTF8String): UTF8String =
      CollationAwareUTF8String.toTitleCaseICU(v)

    override def execLowercase(v: UTF8String, useICU: Boolean): UTF8String =
      CollationAwareUTF8String.toTitleCase(v)

    override def execICU(v: UTF8String, useICU: Boolean, collationId: Int): UTF8String =
      CollationAwareUTF8String.toTitleCase(v, collationId)
  }

  object FindInSet extends BinaryCollationAwareStringExpression[UTF8String, UTF8String, Int] {
    val expressionName: String = "FindInSet"

    override def exec(word: UTF8String, set: UTF8String, collationId: Int): Int = {
      val collation = getCollation(collationId)
      if (collation.supportsBinaryEquality) {
        execBinary(word, set)
      } else {
        execICU(word, set, collationId)
      }
    }

    override def genCode(word: String, set: String, collationId: Int): String = {
      val collation = getCollation(collationId)
      if (collation.supportsBinaryEquality) {
        s"${methodPath}Binary($word, $set)"
      } else {
        s"${methodPath}ICU($word, $set, $collationId)"
      }
    }

    override def execBinary(word: UTF8String, set: UTF8String): Int = {
      set.findInSet(word)
    }

    override def execLowercase(l: UTF8String, r: UTF8String): Int =
      throw new UnsupportedOperationException("FindInSet does not support lowercase comparison")

    override def execICU(
                          word: UTF8String,
                          set: UTF8String,
                          collationId: Int): Int =
      CollationAwareUTF8String.findInSet(word, set, collationId)
  }

  object StringInstr extends BinaryCollationAwareStringExpression[UTF8String, UTF8String, Int] {
    val expressionName: String = "StringInstr"

    override def execBinary(string: UTF8String, substring: UTF8String): Int =
      string.indexOf(substring, 0)

    override def execLowercase(string: UTF8String, substring: UTF8String): Int =
      CollationAwareUTF8String.lowercaseIndexOf(string, substring, 0)

    override def execICU(
                          string: UTF8String,
                          substring: UTF8String,
                          collationId: Int): Int =
      CollationAwareUTF8String.indexOf(string, substring, 0, collationId)
  }

  object StringReplace
    extends TernaryCollationAwareStringExpression[UTF8String, UTF8String, UTF8String, UTF8String] {
    val expressionName: String = "StringReplace"

    override def execBinary(src: UTF8String, search: UTF8String, replace: UTF8String): UTF8String =
      src.replace(search, replace)

    override def execLowercase(
                                src: UTF8String,
                                search: UTF8String,
                                replace: UTF8String): UTF8String =
      CollationAwareUTF8String.lowercaseReplace(src, search, replace)

    override def execICU(
                          src: UTF8String,
                          search: UTF8String,
                          replace: UTF8String,
                          collationId: Int): UTF8String =
      CollationAwareUTF8String.replace(src, search, replace, collationId)
  }

  object StringLocate
    extends TernaryCollationAwareStringExpression[UTF8String, UTF8String, Int, Int] {
    val expressionName: String = "StringLocate"

    override def execBinary(string: UTF8String, substring: UTF8String, start: Int): Int =
      string.indexOf(substring, start)

    override def execLowercase(string: UTF8String, substring: UTF8String, start: Int): Int =
      CollationAwareUTF8String.lowercaseIndexOf(string, substring, start)

    override def execICU(
                          string: UTF8String,
                          substring: UTF8String,
                          start: Int,
                          collationId: Int): Int =
      CollationAwareUTF8String.indexOf(string, substring, start, collationId)
  }

  object SubstringIndex
    extends TernaryCollationAwareStringExpression[UTF8String, UTF8String, Int, UTF8String] {
    val expressionName: String = "SubstringIndex"

    override def execBinary(string: UTF8String, delimiter: UTF8String, count: Int): UTF8String =
      string.subStringIndex(delimiter, count)

    override def execLowercase(string: UTF8String, delimiter: UTF8String, count: Int): UTF8String =
      CollationAwareUTF8String.lowercaseSubStringIndex(string, delimiter, count)

    override def execICU(
                          string: UTF8String,
                          delimiter: UTF8String,
                          count: Int,
                          collationId: Int): UTF8String =
      CollationAwareUTF8String.subStringIndex(string, delimiter, count, collationId)
  }

  object StringTranslate
    extends BinaryCollationAwareStringExpression[
      UTF8String, java.util.Map[String, String], UTF8String] {

    val expressionName: String = "StringTranslate"

    override def execBinary(src: UTF8String, dict: java.util.Map[String, String]): UTF8String =
      src.translate(dict)

    override def execLowercase(src: UTF8String, dict: java.util.Map[String, String]): UTF8String =
      CollationAwareUTF8String.lowercaseTranslate(src, dict)

    override def execICU(
                          src: UTF8String,
                          dict: java.util.Map[String, String],
                          collationId: Int): UTF8String =
      CollationAwareUTF8String.translate(src, dict, collationId)
  }

  object StringTrim
    extends BinaryCollationAwareStringExpression[UTF8String, UTF8String, UTF8String] {
    val expressionName: String = "StringTrim"

    def exec(src: UTF8String): UTF8String =
      src.trim

    def genCode(src: String): String = {
      s"$methodPath($src)"
    }

    override def execBinary(src: UTF8String, trimString: UTF8String): UTF8String =
      src.trim(trimString)

    override def execLowercase(src: UTF8String, trimString: UTF8String): UTF8String =
      CollationAwareUTF8String.lowercaseTrim(src, trimString)

    override def execICU(
                          src: UTF8String,
                          trimString: UTF8String,
                          collationId: Int): UTF8String =
      CollationAwareUTF8String.trim(src, trimString, collationId)
  }


  object StringTrimLeft
    extends BinaryCollationAwareStringExpression[UTF8String, UTF8String, UTF8String] {
    val expressionName: String = "StringTrimLeft"

    def exec(src: UTF8String): UTF8String =
      src.trimLeft

    def genCode(src: String): String = {
      s"$methodPath($src)"
    }

    override def execBinary(src: UTF8String, trimString: UTF8String): UTF8String =
      src.trimLeft(trimString)

    override def execLowercase(src: UTF8String, trimString: UTF8String): UTF8String =
      CollationAwareUTF8String.lowercaseTrimLeft(src, trimString)

    override def execICU(
                          src: UTF8String,
                          trimString: UTF8String,
                          collationId: Int): UTF8String =
      CollationAwareUTF8String.trimLeft(src, trimString, collationId)
  }


  object StringTrimRight
    extends BinaryCollationAwareStringExpression[UTF8String, UTF8String, UTF8String] {
    val expressionName: String = "StringTrimRight"

    def exec(src: UTF8String): UTF8String =
      src.trimRight

    def genCode(src: String): String =
      s"$methodPath($src)"

    override def execBinary(src: UTF8String, trimString: UTF8String): UTF8String =
      src.trimRight(trimString)

    override def execLowercase(src: UTF8String, trimString: UTF8String): UTF8String =
      CollationAwareUTF8String.lowercaseTrimRight(src, trimString)

    override def execICU(
                          src: UTF8String,
                          trimString: UTF8String,
                          collationId: Int): UTF8String =
      CollationAwareUTF8String.trimRight(src, trimString, collationId)
  }

  object CollationAwareRegexp {
    def supportsLowercaseRegex(collationId: Int): Boolean =
      CollationFactory.fetchCollation(collationId).supportsLowercaseEquality

    val lowercaseRegexFlags: Int = Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE

    def collationAwareRegexFlags(collationId: Int): Int =
      if (supportsLowercaseRegex(collationId)) lowercaseRegexFlags
      else 0

    private val lowercaseRegexPrefix =
      UTF8String.fromString("(?ui)")

    def lowercaseRegex(regex: UTF8String): UTF8String =
      UTF8String.concat(lowercaseRegexPrefix, regex)

    def collationAwareRegex(regex: UTF8String, collationId: Int): UTF8String =
      if (supportsLowercaseRegex(collationId)) lowercaseRegex(regex)
      else regex
  }
}

// TODO: Add other collation-aware expressions.
