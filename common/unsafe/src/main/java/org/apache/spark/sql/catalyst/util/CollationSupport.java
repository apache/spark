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
package org.apache.spark.sql.catalyst.util;

import com.ibm.icu.text.StringSearch;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Static entry point for collation-aware expressions (StringExpressions, RegexpExpressions, and
 * other expressions that require custom collation support), as well as private utility methods for
 * collation-aware UTF8String operations needed to implement .
 */
public final class CollationSupport {

  /**
   * Collation-aware string expressions.
   */

  public static class StringSplitSQL {
    public static UTF8String[] exec(UTF8String s, UTF8String d, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        d = collation.trimmer.apply(d);
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(s, d);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(s, d);
      } else {
        return execICU(s, d, collationId);
      }
    }
    public static String genCode(final String s, final String d, final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.splitSQL(%s,-1)", s, d);
      } else {
        return String.format("CollationSupport.StringSplitSQL.exec(%s, %s, %d)", s, d, collationId);
      }
    }
    public static UTF8String[] execBinary(final UTF8String string, final UTF8String delimiter) {
      return string.splitSQL(delimiter, -1);
    }
    public static UTF8String[] execLowercase(final UTF8String string, final UTF8String delimiter) {
      return CollationAwareUTF8String.lowercaseSplitSQL(string, delimiter, -1);
    }
    public static UTF8String[] execICU(final UTF8String string, final UTF8String delimiter,
        final int collationId) {
      return CollationAwareUTF8String.icuSplitSQL(string, delimiter, -1, collationId);
    }
  }

  public static class Contains {
    public static boolean exec(UTF8String l, UTF8String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        l = collation.trimmer.apply(l);
        r = collation.trimmer.apply(r);
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(l, r);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(l, r);
      } else {
        return execICU(l, r, collationId);
      }
    }
    public static String genCode(final String l, final String r, final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.contains(%s)", l, r);
      } else {
        return String.format("CollationSupport.Contains.exec(%s, %s, %d)", l, r, collationId);
      }
    }
    public static boolean execBinary(final UTF8String l, final UTF8String r) {
      return l.contains(r);
    }
    public static boolean execLowercase(final UTF8String l, final UTF8String r) {
      return CollationAwareUTF8String.lowercaseContains(l, r);
    }
    public static boolean execICU(final UTF8String l, final UTF8String r,
        final int collationId) {
      if (r.numBytes() == 0) return true;
      if (l.numBytes() == 0) return false;
      StringSearch stringSearch = CollationFactory.getStringSearch(l, r, collationId);
      return stringSearch.first() != StringSearch.DONE;
    }
  }

  public static class StartsWith {
    public static boolean exec(UTF8String l, UTF8String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        l = collation.trimmer.apply(l);
        r = collation.trimmer.apply(r);
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(l, r);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(l, r);
      } else {
        return execICU(l, r, collationId);
      }
    }
    public static String genCode(final String l, final String r, final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.startsWith(%s)", l, r);
      } else {
        return String.format("CollationSupport.StartsWith.exec(%s, %s, %d)", l, r, collationId);
      }
    }
    public static boolean execBinary(final UTF8String l, final UTF8String r) {
      return l.startsWith(r);
    }
    public static boolean execLowercase(final UTF8String l, final UTF8String r) {
      return CollationAwareUTF8String.lowercaseStartsWith(l, r);
    }
    public static boolean execICU(final UTF8String l, final UTF8String r,
        final int collationId) {
      if (r.numBytes() == 0) return true;
      if (l.numBytes() == 0) return false;
      StringSearch stringSearch = CollationFactory.getStringSearch(l, r, collationId);
      return stringSearch.first() == 0;
    }
  }

  public static class EndsWith {
    public static boolean exec(UTF8String l, UTF8String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        l = collation.trimmer.apply(l);
        r = collation.trimmer.apply(r);
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(l, r);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(l, r);
      } else {
        return execICU(l, r, collationId);
      }
    }
    public static String genCode(final String l, final String r, final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.endsWith(%s)", l, r);
      } else {
        return String.format("CollationSupport.EndsWith.exec(%s, %s, %d)", l, r, collationId);
      }
    }
    public static boolean execBinary(final UTF8String l, final UTF8String r) {
      return l.endsWith(r);
    }
    public static boolean execLowercase(final UTF8String l, final UTF8String r) {
      return CollationAwareUTF8String.lowercaseEndsWith(l, r);
    }
    public static boolean execICU(final UTF8String l, final UTF8String r,
        final int collationId) {
      if (r.numBytes() == 0) return true;
      if (l.numBytes() == 0) return false;
      StringSearch stringSearch = CollationFactory.getStringSearch(l, r, collationId);
      int endIndex = stringSearch.getTarget().getEndIndex();
      return stringSearch.last() == endIndex - stringSearch.getMatchLength();
    }
  }

  public static class Upper {
    public static UTF8String exec(final UTF8String v, final int collationId, boolean useICU) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return useICU ? execBinaryICU(v) : execBinary(v);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(v);
      } else {
        return execICU(v, collationId);
      }
    }
    public static String genCode(final String v, final int collationId, boolean useICU) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.toUpperCase()", v);
      } else {
        return String.format("CollationSupport.Upper.exec(%s, %d, %b)", v, collationId, useICU);
      }
    }
    public static UTF8String execBinary(final UTF8String v) {
      return v.toUpperCase();
    }
    public static UTF8String execBinaryICU(final UTF8String v) {
      return CollationAwareUTF8String.toUpperCase(v);
    }
    public static UTF8String execLowercase(final UTF8String v) {
      return CollationAwareUTF8String.toUpperCase(v);
    }
    public static UTF8String execICU(final UTF8String v, final int collationId) {
      return CollationAwareUTF8String.toUpperCase(v, collationId);
    }
  }

  public static class Lower {
    public static UTF8String exec(final UTF8String v, final int collationId, boolean useICU) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return useICU ? execBinaryICU(v) : execBinary(v);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(v);
      } else {
        return execICU(v, collationId);
      }
    }
    public static String genCode(final String v, final int collationId, boolean useICU) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.toLowerCase()", v);
      } else {
        return String.format("CollationSupport.Lower.exec(%s, %d, %b)", v, collationId, useICU);
      }
    }
    public static UTF8String execBinary(final UTF8String v) {
      return v.toLowerCase();
    }
    public static UTF8String execBinaryICU(final UTF8String v) {
      return CollationAwareUTF8String.toLowerCase(v);
    }
    public static UTF8String execLowercase(final UTF8String v) {
      return CollationAwareUTF8String.toLowerCase(v);
    }
    public static UTF8String execICU(final UTF8String v, final int collationId) {
      return CollationAwareUTF8String.toLowerCase(v, collationId);
    }
  }

  public static class InitCap {
    public static UTF8String exec(final UTF8String v, final int collationId, boolean useICU) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return useICU ? execBinaryICU(v) : execBinary(v);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(v);
      } else {
        return execICU(v, collationId);
      }
    }

    public static String genCode(final String v, final int collationId, boolean useICU) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.toLowerCase().toTitleCase()", v);
      } else {
        return String.format("CollationSupport.InitCap.exec(%s, %d, %b)", v, collationId, useICU);
      }
    }
    public static UTF8String execBinary(final UTF8String v) {
      return v.toLowerCase().toTitleCase();
    }
    public static UTF8String execBinaryICU(final UTF8String v) {
      return CollationAwareUTF8String.toLowerCase(v).toTitleCaseICU();
    }
    public static UTF8String execLowercase(final UTF8String v) {
      return CollationAwareUTF8String.toTitleCase(v);
    }
    public static UTF8String execICU(final UTF8String v, final int collationId) {
      return CollationAwareUTF8String.toTitleCase(v, collationId);
    }
  }

  public static class FindInSet {
    public static int exec(UTF8String word, UTF8String set, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        word = collation.trimmer.apply(word);
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(word, set);
      } else {
        return execCollationAware(word, set, collationId);
      }
    }
    public static String genCode(final String word, final String set, final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.findInSet(%s)", set, word);
      } else {
        return String.format("CollationSupport.FindInSet.exec(%s, %s, %d)", word, set, collationId);
      }
    }
    public static int execBinary(final UTF8String word, final UTF8String set) {
      return set.findInSet(word);
    }
    public static int execCollationAware(final UTF8String word, final UTF8String set,
        final int collationId) {
      return CollationAwareUTF8String.findInSet(word, set, collationId);
    }
  }

  public static class StringInstr {
    public static int exec(UTF8String string, UTF8String substring,
        final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        substring = collation.trimmer.apply(substring);
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(string, substring);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(string, substring);
      } else {
        return execICU(string, substring, collationId);
      }
    }
    public static String genCode(final String string, final String substring,
        final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.indexOf(%s, 0)", string, substring);
      } else {
        return String.format("CollationSupport.StringInstr.exec(%s, %s, %d)", string, substring,
          collationId);
      }
    }
    public static int execBinary(final UTF8String string, final UTF8String substring) {
      return string.indexOf(substring, 0);
    }
    public static int execLowercase(final UTF8String string, final UTF8String substring) {
      return CollationAwareUTF8String.lowercaseIndexOf(string, substring, 0);
    }
    public static int execICU(final UTF8String string, final UTF8String substring,
        final int collationId) {
      return CollationAwareUTF8String.indexOf(string, substring, 0, collationId);
    }
  }

  public static class StringReplace {
    public static UTF8String exec(final UTF8String src, final UTF8String search,
        final UTF8String replace, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(src, search, replace);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(src, search, replace);
      } else {
        return execICU(src, search, replace, collationId);
      }
    }
    public static String genCode(final String src, final String search, final String replace,
        final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.replace(%s, %s)", src, search, replace);
      } else {
      return String.format("CollationSupport.StringReplace.exec(%s, %s, %s, %d)", src, search,
        replace, collationId);
      }
    }
    public static UTF8String execBinary(final UTF8String src, final UTF8String search,
        final UTF8String replace) {
      return src.replace(search, replace);
    }
    public static UTF8String execLowercase(final UTF8String src, final UTF8String search,
        final UTF8String replace) {
      return CollationAwareUTF8String.lowercaseReplace(src, search, replace);
    }
    public static UTF8String execICU(final UTF8String src, final UTF8String search,
        final UTF8String replace, final int collationId) {
      return CollationAwareUTF8String.replace(src, search, replace, collationId);
    }
  }

  public static class StringLocate {
    public static int exec(UTF8String string, UTF8String substring, final int start,
        final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        substring = collation.trimmer.apply(substring);
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(string, substring, start);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(string, substring, start);
      } else {
        return execICU(string, substring, start, collationId);
      }
    }
    public static String genCode(final String string, final String substring, final String start,
        final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.indexOf(%s, %s - 1)", string, substring, start);
      } else {
        return String.format("CollationSupport.StringLocate.exec(%s, %s, %s - 1, %d)", string,
          substring, start, collationId);
      }
    }
    public static int execBinary(final UTF8String string, final UTF8String substring,
        final int start) {
      return string.indexOf(substring, start);
    }
    public static int execLowercase(final UTF8String string, final UTF8String substring,
        final int start) {
      return CollationAwareUTF8String.lowercaseIndexOf(string, substring, start);
    }
    public static int execICU(final UTF8String string, final UTF8String substring, final int start,
        final int collationId) {
      return CollationAwareUTF8String.indexOf(string, substring, start, collationId);
    }
  }

  public static class SubstringIndex {
    public static UTF8String exec(UTF8String string, UTF8String delimiter,
        final int count, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        delimiter = collation.trimmer.apply(delimiter);
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(string, delimiter, count);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(string, delimiter, count);
      } else {
        return execICU(string, delimiter, count, collationId);
      }
    }
    public static String genCode(final String string, final String delimiter,
        final String count, final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.subStringIndex(%s, %s)", string, delimiter, count);
      } else {
        return String.format("CollationSupport.SubstringIndex.exec(%s, %s, %s, %d)", string,
          delimiter, count, collationId);
      }
    }
    public static UTF8String execBinary(final UTF8String string, final UTF8String delimiter,
        final int count) {
      return string.subStringIndex(delimiter, count);
    }
    public static UTF8String execLowercase(final UTF8String string, final UTF8String delimiter,
        final int count) {
      return CollationAwareUTF8String.lowercaseSubStringIndex(string, delimiter, count);
    }
    public static UTF8String execICU(final UTF8String string, final UTF8String delimiter,
        final int count, final int collationId) {
      return CollationAwareUTF8String.subStringIndex(string, delimiter, count, collationId);
    }
  }

  public static class StringTranslate {
    public static UTF8String exec(final UTF8String source, Map<String, String> dict,
        final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(source, dict);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(source, dict);
      } else {
        return execICU(source, dict, collationId);
      }
    }
    public static UTF8String execBinary(final UTF8String source, Map<String, String> dict) {
      return source.translate(dict);
    }
    public static UTF8String execLowercase(final UTF8String source, Map<String, String> dict) {
      return CollationAwareUTF8String.lowercaseTranslate(source, dict);
    }
    public static UTF8String execICU(final UTF8String source, Map<String, String> dict,
        final int collationId) {
      return CollationAwareUTF8String.translate(source, dict, collationId);
    }
  }

  public static class StringTrim {
    public static UTF8String exec(final UTF8String srcString) {
      return execBinary(srcString);
    }
    public static UTF8String exec(
        UTF8String srcString,
        UTF8String trimString,
        final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        srcString = collation.trimmer.apply(srcString);
        trimString = collation.trimmer.apply(trimString);
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(srcString, trimString);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(srcString, trimString);
      } else {
        return execICU(srcString, trimString, collationId);
      }
    }
    public static String genCode(final String srcString) {
      return String.format("CollationSupport.StringTrim.execBinary(%s)", srcString);
    }
    public static String genCode(
        final String srcString,
        final String trimString,
        final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.trim(%s)", srcString, trimString);
      } else {
        return String.format("CollationSupport.StringTrim.exec(%s, %s, %d)", srcString,
          trimString, collationId);
      }
    }
    public static UTF8String execBinary(
        final UTF8String srcString) {
      return srcString.trim();
    }
    public static UTF8String execBinary(
        final UTF8String srcString,
        final UTF8String trimString) {
      return srcString.trim(trimString);
    }
    public static UTF8String execLowercase(
        final UTF8String srcString,
        final UTF8String trimString) {
      return CollationAwareUTF8String.lowercaseTrim(srcString, trimString);
    }
    public static UTF8String execICU(
        final UTF8String srcString,
        final UTF8String trimString,
        final int collationId) {
      return CollationAwareUTF8String.trim(srcString, trimString, collationId);
    }
  }

  public static class StringTrimLeft {
    public static UTF8String exec(UTF8String srcString, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) srcString = collation.trimmer.apply(srcString);
      return srcString.trimLeft();
    }
    public static UTF8String exec(
        UTF8String srcString,
        UTF8String trimString,
        final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        srcString = collation.trimmer.apply(srcString);
        trimString = collation.trimmer.apply(trimString);
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(srcString, trimString);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(srcString, trimString);
      } else {
        return execICU(srcString, trimString, collationId);
      }
    }
    public static String genCode(final String srcString, final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.trimLeft()", srcString);
      } else {
        return String.format("CollationSupport.StringTrimLeft.exec(%s, %d)", srcString,
          collationId);
      }
    }
    public static String genCode(
        final String srcString,
        final String trimString,
        final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.trimLeft(%s)", srcString, trimString);
      } else {
        return String.format("CollationSupport.StringTrimLeft.exec(%s, %s, %d)", srcString,
          trimString, collationId);
      }
    }
    public static UTF8String execBinary(
        final UTF8String srcString,
        final UTF8String trimString) {
      return srcString.trimLeft(trimString);
    }
    public static UTF8String execLowercase(
        final UTF8String srcString,
        final UTF8String trimString) {
      return CollationAwareUTF8String.lowercaseTrimLeft(srcString, trimString);
    }
    public static UTF8String execICU(
        final UTF8String srcString,
        final UTF8String trimString,
        final int collationId) {
      return CollationAwareUTF8String.trimLeft(srcString, trimString, collationId);
    }
  }

  public static class StringTrimRight {
    public static UTF8String exec(UTF8String srcString, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) srcString = collation.trimmer.apply(srcString);
      return srcString.trimRight();
    }
    public static UTF8String exec(
        UTF8String srcString,
        UTF8String trimString,
        final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsTrimming) {
        srcString = collation.trimmer.apply(srcString);
        trimString = collation.trimmer.apply(trimString);
        collation = CollationFactory.fetchCollationIgnoreTrim(collationId);
      }
      if (collation.supportsBinaryEquality) {
        return execBinary(srcString, trimString);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(srcString, trimString);
      } else {
        return execICU(srcString, trimString, collationId);
      }
    }
    public static String genCode(final String srcString, final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.trimRight()", srcString);
      } else {
        return String.format("CollationSupport.StringTrimRight.exec(%s, %d)", srcString,
          collationId);
      }
    }
    public static String genCode(
        final String srcString,
        final String trimString,
        final int collationId) {
      // Short-circuit only for UTF8_BINARY, in order to avoid regression if collation is not used.
      if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
        return String.format("%s.trimRight(%s)", srcString, trimString);
      } else {
        return String.format("CollationSupport.StringTrimRight.exec(%s, %s, %d)", srcString,
          trimString, collationId);
      }
    }
    public static UTF8String execBinary(
        final UTF8String srcString,
        final UTF8String trimString) {
      return srcString.trimRight(trimString);
    }
    public static UTF8String execLowercase(
        final UTF8String srcString,
        final UTF8String trimString) {
      return CollationAwareUTF8String.lowercaseTrimRight(srcString, trimString);
    }
    public static UTF8String execICU(
        final UTF8String srcString,
        final UTF8String trimString,
        final int collationId) {
      return CollationAwareUTF8String.trimRight(srcString, trimString, collationId);
    }
  }

  // TODO: Add more collation-aware string expressions.

  /**
   * Collation-aware regexp expressions.
   */

  public static boolean supportsLowercaseRegex(final int collationId) {
    // for regex, only Unicode case-insensitive matching is possible,
    // so UTF8_LCASE is treated as UNICODE_CI in this context
    return CollationFactory.fetchCollation(collationId).supportsLowercaseEquality;
  }

  public static boolean supportsTrimmingRegex(final int collationId) {
    // for regex, trimming can be enabled by injecting `\s*` prefix and/or suffix to the
    // original regex pattern, depending on the specified trimming sensitivity
    return CollationFactory.fetchCollation(collationId).supportsTrimming;
  }

  static final int lowercaseRegexFlags = Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE;
  public static int collationAwareRegexFlags(final int collationId) {
    return supportsLowercaseRegex(collationId) ? lowercaseRegexFlags : 0;
  }

  private static final UTF8String lowercaseRegexPrefix = UTF8String.fromString("(?ui)");
  public static UTF8String lowercaseRegex(final UTF8String regex) {
    return UTF8String.concat(lowercaseRegexPrefix, regex);
  }

  private static final UTF8String trimmingRegexAffix = UTF8String.fromString("\\s*");
  public static UTF8String trimmingRegex(final UTF8String regex, final int collationId) {
    switch(CollationFactory.fetchCollation(collationId).trimSensitivity) {
      case TRIM:
          return UTF8String.concat(trimmingRegexAffix, regex, trimmingRegexAffix);
      case LTRIM:
          return UTF8String.concat(trimmingRegexAffix, regex);
      case RTRIM:
          return UTF8String.concat(regex, trimmingRegexAffix);
      default:
          return regex;
    }
  }

  public static UTF8String collationAwareRegex(UTF8String regex, final int collationId) {
    if (!supportsLowercaseRegex(collationId) && !supportsTrimmingRegex(collationId)) {
      return regex;
    }
    if (supportsTrimmingRegex(collationId)) {
      regex = trimmingRegex(regex, collationId);
    }
    if (supportsLowercaseRegex(collationId)) {
      regex = lowercaseRegex(regex);
    }
    return regex;
  }

  /**
   * Other collation-aware expressions.
   */

  // TODO: Add other collation-aware expressions.

}
