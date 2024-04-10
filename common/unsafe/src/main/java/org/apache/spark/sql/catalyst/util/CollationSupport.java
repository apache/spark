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

/**
 * Static entry point for collation aware string expressions.
 */
public final class CollationSupport {

  /**
   * Collation aware string expressions.
   */
  public static class Contains {
    public static boolean contains(final UTF8String l, final UTF8String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsBinaryEquality) {
        return containsBinary(l, r);
      } else if (collation.supportsLowercaseEquality) {
        return containsLowercase(l, r);
      } else {
        return containsICU(l, r, collationId);
      }
    }
    public static String containsGenCode(final String l, final String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      String expr = "CollationSupport.Contains.contains";
      if (collation.supportsBinaryEquality) {
        return String.format(expr + "Binary(%s, %s)", l, r);
      } else if (collation.supportsLowercaseEquality) {
        return String.format(expr + "Lowercase(%s, %s)", l, r);
      } else {
        return String.format(expr + "ICU(%s, %s, %d)", l, r, collationId);
      }
    }
    public static boolean containsBinary(final UTF8String l, final UTF8String r) {
      return l.contains(r);
    }
    public static boolean containsLowercase(final UTF8String l, final UTF8String r) {
      return l.toLowerCase().contains(r.toLowerCase());
    }
    public static boolean containsICU(final UTF8String l, final UTF8String r,
        final int collationId) {
      if (r.numBytes() == 0) return true;
      if (l.numBytes() == 0) return false;
      StringSearch stringSearch = CollationFactory.getStringSearch(l, r, collationId);
      return stringSearch.first() != StringSearch.DONE;
    }
  }

  public static class StartsWith {
    public static boolean startsWith(final UTF8String l, final UTF8String r,
        final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsBinaryEquality) {
        return startsWithBinary(l, r);
      } else if (collation.supportsLowercaseEquality) {
        return startsWithLowercase(l, r);
      } else {
        return startsWithICU(l, r, collationId);
      }
    }
    public static String startsWithGenCode(final String l, final String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      String expr = "CollationSupport.StartsWith.startsWith";
      if (collation.supportsBinaryEquality) {
        return String.format(expr + "Binary(%s, %s)", l, r);
      } else if (collation.supportsLowercaseEquality) {
        return String.format(expr + "Lowercase(%s, %s)", l, r);
      } else {
        return String.format(expr + "ICU(%s, %s, %d)", l, r, collationId);
      }
    }
    public static boolean startsWithBinary(final UTF8String l, final UTF8String r) {
      return l.startsWith(r);
    }
    public static boolean startsWithLowercase(final UTF8String l, final UTF8String r) {
      return l.toLowerCase().startsWith(r.toLowerCase());
    }
    public static boolean startsWithICU(final UTF8String l, final UTF8String r,
        final int collationId) {
      return CollationAwareUTF8String.matchAt(l, r, 0, collationId);
    }
  }

  public static class EndsWith {
    public static boolean endsWith(final UTF8String l, final UTF8String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsBinaryEquality) {
        return endsWithBinary(l, r);
      } else if (collation.supportsLowercaseEquality) {
        return endsWithLowercase(l, r);
      } else {
        return endsWithICU(l, r, collationId);
      }
    }
    public static String endsWithGenCode(final String l, final String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      String expr = "CollationSupport.EndsWith.endsWith";
      if (collation.supportsBinaryEquality) {
        return String.format(expr + "Binary(%s, %s)", l, r);
      } else if (collation.supportsLowercaseEquality) {
        return String.format(expr + "Lowercase(%s, %s)", l, r);
      } else {
        return String.format(expr + "ICU(%s, %s, %d)", l, r, collationId);
      }
    }
    public static boolean endsWithBinary(final UTF8String l, final UTF8String r) {
      return l.endsWith(r);
    }
    public static boolean endsWithLowercase(final UTF8String l, final UTF8String r) {
      return l.toLowerCase().endsWith(r.toLowerCase());
    }
    public static boolean endsWithICU(final UTF8String l, final UTF8String r,
        final int collationId) {
      return CollationAwareUTF8String.matchAt(l, r, l.numBytes() - r.numBytes(), collationId);
    }
  }

  /**
   * Utility class for collation support for UTF8String.
   */
  private static class CollationAwareUTF8String {
    private static boolean matchAt(final UTF8String target, final UTF8String pattern,
        final int pos, final int collationId) {
      if (pattern.numChars() + pos > target.numChars() || pos < 0) {
        return false;
      }
      if (pattern.numBytes() == 0 || target.numBytes() == 0) {
        return pattern.numBytes() == 0;
      }
      return CollationFactory.getStringSearch(target.substring(
        pos, pos + pattern.numChars()), pattern, collationId).last() == 0;
    }
  }

}
