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

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;
import static org.apache.spark.unsafe.Platform.copyMemory;

/**
 * Static entry point for collation-aware expressions (StringExpressions, RegexpExpressions, and
 * other expressions that require custom collation support), as well as private utility methods for
 * collation-aware UTF8String operations needed to implement .
 */
public final class CollationSupport {

  /**
   * Collation-aware string expressions.
   */

  public static class Contains {
    public static boolean exec(final UTF8String l, final UTF8String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsBinaryEquality) {
        return execBinary(l, r);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(l, r);
      } else {
        return execICU(l, r, collationId);
      }
    }
    public static String genCode(final String l, final String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      String expr = "CollationSupport.Contains.exec";
      if (collation.supportsBinaryEquality) {
        return String.format(expr + "Binary(%s, %s)", l, r);
      } else if (collation.supportsLowercaseEquality) {
        return String.format(expr + "Lowercase(%s, %s)", l, r);
      } else {
        return String.format(expr + "ICU(%s, %s, %d)", l, r, collationId);
      }
    }
    public static boolean execBinary(final UTF8String l, final UTF8String r) {
      return l.contains(r);
    }
    public static boolean execLowercase(final UTF8String l, final UTF8String r) {
      return l.toLowerCase().contains(r.toLowerCase());
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
    public static boolean exec(final UTF8String l, final UTF8String r,
        final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsBinaryEquality) {
        return execBinary(l, r);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(l, r);
      } else {
        return execICU(l, r, collationId);
      }
    }
    public static String genCode(final String l, final String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      String expr = "CollationSupport.StartsWith.exec";
      if (collation.supportsBinaryEquality) {
        return String.format(expr + "Binary(%s, %s)", l, r);
      } else if (collation.supportsLowercaseEquality) {
        return String.format(expr + "Lowercase(%s, %s)", l, r);
      } else {
        return String.format(expr + "ICU(%s, %s, %d)", l, r, collationId);
      }
    }
    public static boolean execBinary(final UTF8String l, final UTF8String r) {
      return l.startsWith(r);
    }
    public static boolean execLowercase(final UTF8String l, final UTF8String r) {
      return l.toLowerCase().startsWith(r.toLowerCase());
    }
    public static boolean execICU(final UTF8String l, final UTF8String r,
        final int collationId) {
      return CollationAwareUTF8String.matchAt(l, r, 0, collationId);
    }
  }

  public static class EndsWith {
    public static boolean exec(final UTF8String l, final UTF8String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsBinaryEquality) {
        return execBinary(l, r);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(l, r);
      } else {
        return execICU(l, r, collationId);
      }
    }
    public static String genCode(final String l, final String r, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      String expr = "CollationSupport.EndsWith.exec";
      if (collation.supportsBinaryEquality) {
        return String.format(expr + "Binary(%s, %s)", l, r);
      } else if (collation.supportsLowercaseEquality) {
        return String.format(expr + "Lowercase(%s, %s)", l, r);
      } else {
        return String.format(expr + "ICU(%s, %s, %d)", l, r, collationId);
      }
    }
    public static boolean execBinary(final UTF8String l, final UTF8String r) {
      return l.endsWith(r);
    }
    public static boolean execLowercase(final UTF8String l, final UTF8String r) {
      return l.toLowerCase().endsWith(r.toLowerCase());
    }
    public static boolean execICU(final UTF8String l, final UTF8String r,
        final int collationId) {
      return CollationAwareUTF8String.matchAt(l, r, l.numBytes() - r.numBytes(), collationId);
    }
  }

  public static class SubstringIndex {
    public static UTF8String exec(final UTF8String string, final UTF8String delimiter,
        final int count, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsBinaryEquality) {
        return execBinary(string, delimiter, count);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(string, delimiter, count);
      } else {
        return execICU(string, delimiter, count, collationId);
      }
    }
    public static String genCode(final String string, final String delimiter,
        final int count, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      String expr = "CollationSupport.SubstringIndex.exec";
      if (collation.supportsBinaryEquality) {
        return String.format(expr + "Binary(%s, %s, %d)", string, delimiter, count);
      } else if (collation.supportsLowercaseEquality) {
        return String.format(expr + "Lowercase(%s, %s, %d)", string, delimiter, count);
      } else {
        return String.format(expr + "ICU(%s, %s, %d, %d)", string, delimiter, count, collationId);
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
      return CollationAwareUTF8String.collationAwareSubStringIndex(string, delimiter, count,
              collationId);
    }
  }

  // TODO: Add more collation-aware string expressions.

  /**
   * Collation-aware regexp expressions.
   */

  // TODO: Add more collation-aware regexp expressions.

  /**
   * Other collation-aware expressions.
   */

  // TODO: Add other collation-aware expressions.

  /**
   * Utility class for collation-aware UTF8String operations.
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

    private static int collationAwareFind(UTF8String target, UTF8String pattern, int start,
        int collationId) {
      assert (pattern.numBytes() > 0);

      StringSearch stringSearch = CollationFactory.getStringSearch(target, pattern, collationId);
      // Set search start position (start from character at start position)
      stringSearch.setIndex(target.bytePosToChar(start));

      // Return either the byte position or -1 if not found
      return target.charPosToByte(stringSearch.next());
    }

    private static int collationAwareRFind(UTF8String target, UTF8String pattern, int start,
        int collationId) {
      assert (pattern.numBytes() > 0);

      if (target.numBytes() == 0) {
        return -1;
      }

      StringSearch stringSearch = CollationFactory.getStringSearch(target, pattern, collationId);

      int prevStart = -1;
      int matchStart = stringSearch.next();
      while (target.charPosToByte(matchStart) <= start) {
        if (matchStart != StringSearch.DONE) {
          // Found a match, update the start position
          prevStart = matchStart;
          matchStart = stringSearch.next();
        } else {
          return target.charPosToByte(prevStart);
        }
      }

      return target.charPosToByte(prevStart);
    }

    private static UTF8String collationAwareSubStringIndex(final UTF8String string,
        final UTF8String delimiter, int count, final int collationId) {
      if (delimiter.numBytes() == 0 || count == 0) {
        return UTF8String.EMPTY_UTF8;
      }
      if (count > 0) {
        int idx = -1;
        while (count > 0) {
          idx = collationAwareFind(string, delimiter, idx + 1, collationId);
          if (idx >= 0) {
            count --;
          } else {
            // can not find enough delim
            return string;
          }
        }
        if (idx == 0) {
          return UTF8String.EMPTY_UTF8;
        }
        byte[] bytes = new byte[idx];
        copyMemory(string.getBaseObject(), string.getBaseOffset(), bytes, BYTE_ARRAY_OFFSET, idx);
        return UTF8String.fromBytes(bytes);

      } else {
        int idx = string.numBytes() - delimiter.numBytes() + 1;
        count = -count;
        while (count > 0) {
          idx = collationAwareRFind(string, delimiter, idx - 1, collationId);
          if (idx >= 0) {
            count --;
          } else {
            // can not find enough delim
            return string;
          }
        }
        if (idx + delimiter.numBytes() == string.numBytes()) {
          return UTF8String.EMPTY_UTF8;
        }
        int size = string.numBytes() - delimiter.numBytes() - idx;
        byte[] bytes = new byte[size];
        copyMemory(string.getBaseObject(), string.getBaseOffset() + idx + delimiter.numBytes(),
                bytes, BYTE_ARRAY_OFFSET, size);
        return UTF8String.fromBytes(bytes);
      }
    }

    private static UTF8String lowercaseSubStringIndex(final UTF8String string,
      final UTF8String delimiter, int count) {
      if (delimiter.numBytes() == 0 || count == 0) {
        return UTF8String.EMPTY_UTF8;
      }

      UTF8String lowercaseString = string.toLowerCase();
      UTF8String lowercaseDelimiter = delimiter.toLowerCase();

      if (count > 0) {
        int idx = -1;
        while (count > 0) {
          idx = lowercaseString.find(lowercaseDelimiter, idx + 1);
          if (idx >= 0) {
            count --;
          } else {
            // can not find enough delim
            return string;
          }
        }
        if (idx == 0) {
          return UTF8String.EMPTY_UTF8;
        }
        byte[] bytes = new byte[idx];
        copyMemory(string.getBaseObject(), string.getBaseOffset(), bytes, BYTE_ARRAY_OFFSET, idx);
        return UTF8String.fromBytes(bytes);

      } else {
        int idx = string.numBytes() - delimiter.numBytes() + 1;
        count = -count;
        while (count > 0) {
          idx = lowercaseString.rfind(lowercaseDelimiter, idx - 1);
          if (idx >= 0) {
            count --;
          } else {
            // can not find enough delim
            return string;
          }
        }
        if (idx + delimiter.numBytes() == string.numBytes()) {
          return UTF8String.EMPTY_UTF8;
        }
        int size = string.numBytes() - delimiter.numBytes() - idx;
        byte[] bytes = new byte[size];
        copyMemory(string.getBaseObject(), string.getBaseOffset() + idx + delimiter.numBytes(),
                bytes, BYTE_ARRAY_OFFSET, size);
        return UTF8String.fromBytes(bytes);
      }
    }

  }

}
