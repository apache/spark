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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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

  public static class StringTranslate {
    public static UTF8String exec(final UTF8String source, Map<String, String> dict, final int collationId) {
      CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
      if (collation.supportsBinaryEquality) {
        return execBinary(source, dict);
      } else if (collation.supportsLowercaseEquality) {
        return execLowercase(source, dict);
      } else {
        return execICU(source, dict, collationId);
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
    public static UTF8String execBinary(final UTF8String source, Map<String, String> dict) {
      return source.translate(dict);
    }
    public static UTF8String execLowercase(final UTF8String source, Map<String, String> dict) {
      String srcStr = source.toString();
      StringBuilder sb = new StringBuilder();
      int charCount = 0;
      for (int k = 0; k < srcStr.length(); k += charCount) {
        int codePoint = srcStr.codePointAt(k);
        charCount = Character.charCount(codePoint);
        String subStr = srcStr.substring(k, k + charCount);
        String translated = dict.get(subStr.toLowerCase());
        if (null == translated) {
          sb.append(subStr);
        } else if (!"\0".equals(translated)) {
          sb.append(translated);
        }
      }
      return UTF8String.fromString(sb.toString());
    }
    public static UTF8String execICU(final UTF8String source, Map<String, String> dict,
                                  final int collationId) {
      return source.translate(CollationAwareUTF8String.getCollationAwareDict(source, dict, collationId));
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

    private static Map<String, String> getCollationAwareDict(UTF8String string,
        Map<String, String> dict, int collationId) {
      String srcStr = string.toString();

      Map<String, String> collationAwareDict = new HashMap<>();
      for (String key : dict.keySet()) {
        StringSearch stringSearch =
                CollationFactory.getStringSearch(string, UTF8String.fromString(key), collationId);

        int pos = 0;
        while ((pos = stringSearch.next()) != StringSearch.DONE) {
          int codePoint = srcStr.codePointAt(pos);
          int charCount = Character.charCount(codePoint);
          String newKey = srcStr.substring(pos, pos + charCount);

          boolean exists = false;
          for (String existingKey : collationAwareDict.keySet()) {
            if (stringSearch.getCollator().compare(existingKey, newKey) == 0) {
              collationAwareDict.put(newKey, collationAwareDict.get(existingKey));
              exists = true;
              break;
            }
          }

          if (!exists) {
            collationAwareDict.put(newKey, dict.get(key));
          }
        }
      }

      return collationAwareDict;
    }

  }

}
