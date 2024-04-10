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
public final class CollationStringExpressions {

  public static class Contains {
    public static boolean containsBinary(UTF8String l, UTF8String r) {
      return l.contains(r);
    }
    public static boolean containsLowercase(UTF8String l, UTF8String r) {
      return l.toLowerCase().contains(r.toLowerCase());
    }
    public static boolean containsICU(UTF8String l, UTF8String r, int collationId) {
      if (r.numBytes() == 0) return true;
      if (l.numBytes() == 0) return false;
      StringSearch stringSearch = CollationFactory.getStringSearch(l, r, collationId);
      return stringSearch.first() != StringSearch.DONE;
    }
  }

  public static class StartsWith {
    public static boolean startsWithBinary(final UTF8String l, final UTF8String r) {
      return l.startsWith(r);
    }
    public static boolean startsWithLowercase(final UTF8String l, final UTF8String r) {
      return l.toLowerCase().startsWith(r.toLowerCase());
    }
    public static boolean startsWithICU(final UTF8String l, final UTF8String r, final int collationId) {
      return CollationAwareUTF8String.matchAt(l, r, 0, collationId);
    }
  }

  public static class EndsWith {
    public static boolean endsWithBinary(final UTF8String l, final UTF8String r) {
      return l.endsWith(r);
    }
    public static boolean endsWithLowercase(final UTF8String l, final UTF8String r) {
      return l.toLowerCase().endsWith(r.toLowerCase());
    }
    public static boolean endsWithICU(final UTF8String l, final UTF8String r, final int collationId) {
      return CollationAwareUTF8String.matchAt(l, r, l.numBytes() - r.numBytes(), collationId);
    }
  }

  private static class CollationAwareUTF8String {
    private static boolean matchAt(final UTF8String first, final UTF8String second, final int pos, final int collationId) {
      if (second.numChars() + pos > first.numChars() || pos < 0) {
        return false;
      }
      if (second.numBytes() == 0 || first.numBytes() == 0) {
        return second.numBytes() == 0;
      }
      return CollationFactory.getStringSearch(
              first.substring(pos, pos + second.numChars()), second, collationId).last() == 0;
    }
  }

}
