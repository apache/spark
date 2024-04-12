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

import org.apache.spark.sql.catalyst.util.collationAwareStringFunctions.CollationAwareBinaryFunction;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Static entry point for collation-aware expressions (StringExpressions, RegexpExpressions, and
 * other expressions that require custom collation support), as well as private utility methods for
 * collation-aware UTF8String operations needed to implement .
 */
public final class CollationSupport {

  public static class Contains extends CollationAwareBinaryFunction<UTF8String, UTF8String, Boolean> {
    private Contains() {}

    public static final Contains INSTANCE = new Contains();

    @Override
    public Boolean execBinary(UTF8String first, UTF8String second) {
      return first.contains(second);
    }

    @Override
    public Boolean execLowercase(UTF8String first, UTF8String second) {
      return first.toLowerCase().contains(second.toLowerCase());
    }

    @Override
    public Boolean execICU(UTF8String first, UTF8String second, int collationId) {
      if (second.numBytes() == 0) return true;
      if (first.numBytes() == 0) return false;
      StringSearch stringSearch = CollationFactory.getStringSearch(first, second, collationId);
      return stringSearch.first() != StringSearch.DONE;
    }
  }

  public static class StartsWith extends CollationAwareBinaryFunction<UTF8String, UTF8String, Boolean> {
    private StartsWith() {}

    public static final StartsWith INSTANCE = new StartsWith();

    @Override
    public Boolean execBinary(UTF8String first, UTF8String second) {
      return first.startsWith(second);
    }

    @Override
    public Boolean execLowercase(UTF8String first, UTF8String second) {
      CollationAwareBinaryFunction<UTF8String, UTF8String, Boolean> xx = new StartsWith();
      return first.toLowerCase().startsWith(second.toLowerCase());
    }

    @Override
    public Boolean execICU(UTF8String first, UTF8String second, int collationId) {
      return CollationAwareUTF8String.matchAt(first, second, 0, collationId);
    }
  }

  public static class EndsWith extends CollationAwareBinaryFunction<UTF8String, UTF8String, Boolean> {
    private EndsWith() {}

    public static final EndsWith INSTANCE = new EndsWith();

    @Override
    public Boolean execBinary(UTF8String first, UTF8String second) {
      return first.endsWith(second);
    }

    @Override
    public Boolean execLowercase(UTF8String first, UTF8String second) {
      return first.toLowerCase().endsWith(second.toLowerCase());
    }

    @Override
    public Boolean execICU(UTF8String first, UTF8String second, int collationId) {
      return CollationAwareUTF8String.matchAt(first, second, first.numBytes() - second.numBytes(), collationId);
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

  }

}
