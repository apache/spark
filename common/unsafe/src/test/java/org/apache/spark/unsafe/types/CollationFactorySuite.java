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

package org.apache.spark.unsafe.types;

import org.apache.spark.SparkIllegalArgumentException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.spark.sql.catalyst.util.CollationFactory.*;
import static org.apache.spark.unsafe.types.UTF8String.*;

import java.util.Arrays;

public class CollationFactorySuite {
    @Test
    public void collationIdStability() {
      CollationInfo collationInfo = getInstance().fetchCollationInfo(0);
      assertEquals(collationInfo.collationName, "UCS_BASIC");
      assertTrue(collationInfo.isBinaryCollation);

      collationInfo = getInstance().fetchCollationInfo(1);
      assertEquals(collationInfo.collationName, "UCS_BASIC_LCASE");
      assertFalse(collationInfo.isBinaryCollation);

      collationInfo = getInstance().fetchCollationInfo(2);
      assertEquals(collationInfo.collationName, "UNICODE");
      assertTrue(collationInfo.isBinaryCollation);

      collationInfo = getInstance().fetchCollationInfo(3);
      assertEquals(collationInfo.collationName, "UNICODE_CI");
      assertFalse(collationInfo.isBinaryCollation);
    }

    @Test
    public void collationFetchInvalidName() {
      assertThrows(
        SparkIllegalArgumentException.class,
        () -> getInstance().collationNameToId("INVALID_NAME"),
        "Invalid collation name: INVALID_NAME");
    }

    private record CollationTestCase<R>(
      String collationName, String s1, String s2, R expectedResult) {}

    @Test
    public void collationAwareEqualityChecks() {
        var checks = Arrays.asList(
          new CollationTestCase<>("UCS_BASIC", "aaa", "aaa", true),
          new CollationTestCase<>("UCS_BASIC", "aaa", "AAA", false),
          new CollationTestCase<>("UCS_BASIC", "aaa", "bbb", false),
          new CollationTestCase<>("UCS_BASIC_LCASE", "aaa", "aaa", true),
          new CollationTestCase<>("UCS_BASIC_LCASE", "aaa", "AAA", true),
          new CollationTestCase<>("UCS_BASIC_LCASE", "aaa", "AaA", true),
          new CollationTestCase<>("UCS_BASIC_LCASE", "aaa", "AaA", true),
          new CollationTestCase<>("UCS_BASIC_LCASE", "aaa", "aa", false),
          new CollationTestCase<>("UCS_BASIC_LCASE", "aaa", "bbb", false),
          new CollationTestCase<>("UNICODE", "aaa", "aaa", true),
          new CollationTestCase<>("UNICODE", "aaa", "AAA", false),
          new CollationTestCase<>("UNICODE", "aaa", "bbb", false),
          new CollationTestCase<>("UNICODE_CI", "aaa", "aaa", true),
          new CollationTestCase<>("UNICODE_CI", "aaa", "AAA", true),
          new CollationTestCase<>("UNICODE_CI", "aaa", "bbb", false)
        );

        checks.forEach(check -> {
          CollationInfo collationInfo = getInstance().fetchCollationInfo(check.collationName);
          // Equality check.
          assertEquals(
            collationInfo.equalsFunction.apply(fromString(check.s1), fromString(check.s2)),
            check.expectedResult, String.format(
              "Collation %s: %s == %s",
              check.collationName,
              check.s1,
              check.s2));
          // Hash check.
          int hash1 = collationInfo.hashFunction.apply(fromString(check.s1));
          int hash2 = collationInfo.hashFunction.apply(fromString(check.s2));
          assertEquals(hash1 == hash2,
            check.expectedResult, String.format(
            "Collation %s: hash(%s) == hash(%s)",
            check.collationName,
            check.s1,
            check.s2));
        });
    }

    @Test
    public void collationAwareComparisonChecks() {
      var checks = Arrays.asList(
        new CollationTestCase<>("UCS_BASIC", "aaa", "aaa", 0),
        new CollationTestCase<>("UCS_BASIC", "aaa", "AAA", 1),
        new CollationTestCase<>("UCS_BASIC", "aaa", "bbb", -1),
        new CollationTestCase<>("UCS_BASIC", "aaa", "BBB", 1),
        new CollationTestCase<>("UCS_BASIC_LCASE", "aaa", "aaa", 0),
        new CollationTestCase<>("UCS_BASIC_LCASE", "aaa", "AAA", 0),
        new CollationTestCase<>("UCS_BASIC_LCASE", "aaa", "AaA", 0),
        new CollationTestCase<>("UCS_BASIC_LCASE", "aaa", "AaA", 0),
        new CollationTestCase<>("UCS_BASIC_LCASE", "aaa", "aa", 1),
        new CollationTestCase<>("UCS_BASIC_LCASE", "aaa", "bbb", -1),
        new CollationTestCase<>("UNICODE", "aaa", "aaa", 0),
        new CollationTestCase<>("UNICODE", "aaa", "AAA", -1),
        new CollationTestCase<>("UNICODE", "aaa", "bbb", -1),
        new CollationTestCase<>("UNICODE", "aaa", "BBB", -1),
        new CollationTestCase<>("UNICODE_CI", "aaa", "aaa", 0),
        new CollationTestCase<>("UNICODE_CI", "aaa", "AAA", 0),
        new CollationTestCase<>("UNICODE_CI", "aaa", "bbb", -1)
      );

      checks.forEach(check -> {
        CollationInfo collationInfo = getInstance().fetchCollationInfo(check.collationName);
        int result = collationInfo.comparator.compare(fromString(check.s1), fromString(check.s2));
        result = Integer.signum(result);
        assertEquals(result, check.expectedResult, String.format(
          "Collation %s: %s compareTo %s",
          check.collationName,
          check.s1,
          check.s2));
        });
    }
}