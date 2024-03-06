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

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

import com.ibm.icu.util.ULocale;
import com.ibm.icu.text.Collator;

import org.apache.spark.SparkException;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Static entry point for collation aware string functions.
 * Provides functionality to the UTF8String object which respects defined collation settings.
 */
public final class CollationFactory {
  /**
   * Entry encapsulating all information about a collation.
   */
  public static class Collation {
    public final String collationName;
    public final Collator collator;
    public final Comparator<UTF8String> comparator;

    /**
     * Version of the collation. This is the version of the ICU library Collator.
     * For non-ICU collations (e.g. UTF8 Binary) the version is set to "1.0".
     * When using ICU Collator this version is exposed through collator.getVersion().
     * Whenever the collation is updated, the version should be updated as well or kept
     * for backwards compatibility.
     */
    public final String version;

    /**
     * Collation sensitive hash function. Output for two UTF8Strings will be the same if they are
     * equal according to the collation.
     */
    public final ToLongFunction<UTF8String> hashFunction;

    /**
     * Potentially faster way than using comparator to compare two UTF8Strings for equality.
     * Falls back to binary comparison if the collation is binary.
     */
    public final BiFunction<UTF8String, UTF8String, Boolean> equalsFunction;

    /**
     * Binary collation implies that UTF8Strings are considered equal only if they are
     * byte for byte equal. All accent or case-insensitive collations are considered non-binary.
     */
    public final boolean isBinaryCollation;

    public Collation(
        String collationName,
        Collator collator,
        Comparator<UTF8String> comparator,
        String version,
        ToLongFunction<UTF8String> hashFunction,
        boolean isBinaryCollation) {
      this.collationName = collationName;
      this.collator = collator;
      this.comparator = comparator;
      this.version = version;
      this.hashFunction = hashFunction;
      this.isBinaryCollation = isBinaryCollation;

      if (isBinaryCollation) {
        this.equalsFunction = UTF8String::equals;
      } else {
        this.equalsFunction = (s1, s2) -> this.comparator.compare(s1, s2) == 0;
      }
    }

    /**
     * Constructor with comparators that are inherited from the given collator.
     */
    public Collation(
        String collationName, Collator collator, String version, boolean isBinaryCollation) {
      this(
        collationName,
        collator,
        (s1, s2) -> collator.compare(s1.toString(), s2.toString()),
        version,
        s -> (long)collator.getCollationKey(s.toString()).hashCode(),
        isBinaryCollation);
    }
  }

  private static final Collation[] collationTable = new Collation[4];
  private static final HashMap<String, Integer> collationNameToIdMap = new HashMap<>();

  public static final int DEFAULT_COLLATION_ID = 0;
  public static final int LOWERCASE_COLLATION_ID = 1;

  static {
    // Binary comparison. This is the default collation.
    // No custom comparators will be used for this collation.
    // Instead, we rely on byte for byte comparison.
    collationTable[0] = new Collation(
      "UCS_BASIC",
      null,
      UTF8String::binaryCompare,
      "1.0",
      s -> (long)s.hashCode(),
      true);

    // Case-insensitive UTF8 binary collation.
    // TODO: Do in place comparisons instead of creating new strings.
    collationTable[1] = new Collation(
      "UCS_BASIC_LCASE",
      null,
      (s1, s2) -> s1.toLowerCase().binaryCompare(s2.toLowerCase()),
      "1.0",
      (s) -> (long)s.toLowerCase().hashCode(),
      false);

    // UNICODE case sensitive comparison (ROOT locale, in ICU).
    collationTable[2] = new Collation(
      "UNICODE", Collator.getInstance(ULocale.ROOT), "153.120.0.0", true);
    collationTable[2].collator.setStrength(Collator.TERTIARY);

    // UNICODE case-insensitive comparison (ROOT locale, in ICU + Secondary strength).
    collationTable[3] = new Collation(
      "UNICODE_CI", Collator.getInstance(ULocale.ROOT), "153.120.0.0", false);
    collationTable[3].collator.setStrength(Collator.SECONDARY);

    for (int i = 0; i < collationTable.length; i++) {
      collationNameToIdMap.put(collationTable[i].collationName, i);
    }
  }

  /**
   * Returns the collation id for the given collation name.
   */
  public static int collationNameToId(String collationName) throws SparkException {
    String normalizedName = collationName.toUpperCase();
    if (collationNameToIdMap.containsKey(normalizedName)) {
      return collationNameToIdMap.get(normalizedName);
    } else {
      Collation suggestion = Collections.min(List.of(collationTable), Comparator.comparingInt(
        c -> UTF8String.fromString(c.collationName).levenshteinDistance(
          UTF8String.fromString(normalizedName))));

      Map<String, String> params = new HashMap<>();
      params.put("collationName", collationName);
      params.put("proposal", suggestion.collationName);

      throw new SparkException(
        "COLLATION_INVALID_NAME", SparkException.constructMessageParams(params), null);
    }
  }

  public static Collation fetchCollation(int collationId) {
    return collationTable[collationId];
  }

  public static Collation fetchCollation(String collationName) throws SparkException {
    int collationId = collationNameToId(collationName);
    return collationTable[collationId];
  }
}
