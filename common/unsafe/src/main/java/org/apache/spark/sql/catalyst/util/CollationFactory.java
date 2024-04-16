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

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.text.StringSearch;
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
     * Support for Binary Equality implies that it is possible to check equality on
     * byte by byte level. This allows for the usage of binaryEquals call on UTF8Strings
     * which is more performant than calls to external ICU library.
     */
    public final boolean supportsBinaryEquality;
    /**
     * Support for Binary Ordering implies that it is possible to check equality and ordering on
     * byte by byte level. This allows for the usage of binaryEquals and binaryCompare calls on
     * UTF8Strings which is more performant than calls to external ICU library. Support for
     * Binary Ordering implies support for Binary Equality.
     */
    public final boolean supportsBinaryOrdering;

    /**
     * Support for Lowercase Equality implies that it is possible to check equality on
     * byte by byte level, but only after calling "UTF8String.toLowerCase" on both arguments.
     * This allows custom collation support for UTF8_BINARY_LCASE collation in various Spark
     * expressions, as this particular collation is not supported by the external ICU library.
     */
    public final boolean supportsLowercaseEquality;

    public Collation(
        String collationName,
        Collator collator,
        Comparator<UTF8String> comparator,
        String version,
        ToLongFunction<UTF8String> hashFunction,
        boolean supportsBinaryEquality,
        boolean supportsBinaryOrdering,
        boolean supportsLowercaseEquality) {
      this.collationName = collationName;
      this.collator = collator;
      this.comparator = comparator;
      this.version = version;
      this.hashFunction = hashFunction;
      this.supportsBinaryEquality = supportsBinaryEquality;
      this.supportsBinaryOrdering = supportsBinaryOrdering;
      this.supportsLowercaseEquality = supportsLowercaseEquality;

      // De Morgan's Law to check supportsBinaryOrdering => supportsBinaryEquality
      assert(!supportsBinaryOrdering || supportsBinaryEquality);
      // No Collation can simultaneously support binary equality and lowercase equality
      assert(!supportsBinaryEquality || !supportsLowercaseEquality);

      if (supportsBinaryEquality) {
        this.equalsFunction = UTF8String::equals;
      } else {
        this.equalsFunction = (s1, s2) -> this.comparator.compare(s1, s2) == 0;
      }
    }

    /**
     * Constructor with comparators that are inherited from the given collator.
     */
    public Collation(
        String collationName,
        Collator collator,
        String version,
        boolean supportsBinaryEquality,
        boolean supportsBinaryOrdering,
        boolean supportsLowercaseEquality) {
      this(
        collationName,
        collator,
        (s1, s2) -> collator.compare(s1.toString(), s2.toString()),
        version,
        s -> (long)collator.getCollationKey(s.toString()).hashCode(),
        supportsBinaryEquality,
        supportsBinaryOrdering,
        supportsLowercaseEquality);
    }
  }

  private static final Collation[] collationTable = new Collation[4];
  private static final HashMap<String, Integer> collationNameToIdMap = new HashMap<>();

  public static final int UTF8_BINARY_COLLATION_ID = 0;
  public static final int UTF8_BINARY_LCASE_COLLATION_ID = 1;

  static {
    // Binary comparison. This is the default collation.
    // No custom comparators will be used for this collation.
    // Instead, we rely on byte for byte comparison.
    collationTable[0] = new Collation(
      "UTF8_BINARY",
      null,
      UTF8String::binaryCompare,
      "1.0",
      s -> (long)s.hashCode(),
      true,
      true,
      false);

    // Case-insensitive UTF8 binary collation.
    // TODO: Do in place comparisons instead of creating new strings.
    collationTable[1] = new Collation(
      "UTF8_BINARY_LCASE",
      null,
      UTF8String::compareLowerCase,
      "1.0",
      (s) -> (long)s.toLowerCase().hashCode(),
      false,
      false,
      true);

    // UNICODE case sensitive comparison (ROOT locale, in ICU).
    collationTable[2] = new Collation(
      "UNICODE", Collator.getInstance(ULocale.ROOT), "153.120.0.0", true, false, false);
    collationTable[2].collator.setStrength(Collator.TERTIARY);
    collationTable[2].collator.freeze();

    // UNICODE case-insensitive comparison (ROOT locale, in ICU + Secondary strength).
    collationTable[3] = new Collation(
      "UNICODE_CI", Collator.getInstance(ULocale.ROOT), "153.120.0.0", false, false, false);
    collationTable[3].collator.setStrength(Collator.SECONDARY);
    collationTable[3].collator.freeze();

    for (int i = 0; i < collationTable.length; i++) {
      collationNameToIdMap.put(collationTable[i].collationName, i);
    }
  }

  /**
   * Returns a StringSearch object for the given pattern and target strings, under collation
   * rules corresponding to the given collationId. The external ICU library StringSearch object can
   * be used to find occurrences of the pattern in the target string, while respecting collation.
   */
  public static StringSearch getStringSearch(
      final UTF8String targetUTF8String,
      final UTF8String patternUTF8String,
      final int collationId) {
    String pattern = patternUTF8String.toString();
    CharacterIterator target = new StringCharacterIterator(targetUTF8String.toString());
    Collator collator = CollationFactory.fetchCollation(collationId).collator;
    return new StringSearch(pattern, target, (RuleBasedCollator) collator);
  }

  /**
   * Returns if the given collationName is valid one.
   */
  public static boolean isValidCollation(String collationName) {
    return collationNameToIdMap.containsKey(collationName.toUpperCase());
  }

  /**
   * Returns closest valid name to collationName
   */
  public static String getClosestCollation(String collationName) {
    Collation suggestion = Collections.min(List.of(collationTable), Comparator.comparingInt(
            c -> UTF8String.fromString(c.collationName).levenshteinDistance(
                    UTF8String.fromString(collationName.toUpperCase()))));
    return suggestion.collationName;
  }

  /**
   * Returns a collation-unaware StringSearch object for the given pattern and target strings.
   * While this object does not respect collation, it can be used to find occurrences of the pattern
   * in the target string for UTF8_BINARY or UTF8_BINARY_LCASE (if arguments are lowercased).
   */
  public static StringSearch getStringSearch(
          final UTF8String targetUTF8String,
          final UTF8String patternUTF8String) {
    return new StringSearch(patternUTF8String.toString(), targetUTF8String.toString());
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
