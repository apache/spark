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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import com.ibm.icu.text.CollationKey;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.text.StringSearch;
import com.ibm.icu.util.ULocale;
import com.ibm.icu.util.VersionInfo;

import org.apache.spark.SparkException;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Static entry point for collation aware string functions.
 * Provides functionality to the UTF8String object which respects defined collation settings.
 */
public final class CollationFactory {

  /**
   * Identifier for single a collation.
   */
  public static class CollationIdentifier {
    private final String provider;
    private final String name;
    private final String version;

    public CollationIdentifier(String provider, String collationName, String version) {
      this.provider = provider;
      this.name = collationName;
      this.version = version;
    }

    public static CollationIdentifier fromString(String identifier) {
      long numDots = identifier.chars().filter(ch -> ch == '.').count();
      assert(numDots > 0);

      if (numDots == 1) {
        String[] parts = identifier.split("\\.", 2);
        return new CollationIdentifier(parts[0], parts[1], null);
      }

      String[] parts = identifier.split("\\.", 3);
      return new CollationIdentifier(parts[0], parts[1], parts[2]);
    }

    /**
     * Returns the identifier's string value without the version.
     * This is used for the table schema as the schema doesn't care about the version,
     * only the statistics do.
     */
    public String toStringWithoutVersion() {
      return String.format("%s.%s", provider, name);
    }

    public String getProvider() {
      return provider;
    }

    public String getName() {
      return name;
    }

    public Optional<String> getVersion() {
      return Optional.ofNullable(version);
    }
  }

  public record CollationMeta(
    String catalog,
    String schema,
    String collationName,
    String language,
    String country,
    String icuVersion,
    String padAttribute,
    boolean accentSensitivity,
    boolean caseSensitivity,
    String spaceTrimming) { }

  /**
   * Entry encapsulating all information about a collation.
   */
  public static class Collation {
    public final String collationName;
    public final String provider;
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
     * Support for Lowercase Equality implies that it is possible to check equality on byte by
     * byte level, but only after calling "UTF8String.lowerCaseCodePoints" on both arguments.
     * This allows custom collation support for UTF8_LCASE collation in various Spark
     * expressions, as this particular collation is not supported by the external ICU library.
     */
    public final boolean supportsLowercaseEquality;

    /**
     * Support for Space Trimming implies that that based on specifier (for now only right trim)
     * leading, trailing or both spaces are removed from the input string before comparison.
     */
    public final boolean supportsSpaceTrimming;

    /**
     * Is Utf8 binary type as indicator if collation base type is UTF8 binary. Note currently only
     * collations Utf8_Binary and Utf8_Binary_RTRIM are considered as Utf8 binary type.
     */
    public final boolean isUtf8BinaryType;

    /**
     * Is Utf8 lcase type as indicator if collation base type is UTF8 lcase. Note currently only
     * collations Utf8_Lcase and Utf8_Lcase_RTRIM are considered as Utf8 Lcase type.
     */
    public final boolean isUtf8LcaseType;

    public Collation(
        String collationName,
        String provider,
        Collator collator,
        Comparator<UTF8String> comparator,
        String version,
        ToLongFunction<UTF8String> hashFunction,
        BiFunction<UTF8String, UTF8String, Boolean> equalsFunction,
        boolean isUtf8BinaryType,
        boolean isUtf8LcaseType,
        boolean supportsSpaceTrimming) {
      this.collationName = collationName;
      this.provider = provider;
      this.collator = collator;
      this.comparator = comparator;
      this.version = version;
      this.hashFunction = hashFunction;
      this.isUtf8BinaryType = isUtf8BinaryType;
      this.isUtf8LcaseType = isUtf8LcaseType;
      this.equalsFunction = equalsFunction;
      this.supportsSpaceTrimming = supportsSpaceTrimming;
      this.supportsBinaryEquality = !supportsSpaceTrimming && isUtf8BinaryType;
      this.supportsBinaryOrdering = !supportsSpaceTrimming && isUtf8BinaryType;
      this.supportsLowercaseEquality = !supportsSpaceTrimming && isUtf8LcaseType;
      // No Collation can simultaneously support binary equality and lowercase equality
      assert(!supportsBinaryEquality || !supportsLowercaseEquality);

      assert(SUPPORTED_PROVIDERS.contains(provider));
    }

    /**
     * Collation ID is defined as 32-bit integer. We specify binary layouts for different classes of
     * collations. Classes of collations are differentiated by most significant 3 bits (bit 31, 30
     * and 29), bit 31 being most significant and bit 0 being least significant.
     * ---
     * General collation ID binary layout:
     * bit 31:    1 for INDETERMINATE (requires all other bits to be 1 as well), 0 otherwise.
     * bit 30:    0 for predefined, 1 for user-defined.
     * Following bits are specified for predefined collations:
     * bit 29:    0 for UTF8_BINARY, 1 for ICU collations.
     * bit 28-24: Reserved.
     * bit 23-22: Reserved for version.
     * bit 21-19 Zeros, reserved for future trimmings.
     * bit 18 0 = none, 1 = right trim.
     * bit 17-0:  Depend on collation family.
     * ---
     * INDETERMINATE collation ID binary layout:
     * bit 31-0: 1
     * INDETERMINATE collation ID is equal to -1.
     * ---
     * User-defined collation ID binary layout:
     * bit 31:   0
     * bit 30:   1
     * bit 29-0: Undefined, reserved for future use.
     * ---
     * UTF8_BINARY collation ID binary layout:
     * bit 31-24: Zeroes.
     * bit 23-22: Zeroes, reserved for version.
     * bit 21-19 Zeros, reserved for future trimmings.
     * bit 18 0 = none, 1 = right trim.
     * bit 17-3:  Zeroes.
     * bit 2:     0, reserved for accent sensitivity.
     * bit 1:     0, reserved for uppercase and case-insensitive.
     * bit 0:     0 = case-sensitive, 1 = lowercase.
     * ---
     * ICU collation ID binary layout:
     * bit 31-30: Zeroes.
     * bit 29:    1
     * bit 28-24: Zeroes.
     * bit 23-22: Zeroes, reserved for version.
     * bit 21-18: Reserved for space trimming.
     * 0000 = none, 0001 = right trim. Bits 21-19 remain reserved and fixed to 0.
     * bit 17:    0 = case-sensitive, 1 = case-insensitive.
     * bit 16:    0 = accent-sensitive, 1 = accent-insensitive.
     * bit 15-14: Zeroes, reserved for punctuation sensitivity.
     * bit 13-12: Zeroes, reserved for first letter preference.
     * bit 11-0:  Locale ID as specified in `ICULocaleToId` mapping.
     * ---
     * Some illustrative examples of collation name to ID mapping:
     * - UTF8_BINARY        -> 0
     * - UTF8_BINARY_RTRIM  -> 0x00040000
     * - UTF8_LCASE         -> 1
     * - UTF8_LCASE_RTRIM   -> 0x00040001
     * - UNICODE            -> 0x20000000
     * - UNICODE_AI         -> 0x20010000
     * - UNICODE_CI         -> 0x20020000
     * - UNICODE_RTRIM      -> 0x20040000
     * - UNICODE_CI_AI      -> 0x20030000
     * - UNICODE_CI_RTRIM   -> 0x20060000
     * - UNICODE_AI_RTRIM   -> 0x20050000
     * - UNICODE_CI_AI_RTRIM-> 0x20070000
     * - af                 -> 0x20000001
     * - af_CI_AI           -> 0x20030001
     */
    private abstract static class CollationSpec {

      /**
       * Bit 30 in collation ID having value 0 for predefined and 1 for user-defined collation.
       */
      private enum DefinitionOrigin {
        PREDEFINED, USER_DEFINED
      }

      /**
       * Bit 29 in collation ID having value 0 for UTF8_BINARY family and 1 for ICU family of
       * collations.
       */
      protected enum ImplementationProvider {
        UTF8_BINARY, ICU
      }

      /**
       * Bit 18 in collation ID having value 0 for none and 1 for right trimming.
       * Bits 21, 20, 19 remained reserved (and fixed to 0) for future use.
       */
      protected enum SpaceTrimming {
        NONE, RTRIM
      }

      /**
       * Offset in binary collation ID layout.
       */
      private static final int DEFINITION_ORIGIN_OFFSET = 30;

      /**
       * Bitmask corresponding to width in bits in binary collation ID layout.
       */
      private static final int DEFINITION_ORIGIN_MASK = 0b1;

      /**
       * Offset in binary collation ID layout.
       */
      protected static final int IMPLEMENTATION_PROVIDER_OFFSET = 29;

      /**
       * Bitmask corresponding to width in bits in binary collation ID layout.
       */
      protected static final int IMPLEMENTATION_PROVIDER_MASK = 0b1;


      /**
       * Offset in binary collation ID layout.
       */
      protected static final int SPACE_TRIMMING_OFFSET = 18;

      /**
       * Bitmask corresponding to width in bits in binary collation ID layout.
       */
      protected static final int SPACE_TRIMMING_MASK = 0b1;

      private static final int INDETERMINATE_COLLATION_ID = -1;

      /**
       * Thread-safe cache mapping collation IDs to corresponding `Collation` instances.
       * We add entries to this cache lazily as new `Collation` instances are requested.
       */
      private static final Map<Integer, Collation> collationMap = new ConcurrentHashMap<>();

      /**
       * Utility function to retrieve `ImplementationProvider` enum instance from collation ID.
       */
      private static ImplementationProvider getImplementationProvider(int collationId) {
        return ImplementationProvider.values()[SpecifierUtils.getSpecValue(collationId,
          IMPLEMENTATION_PROVIDER_OFFSET, IMPLEMENTATION_PROVIDER_MASK)];
      }

      /**
       * Utility function to retrieve `DefinitionOrigin` enum instance from collation ID.
       */
      private static DefinitionOrigin getDefinitionOrigin(int collationId) {
        return DefinitionOrigin.values()[SpecifierUtils.getSpecValue(collationId,
          DEFINITION_ORIGIN_OFFSET, DEFINITION_ORIGIN_MASK)];
      }

      /**
       * Utility function to retrieve `SpaceTrimming` enum instance from collation ID.
       */
      protected static SpaceTrimming getSpaceTrimming(int collationId) {
        return SpaceTrimming.values()[SpecifierUtils.getSpecValue(collationId,
          SPACE_TRIMMING_OFFSET, SPACE_TRIMMING_MASK)];
      }

      protected static UTF8String applyTrimmingPolicy(UTF8String s, int collationId) {
        return applyTrimmingPolicy(s, getSpaceTrimming(collationId));
      }

      /**
       * Utility function to trim spaces when collation uses space trimming.
       */
      protected static UTF8String applyTrimmingPolicy(UTF8String s, SpaceTrimming spaceTrimming) {
        if(spaceTrimming == SpaceTrimming.RTRIM){
          return s.trimRight();
        }
        return s; // No trimming.
      }

      /**
       * Main entry point for retrieving `Collation` instance from collation ID.
       */
      private static Collation fetchCollation(int collationId) {
        // User-defined collations and INDETERMINATE collations cannot produce a `Collation`
        // instance.
        assert (collationId >= 0 && getDefinitionOrigin(collationId)
          == DefinitionOrigin.PREDEFINED);
        if (collationId == UTF8_BINARY_COLLATION_ID) {
          // Skip cache.
          return CollationSpecUTF8.UTF8_BINARY_COLLATION;
        } else if (collationMap.containsKey(collationId)) {
          // Already in cache.
          return collationMap.get(collationId);
        } else {
          // Build `Collation` instance and put into cache.
          CollationSpec spec;
          ImplementationProvider implementationProvider = getImplementationProvider(collationId);
          if (implementationProvider == ImplementationProvider.UTF8_BINARY) {
            spec = CollationSpecUTF8.fromCollationId(collationId);
          } else {
            spec = CollationSpecICU.fromCollationId(collationId);
          }
          Collation collation = spec.buildCollation();
          collationMap.put(collationId, collation);
          return collation;
        }
      }

      /**
       * Method for constructing errors thrown on providing invalid collation name.
       */
      protected static SparkException collationInvalidNameException(String collationName) {
        Map<String, String> params = new HashMap<>();
        final int maxSuggestions = 3;
        params.put("collationName", collationName);
        params.put("proposals", getClosestSuggestionsOnInvalidName(collationName, maxSuggestions));
        return new SparkException("COLLATION_INVALID_NAME",
          SparkException.constructMessageParams(params), null);
      }

      private static int collationNameToId(String collationName) throws SparkException {
        // Collation names provided by user are treated as case-insensitive.
        String collationNameUpper = collationName.toUpperCase();
        if (collationNameUpper.startsWith("UTF8_")) {
          return CollationSpecUTF8.collationNameToId(collationName, collationNameUpper);
        } else {
          return CollationSpecICU.collationNameToId(collationName, collationNameUpper);
        }
      }

      protected abstract Collation buildCollation();

      protected abstract CollationMeta buildCollationMeta();

      protected abstract String normalizedCollationName();

      static List<CollationIdentifier> listCollations() {
        return Stream.concat(
          CollationSpecUTF8.listCollations().stream(),
          CollationSpecICU.listCollations().stream()).toList();
      }

      static CollationMeta loadCollationMeta(CollationIdentifier collationIdentifier) {
        CollationMeta collationSpecUTF8 =
          CollationSpecUTF8.loadCollationMeta(collationIdentifier);
        if (collationSpecUTF8 == null) {
          return CollationSpecICU.loadCollationMeta(collationIdentifier);
        }
        return collationSpecUTF8;
      }
    }

    private static class CollationSpecUTF8 extends CollationSpec {

      /**
       * Bit 0 in collation ID having value 0 for plain UTF8_BINARY and 1 for UTF8_LCASE
       * collation.
       */
      private enum CaseSensitivity {
        UNSPECIFIED, LCASE
      }

      /**
       * Offset in binary collation ID layout.
       */
      private static final int CASE_SENSITIVITY_OFFSET = 0;

      /**
       * Bitmask corresponding to width in bits in binary collation ID layout.
       */
      private static final int CASE_SENSITIVITY_MASK = 0b1;

      private static final String UTF8_BINARY_COLLATION_NAME = "UTF8_BINARY";
      private static final String UTF8_LCASE_COLLATION_NAME = "UTF8_LCASE";

      private static final int UTF8_BINARY_COLLATION_ID =
        new CollationSpecUTF8(CaseSensitivity.UNSPECIFIED, SpaceTrimming.NONE).collationId;
      private static final int UTF8_LCASE_COLLATION_ID =
        new CollationSpecUTF8(CaseSensitivity.LCASE, SpaceTrimming.NONE).collationId;
      protected static Collation UTF8_BINARY_COLLATION =
        new CollationSpecUTF8(CaseSensitivity.UNSPECIFIED, SpaceTrimming.NONE).buildCollation();
      protected static Collation UTF8_LCASE_COLLATION =
        new CollationSpecUTF8(CaseSensitivity.LCASE, SpaceTrimming.NONE).buildCollation();

      private final CaseSensitivity caseSensitivity;
      private final SpaceTrimming spaceTrimming;
      private final int collationId;

      private CollationSpecUTF8(
          CaseSensitivity caseSensitivity,
          SpaceTrimming spaceTrimming) {
        this.caseSensitivity = caseSensitivity;
        this.spaceTrimming = spaceTrimming;

        int collationId =
          SpecifierUtils.setSpecValue(0, CASE_SENSITIVITY_OFFSET, caseSensitivity);
        this.collationId =
          SpecifierUtils.setSpecValue(collationId, SPACE_TRIMMING_OFFSET, spaceTrimming);
      }

      private static int collationNameToId(String originalName, String collationName)
          throws SparkException {

        int baseId;
        String collationNamePrefix;

        if (collationName.startsWith(UTF8_BINARY_COLLATION.collationName)) {
          baseId = UTF8_BINARY_COLLATION_ID;
          collationNamePrefix = UTF8_BINARY_COLLATION.collationName;
        } else if (collationName.startsWith(UTF8_LCASE_COLLATION.collationName)) {
          baseId = UTF8_LCASE_COLLATION_ID;
          collationNamePrefix = UTF8_LCASE_COLLATION.collationName;
        } else {
          // Throw exception with original (before case conversion) collation name.
          throw collationInvalidNameException(originalName);
        }

        String remainingSpecifiers = collationName.substring(collationNamePrefix.length());
        if(remainingSpecifiers.isEmpty()) {
          return baseId;
        }
        if(!remainingSpecifiers.startsWith("_")){
          throw collationInvalidNameException(originalName);
        }

        SpaceTrimming spaceTrimming = SpaceTrimming.NONE;
        String remainingSpec = remainingSpecifiers.substring(1);
        if (remainingSpec.equals("RTRIM")) {
          spaceTrimming = SpaceTrimming.RTRIM;
        } else {
          throw collationInvalidNameException(originalName);
        }

        return SpecifierUtils.setSpecValue(baseId, SPACE_TRIMMING_OFFSET, spaceTrimming);
      }

      private static CollationSpecUTF8 fromCollationId(int collationId) {
        // Extract case sensitivity from collation ID.
        int caseConversionOrdinal = SpecifierUtils.getSpecValue(collationId,
          CASE_SENSITIVITY_OFFSET, CASE_SENSITIVITY_MASK);
        // Extract space trimming from collation ID.
        int spaceTrimmingOrdinal = getSpaceTrimming(collationId).ordinal();
        assert(isValidCollationId(collationId));
        return new CollationSpecUTF8(
          CaseSensitivity.values()[caseConversionOrdinal],
          SpaceTrimming.values()[spaceTrimmingOrdinal]);
      }

      private static boolean isValidCollationId(int collationId) {
        collationId = SpecifierUtils.removeSpec(
          collationId,
          SPACE_TRIMMING_OFFSET,
          SPACE_TRIMMING_MASK);
        collationId = SpecifierUtils.removeSpec(
          collationId,
          CASE_SENSITIVITY_OFFSET,
          CASE_SENSITIVITY_MASK);
        return collationId == 0;
      }

      @Override
      protected Collation buildCollation() {
        if (caseSensitivity == CaseSensitivity.UNSPECIFIED) {
          Comparator<UTF8String> comparator;
          ToLongFunction<UTF8String> hashFunction;
          BiFunction<UTF8String, UTF8String, Boolean> equalsFunction;
          boolean supportsSpaceTrimming = spaceTrimming != SpaceTrimming.NONE;

          if (spaceTrimming == SpaceTrimming.NONE) {
            comparator = UTF8String::binaryCompare;
            hashFunction = s -> (long) s.hashCode();
            equalsFunction = UTF8String::equals;
          } else {
            comparator = (s1, s2) -> applyTrimmingPolicy(s1, spaceTrimming).binaryCompare(
              applyTrimmingPolicy(s2, spaceTrimming));
            hashFunction = s -> (long) applyTrimmingPolicy(s, spaceTrimming).hashCode();
            equalsFunction = (s1, s2) -> applyTrimmingPolicy(s1, spaceTrimming).equals(
              applyTrimmingPolicy(s2, spaceTrimming));
          }

          return new Collation(
            normalizedCollationName(),
            PROVIDER_SPARK,
            null,
            comparator,
            "1.0",
            hashFunction,
            equalsFunction,
            /* isUtf8BinaryType = */ true,
            /* isUtf8LcaseType = */ false,
            spaceTrimming != SpaceTrimming.NONE);
        } else {
          Comparator<UTF8String> comparator;
          ToLongFunction<UTF8String> hashFunction;

          if (spaceTrimming == SpaceTrimming.NONE) {
            comparator = CollationAwareUTF8String::compareLowerCase;
            hashFunction = s ->
              (long) CollationAwareUTF8String.lowerCaseCodePoints(s).hashCode();
          } else {
            comparator = (s1, s2) -> CollationAwareUTF8String.compareLowerCase(
              applyTrimmingPolicy(s1, spaceTrimming),
              applyTrimmingPolicy(s2, spaceTrimming));
            hashFunction = s -> (long) CollationAwareUTF8String.lowerCaseCodePoints(
              applyTrimmingPolicy(s, spaceTrimming)).hashCode();
          }

          return new Collation(
            normalizedCollationName(),
            PROVIDER_SPARK,
            null,
            comparator,
            "1.0",
            hashFunction,
            (s1, s2) -> comparator.compare(s1, s2) == 0,
            /* isUtf8BinaryType = */ false,
            /* isUtf8LcaseType = */ true,
            spaceTrimming != SpaceTrimming.NONE);
        }
      }

      @Override
      protected CollationMeta buildCollationMeta() {
        if (caseSensitivity == CaseSensitivity.UNSPECIFIED) {
          return new CollationMeta(
            CATALOG,
            SCHEMA,
            normalizedCollationName(),
            /* language = */ null,
            /* country = */ null,
            /* icuVersion = */ null,
            COLLATION_PAD_ATTRIBUTE,
            /* accentSensitivity = */ true,
            /* caseSensitivity = */ true,
            spaceTrimming.toString());
        } else {
          return new CollationMeta(
            CATALOG,
            SCHEMA,
            normalizedCollationName(),
            /* language = */ null,
            /* country = */ null,
            /* icuVersion = */ null,
            COLLATION_PAD_ATTRIBUTE,
            /* accentSensitivity = */ true,
            /* caseSensitivity = */ false,
            spaceTrimming.toString());
        }
      }

      /**
       * Compute normalized collation name. Components of collation name are given in order:
       * - Base collation name (UTF8_BINARY or UTF8_LCASE)
       * - Optional space trimming when non-default preceded by underscore
       * Examples: UTF8_BINARY, UTF8_BINARY_LCASE_LTRIM, UTF8_BINARY_TRIM.
       */
      @Override
      protected String normalizedCollationName() {
        StringBuilder builder = new StringBuilder();
        if(caseSensitivity == CaseSensitivity.UNSPECIFIED){
          builder.append(UTF8_BINARY_COLLATION_NAME);
        } else{
          builder.append(UTF8_LCASE_COLLATION_NAME);
        }
        if (spaceTrimming != SpaceTrimming.NONE) {
          builder.append('_');
          builder.append(spaceTrimming.toString());
        }
        return builder.toString();
      }

      static List<CollationIdentifier> listCollations() {
        CollationIdentifier UTF8_BINARY_COLLATION_IDENT =
          new CollationIdentifier(PROVIDER_SPARK, UTF8_BINARY_COLLATION_NAME, "1.0");
        CollationIdentifier UTF8_LCASE_COLLATION_IDENT =
          new CollationIdentifier(PROVIDER_SPARK, UTF8_LCASE_COLLATION_NAME, "1.0");
        return Arrays.asList(UTF8_BINARY_COLLATION_IDENT, UTF8_LCASE_COLLATION_IDENT);
      }

      static CollationMeta loadCollationMeta(CollationIdentifier collationIdentifier) {
        try {
          int collationId = CollationSpecUTF8.collationNameToId(
            collationIdentifier.name, collationIdentifier.name.toUpperCase());
          return CollationSpecUTF8.fromCollationId(collationId).buildCollationMeta();
        } catch (SparkException ignored) {
          // ignore
          return null;
        }
      }
    }

    private static class CollationSpecICU extends CollationSpec {

      /**
       * Bit 17 in collation ID having value 0 for case-sensitive and 1 for case-insensitive
       * collation.
       */
      private enum CaseSensitivity {
        CS, CI
      }

      /**
       * Bit 16 in collation ID having value 0 for accent-sensitive and 1 for accent-insensitive
       * collation.
       */
      private enum AccentSensitivity {
        AS, AI
      }

      /**
       * Offset in binary collation ID layout.
       */
      private static final int CASE_SENSITIVITY_OFFSET = 17;

      /**
       * Bitmask corresponding to width in bits in binary collation ID layout.
       */
      private static final int CASE_SENSITIVITY_MASK = 0b1;

      /**
       * Offset in binary collation ID layout.
       */
      private static final int ACCENT_SENSITIVITY_OFFSET = 16;

      /**
       * Bitmask corresponding to width in bits in binary collation ID layout.
       */
      private static final int ACCENT_SENSITIVITY_MASK = 0b1;

      /**
       * Array of locale names, each locale ID corresponds to the index in this array.
       */
      private static final String[] ICULocaleNames;

      /**
       * Mapping of locale names to corresponding `ULocale` instance.
       */
      private static final Map<String, ULocale> ICULocaleMap = new HashMap<>();

      /**
       * Used to parse user input collation names which are converted to uppercase.
       */
      private static final Map<String, String> ICULocaleMapUppercase = new HashMap<>();

      /**
       * Reverse mapping of `ICULocaleNames`.
       */
      private static final Map<String, Integer> ICULocaleToId = new HashMap<>();

      /**
       * ICU library Collator version passed to `Collation` instance.
       */
      private static final String ICU_COLLATOR_VERSION = "153.120.0.0";

      static {
        ICULocaleMap.put("UNICODE", ULocale.ROOT);
        // ICU-implemented `ULocale`s which have corresponding `Collator` installed.
        ULocale[] locales = Collator.getAvailableULocales();
        // Build locale names in format: language["_" optional script]["_" optional country code].
        // Examples: en, en_USA, sr_Cyrl_SRB
        for (ULocale locale : locales) {
          // Skip variants.
          if (locale.getVariant().isEmpty()) {
            String language = locale.getLanguage();
            // Require non-empty language as first component of locale name.
            assert (!language.isEmpty());
            StringBuilder builder = new StringBuilder(language);
            // Script tag.
            String script = locale.getScript();
            if (!script.isEmpty()) {
              builder.append('_');
              builder.append(script);
            }
            // 3-letter country code.
            String country = locale.getISO3Country();
            if (!country.isEmpty()) {
              builder.append('_');
              builder.append(country);
            }
            String localeName = builder.toString();
            // Verify locale names are unique.
            assert (!ICULocaleMap.containsKey(localeName));
            ICULocaleMap.put(localeName, locale);
          }
        }
        // Construct uppercase-normalized locale name mapping.
        for (String localeName : ICULocaleMap.keySet()) {
          String localeUppercase = localeName.toUpperCase();
          // Locale names are unique case-insensitively.
          assert (!ICULocaleMapUppercase.containsKey(localeUppercase));
          ICULocaleMapUppercase.put(localeUppercase, localeName);
        }
        // Construct locale name to ID mapping. Locale ID is defined as index in `ICULocaleNames`.
        ICULocaleNames = ICULocaleMap.keySet().toArray(new String[0]);
        Arrays.sort(ICULocaleNames);
        // Maximum number of locale IDs as defined by binary layout.
        assert (ICULocaleNames.length <= (1 << 12));
        for (int i = 0; i < ICULocaleNames.length; ++i) {
          ICULocaleToId.put(ICULocaleNames[i], i);
        }
      }

      private static final int UNICODE_COLLATION_ID = new CollationSpecICU(
        "UNICODE",
        CaseSensitivity.CS,
        AccentSensitivity.AS,
        SpaceTrimming.NONE).collationId;

      private static final int UNICODE_CI_COLLATION_ID = new CollationSpecICU(
        "UNICODE",
        CaseSensitivity.CI,
        AccentSensitivity.AS,
        SpaceTrimming.NONE).collationId;

      private final CaseSensitivity caseSensitivity;
      private final AccentSensitivity accentSensitivity;
      private final SpaceTrimming spaceTrimming;
      private final String locale;
      private final int collationId;

      private CollationSpecICU(
          String locale,
          CaseSensitivity caseSensitivity,
          AccentSensitivity accentSensitivity,
          SpaceTrimming spaceTrimming) {
        this.locale = locale;
        this.caseSensitivity = caseSensitivity;
        this.accentSensitivity = accentSensitivity;
        this.spaceTrimming = spaceTrimming;
        // Construct collation ID from locale, case-sensitivity and accent-sensitivity specifiers.
        int collationId = ICULocaleToId.get(locale);
        // Mandatory ICU implementation provider.
        collationId = SpecifierUtils.setSpecValue(collationId, IMPLEMENTATION_PROVIDER_OFFSET,
          ImplementationProvider.ICU);
        collationId = SpecifierUtils.setSpecValue(collationId, CASE_SENSITIVITY_OFFSET,
          caseSensitivity);
        collationId = SpecifierUtils.setSpecValue(collationId, ACCENT_SENSITIVITY_OFFSET,
          accentSensitivity);
        collationId = SpecifierUtils.setSpecValue(collationId, SPACE_TRIMMING_OFFSET,
          spaceTrimming);
        this.collationId = collationId;
      }

      private static int collationNameToId(
          String originalName, String collationName) throws SparkException {
        // Search for the longest locale match because specifiers are designed to be different from
        // script tag and country code, meaning the only valid locale name match can be the longest
        // one.
        int lastPos = -1;
        for (int i = 1; i <= collationName.length(); i++) {
          String localeName = collationName.substring(0, i);
          if (ICULocaleMapUppercase.containsKey(localeName)) {
            lastPos = i;
          }
        }
        if (lastPos == -1) {
          throw collationInvalidNameException(originalName);
        }
        String locale = collationName.substring(0, lastPos);
        int collationId = ICULocaleToId.get(ICULocaleMapUppercase.get(locale));
        collationId = SpecifierUtils.setSpecValue(collationId,
          IMPLEMENTATION_PROVIDER_OFFSET, ImplementationProvider.ICU);

        // No other specifiers present.
        if(collationName.equals(locale)){
          return collationId;
        }
        if(collationName.charAt(locale.length()) != '_'){
          throw collationInvalidNameException(originalName);
        }
        // Extract remaining specifiers and trim "_" separator.
        String remainingSpecifiers = collationName.substring(lastPos + 1);

        // Initialize default specifier flags.
        // Case sensitive, accent sensitive, no space trimming.
        boolean isCaseSpecifierSet = false;
        boolean isAccentSpecifierSet = false;
        boolean isSpaceTrimmingSpecifierSet = false;
        CaseSensitivity caseSensitivity = CaseSensitivity.CS;
        AccentSensitivity accentSensitivity = AccentSensitivity.AS;
        SpaceTrimming spaceTrimming = SpaceTrimming.NONE;

        String[] specifiers = remainingSpecifiers.split("_");

        // Iterate through specifiers and set corresponding flags
        for (String specifier : specifiers) {
          switch (specifier) {
            case "CI":
            case "CS":
              if (isCaseSpecifierSet) {
                throw collationInvalidNameException(originalName);
              }
              caseSensitivity = CaseSensitivity.valueOf(specifier);
              isCaseSpecifierSet = true;
              break;
            case "AI":
            case "AS":
              if (isAccentSpecifierSet) {
                throw collationInvalidNameException(originalName);
              }
              accentSensitivity = AccentSensitivity.valueOf(specifier);
              isAccentSpecifierSet = true;
              break;
            case "RTRIM":
              if (isSpaceTrimmingSpecifierSet) {
                throw collationInvalidNameException(originalName);
              }
              spaceTrimming = SpaceTrimming.valueOf(specifier);
              isSpaceTrimmingSpecifierSet = true;
              break;
            default:
              throw collationInvalidNameException(originalName);
          }
        }

        // Build collation ID from computed specifiers.
        collationId = SpecifierUtils.setSpecValue(collationId,
          CASE_SENSITIVITY_OFFSET, caseSensitivity);
        collationId = SpecifierUtils.setSpecValue(collationId,
          ACCENT_SENSITIVITY_OFFSET, accentSensitivity);
        collationId = SpecifierUtils.setSpecValue(collationId,
          SPACE_TRIMMING_OFFSET, spaceTrimming);
        return collationId;
      }

      private static CollationSpecICU fromCollationId(int collationId) {
        // Parse specifiers from collation ID.
        int spaceTrimmingOrdinal = SpecifierUtils.getSpecValue(collationId,
          SPACE_TRIMMING_OFFSET, SPACE_TRIMMING_MASK);
        int caseSensitivityOrdinal = SpecifierUtils.getSpecValue(collationId,
          CASE_SENSITIVITY_OFFSET, CASE_SENSITIVITY_MASK);
        int accentSensitivityOrdinal = SpecifierUtils.getSpecValue(collationId,
          ACCENT_SENSITIVITY_OFFSET, ACCENT_SENSITIVITY_MASK);
        collationId = SpecifierUtils.removeSpec(collationId,
          IMPLEMENTATION_PROVIDER_OFFSET, IMPLEMENTATION_PROVIDER_MASK);
        collationId = SpecifierUtils.removeSpec(collationId,
          SPACE_TRIMMING_OFFSET, SPACE_TRIMMING_MASK);
        collationId = SpecifierUtils.removeSpec(collationId,
          CASE_SENSITIVITY_OFFSET, CASE_SENSITIVITY_MASK);
        collationId = SpecifierUtils.removeSpec(collationId,
          ACCENT_SENSITIVITY_OFFSET, ACCENT_SENSITIVITY_MASK);
        // Locale ID remains after removing all other specifiers.
        int localeId = collationId;
        // Verify locale ID is valid against `ICULocaleNames` array.
        assert(localeId >= 0 && localeId < ICULocaleNames.length);
        CaseSensitivity caseSensitivity = CaseSensitivity.values()[caseSensitivityOrdinal];
        AccentSensitivity accentSensitivity = AccentSensitivity.values()[accentSensitivityOrdinal];
        SpaceTrimming spaceTrimming = SpaceTrimming.values()[spaceTrimmingOrdinal];
        String locale = ICULocaleNames[localeId];
        return new CollationSpecICU(locale, caseSensitivity, accentSensitivity, spaceTrimming);
      }

      @Override
      protected Collation buildCollation() {
        ULocale.Builder builder = new ULocale.Builder();
        builder.setLocale(ICULocaleMap.get(locale));
        // Compute unicode locale keyword for all combinations of case/accent sensitivity.
        if (caseSensitivity == CaseSensitivity.CS &&
            accentSensitivity == AccentSensitivity.AS) {
          builder.setUnicodeLocaleKeyword("ks", "level3");
        } else if (caseSensitivity == CaseSensitivity.CS &&
            accentSensitivity == AccentSensitivity.AI) {
          builder
            .setUnicodeLocaleKeyword("ks", "level1")
            .setUnicodeLocaleKeyword("kc", "true");
        } else if (caseSensitivity == CaseSensitivity.CI &&
            accentSensitivity == AccentSensitivity.AS) {
          builder.setUnicodeLocaleKeyword("ks", "level2");
        } else if (caseSensitivity == CaseSensitivity.CI &&
            accentSensitivity == AccentSensitivity.AI) {
          builder.setUnicodeLocaleKeyword("ks", "level1");
        }
        ULocale resultLocale = builder.build();
        Collator collator = Collator.getInstance(resultLocale);
        // Freeze ICU collator to ensure thread safety.
        collator.freeze();

        Comparator<UTF8String> comparator;
        ToLongFunction<UTF8String> hashFunction;

        if (spaceTrimming == SpaceTrimming.NONE) {
          hashFunction = s -> (long) collator.getCollationKey(
            s.toValidString()).hashCode();
          comparator = (s1, s2) ->
            collator.compare(s1.toValidString(), s2.toValidString());
        } else {
          comparator = (s1, s2) -> collator.compare(
            applyTrimmingPolicy(s1, spaceTrimming).toValidString(),
            applyTrimmingPolicy(s2, spaceTrimming).toValidString());
          hashFunction = s -> (long) collator.getCollationKey(
            applyTrimmingPolicy(s, spaceTrimming).toValidString()).hashCode();
        }

        return new Collation(
          normalizedCollationName(),
          PROVIDER_ICU,
          collator,
          comparator,
          ICU_COLLATOR_VERSION,
          hashFunction,
          (s1, s2) -> comparator.compare(s1, s2) == 0,
          /* isUtf8BinaryType = */ false,
          /* isUtf8LcaseType = */ false,
          spaceTrimming != SpaceTrimming.NONE);
      }

      @Override
      protected CollationMeta buildCollationMeta() {
        return new CollationMeta(
          CATALOG,
          SCHEMA,
          normalizedCollationName(),
          ICULocaleMap.get(locale).getDisplayLanguage(),
          ICULocaleMap.get(locale).getDisplayCountry(),
          VersionInfo.ICU_VERSION.toString(),
          COLLATION_PAD_ATTRIBUTE,
          accentSensitivity == AccentSensitivity.AS,
          caseSensitivity == CaseSensitivity.CS,
          spaceTrimming.toString());
      }

      /**
       * Compute normalized collation name. Components of collation name are given in order:
       * - Locale name
       * - Optional case sensitivity when non-default preceded by underscore
       * - Optional accent sensitivity when non-default preceded by underscore
       * - Optional space trimming when non-default preceded by underscore
       * Examples: en, en_USA_CI_LTRIM, en_USA_CI_AI, en_USA_CI_AI_TRIM, sr_Cyrl_SRB_AI.
       */
      @Override
      protected String normalizedCollationName() {
        StringBuilder builder = new StringBuilder();
        builder.append(locale);
        if (caseSensitivity != CaseSensitivity.CS) {
          builder.append('_');
          builder.append(caseSensitivity.toString());
        }
        if (accentSensitivity != AccentSensitivity.AS) {
          builder.append('_');
          builder.append(accentSensitivity.toString());
        }
        if(spaceTrimming != SpaceTrimming.NONE) {
          builder.append('_');
          builder.append(spaceTrimming.toString());
        }
        return builder.toString();
      }

      private static List<String> allCollationNames() {
        List<String> collationNames = new ArrayList<>();
        List<String> caseAccentSpecifiers = Arrays.asList("", "_AI", "_CI", "_CI_AI");
        for (String locale : ICULocaleToId.keySet()) {
          for (String caseAccent : caseAccentSpecifiers) {
            String collationName = locale + caseAccent;
            collationNames.add(collationName);
          }
        }
        return collationNames.stream().sorted().toList();
      }

      static List<CollationIdentifier> listCollations() {
        return allCollationNames().stream().map(name ->
          new CollationIdentifier(PROVIDER_ICU, name, VersionInfo.ICU_VERSION.toString())).toList();
      }

      static CollationMeta loadCollationMeta(CollationIdentifier collationIdentifier) {
        try {
          int collationId = CollationSpecICU.collationNameToId(
            collationIdentifier.name, collationIdentifier.name.toUpperCase());
          return CollationSpecICU.fromCollationId(collationId).buildCollationMeta();
        } catch (SparkException ignored) {
          // ignore
          return null;
        }
      }
    }

    /**
     * Utility class for manipulating conversions between collation IDs and specifier enums/locale
     * IDs. Scope bitwise operations here to avoid confusion.
     */
    private static class SpecifierUtils {
      private static int getSpecValue(int collationId, int offset, int mask) {
        return (collationId >> offset) & mask;
      }

      private static int removeSpec(int collationId, int offset, int mask) {
        return collationId & ~(mask << offset);
      }

      private static int setSpecValue(int collationId, int offset, Enum spec) {
        return collationId | (spec.ordinal() << offset);
      }
    }

    /** Returns the collation identifier. */
    public CollationIdentifier identifier() {
      return new CollationIdentifier(provider, collationName, version);
    }
  }

  public static final String CATALOG = "SYSTEM";
  public static final String SCHEMA = "BUILTIN";
  public static final String PROVIDER_SPARK = "spark";
  public static final String PROVIDER_ICU = "icu";
  public static final List<String> SUPPORTED_PROVIDERS = List.of(PROVIDER_SPARK, PROVIDER_ICU);
  public static final String COLLATION_PAD_ATTRIBUTE = "NO_PAD";

  public static final int UTF8_BINARY_COLLATION_ID =
    Collation.CollationSpecUTF8.UTF8_BINARY_COLLATION_ID;
  public static final int UTF8_LCASE_COLLATION_ID =
    Collation.CollationSpecUTF8.UTF8_LCASE_COLLATION_ID;
  public static final int UNICODE_COLLATION_ID =
    Collation.CollationSpecICU.UNICODE_COLLATION_ID;
  public static final int UNICODE_CI_COLLATION_ID =
    Collation.CollationSpecICU.UNICODE_CI_COLLATION_ID;
  public static final int INDETERMINATE_COLLATION_ID =
    Collation.CollationSpec.INDETERMINATE_COLLATION_ID;

  /**
   * Returns a StringSearch object for the given pattern and target strings, under collation
   * rules corresponding to the given collationId. The external ICU library StringSearch object can
   * be used to find occurrences of the pattern in the target string, while respecting collation.
   * When given invalid UTF8Strings, the method will first convert them to valid strings, and then
   * instantiate the StringSearch object. However, original UTF8Strings will remain unchanged.
   */
  public static StringSearch getStringSearch(
      final UTF8String targetUTF8String,
      final UTF8String patternUTF8String,
      final int collationId) {
    return getStringSearch(targetUTF8String.toValidString(), patternUTF8String.toValidString(),
      collationId);
  }

  /**
   * Returns a StringSearch object for the given pattern and target strings, under collation
   * rules corresponding to the given collationId. The external ICU library StringSearch object can
   * be used to find occurrences of the pattern in the target string, while respecting collation.
   */
  public static StringSearch getStringSearch(
      final String targetString,
      final String patternString,
      final int collationId) {
    CharacterIterator target = new StringCharacterIterator(targetString);
    Collator collator = CollationFactory.fetchCollation(collationId).collator;
    return new StringSearch(patternString, target, (RuleBasedCollator) collator);
  }

  /**
   * Returns a collation-unaware StringSearch object for the given pattern and target strings.
   * While this object does not respect collation, it can be used to find occurrences of the pattern
   * in the target string for UTF8_BINARY or UTF8_LCASE (if arguments are lowercased).
   * When given invalid UTF8Strings, the method will first convert them to valid strings, and then
   * instantiate the StringSearch object. However, original UTF8Strings will remain unchanged.
   */
  public static StringSearch getStringSearch(
      final UTF8String targetUTF8String,
      final UTF8String patternUTF8String) {
    return new StringSearch(patternUTF8String.toValidString(), targetUTF8String.toValidString());
  }

  /**
   * Returns the collation ID for the given collation name.
   */
  public static int collationNameToId(String collationName) throws SparkException {
    // If collation name is given as a fully qualified name, extract the actual collation name as
    // the last part of the [catalog].[schema].[collation_name] name.
    long numDots = collationName.chars().filter(ch -> ch == '.').count();
    if (numDots > 0) {
      String[] nameParts = collationName.split("\\.");
      // Currently only predefined collations are supported.
      if (numDots != 2 || !CollationFactory.CATALOG.equalsIgnoreCase(nameParts[0]) ||
          !CollationFactory.SCHEMA.equalsIgnoreCase(nameParts[1])) {
        throw CollationFactory.Collation.CollationSpec.collationInvalidNameException(collationName);
      }
      return Collation.CollationSpec.collationNameToId(nameParts[2]);
    }
    return Collation.CollationSpec.collationNameToId(collationName);
  }

  /**
   * Returns the fully qualified collation name for the given collation ID.
   */
  public static String fullyQualifiedName(int collationId) {
    Collation.CollationSpec.DefinitionOrigin definitionOrigin =
        Collation.CollationSpec.getDefinitionOrigin(collationId);
    // Currently only predefined collations are supported.
    assert definitionOrigin == Collation.CollationSpec.DefinitionOrigin.PREDEFINED;
    return String.format("%s.%s.%s", CATALOG, SCHEMA,
        Collation.CollationSpec.fetchCollation(collationId).collationName);
  }

  public static boolean isCaseInsensitive(int collationId) {
    return Collation.CollationSpecICU.fromCollationId(collationId).caseSensitivity ==
            Collation.CollationSpecICU.CaseSensitivity.CI;
  }

  public static boolean isAccentInsensitive(int collationId) {
    return Collation.CollationSpecICU.fromCollationId(collationId).accentSensitivity ==
            Collation.CollationSpecICU.AccentSensitivity.AI;
  }

  public static void assertValidProvider(String provider) throws SparkException {
    if (!SUPPORTED_PROVIDERS.contains(provider.toLowerCase())) {
      Map<String, String> params = Map.of(
        "provider", provider,
        "supportedProviders", String.join(", ", SUPPORTED_PROVIDERS)
      );

      throw new SparkException(
        "COLLATION_INVALID_PROVIDER", SparkException.constructMessageParams(params), null);
    }
  }

  public static Collation fetchCollation(int collationId) {
    return Collation.CollationSpec.fetchCollation(collationId);
  }

  public static Collation fetchCollation(String collationName) throws SparkException {
    return fetchCollation(collationNameToId(collationName));
  }

  public static String[] getICULocaleNames() {
    return Collation.CollationSpecICU.ICULocaleNames;
  }

  public static UTF8String getCollationKey(UTF8String input, int collationId) {
    Collation collation = fetchCollation(collationId);
    if (collation.supportsSpaceTrimming) {
      input = Collation.CollationSpec.applyTrimmingPolicy(input, collationId);
    }
    if (collation.isUtf8BinaryType) {
      return input;
    } else if (collation.isUtf8LcaseType) {
      return CollationAwareUTF8String.lowerCaseCodePoints(input);
    } else {
      CollationKey collationKey = collation.collator.getCollationKey(
        input.toValidString());
      return UTF8String.fromBytes(collationKey.toByteArray());
    }
  }

  public static byte[] getCollationKeyBytes(UTF8String input, int collationId) {
    Collation collation = fetchCollation(collationId);
    if (collation.supportsSpaceTrimming) {
      input = Collation.CollationSpec.applyTrimmingPolicy(input, collationId);
    }
    if (collation.isUtf8BinaryType) {
      return input.getBytes();
    } else if (collation.isUtf8LcaseType) {
      return CollationAwareUTF8String.lowerCaseCodePoints(input).getBytes();
    } else {
      return collation.collator.getCollationKey(
        input.toValidString()).toByteArray();
    }
  }

  /**
   * Returns same string if collation name is valid or the closest suggestion if it is invalid.
   */
  public static String getClosestSuggestionsOnInvalidName(
      String collationName, int maxSuggestions) {
    String[] validRootNames;
    String[] validModifiers;
    if (collationName.startsWith("UTF8_")) {
      validRootNames = new String[]{
        Collation.CollationSpecUTF8.UTF8_BINARY_COLLATION.collationName,
        Collation.CollationSpecUTF8.UTF8_LCASE_COLLATION.collationName
      };
      validModifiers = new String[0];
    } else {
      validRootNames = getICULocaleNames();
      validModifiers = new String[]{"_CI", "_AI", "_CS", "_AS"};
    }

    // Split modifiers and locale name.
    final int MODIFIER_LENGTH = 3;
    String localeName = collationName.toUpperCase();
    List<String> modifiers = new ArrayList<>();
    while (Arrays.stream(validModifiers).anyMatch(localeName::endsWith)) {
      modifiers.add(localeName.substring(localeName.length() - MODIFIER_LENGTH));
      localeName = localeName.substring(0, localeName.length() - MODIFIER_LENGTH);
    }

    // Suggest version with unique modifiers.
    Collections.reverse(modifiers);
    modifiers = modifiers.stream().distinct().toList();

    // Remove conflicting settings.
    if (modifiers.contains("_CI") && modifiers.contains(("_CS"))) {
      modifiers = modifiers.stream().filter(m -> !m.equals("_CI")).toList();
    }

    if (modifiers.contains("_AI") && modifiers.contains(("_AS"))) {
      modifiers = modifiers.stream().filter(m -> !m.equals("_AI")).toList();
    }

    final String finalLocaleName = localeName;
    Comparator<String> distanceComparator = (c1, c2) -> {
      int distance1 = UTF8String.fromString(c1.toUpperCase())
              .levenshteinDistance(UTF8String.fromString(finalLocaleName));
      int distance2 = UTF8String.fromString(c2.toUpperCase())
              .levenshteinDistance(UTF8String.fromString(finalLocaleName));
      return Integer.compare(distance1, distance2);
    };

    String[] rootNamesByDistance = Arrays.copyOf(validRootNames, validRootNames.length);
    Arrays.sort(rootNamesByDistance, distanceComparator);
    Function<String, Boolean> isCollationNameValid = name -> {
      try {
        collationNameToId(name);
        return true;
      } catch (SparkException e) {
        return false;
      }
    };

    final int suggestionThreshold = 3;
    final ArrayList<String> suggestions = new ArrayList<>(maxSuggestions);
    for (int i = 0; i < maxSuggestions; i++) {
      // Add at least one suggestion.
      // Add others if distance from the original is lower than threshold.
      String suggestion = rootNamesByDistance[i] + String.join("", modifiers);
      assert(isCollationNameValid.apply(suggestion));
      if (suggestions.isEmpty()) {
        suggestions.add(suggestion);
      } else {
        int distance = UTF8String.fromString(suggestion.toUpperCase())
          .levenshteinDistance(UTF8String.fromString(collationName.toUpperCase()));
        if (distance < suggestionThreshold) {
          suggestions.add(suggestion);
        } else {
          break;
        }
      }
    }

    return String.join(", ", suggestions);
  }

  public static List<CollationIdentifier> listCollations() {
    return Collation.CollationSpec.listCollations();
  }

  public static CollationMeta loadCollationMeta(CollationIdentifier collationIdentifier) {
    return Collation.CollationSpec.loadCollationMeta(collationIdentifier);
  }
}
