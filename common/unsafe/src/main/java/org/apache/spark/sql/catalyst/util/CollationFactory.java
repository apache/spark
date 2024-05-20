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
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.text.StringSearch;
import com.ibm.icu.util.ULocale;
import com.ibm.icu.text.CollationKey;
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
     * Collation id is defined as 32-bit integer.
     * We specify binary layouts for different classes of collations.
     * Classes of collations are differentiated by most significant 3 bits (bit 31, 30 and 29),
     * bit 31 being most significant and bit 0 being least significant.
     * ---
     * INDETERMINATE collation id binary layout:
     * bit 31-0: 1
     * INDETERMINATE collation id is equal to -1
     * ---
     * user-defined collation id binary layout:
     * bit 31:   0
     * bit 30:   1
     * bit 29-0: undefined, reserved for future use
     * ---
     * UTF8_BINARY collation id binary layout:
     * bit 31-22: zeroes
     * bit 21-18: zeroes, reserved for space trimming
     * bit 17-16: zeroes, reserved for version
     * bit 15-3:  zeroes
     * bit 2:     0, reserved for accent sensitivity
     * bit 1:     0, reserved for uppercase and case-insensitive
     * bit 0:     0 = case-sensitive, 1 = lowercase
     * ---
     * ICU collation id binary layout:
     * bit 31-30: zeroes
     * bit 29:    1
     * bit 28-24: zeroes
     * bit 23-22: zeroes, reserved for version
     * bit 21-18: zeroes, reserved for space trimming
     * bit 17:    0 = case-sensitive, 1 = case-insensitive
     * bit 16:    0 = accent-sensitive, 1 = accent-insensitive
     * bit 15-14: zeroes, reserved for punctuation sensitivity
     * bit 13-12: zeroes, reserved for first letter preference
     * bit 11-0:  locale id as specified in `ICULocaleToId` mapping
     * ---
     * Some illustrative examples of collation name to id mapping:
     * - UTF8_BINARY       -> 0
     * - UTF8_BINARY_LCASE -> 1
     * - UNICODE           -> 0x20000000
     * - UNICODE_AI        -> 0x20010000
     * - UNICODE_CI        -> 0x20020000
     * - UNICODE_CI_AI     -> 0x20030000
     * - af                -> 0x20000001
     * - af_CI_AI          -> 0x20030001
     */
    private abstract static class CollationSpec {

      private enum DefinitionOrigin {
        PREDEFINED, USER_DEFINED
      }

      protected enum ImplementationProvider {
        UTF8_BINARY, ICU
      }

      private static final int DEFINITION_ORIGIN_OFFSET = 30;
      private static final int DEFINITION_ORIGIN_MASK = 0b1;
      protected static final int IMPLEMENTATION_PROVIDER_OFFSET = 29;
      protected static final int IMPLEMENTATION_PROVIDER_MASK = 0b1;

      private static final int INDETERMINATE_COLLATION_ID = -1;

      private static final Map<Integer, Collation> collationMap = new ConcurrentHashMap<>();

      private static ImplementationProvider getImplementationProvider(int collationId) {
        return ImplementationProvider.values()[SpecifierUtils.getSpecValue(collationId,
          IMPLEMENTATION_PROVIDER_OFFSET, IMPLEMENTATION_PROVIDER_MASK)];
      }

      private static DefinitionOrigin getDefinitionOrigin(int collationId) {
        return DefinitionOrigin.values()[SpecifierUtils.getSpecValue(collationId,
          DEFINITION_ORIGIN_OFFSET, DEFINITION_ORIGIN_MASK)];
      }

      private static Collation fetchCollation(int collationId) {
        assert (collationId >= 0 && getDefinitionOrigin(collationId)
          == DefinitionOrigin.PREDEFINED);
        if (collationId == UTF8_BINARY_COLLATION_ID) {
          return CollationSpecUTF8Binary.UTF8_BINARY_COLLATION;
        } else if (collationMap.containsKey(collationId)) {
          return collationMap.get(collationId);
        } else {
          CollationSpec spec;
          ImplementationProvider implementationProvider = getImplementationProvider(collationId);
          if (implementationProvider == ImplementationProvider.UTF8_BINARY) {
            spec = CollationSpecUTF8Binary.fromCollationId(collationId);
          } else {
            spec = CollationSpecICU.fromCollationId(collationId);
          }
          Collation collation = spec.buildCollation();
          collationMap.put(collationId, collation);
          return collation;
        }
      }

      protected static SparkException collationInvalidNameException(String collationName) {
        return new SparkException("COLLATION_INVALID_NAME",
          SparkException.constructMessageParams(Map.of("collationName", collationName)), null);
      }

      private static int collationNameToId(String collationName) throws SparkException {
        String collationNameUpper = collationName.toUpperCase();
        if (collationNameUpper.startsWith("UTF8_BINARY")) {
          return CollationSpecUTF8Binary.collationNameToId(collationName, collationNameUpper);
        } else {
          return CollationSpecICU.collationNameToId(collationName, collationNameUpper);
        }
      }

      protected abstract Collation buildCollation();
    }

    private static class CollationSpecUTF8Binary extends CollationSpec {

      private static final int CASE_SENSITIVITY_OFFSET = 0;
      private static final int CASE_SENSITIVITY_MASK = 0b1;

      private enum CaseSensitivity {
        UNSPECIFIED, LCASE
      }

      private static final int UTF8_BINARY_COLLATION_ID =
        new CollationSpecUTF8Binary(CaseSensitivity.UNSPECIFIED).collationId;
      private static final int UTF8_BINARY_LCASE_COLLATION_ID =
        new CollationSpecUTF8Binary(CaseSensitivity.LCASE).collationId;
      protected static Collation UTF8_BINARY_COLLATION =
        new CollationSpecUTF8Binary(CaseSensitivity.UNSPECIFIED).buildCollation();
      protected static Collation UTF8_BINARY_LCASE_COLLATION =
        new CollationSpecUTF8Binary(CaseSensitivity.LCASE).buildCollation();

      private final int collationId;

      private CollationSpecUTF8Binary(CaseSensitivity caseSensitivity) {
        this.collationId =
          SpecifierUtils.setSpecValue(0, CASE_SENSITIVITY_OFFSET, caseSensitivity);
      }

      private static int collationNameToId(String originalName, String collationName)
          throws SparkException {
        if (UTF8_BINARY_COLLATION.collationName.equals(collationName)) {
          return UTF8_BINARY_COLLATION_ID;
        } else if (UTF8_BINARY_LCASE_COLLATION.collationName.equals(collationName)) {
          return UTF8_BINARY_LCASE_COLLATION_ID;
        } else {
          throw collationInvalidNameException(originalName);
        }
      }

      private static CollationSpecUTF8Binary fromCollationId(int collationId) {
        int caseConversionOrdinal = SpecifierUtils.getSpecValue(collationId,
          CASE_SENSITIVITY_OFFSET, CASE_SENSITIVITY_MASK);
        assert (SpecifierUtils.removeSpec(collationId,
          CASE_SENSITIVITY_OFFSET, CASE_SENSITIVITY_MASK) == 0);
        return new CollationSpecUTF8Binary(CaseSensitivity.values()[caseConversionOrdinal]);
      }

      @Override
      protected Collation buildCollation() {
        if (collationId == UTF8_BINARY_COLLATION_ID) {
          return new Collation("UTF8_BINARY", null, UTF8String::binaryCompare, "1.0",
            s -> (long) s.hashCode(), true, true, false);
        } else {
          return new Collation("UTF8_BINARY_LCASE", null, UTF8String::compareLowerCase, "1.0",
            s -> (long) s.toLowerCase().hashCode(), false, false, true);
        }
      }
    }

    private static class CollationSpecICU extends CollationSpec {

      private enum CaseSensitivity {
        CS, CI
      }

      private enum AccentSensitivity {
        AS, AI
      }

      private static final int CASE_SENSITIVITY_OFFSET = 17;
      private static final int CASE_SENSITIVITY_MASK = 0b1;
      private static final int ACCENT_SENSITIVITY_OFFSET = 16;
      private static final int ACCENT_SENSITIVITY_MASK = 0b1;

      // Array of locale names, each locale id corresponds to the index in this array
      private static final String[] ICULocaleNames;

      // Mapping of locale names to corresponding `ULocale` instance
      private static final Map<String, ULocale> ICULocaleMap = new HashMap<>();

      // Used to parse user input collation names which are converted to uppercase
      private static final Map<String, String> ICULocaleMapUppercase = new HashMap<>();

      // Reverse mapping of `ICULocaleNames`
      private static final Map<String, Integer> ICULocaleToId = new HashMap<>();

      private static final String ICU_COLLATOR_VERSION = "153.120.0.0";

      static {
        ICULocaleMap.put("UNICODE", ULocale.ROOT);
        ULocale[] locales = Collator.getAvailableULocales();
        for (ULocale locale : locales) {
          if (locale.getVariant().isEmpty()) {
            String language = locale.getLanguage();
            assert (!language.isEmpty());
            StringBuilder builder = new StringBuilder(language);
            String script = locale.getScript();
            if (!script.isEmpty()) {
              builder.append('_');
              builder.append(script);
            }
            String country = locale.getISO3Country();
            if (!country.isEmpty()) {
              builder.append('_');
              builder.append(country);
            }
            String localeName = builder.toString();
            // locale names are unique
            assert (!ICULocaleMap.containsKey(localeName));
            ICULocaleMap.put(localeName, locale);
          }
        }
        for (String localeName : ICULocaleMap.keySet()) {
          String localeUppercase = localeName.toUpperCase();
          // Locale names are unique case-insensitively.
          assert (!ICULocaleMapUppercase.containsKey(localeUppercase));
          ICULocaleMapUppercase.put(localeUppercase, localeName);
        }
        ICULocaleNames = ICULocaleMap.keySet().toArray(new String[0]);
        Arrays.sort(ICULocaleNames);
        // maximum number of locale ids as defined by binary layout
        assert (ICULocaleNames.length <= (1 << 12));
        for (int i = 0; i < ICULocaleNames.length; ++i) {
          ICULocaleToId.put(ICULocaleNames[i], i);
        }
      }

      private static final int UNICODE_COLLATION_ID =
        new CollationSpecICU("UNICODE", CaseSensitivity.CS, AccentSensitivity.AS).collationId;
      private static final int UNICODE_CI_COLLATION_ID =
        new CollationSpecICU("UNICODE", CaseSensitivity.CI, AccentSensitivity.AS).collationId;

      private final CaseSensitivity caseSensitivity;
      private final AccentSensitivity accentSensitivity;
      private final String locale;
      private final int collationId;

      private CollationSpecICU(String locale, CaseSensitivity caseSensitivity,
          AccentSensitivity accentSensitivity) {
        this.locale = locale;
        this.caseSensitivity = caseSensitivity;
        this.accentSensitivity = accentSensitivity;
        int collationId = ICULocaleToId.get(locale);
        collationId = SpecifierUtils.setSpecValue(collationId, IMPLEMENTATION_PROVIDER_OFFSET,
          ImplementationProvider.ICU);
        collationId = SpecifierUtils.setSpecValue(collationId, CASE_SENSITIVITY_OFFSET,
          caseSensitivity);
        collationId = SpecifierUtils.setSpecValue(collationId, ACCENT_SENSITIVITY_OFFSET,
          accentSensitivity);
        this.collationId = collationId;
      }

      private static int collationNameToId(
          String originalName, String collationName) throws SparkException {
        // search for the longest locale match because specifiers are designed to be different from
        // script tag and country code, meaning the only valid locale name match can be
        // the longest one
        int lastPos = -1;
        for (int i = 1; i <= collationName.length(); i++) {
          String localeName = collationName.substring(0, i);
          if (ICULocaleMapUppercase.containsKey(localeName)) {
            lastPos = i;
          }
        }
        if (lastPos == -1) {
          throw collationInvalidNameException(originalName);
        } else {
          String locale = collationName.substring(0, lastPos);
          int collationId = ICULocaleToId.get(ICULocaleMapUppercase.get(locale));

          // try all combinations of AS/AI and CS/CI
          CaseSensitivity caseSensitivity;
          AccentSensitivity accentSensitivity;
          if (collationName.equals(locale) ||
              collationName.equals(locale + "_AS") ||
              collationName.equals(locale + "_CS") ||
              collationName.equals(locale + "_AS_CS") ||
              collationName.equals(locale + "_CS_AS")
          ) {
            caseSensitivity = CaseSensitivity.CS;
            accentSensitivity = AccentSensitivity.AS;
          } else if (collationName.equals(locale + "_CI") ||
              collationName.equals(locale + "_AS_CI") ||
              collationName.equals(locale + "_CI_AS")) {
            caseSensitivity = CaseSensitivity.CI;
            accentSensitivity = AccentSensitivity.AS;
          } else if (collationName.equals(locale + "_AI") ||
              collationName.equals(locale + "_CS_AI") ||
              collationName.equals(locale + "_AI_CS")) {
            caseSensitivity = CaseSensitivity.CS;
            accentSensitivity = AccentSensitivity.AI;
          } else if (collationName.equals(locale + "_AI_CI") ||
              collationName.equals(locale + "_CI_AI")) {
            caseSensitivity = CaseSensitivity.CI;
            accentSensitivity = AccentSensitivity.AI;
          } else {
            throw collationInvalidNameException(originalName);
          }

          collationId = SpecifierUtils.setSpecValue(collationId,
            IMPLEMENTATION_PROVIDER_OFFSET, ImplementationProvider.ICU);
          collationId = SpecifierUtils.setSpecValue(collationId,
            CASE_SENSITIVITY_OFFSET, caseSensitivity);
          collationId = SpecifierUtils.setSpecValue(collationId,
            ACCENT_SENSITIVITY_OFFSET, accentSensitivity);
          return collationId;
        }
      }

      private static CollationSpecICU fromCollationId(int collationId) {
        int caseSensitivityOrdinal = SpecifierUtils.getSpecValue(collationId,
          CASE_SENSITIVITY_OFFSET, CASE_SENSITIVITY_MASK);
        int accentSensitivityOrdinal = SpecifierUtils.getSpecValue(collationId,
          ACCENT_SENSITIVITY_OFFSET, ACCENT_SENSITIVITY_MASK);
        collationId = SpecifierUtils.removeSpec(collationId,
          IMPLEMENTATION_PROVIDER_OFFSET, IMPLEMENTATION_PROVIDER_MASK);
        collationId = SpecifierUtils.removeSpec(collationId,
          CASE_SENSITIVITY_OFFSET, CASE_SENSITIVITY_MASK);
        collationId = SpecifierUtils.removeSpec(collationId,
          ACCENT_SENSITIVITY_OFFSET, ACCENT_SENSITIVITY_MASK);
        int localeId = collationId;
        assert (localeId < ICULocaleNames.length);
        CaseSensitivity caseSensitivity = CaseSensitivity.values()[caseSensitivityOrdinal];
        AccentSensitivity accentSensitivity = AccentSensitivity.values()[accentSensitivityOrdinal];
        String locale = ICULocaleNames[localeId];
        return new CollationSpecICU(locale, caseSensitivity, accentSensitivity);
      }

      @Override
      protected Collation buildCollation() {
        ULocale.Builder builder = new ULocale.Builder();
        builder.setLocale(ICULocaleMap.get(locale));
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
        collator.freeze();
        return new Collation(
          collationName(),
          collator,
          (s1, s2) -> collator.compare(s1.toString(), s2.toString()),
          ICU_COLLATOR_VERSION,
          s -> (long) collator.getCollationKey(s.toString()).hashCode(),
          collationId == UNICODE_COLLATION_ID,
          false,
          false);
      }

      private String collationName() {
        StringBuilder builder = new StringBuilder();
        if (locale.equals("UNICODE")) {
          builder.append("UNICODE");
        } else {
          ULocale uLocale = ICULocaleMap.get(locale);
          builder.append(uLocale.getLanguage());
          String script = uLocale.getScript();
          if (!script.isEmpty()) {
            builder.append('_');
            builder.append(script);
          }
          String country = uLocale.getISO3Country();
          if (!country.isEmpty()) {
            builder.append('_');
            builder.append(country);
          }
        }
        if (caseSensitivity != CaseSensitivity.CS) {
          builder.append('_');
          builder.append(caseSensitivity.toString());
        }
        if (accentSensitivity != AccentSensitivity.AS) {
          builder.append('_');
          builder.append(accentSensitivity.toString());
        }
        return builder.toString();
      }
    }

    // scope bitwise operations here to avoid confusion
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
  }

  public static final int UTF8_BINARY_COLLATION_ID =
    Collation.CollationSpecUTF8Binary.UTF8_BINARY_COLLATION_ID;
  public static final int UTF8_BINARY_LCASE_COLLATION_ID =
    Collation.CollationSpecUTF8Binary.UTF8_BINARY_LCASE_COLLATION_ID;
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
   */
  public static StringSearch getStringSearch(
      final UTF8String targetUTF8String,
      final UTF8String patternUTF8String,
      final int collationId) {
    return getStringSearch(targetUTF8String.toString(), patternUTF8String.toString(), collationId);
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
    return Collation.CollationSpec.collationNameToId(collationName);
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
    if (collation.supportsBinaryEquality) {
      return input;
    } else if (collation.supportsLowercaseEquality) {
      return input.toLowerCase();
    } else {
      CollationKey collationKey = collation.collator.getCollationKey(input.toString());
      return UTF8String.fromBytes(collationKey.toByteArray());
    }
  }

}
