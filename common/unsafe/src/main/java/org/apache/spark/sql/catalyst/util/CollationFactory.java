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

import com.ibm.icu.lang.UCharacter;
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
     * collation id (32-bit integer) layout:
     * bit 31:    0 = predefined collation, 1 = user-defined collation
     * bit 30:    0 = utf8-binary, 1 = ICU
     * bit 29:    0 = case-sensitive, 1 = case-insensitive
     * bit 28:    0 = accent-sensitive, 1 = accent-insensitive
     * bit 27-26: 00 = unspecified, 01 = punctuation-sensitive, 10 = punctuation-insensitive
     * bit 25-24: 00 = unspecified, 01 = first-lower, 10 = first-upper
     * bit 23-22: 00 = unspecified, 01 = to-lower, 10 = to-upper
     * bit 21-20: 00 = unspecified, 01 = trim-left, 10 = trim-right, 11 = trim-both
     * bit 19-18: zeroes, reserved for version
     * bit 17-16: zeroes
     * bit 15-0:  locale id for ICU collations / zeroes for utf8-binary
     */
    private static class CollationSpec {
      private enum ImplementationProvider {
        UTF8_BINARY, ICU
      }

      private enum CaseSensitivity {
        CS, CI
      }

      private enum AccentSensitivity {
        AS, AI
      }

      private enum PunctuationSensitivity {
        UNSPECIFIED, PS, PI
      }

      private enum FirstLetterPreference {
        UNSPECIFIED, FU, FL
      }

      private enum CaseConversion {
        UNSPECIFIED, LCASE, UCASE
      }

      private enum SpaceTrimming {
        UNSPECIFIED, LTRIM, RTRIM, TRIM
      }

      private static final int implementationProviderOffset = 30;
      private static final int implementationProviderLen = 1;
      private static final int caseSensitivityOffset = 29;
      private static final int caseSensitivityLen = 1;
      private static final int accentSensitivityOffset = 28;
      private static final int accentSensitivityLen = 1;
      private static final int punctuationSensitivityOffset = 26;
      private static final int punctuationSensitivityLen = 2;
      private static final int firstLetterPreferenceOffset = 24;
      private static final int firstLetterPreferenceLen = 2;
      private static final int caseConversionOffset = 22;
      private static final int caseConversionLen = 2;
      private static final int spaceTrimmingOffset = 20;
      private static final int spaceTrimmingLen = 2;
      private static final int localeOffset = 0;
      private static final int localeLen = 16;

      private static final String[] ICULocaleNames;
      private static final Map<String, ULocale> ICULocaleMap = new HashMap<>();
      private static final Map<String, String> ICULocaleMapUppercase = new HashMap<>();
      private static final Map<String, Integer> ICULocaleToId = new HashMap<>();

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
            assert (!ICULocaleMap.containsKey(localeName));
            ICULocaleMap.put(localeName, locale);
          }
        }
        for (String localeName : ICULocaleMap.keySet()) {
          String localeUppercase = localeName.toUpperCase();
          assert (!ICULocaleMapUppercase.containsKey(localeUppercase));
          ICULocaleMapUppercase.put(localeUppercase, localeName);
        }
        ICULocaleNames = ICULocaleMap.keySet().toArray(new String[0]);
        Arrays.sort(ICULocaleNames);
        assert (ICULocaleNames.length <= (1 << 16));
        for (int i = 0; i < ICULocaleNames.length; i++) {
          ICULocaleToId.put(ICULocaleNames[i], i);
        }
      }

      public static final int UTF8_BINARY_COLLATION_ID =
        new CollationSpec(
          ImplementationProvider.UTF8_BINARY,
          null,
          CaseSensitivity.CS,
          AccentSensitivity.AS,
          PunctuationSensitivity.UNSPECIFIED,
          FirstLetterPreference.UNSPECIFIED,
          CaseConversion.UNSPECIFIED,
          SpaceTrimming.UNSPECIFIED
        ).getCollationId();

      public static final int UTF8_BINARY_LCASE_COLLATION_ID =
        new CollationSpec(
          ImplementationProvider.UTF8_BINARY,
          null,
          CaseSensitivity.CS,
          AccentSensitivity.AS,
          PunctuationSensitivity.UNSPECIFIED,
          FirstLetterPreference.UNSPECIFIED,
          CaseConversion.LCASE,
          SpaceTrimming.UNSPECIFIED
        ).getCollationId();

      public static final int UNICODE_COLLATION_ID =
        new CollationSpec(
          ImplementationProvider.ICU,
          "UNICODE",
          CaseSensitivity.CS,
          AccentSensitivity.AS,
          PunctuationSensitivity.UNSPECIFIED,
          FirstLetterPreference.UNSPECIFIED,
          CaseConversion.UNSPECIFIED,
          SpaceTrimming.UNSPECIFIED
        ).getCollationId();

      public static final int UNICODE_CI_COLLATION_ID =
        new CollationSpec(
          ImplementationProvider.ICU,
          "UNICODE",
          CaseSensitivity.CI,
          AccentSensitivity.AS,
          PunctuationSensitivity.UNSPECIFIED,
          FirstLetterPreference.UNSPECIFIED,
          CaseConversion.UNSPECIFIED,
          SpaceTrimming.UNSPECIFIED
        ).getCollationId();

      private final ImplementationProvider implementationProvider;
      private final CaseSensitivity caseSensitivity;
      private final AccentSensitivity accentSensitivity;
      private final PunctuationSensitivity punctuationSensitivity;
      private final FirstLetterPreference firstLetterPreference;
      private final CaseConversion caseConversion;
      private final SpaceTrimming spaceTrimming;
      private final String locale;
      private final int collationId;

      private CollationSpec(
          ImplementationProvider implementationProvider,
          String locale,
          CaseSensitivity caseSensitivity,
          AccentSensitivity accentSensitivity,
          PunctuationSensitivity punctuationSensitivity,
          FirstLetterPreference firstLetterPreference,
          CaseConversion caseConversion,
          SpaceTrimming spaceTrimming) {
        this.implementationProvider = implementationProvider;
        this.locale = locale;
        this.caseSensitivity = caseSensitivity;
        this.accentSensitivity = accentSensitivity;
        this.punctuationSensitivity = punctuationSensitivity;
        this.firstLetterPreference = firstLetterPreference;
        this.caseConversion = caseConversion;
        this.spaceTrimming = spaceTrimming;
        this.collationId = getCollationId();
      }

      public int getCollationId() {
        int collationId = 0;
        collationId |= implementationProvider.ordinal() << implementationProviderOffset;
        collationId |= caseSensitivity.ordinal() << caseSensitivityOffset;
        collationId |= accentSensitivity.ordinal() << accentSensitivityOffset;
        collationId |= punctuationSensitivity.ordinal() << punctuationSensitivityOffset;
        collationId |= firstLetterPreference.ordinal() << firstLetterPreferenceOffset;
        collationId |= caseConversion.ordinal() << caseConversionOffset;
        collationId |= spaceTrimming.ordinal() << spaceTrimmingOffset;
        if (implementationProvider == ImplementationProvider.ICU) {
          collationId |= ICULocaleToId.get(locale);
        }
        return collationId;
      }

      public static CollationSpec fromCollationId(int collationId) {
        ImplementationProvider implementationProvider = ImplementationProvider.values()[
          (collationId >> implementationProviderOffset) & ((1 << implementationProviderLen) - 1)];
        CaseSensitivity caseSensitivity = CaseSensitivity.values()[
          (collationId >> caseSensitivityOffset) & ((1 << caseSensitivityLen) - 1)];
        AccentSensitivity accentSensitivity = AccentSensitivity.values()[
          (collationId >> accentSensitivityOffset) & ((1 << accentSensitivityLen) - 1)];
        PunctuationSensitivity punctuationSensitivity = PunctuationSensitivity.values()[
          (collationId >> punctuationSensitivityOffset) & ((1 << punctuationSensitivityLen) - 1)];
        FirstLetterPreference firstLetterPreference = FirstLetterPreference.values()[
          (collationId >> firstLetterPreferenceOffset) & ((1 << firstLetterPreferenceLen) - 1)];
        CaseConversion caseConversion = CaseConversion.values()[
          (collationId >> caseConversionOffset) & ((1 << caseConversionLen) - 1)];
        SpaceTrimming spaceTrimming = SpaceTrimming.values()[
          (collationId >> spaceTrimmingOffset) & ((1 << spaceTrimmingLen) - 1)];
        String locale;
        if (implementationProvider == ImplementationProvider.UTF8_BINARY) {
          locale = "UTF8_BINARY";
        } else {
          locale = ICULocaleNames[(collationId >> localeOffset) & ((1 << localeLen) - 1)];
        }
        return new CollationSpec(
          implementationProvider,
          locale,
          caseSensitivity,
          accentSensitivity,
          punctuationSensitivity,
          firstLetterPreference,
          caseConversion,
          spaceTrimming
        );
      }

      public static int collationNameToId(String originalCollationName) throws SparkException {
        String collationName = originalCollationName.toUpperCase();
        try {
          if (collationName.startsWith("UTF8_BINARY")) {
            return collationUTF8BinaryNameToId(collationName);
          } else {
            return collationICUNameToId(collationName);
          }
        } catch (SparkException e) {
          throw new SparkException(
            "COLLATION_INVALID_NAME",
            SparkException.constructMessageParams(Map.of("collationName", originalCollationName)),
            e);
        }
      }

      private static int collationUTF8BinaryNameToId(String collationName) throws SparkException {
        int collationId = 0;
        collationId |= ImplementationProvider.UTF8_BINARY.ordinal() << implementationProviderOffset;
        collationId |= parseSpecifiers(collationName.substring("UTF8_BINARY".length()));
        return collationId;
      }

      private static int collationICUNameToId(String collationName) throws SparkException {
        int lastPos = -1;
        for (int i = 1; i <= collationName.length(); i++) {
          String localeName = collationName.substring(0, i);
          if (ICULocaleMapUppercase.containsKey(localeName)) {
            lastPos = i;
          }
        }
        if (lastPos == -1) {
          throw new SparkException("Invalid locale in collation name value " + collationName);
        } else {
          int collationId = 0;
          collationId |= ImplementationProvider.ICU.ordinal() << implementationProviderOffset;
          collationId |= parseSpecifiers(collationName.substring(lastPos));
          String normalizedLocaleName = ICULocaleMapUppercase.get(
            collationName.substring(0, lastPos));
          collationId |= ICULocaleToId.get(normalizedLocaleName) << localeOffset;
          return collationId;
        }
      }

      private static int parseSpecifiers(String specString) throws SparkException {
        int specifiers = 0;
        String[] parts = specString.split("_");
        for (String part : parts) {
          if (!part.isEmpty()) {
            if (part.equals("UNSPECIFIED")) {
              throw new SparkException("UNSPECIFIED collation specifier reserved for internal use");
            } else if (Arrays.stream(CaseSensitivity.values()).anyMatch(
                (s) -> s.toString().equals(part))) {
              specifiers |=
                CaseSensitivity.valueOf(part).ordinal() << caseSensitivityOffset;
            } else if (Arrays.stream(AccentSensitivity.values()).anyMatch(
                (s) -> part.equals(s.toString()))) {
              specifiers |=
                AccentSensitivity.valueOf(part).ordinal() << accentSensitivityOffset;
            } else if (Arrays.stream(PunctuationSensitivity.values()).anyMatch(
                (s) -> part.equals(s.toString()))) {
              specifiers |=
                PunctuationSensitivity.valueOf(part).ordinal() << punctuationSensitivityOffset;
            } else if (Arrays.stream(FirstLetterPreference.values()).anyMatch(
                (s) -> part.equals(s.toString()))) {
              specifiers |=
                FirstLetterPreference.valueOf(part).ordinal() << firstLetterPreferenceOffset;
            } else if (Arrays.stream(CaseConversion.values()).anyMatch(
                (s) -> part.equals(s.toString()))) {
              specifiers |=
                CaseConversion.valueOf(part).ordinal() << caseConversionOffset;
            } else if (Arrays.stream(SpaceTrimming.values()).anyMatch(
                (s) -> part.equals(s.toString()))) {
              specifiers |=
                SpaceTrimming.valueOf(part).ordinal() << spaceTrimmingOffset;
            } else {
              throw new SparkException("Invalid collation specifier value " + part);
            }
          }
        }
        return specifiers;
      }

      public Collation buildCollation() {
        if (implementationProvider == ImplementationProvider.UTF8_BINARY) {
          return buildUTF8BinaryCollation();
        } else {
          return buildICUCollation();
        }
      }

      public Collation buildUTF8BinaryCollation() {
        Comparator<UTF8String> comparator;
        if (collationId == UTF8_BINARY_COLLATION_ID) {
          comparator = UTF8String::binaryCompare;
        } else {
          comparator = (s1, s2) -> {
            UTF8String convertedS1 = caseAndTrimmingConversionUTF8Binary(s1);
            UTF8String convertedS2 = caseAndTrimmingConversionUTF8Binary(s2);
            return convertedS1.binaryCompare(convertedS2);
          };
        }
        return new Collation(
          collationName(),
          null,
          comparator,
          "1.0",
          s -> (long) caseAndTrimmingConversionUTF8Binary(s).hashCode(),
          collationId == UTF8_BINARY_COLLATION_ID,
          collationId == UTF8_BINARY_COLLATION_ID,
          collationId == UTF8_BINARY_LCASE_COLLATION_ID
        );
      }

      private UTF8String caseAndTrimmingConversionUTF8Binary(UTF8String s) {
        UTF8String temp;
        if (spaceTrimming == SpaceTrimming.LTRIM) {
          temp = s.trimLeft();
        } else if (spaceTrimming == SpaceTrimming.RTRIM) {
          temp = s.trimRight();
        } else if (spaceTrimming == SpaceTrimming.TRIM) {
          temp = s.trim();
        } else {
          temp = s;
        }
        if (caseConversion == CaseConversion.LCASE) {
          return temp.toLowerCase();
        } else if (caseConversion == CaseConversion.UCASE) {
          return temp.toUpperCase();
        } else {
          return temp;
        }
      }

      public Collation buildICUCollation() {
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
        if (punctuationSensitivity == PunctuationSensitivity.PS) {
          builder.setUnicodeLocaleKeyword("ka", "noignore");
        } else if (punctuationSensitivity == PunctuationSensitivity.PI) {
          builder.setUnicodeLocaleKeyword("ka", "shifted");
        }
        if (firstLetterPreference == FirstLetterPreference.FL) {
          builder.setUnicodeLocaleKeyword("kf", "lower");
        } else if (firstLetterPreference == FirstLetterPreference.FU) {
          builder.setUnicodeLocaleKeyword("kf", "upper");
        }
        ULocale resultLocale = builder.build();
        Collator collator = Collator.getInstance(resultLocale);
        Comparator<UTF8String> comparator = (s1, s2) -> collator.compare(
          caseAndTrimmingConversionICU(resultLocale, s1),
          caseAndTrimmingConversionICU(resultLocale, s2));
        return new Collation(
          collationName(),
          collator,
          comparator,
          "153.120.0.0",
          s -> (long) collator.getCollationKey(s.toString()).hashCode(),
          collationId == UNICODE_COLLATION_ID,
          false,
          false);
      }

      private String caseAndTrimmingConversionICU(ULocale locale, UTF8String s) {
        String temp;
        if (spaceTrimming == SpaceTrimming.LTRIM) {
          temp = s.trimLeft().toString();
        } else if (spaceTrimming == SpaceTrimming.RTRIM) {
          temp = s.trimRight().toString();
        } else if (spaceTrimming == SpaceTrimming.TRIM) {
          temp = s.trim().toString();
        } else {
          temp = s.toString();
        }
        if (caseConversion == CaseConversion.LCASE) {
          return UCharacter.toLowerCase(locale, temp);
        } else if (caseConversion == CaseConversion.UCASE) {
          return UCharacter.toUpperCase(locale, temp);
        } else {
          return temp;
        }
      }

      private String collationName() {
        StringBuilder builder = new StringBuilder();
        if (implementationProvider == ImplementationProvider.UTF8_BINARY) {
          builder.append("UTF8_BINARY");
        } else {
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
          if (punctuationSensitivity != PunctuationSensitivity.UNSPECIFIED) {
            builder.append('_');
            builder.append(punctuationSensitivity.toString());
          }
          if (firstLetterPreference != FirstLetterPreference.UNSPECIFIED) {
            builder.append('_');
            builder.append(firstLetterPreference.toString());
          }
        }
        if (caseConversion != CaseConversion.UNSPECIFIED) {
          builder.append('_');
          builder.append(caseConversion.toString());
        }
        if (spaceTrimming != SpaceTrimming.UNSPECIFIED) {
          builder.append('_');
          builder.append(spaceTrimming.toString());
        }
        return builder.toString();
      }
    }
  }

  public static final int UTF8_BINARY_COLLATION_ID =
    Collation.CollationSpec.UTF8_BINARY_COLLATION_ID;
  public static final int UTF8_BINARY_LCASE_COLLATION_ID =
    Collation.CollationSpec.UTF8_BINARY_LCASE_COLLATION_ID;
  public static final int UNICODE_COLLATION_ID =
    Collation.CollationSpec.UNICODE_COLLATION_ID;
  public static final int UNICODE_CI_COLLATION_ID =
    Collation.CollationSpec.UNICODE_CI_COLLATION_ID;

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
   * Returns if the given collationName is valid one.
   */
  public static boolean isValidCollation(String collationName) {
    try {
      fetchCollation(collationName);
      return true;
    } catch (SparkException e) {
      return false;
    }
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
    return Collation.CollationSpec.fromCollationId(collationId).buildCollation();
  }

  public static Collation fetchCollation(String collationName) throws SparkException {
    return fetchCollation(collationNameToId(collationName));
  }
}
