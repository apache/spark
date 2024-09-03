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

package org.apache.spark.sql.catalyst.expressions;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.spark.SparkBuildInfo;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.VersionUtils;

/**
 * A utility class for constructing expressions.
 */
public class ExpressionImplUtils {
  private static final SecureRandom secureRandom = new SecureRandom();

  private static final int GCM_IV_LEN = 12;
  private static final int GCM_TAG_LEN = 128;
  private static final int CBC_IV_LEN = 16;

  enum CipherMode {
    ECB("ECB", 0, 0, "AES/ECB/PKCS5Padding", false, false),
    CBC("CBC", CBC_IV_LEN, 0, "AES/CBC/PKCS5Padding", true, false),
    GCM("GCM", GCM_IV_LEN, GCM_TAG_LEN, "AES/GCM/NoPadding", true, true);

    private final String name;
    final int ivLength;
    final int tagLength;
    final String transformation;
    final boolean usesSpec;
    final boolean supportsAad;

    CipherMode(String name,
               int ivLen,
               int tagLen,
               String transformation,
               boolean usesSpec,
               boolean supportsAad) {
      this.name = name;
      this.ivLength = ivLen;
      this.tagLength = tagLen;
      this.transformation = transformation;
      this.usesSpec = usesSpec;
      this.supportsAad = supportsAad;
    }

    static CipherMode fromString(String modeName, String padding) {
      boolean isNone = padding.equalsIgnoreCase("NONE");
      boolean isPkcs = padding.equalsIgnoreCase("PKCS");
      boolean isDefault = padding.equalsIgnoreCase("DEFAULT");
      if (modeName.equalsIgnoreCase(ECB.name) && (isPkcs || isDefault)) {
        return ECB;
      } else if (modeName.equalsIgnoreCase(CBC.name) && (isPkcs || isDefault)) {
        return CBC;
      } else if (modeName.equalsIgnoreCase(GCM.name) && (isNone || isDefault)) {
        return GCM;
      }
      throw QueryExecutionErrors.aesModeUnsupportedError(modeName, padding);
    }
  }

  /**
   * Function to check if a given number string is a valid Luhn number
   * @param numberString
   *  the number string to check
   * @return
   *  true if the number string is a valid Luhn number, false otherwise.
   */
  public static boolean isLuhnNumber(UTF8String numberString) {
    String digits = numberString.toString();
    // Empty string is not a valid Luhn number.
    if (digits.isEmpty()) return false;
    int checkSum = 0;
    boolean isSecond = false;
    for (int i = digits.length() - 1; i >= 0; i--) {
      char ch = digits.charAt(i);
      if (!Character.isDigit(ch)) return false;

      int digit = Character.getNumericValue(ch);
      // Double the digit if it's the second digit in the sequence.
      int doubled = isSecond ? digit * 2 : digit;
      // Add the two digits of the doubled number to the sum.
      checkSum += doubled % 10 + doubled / 10;
      // Toggle the isSecond flag for the next iteration.
      isSecond = !isSecond;
    }
    // Check if the final sum is divisible by 10.
    return checkSum % 10 == 0;
  }

  /**
   * Function to validate a given UTF8 string according to Unicode rules.
   *
   * @param utf8String
   *  the input string to validate against possible invalid byte sequences
   * @return
   *  the original string if the input string is a valid UTF8String, throw exception otherwise.
   */
  public static UTF8String validateUTF8String(UTF8String utf8String) {
    if (utf8String.isValid()) return utf8String;
    else throw QueryExecutionErrors.invalidUTF8StringError(utf8String);
  }

  /**
   * Function to try to validate a given UTF8 string according to Unicode rules.
   *
   * @param utf8String
   *  the input string to validate against possible invalid byte sequences
   * @return
   *  the original string if the input string is a valid UTF8String, null otherwise.
   */
  public static UTF8String tryValidateUTF8String(UTF8String utf8String) {
    if (utf8String.isValid()) return utf8String;
    else return null;
  }

  public static byte[] aesEncrypt(byte[] input,
                                  byte[] key,
                                  UTF8String mode,
                                  UTF8String padding,
                                  byte[] iv,
                                  byte[] aad) {
    return aesInternal(
            input,
            key,
            mode.toString(),
            padding.toString(),
            Cipher.ENCRYPT_MODE,
            iv,
            aad
    );
  }

  public static byte[] aesDecrypt(byte[] input,
                                  byte[] key,
                                  UTF8String mode,
                                  UTF8String padding,
                                  byte[] aad) {
    return aesInternal(
            input,
            key,
            mode.toString(),
            padding.toString(),
            Cipher.DECRYPT_MODE,
            null,
            aad
    );
  }

  /**
   * Function to return the Spark version.
   * @return
   *  Space separated version and revision.
   */
  public static UTF8String getSparkVersion() {
    String shortVersion = VersionUtils.shortVersion(SparkBuildInfo.spark_version());
    String revision = SparkBuildInfo.spark_revision();
    return UTF8String.fromString(shortVersion + " " + revision);
  }

  private static SecretKeySpec getSecretKeySpec(byte[] key) {
    return switch (key.length) {
      case 16, 24, 32 -> new SecretKeySpec(key, 0, key.length, "AES");
      default -> throw QueryExecutionErrors.invalidAesKeyLengthError(key.length);
    };
  }

  private static byte[] generateIv(CipherMode mode) {
    byte[] iv = new byte[mode.ivLength];
    secureRandom.nextBytes(iv);
    return iv;
  }

  private static AlgorithmParameterSpec getParamSpec(CipherMode mode, byte[] input) {
    return switch (mode) {
      case CBC -> new IvParameterSpec(input, 0, mode.ivLength);
      case GCM -> new GCMParameterSpec(mode.tagLength, input, 0, mode.ivLength);
      default -> null;
    };
  }

  private static byte[] aesInternal(
      byte[] input,
      byte[] key,
      String mode,
      String padding,
      int opmode,
      byte[] iv,
      byte[] aad) {
    try {
      SecretKeySpec secretKey = getSecretKeySpec(key);
      CipherMode cipherMode = CipherMode.fromString(mode, padding);
      Cipher cipher = Cipher.getInstance(cipherMode.transformation);
      if (opmode == Cipher.ENCRYPT_MODE) {
        // This may be 0-length for ECB
        if (iv == null || iv.length == 0) {
          iv = generateIv(cipherMode);
        } else if (!cipherMode.usesSpec) {
          // If the caller passes an IV, ensure the mode actually uses it.
          throw QueryExecutionErrors.aesUnsupportedIv(mode);
        }
        if (iv.length != cipherMode.ivLength) {
          throw QueryExecutionErrors.invalidAesIvLengthError(mode, iv.length);
        }

        if (cipherMode.usesSpec) {
          AlgorithmParameterSpec algSpec = getParamSpec(cipherMode, iv);
          cipher.init(opmode, secretKey, algSpec);
        } else {
          cipher.init(opmode, secretKey);
        }

        // If the cipher mode supports additional authenticated data and it is provided, update it
        if (aad != null && aad.length != 0) {
          if (cipherMode.supportsAad != true) {
            throw QueryExecutionErrors.aesUnsupportedAad(mode);
          }
          cipher.updateAAD(aad);
        }

        byte[] encrypted = cipher.doFinal(input, 0, input.length);
        if (iv.length > 0) {
          ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + encrypted.length);
          byteBuffer.put(iv);
          byteBuffer.put(encrypted);
          return byteBuffer.array();
        } else {
          return encrypted;
        }
      } else {
        assert(opmode == Cipher.DECRYPT_MODE);
        if (cipherMode.usesSpec) {
          AlgorithmParameterSpec algSpec = getParamSpec(cipherMode, input);
          cipher.init(opmode, secretKey, algSpec);
          if (aad != null && aad.length != 0) {
            if (cipherMode.supportsAad != true) {
              throw QueryExecutionErrors.aesUnsupportedAad(mode);
            }
            cipher.updateAAD(aad);
          }
          return cipher.doFinal(input, cipherMode.ivLength, input.length - cipherMode.ivLength);
        } else {
          cipher.init(opmode, secretKey);
          return cipher.doFinal(input, 0, input.length);
        }
      }
    } catch (GeneralSecurityException e) {
      throw QueryExecutionErrors.aesCryptoError(e.getMessage());
    }
  }

  public static ArrayData getSentences(
      UTF8String str,
      UTF8String language,
      UTF8String country) {
    if (str == null) return null;
    Locale locale;
    if (language == null && country == null) {
      locale = Locale.US;
    } else if (language == null) {
      locale = Locale.US;
    } else if (country == null) {
      locale = new Locale(language.toString());
    } else {
      locale = new Locale(language.toString(), country.toString());
    }
    String sentences = str.toString();
    BreakIterator sentenceInstance = BreakIterator.getSentenceInstance(locale);
    sentenceInstance.setText(sentences);

    int sentenceIndex = 0;
    List<GenericArrayData> res = new ArrayList<>();
    while (sentenceInstance.next() != BreakIterator.DONE) {
      String sentence = sentences.substring(sentenceIndex, sentenceInstance.current());
      sentenceIndex = sentenceInstance.current();
      BreakIterator wordInstance = BreakIterator.getWordInstance(locale);
      wordInstance.setText(sentence);
      int wordIndex = 0;
      List<UTF8String> words = new ArrayList<>();
      while (wordInstance.next() != BreakIterator.DONE) {
        String word = sentence.substring(wordIndex, wordInstance.current());
        wordIndex = wordInstance.current();
        if (Character.isLetterOrDigit(word.charAt(0))) {
          words.add(UTF8String.fromString(word));
        }
      }
      res.add(new GenericArrayData(words.toArray(new UTF8String[0])));
    }
    return new GenericArrayData(res.toArray(new GenericArrayData[0]));
  }
}
