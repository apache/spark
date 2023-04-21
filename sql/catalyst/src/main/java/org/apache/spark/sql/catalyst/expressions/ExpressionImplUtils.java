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

import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.unsafe.types.UTF8String;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;


/**
 * An utility class for constructing expressions.
 */
public class ExpressionImplUtils {
  private static final ThreadLocal<SecureRandom> threadLocalSecureRandom =
    new ThreadLocal<SecureRandom>() {
      @Override
      public SecureRandom initialValue() {
        return new SecureRandom();
      }
    };

  private static final int GCM_IV_LEN = 12;
  private static final int GCM_TAG_LEN = 128;
  private static final int CBC_IV_LEN = 16;

  enum CipherMode {
    ECB("ECB", 0, 0, "AES/ECB/PKCS5Padding", false),
    CBC("CBC", CBC_IV_LEN, 0, "AES/CBC/PKCS5Padding", true),
    GCM("GCM", GCM_IV_LEN, GCM_TAG_LEN, "AES/GCM/NoPadding", true);

    private final String name;
    final int ivLength;
    final int tagLength;
    final String transformation;
    final boolean usesSpec;

    CipherMode(String name, int ivLen, int tagLen, String transformation, boolean usesSpec) {
      this.name = name;
      this.ivLength = ivLen;
      this.tagLength = tagLen;
      this.transformation = transformation;
      this.usesSpec = usesSpec;
    }

    static CipherMode fromString(String modeName, String padding) {
      if (modeName.equalsIgnoreCase("ECB") &&
              (padding.equalsIgnoreCase("PKCS") || padding.equalsIgnoreCase("DEFAULT"))) {
        return ECB;
      } else if (modeName.equalsIgnoreCase("CBC") &&
              (padding.equalsIgnoreCase("PKCS") || padding.equalsIgnoreCase("DEFAULT"))) {
        return CBC;
      } else if (modeName.equalsIgnoreCase("GCM") &&
              (padding.equalsIgnoreCase("NONE") || padding.equalsIgnoreCase("DEFAULT"))) {
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

  public static byte[] aesEncrypt(byte[] input, byte[] key, UTF8String mode, UTF8String padding) {
    return aesInternal(input, key, mode.toString(), padding.toString(), Cipher.ENCRYPT_MODE);
  }

  public static byte[] aesDecrypt(byte[] input, byte[] key, UTF8String mode, UTF8String padding) {
    return aesInternal(input, key, mode.toString(), padding.toString(), Cipher.DECRYPT_MODE);
  }

  private static SecretKeySpec getSecretKeySpec(byte[] key) {
    switch (key.length) {
      case 16: case 24: case 32:
        return new SecretKeySpec(key, 0, key.length, "AES");
      default:
        throw QueryExecutionErrors.invalidAesKeyLengthError(key.length);
    }
  }

  private static byte[] generateIv(CipherMode mode) {
    byte[] iv = new byte[mode.ivLength];
    threadLocalSecureRandom.get().nextBytes(iv);
    return iv;
  }

  private static AlgorithmParameterSpec getParameterSpec(CipherMode mode, byte[] input, int offset) {
    switch (mode) {
      case CBC:
        return new IvParameterSpec(input, offset, mode.ivLength);
      case GCM:
        return new GCMParameterSpec(mode.tagLength, input, offset, mode.ivLength);
      default:
        return null;
    }
  }

  private static byte[] aesInternal(
      byte[] input,
      byte[] key,
      String mode,
      String padding,
      int opmode) {
    try {
      SecretKeySpec secretKey = getSecretKeySpec(key);
      CipherMode cipherMode = CipherMode.fromString(mode, padding);
      Cipher cipher = Cipher.getInstance(cipherMode.transformation);
      if (opmode == Cipher.ENCRYPT_MODE) {
        // This may be 0-length for ECB
        byte[] iv = generateIv(cipherMode);
        if (cipherMode.usesSpec) {
          AlgorithmParameterSpec algSpec = getParameterSpec(cipherMode, iv, 0);
          cipher.init(opmode, secretKey, algSpec);
        } else {
          cipher.init(opmode, secretKey);
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
      } else if (opmode == Cipher.DECRYPT_MODE) {
        if (cipherMode.usesSpec) {
          AlgorithmParameterSpec algSpec = getParameterSpec(cipherMode, input, 0);
          cipher.init(opmode, secretKey, algSpec);
          return cipher.doFinal(input, cipherMode.ivLength, input.length - cipherMode.ivLength);
        } else {
          cipher.init(opmode, secretKey);
          return cipher.doFinal(input, 0, input.length);
        }
      } else {
        throw QueryExecutionErrors.aesModeUnsupportedError(mode, padding);
      }
    } catch (GeneralSecurityException e) {
      throw QueryExecutionErrors.aesCryptoError(e.getMessage());
    }
  }
}
