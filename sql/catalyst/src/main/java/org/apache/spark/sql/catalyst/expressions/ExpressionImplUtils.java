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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * An utility class for constructing expressions.
 */
public class ExpressionImplUtils {
  private static final SecureRandom secureRandom = new SecureRandom();
  private static final int GCM_IV_LEN = 12;
  private static final int GCM_TAG_LEN = 128;

  private static final int CBC_IV_LEN = 16;
  private static final int CBC_SALT_LEN = 8;
  /** OpenSSL's magic initial bytes. */
  private static final String SALTED_STR = "Salted__";
  private static final byte[] SALTED_MAGIC = SALTED_STR.getBytes(US_ASCII);


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

  private static byte[] aesInternal(
      byte[] input,
      byte[] key,
      String mode,
      String padding,
      int opmode) {
    SecretKeySpec secretKey;

    switch (key.length) {
      case 16:
      case 24:
      case 32:
        secretKey = new SecretKeySpec(key, 0, key.length, "AES");
        break;
      default:
        throw QueryExecutionErrors.invalidAesKeyLengthError(key.length);
      }

    try {
      if (mode.equalsIgnoreCase("ECB") &&
          (padding.equalsIgnoreCase("PKCS") || padding.equalsIgnoreCase("DEFAULT"))) {
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(opmode, secretKey);
        return cipher.doFinal(input, 0, input.length);
      } else if (mode.equalsIgnoreCase("GCM") &&
          (padding.equalsIgnoreCase("NONE") || padding.equalsIgnoreCase("DEFAULT"))) {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        if (opmode == Cipher.ENCRYPT_MODE) {
          byte[] iv = new byte[GCM_IV_LEN];
          secureRandom.nextBytes(iv);
          GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LEN, iv);
          cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);
          byte[] encrypted = cipher.doFinal(input, 0, input.length);
          ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + encrypted.length);
          byteBuffer.put(iv);
          byteBuffer.put(encrypted);
          return byteBuffer.array();
        } else {
          assert(opmode == Cipher.DECRYPT_MODE);
          GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LEN, input, 0, GCM_IV_LEN);
          cipher.init(Cipher.DECRYPT_MODE, secretKey, parameterSpec);
          return cipher.doFinal(input, GCM_IV_LEN, input.length - GCM_IV_LEN);
        }
      } else if (mode.equalsIgnoreCase("CBC") &&
          (padding.equalsIgnoreCase("PKCS") || padding.equalsIgnoreCase("DEFAULT"))) {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        if (opmode == Cipher.ENCRYPT_MODE) {
          byte[] salt = new byte[CBC_SALT_LEN];
          secureRandom.nextBytes(salt);
          final byte[] keyAndIv = getKeyAndIv(key, salt);
          final byte[] keyValue = Arrays.copyOfRange(keyAndIv, 0, key.length);
          final byte[] iv = Arrays.copyOfRange(keyAndIv, key.length, key.length + CBC_IV_LEN);
          cipher.init(
            Cipher.ENCRYPT_MODE,
            new SecretKeySpec(keyValue, "AES"),
            new IvParameterSpec(iv));
          byte[] encrypted = cipher.doFinal(input, 0, input.length);
          ByteBuffer byteBuffer = ByteBuffer.allocate(
            SALTED_MAGIC.length + CBC_SALT_LEN + encrypted.length);
          byteBuffer.put(SALTED_MAGIC);
          byteBuffer.put(salt);
          byteBuffer.put(encrypted);
          return byteBuffer.array();
        } else {
          assert(opmode == Cipher.DECRYPT_MODE);
          final byte[] shouldBeMagic = Arrays.copyOfRange(input, 0, SALTED_MAGIC.length);
          if (!Arrays.equals(shouldBeMagic, SALTED_MAGIC)) {
            throw QueryExecutionErrors.aesInvalidSalt(shouldBeMagic);
          }
          final byte[] salt = Arrays.copyOfRange(
            input, SALTED_MAGIC.length, SALTED_MAGIC.length + CBC_SALT_LEN);
          final byte[] keyAndIv = getKeyAndIv(key, salt);
          final byte[] keyValue = Arrays.copyOfRange(keyAndIv, 0, key.length);
          final byte[] iv = Arrays.copyOfRange(keyAndIv, key.length, key.length + CBC_IV_LEN);
          cipher.init(
            Cipher.DECRYPT_MODE,
            new SecretKeySpec(keyValue, "AES"),
            new IvParameterSpec(iv, 0, CBC_IV_LEN));
          return cipher.doFinal(input, CBC_IV_LEN, input.length - CBC_IV_LEN);
        }
      } else {
        throw QueryExecutionErrors.aesModeUnsupportedError(mode, padding);
      }
    } catch (GeneralSecurityException e) {
      throw QueryExecutionErrors.aesCryptoError(e.getMessage());
    }
  }

  // Derive the key and init vector in the same way as OpenSSL's EVP_BytesToKey
  // since the version 1.1.0c which switched to SHA-256 as the hash.
  private static byte[] getKeyAndIv(byte[] key, byte[] salt) throws NoSuchAlgorithmException {
    final byte[] keyAndSalt = arrConcat(key, salt);
    byte[] hash = new byte[0];
    byte[] keyAndIv = new byte[0];
    for (int i = 0; i < 3 && keyAndIv.length < key.length + CBC_IV_LEN; i++) {
      final byte[] hashData = arrConcat(hash, keyAndSalt);
      final MessageDigest md = MessageDigest.getInstance("SHA-256");
      hash = md.digest(hashData);
      keyAndIv = arrConcat(keyAndIv, hash);
    }
    return keyAndIv;
  }

  private static byte[] arrConcat(final byte[] arr1, final byte[] arr2) {
    final byte[] res = new byte[arr1.length + arr2.length];
    System.arraycopy(arr1, 0, res, 0, arr1.length);
    System.arraycopy(arr2, 0, res, arr1.length, arr2.length);
    return res;
  }
}
