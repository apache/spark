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
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

/**
 * An utility class for constructing expressions.
 */
public class ExpressionImplUtils {
  private static final SecureRandom secureRandom = new SecureRandom();
  private static final int GCM_IV_LEN = 12;
  private static final int GCM_TAG_LEN = 128;

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
      } else {
        throw QueryExecutionErrors.aesModeUnsupportedError(mode, padding);
      }
    } catch (GeneralSecurityException e) {
      throw QueryExecutionErrors.aesCryptoError(e.getMessage());
    }
  }
}
