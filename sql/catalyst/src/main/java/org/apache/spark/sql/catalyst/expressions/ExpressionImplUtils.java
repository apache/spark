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
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;

/**
 * An utility class for constructing expressions.
 */
public class ExpressionImplUtils {
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
    int inputLength = input.length;
    int keyLength = key.length;
    SecretKeySpec secretKey;

    if (!mode.equalsIgnoreCase("ECB") || !padding.equalsIgnoreCase("PKCS")) {
      throw QueryExecutionErrors.aesModeUnsupportedError(mode, padding);
    }

    switch (keyLength) {
      case 16:
      case 24:
      case 32:
        secretKey = new SecretKeySpec(key, 0, keyLength, "AES");
        break;
      default:
        throw QueryExecutionErrors.invalidAesKeyLengthError(keyLength);
      }

    try {
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      cipher.init(opmode, secretKey);
      return cipher.doFinal(input, 0, inputLength);
    } catch (GeneralSecurityException e) {
        throw new RuntimeException(e);
    }
  }
}
