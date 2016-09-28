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

package org.apache.spark.network.sasl.aes;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Properties;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;
import javax.security.sasl.SaslException;

import org.apache.commons.crypto.cipher.CryptoCipher;
import org.apache.commons.crypto.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AES cipher for encryption and decryption.
 */
public class SparkAesCipher {
  public final static String AES_CBC_NOPADDING = "AES/CBC/NoPadding";
  public final static String AES_CTR_NOPADDING = "AES/CTR/NoPadding";
  public final static String AES_CTR_PKCS5Padding = "AES/CTR/PKCS5Padding";
  private static final Logger logger = LoggerFactory.getLogger(SparkAesCipher.class);
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  public final static String supportedTransformation[] = {
    AES_CBC_NOPADDING, AES_CTR_NOPADDING, AES_CTR_PKCS5Padding
  };

  private final CryptoCipher encryptor;
  private final CryptoCipher decryptor;

  public SparkAesCipher(
      String cipherTransformation,
      Properties properties,
      byte[] inKey,
      byte[] outKey,
      byte[] inIv,
      byte[] outIv) throws IOException {
    if (!Arrays.asList(supportedTransformation).contains(cipherTransformation)) {
      logger.warn("AES cipher transformation is not supported: " + cipherTransformation);
      cipherTransformation = "AES/CTR/NoPadding";
      logger.warn("Use default AES/CTR/NoPadding");
    }

    final SecretKeySpec inKeySpec = new SecretKeySpec(inKey, "AES");
    final IvParameterSpec inIvSpec = new IvParameterSpec(inIv);
    final SecretKeySpec outKeySpec = new SecretKeySpec(outKey, "AES");
    final IvParameterSpec outIvSpec = new IvParameterSpec(outIv);

    // Encryptor
    encryptor = Utils.getCipherInstance(cipherTransformation, properties);
    try {
      logger.debug("Initialize encryptor");
      encryptor.init(Cipher.ENCRYPT_MODE, outKeySpec, outIvSpec);
    } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
      throw new IOException("Failed to initialize encryptor", e);
    }

    // Decryptor
    decryptor = Utils.getCipherInstance(cipherTransformation, properties);
    try {
      logger.debug("Initialize decryptor");
      decryptor.init(Cipher.DECRYPT_MODE, inKeySpec, inIvSpec);
    } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
      throw new IOException("Failed to initialize decryptor", e);
    }
  }

  /**
   * Encrypts input data. The result composes of (msg, padding if needed, mac) and sequence num.
   * @param data the input byte array
   * @param offset the offset in input where the input starts
   * @param len the input length
   * @return the new encrypted byte array.
   * @throws SaslException if error happens
   */
  public byte[] wrap(byte[] data, int offset, int len) throws SaslException {
    // Padding based on cipher
    byte[] padding;
    if (AES_CBC_NOPADDING.equals(encryptor.getAlgorithm())) {
      int bs = encryptor.getBlockSize();
      int pad = bs - len % bs;
      padding = new byte[pad];
      for (int i = 0; i < pad; i ++) {
        padding[i] = (byte) pad;
      }
    } else {
      padding = EMPTY_BYTE_ARRAY;
    }

    byte[] toBeEncrypted = new byte[len + padding.length];
    System.arraycopy(data, offset, toBeEncrypted, 0, len);
    System.arraycopy(padding, 0, toBeEncrypted, len, padding.length);

    byte[] encrypted = new byte[len + padding.length];

    try {
      encryptor.update(toBeEncrypted, 0, toBeEncrypted.length, encrypted, 0);
    } catch (ShortBufferException sbe) {
      // This should not happen
      throw new SaslException("Error happens during encrypt data", sbe);
    }

    return encrypted;
  }

  /**
   * Decrypt input data. The input composes of (msg, padding if needed, mac) and sequence num.
   * The result is msg.
   * @param data the input byte array
   * @param offset the offset in input where the input starts
   * @param len the input length
   * @return the new decrypted byte array.
   * @throws SaslException if error happens
   */
  public byte[] unwrap(byte[] data, int offset, int len) throws SaslException {
    byte[] decrypted = new byte[len];

    // Decrypt
    try {
      decryptor.update(data, offset, len, decrypted, 0);
    } catch (ShortBufferException sbe) {
      throw new SaslException("Error happens during decrypt data", sbe);
    }

    // Modify msg length if padding
    if (AES_CBC_NOPADDING.equals(decryptor.getAlgorithm())) {
      len -= (int) decrypted[len - 1];
    }

    return Arrays.copyOf(decrypted, len);
  }
}
