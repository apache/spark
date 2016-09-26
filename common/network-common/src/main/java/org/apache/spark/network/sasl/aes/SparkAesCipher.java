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

  private final Integrity integrity;

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

    integrity = new Integrity(outKey, inKey);
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
    byte[] mac = integrity.getHMAC(data, offset, len);
    integrity.incMySeqNum();

    // Padding based on cipher
    byte[] padding;
    if (AES_CBC_NOPADDING.equals(encryptor.getAlgorithm())) {
      int bs = encryptor.getBlockSize();
      int pad = bs - (len + Integrity.HMAC_SIZE) % bs;
      padding = new byte[pad];
      for (int i = 0; i < pad; i ++) {
        padding[i] = (byte) pad;
      }
    } else {
      padding = EMPTY_BYTE_ARRAY;
    }

    // Input contains {msg, pad, HMAC(Key, {SeqNum, msg}[0..9])}
    byte[] toBeEncrypted = new byte[len + Integrity.HMAC_SIZE + padding.length];
    System.arraycopy(data, offset, toBeEncrypted, 0, len);
    System.arraycopy(padding, 0, toBeEncrypted, len, padding.length);
    System.arraycopy(mac, 0, toBeEncrypted, len+padding.length, Integrity.HMAC_SIZE);

    // Output contains {msg, pad, HMAC(Key, {SeqNum, msg}[0..9]),SeqNum}
    byte[] encrypted = new byte[len + Integrity.HMAC_SIZE +
      padding.length + Integrity.SEQ_NUM_SIZE];

    try {
      encryptor.update(toBeEncrypted, 0, toBeEncrypted.length, encrypted, 0);
    } catch (ShortBufferException sbe) {
      // This should not happen
      throw new SaslException("Error happens during encrypt data", sbe);
    }

    // Append seqNum used for mac
    System.arraycopy(integrity.getSeqNum(), 0,
      encrypted, toBeEncrypted.length, Integrity.SEQ_NUM_SIZE);

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
    // Get plaintext and seqNum
    byte[] decrypted = new byte[len - Integrity.SEQ_NUM_SIZE];
    byte[] peerSeqNum = new byte[Integrity.SEQ_NUM_SIZE];
    System.arraycopy(data, offset + decrypted.length, peerSeqNum, 0, Integrity.SEQ_NUM_SIZE);

    // Decrypt
    try {
      decryptor.update(data, offset, len - Integrity.SEQ_NUM_SIZE, decrypted, 0);
    } catch (ShortBufferException sbe) {
      throw new SaslException("Error happens during decrypt data", sbe);
    }

    // Get mac
    byte[] mac = new byte[Integrity.HMAC_SIZE];
    System.arraycopy(decrypted, decrypted.length - Integrity.HMAC_SIZE,
      mac, 0, Integrity.HMAC_SIZE);

    // Modify msg length if padding
    int origMsgLen = decrypted.length - Integrity.HMAC_SIZE;
    int realMsgLen = origMsgLen;
    if (AES_CBC_NOPADDING.equals(decryptor.getAlgorithm())) {
      realMsgLen -= (int) decrypted[origMsgLen - 1];
    }

    // Check mac integrity and msg sequence
    if (!integrity.compareHMAC(mac, peerSeqNum, decrypted, 0, realMsgLen)) {
      throw new SaslException("Unmatched MAC");
    }

    // Check SeqNum
    if (!integrity.comparePeerSeqNum(peerSeqNum)) {
      throw new SaslException("Out of order sequencing of messages. Got: " + integrity.byteToInt
        (peerSeqNum) + " Expected: " + integrity.peerSeqNum);
    }

    integrity.incPeerSeqNum();

    return Arrays.copyOf(decrypted, realMsgLen);
  }

  /**
   * Helper class for providing integrity protection.
   */
  private static class Integrity {
    public static final byte SEQ_NUM_SIZE = 4;
    public static final byte HMAC_SIZE = 10;
    private int mySeqNum = 0;
    private int peerSeqNum = 0;
    private byte[] seqNum = new byte[4];

    private byte[] myKey;
    private byte[] peerKey;

    Integrity(byte[] outKey, byte[] inKey) throws IOException {
      myKey = outKey;
      peerKey = inKey;
    }

    byte[] getHMAC(byte[] msg, int start, int len) throws SaslException {
      intToByte(mySeqNum);
      return calculateHMAC(myKey, seqNum, msg, start, len);
    }

    boolean compareHMAC(byte[] expectedHMAC, byte[] peerSeqNum, byte[] msg, int start,
        int len) throws SaslException {
      byte[] mac = calculateHMAC(peerKey, peerSeqNum, msg, start, len);
      return Arrays.equals(mac, expectedHMAC);
    }

    boolean comparePeerSeqNum(byte[] peerSeqNum) {
      return this.peerSeqNum == byteToInt(peerSeqNum);
    }

    byte[] getSeqNum() {
      return seqNum;
    }

    void incMySeqNum() {
      mySeqNum ++;
    }

    void incPeerSeqNum() {
      peerSeqNum ++;
    }

    private byte[] calculateHMAC(byte[] key, byte[] seqNum, byte[] msg, int start,
        int len) throws SaslException {
      byte[] seqAndMsg = new byte[SEQ_NUM_SIZE + len];
      System.arraycopy(seqNum, 0, seqAndMsg, 0, SEQ_NUM_SIZE);
      System.arraycopy(msg, start, seqAndMsg, SEQ_NUM_SIZE, len);

      try {
        SecretKey keyKi = new SecretKeySpec(key, "HmacMD5");
        Mac m = Mac.getInstance("HmacMD5");
        m.init(keyKi);
        m.update(seqAndMsg);
        byte[] hMAC_MD5 = m.doFinal();

        /* First 10 bytes of HMAC_MD5 digest */
        byte macBuffer[] = new byte[HMAC_SIZE];
        System.arraycopy(hMAC_MD5, 0, macBuffer, 0, HMAC_SIZE);

        return macBuffer;
      } catch (InvalidKeyException e) {
        throw new SaslException("Invalid bytes used for key of HMAC-MD5 hash.", e);
      } catch (NoSuchAlgorithmException e) {
        throw new SaslException("Error creating instance of MD5 MAC algorithm", e);
      }
    }

    private void intToByte(int num) {
      for(int i = 3; i >= 0; i --) {
        seqNum[i] = (byte)(num & 0xff);
        num >>>= 8;
      }
    }

    private int byteToInt(byte[] seqNum) {
      int answer = 0;
      for (int i = 0; i < 4; i ++) {
        answer <<= 8;
        answer |= ((int)seqNum[i] & 0xff);
      }
      return answer;
    }
  }
}
