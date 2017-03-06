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

package org.apache.spark.network.crypto;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Properties;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;
import org.apache.commons.crypto.cipher.CryptoCipher;
import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.commons.crypto.random.CryptoRandom;
import org.apache.commons.crypto.random.CryptoRandomFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.util.TransportConf;

/**
 * A helper class for abstracting authentication and key negotiation details. This is used by
 * both client and server sides, since the operations are basically the same.
 */
class AuthEngine implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(AuthEngine.class);
  private static final BigInteger ONE = new BigInteger(new byte[] { 0x1 });

  private final byte[] appId;
  private final char[] secret;
  private final TransportConf conf;
  private final Properties cryptoConf;
  private final CryptoRandom random;

  private byte[] authNonce;

  @VisibleForTesting
  byte[] challenge;

  private TransportCipher sessionCipher;
  private CryptoCipher encryptor;
  private CryptoCipher decryptor;

  AuthEngine(String appId, String secret, TransportConf conf) throws GeneralSecurityException {
    this.appId = appId.getBytes(UTF_8);
    this.conf = conf;
    this.cryptoConf = conf.cryptoConf();
    this.secret = secret.toCharArray();
    this.random = CryptoRandomFactory.getCryptoRandom(cryptoConf);
  }

  /**
   * Create the client challenge.
   *
   * @return A challenge to be sent the remote side.
   */
  ClientChallenge challenge() throws GeneralSecurityException, IOException {
    this.authNonce = randomBytes(conf.encryptionKeyLength() / Byte.SIZE);
    SecretKeySpec authKey = generateKey(conf.keyFactoryAlgorithm(), conf.keyFactoryIterations(),
      authNonce, conf.encryptionKeyLength());
    initializeForAuth(conf.cipherTransformation(), authNonce, authKey);

    this.challenge = randomBytes(conf.encryptionKeyLength() / Byte.SIZE);
    return new ClientChallenge(new String(appId, UTF_8),
      conf.keyFactoryAlgorithm(),
      conf.keyFactoryIterations(),
      conf.cipherTransformation(),
      conf.encryptionKeyLength(),
      authNonce,
      challenge(appId, authNonce, challenge));
  }

  /**
   * Validates the client challenge, and create the encryption backend for the channel from the
   * parameters sent by the client.
   *
   * @param clientChallenge The challenge from the client.
   * @return A response to be sent to the client.
   */
  ServerResponse respond(ClientChallenge clientChallenge)
    throws GeneralSecurityException, IOException {

    SecretKeySpec authKey = generateKey(clientChallenge.kdf, clientChallenge.iterations,
      clientChallenge.nonce, clientChallenge.keyLength);
    initializeForAuth(clientChallenge.cipher, clientChallenge.nonce, authKey);

    byte[] challenge = validateChallenge(clientChallenge.nonce, clientChallenge.challenge);
    byte[] response = challenge(appId, clientChallenge.nonce, rawResponse(challenge));
    byte[] sessionNonce = randomBytes(conf.encryptionKeyLength() / Byte.SIZE);
    byte[] inputIv = randomBytes(conf.ivLength());
    byte[] outputIv = randomBytes(conf.ivLength());

    SecretKeySpec sessionKey = generateKey(clientChallenge.kdf, clientChallenge.iterations,
      sessionNonce, clientChallenge.keyLength);
    this.sessionCipher = new TransportCipher(cryptoConf, clientChallenge.cipher, sessionKey,
      inputIv, outputIv);

    // Note the IVs are swapped in the response.
    return new ServerResponse(response, encrypt(sessionNonce), encrypt(outputIv), encrypt(inputIv));
  }

  /**
   * Validates the server response and initializes the cipher to use for the session.
   *
   * @param serverResponse The response from the server.
   */
  void validate(ServerResponse serverResponse) throws GeneralSecurityException {
    byte[] response = validateChallenge(authNonce, serverResponse.response);

    byte[] expected = rawResponse(challenge);
    Preconditions.checkArgument(Arrays.equals(expected, response));

    byte[] nonce = decrypt(serverResponse.nonce);
    byte[] inputIv = decrypt(serverResponse.inputIv);
    byte[] outputIv = decrypt(serverResponse.outputIv);

    SecretKeySpec sessionKey = generateKey(conf.keyFactoryAlgorithm(), conf.keyFactoryIterations(),
      nonce, conf.encryptionKeyLength());
    this.sessionCipher = new TransportCipher(cryptoConf, conf.cipherTransformation(), sessionKey,
      inputIv, outputIv);
  }

  TransportCipher sessionCipher() {
    Preconditions.checkState(sessionCipher != null);
    return sessionCipher;
  }

  @Override
  public void close() throws IOException {
    // Close ciphers (by calling "doFinal()" with dummy data) and the random instance so that
    // internal state is cleaned up. Error handling here is just for paranoia, and not meant to
    // accurately report the errors when they happen.
    RuntimeException error = null;
    byte[] dummy = new byte[8];
    try {
      doCipherOp(encryptor, dummy, true);
    } catch (Exception e) {
      error = new RuntimeException(e);
    }
    try {
      doCipherOp(decryptor, dummy, true);
    } catch (Exception e) {
      error = new RuntimeException(e);
    }
    random.close();

    if (error != null) {
      throw error;
    }
  }

  @VisibleForTesting
  byte[] challenge(byte[] appId, byte[] nonce, byte[] challenge) throws GeneralSecurityException {
    return encrypt(Bytes.concat(appId, nonce, challenge));
  }

  @VisibleForTesting
  byte[] rawResponse(byte[] challenge) {
    BigInteger orig = new BigInteger(challenge);
    BigInteger response = orig.add(ONE);
    return response.toByteArray();
  }

  private byte[] decrypt(byte[] in) throws GeneralSecurityException {
    return doCipherOp(decryptor, in, false);
  }

  private byte[] encrypt(byte[] in) throws GeneralSecurityException {
    return doCipherOp(encryptor, in, false);
  }

  private void initializeForAuth(String cipher, byte[] nonce, SecretKeySpec key)
    throws GeneralSecurityException {

    // commons-crypto currently only supports ciphers that require an initial vector; so
    // create a dummy vector so that we can initialize the ciphers. In the future, if
    // different ciphers are supported, this will have to be configurable somehow.
    byte[] iv = new byte[conf.ivLength()];
    System.arraycopy(nonce, 0, iv, 0, Math.min(nonce.length, iv.length));

    encryptor = CryptoCipherFactory.getCryptoCipher(cipher, cryptoConf);
    encryptor.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));

    decryptor = CryptoCipherFactory.getCryptoCipher(cipher, cryptoConf);
    decryptor.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));
  }

  /**
   * Validates an encrypted challenge as defined in the protocol, and returns the byte array
   * that corresponds to the actual challenge data.
   */
  private byte[] validateChallenge(byte[] nonce, byte[] encryptedChallenge)
    throws GeneralSecurityException {

    byte[] challenge = decrypt(encryptedChallenge);
    checkSubArray(appId, challenge, 0);
    checkSubArray(nonce, challenge, appId.length);
    return Arrays.copyOfRange(challenge, appId.length + nonce.length, challenge.length);
  }

  private SecretKeySpec generateKey(String kdf, int iterations, byte[] salt, int keyLength)
    throws GeneralSecurityException {

    SecretKeyFactory factory = SecretKeyFactory.getInstance(kdf);
    PBEKeySpec spec = new PBEKeySpec(secret, salt, iterations, keyLength);

    long start = System.nanoTime();
    SecretKey key = factory.generateSecret(spec);
    long end = System.nanoTime();

    LOG.debug("Generated key with {} iterations in {} us.", conf.keyFactoryIterations(),
      (end - start) / 1000);

    return new SecretKeySpec(key.getEncoded(), conf.keyAlgorithm());
  }

  private byte[] doCipherOp(CryptoCipher cipher, byte[] in, boolean isFinal)
    throws GeneralSecurityException {

    Preconditions.checkState(cipher != null);

    int scale = 1;
    while (true) {
      int size = in.length * scale;
      byte[] buffer = new byte[size];
      try {
        int outSize = isFinal ? cipher.doFinal(in, 0, in.length, buffer, 0)
          : cipher.update(in, 0, in.length, buffer, 0);
        if (outSize != buffer.length) {
          byte[] output = new byte[outSize];
          System.arraycopy(buffer, 0, output, 0, output.length);
          return output;
        } else {
          return buffer;
        }
      } catch (ShortBufferException e) {
        // Try again with a bigger buffer.
        scale *= 2;
      }
    }
  }

  private byte[] randomBytes(int count) {
    byte[] bytes = new byte[count];
    random.nextBytes(bytes);
    return bytes;
  }

  /** Checks that the "test" array is in the data array starting at the given offset. */
  private void checkSubArray(byte[] test, byte[] data, int offset) {
    Preconditions.checkArgument(data.length >= test.length + offset);
    for (int i = 0; i < test.length; i++) {
      Preconditions.checkArgument(test[i] == data[i + offset]);
    }
  }

}
