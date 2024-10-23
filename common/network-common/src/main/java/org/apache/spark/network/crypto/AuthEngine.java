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

import javax.crypto.spec.SecretKeySpec;
import java.io.Closeable;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;
import com.google.crypto.tink.subtle.AesGcmJce;
import com.google.crypto.tink.subtle.Hkdf;
import com.google.crypto.tink.subtle.Random;
import com.google.crypto.tink.subtle.X25519;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import static java.nio.charset.StandardCharsets.UTF_8;
import org.apache.spark.network.util.TransportConf;

/**
 * A helper class for abstracting authentication and key negotiation details.
 * This supports a forward-secure authentication protocol based on X25519 Diffie-Hellman Key
 * Exchange, using a pre-shared key to derive an AES-GCM key encrypting key.
 */
class AuthEngine implements Closeable {
  public static final byte[] DERIVED_KEY_INFO = "derivedKey".getBytes(UTF_8);
  public static final byte[] INPUT_IV_INFO = "inputIv".getBytes(UTF_8);
  public static final byte[] OUTPUT_IV_INFO = "outputIv".getBytes(UTF_8);
  private static final String MAC_ALGORITHM = "HMACSHA256";
  private static final String LEGACY_CIPHER_ALGORITHM = "AES/CTR/NoPadding";
  private static final String CIPHER_ALGORITHM = "AES/GCM/NoPadding";
  private static final int AES_GCM_KEY_SIZE_BYTES = 16;
  private static final byte[] EMPTY_TRANSCRIPT = new byte[0];
  private static final int UNSAFE_SKIP_HKDF_VERSION = 1;

  private final String appId;
  private final byte[] preSharedSecret;
  private final TransportConf conf;
  private final Properties cryptoConf;
  private final boolean unsafeSkipFinalHkdf;

  private byte[] clientPrivateKey;
  private TransportCipher sessionCipher;

  AuthEngine(String appId, String preSharedSecret, TransportConf conf) {
    Preconditions.checkNotNull(appId);
    Preconditions.checkNotNull(preSharedSecret);
    this.appId = appId;
    this.preSharedSecret = preSharedSecret.getBytes(UTF_8);
    this.conf = conf;
    this.cryptoConf = conf.cryptoConf();
    // This is for backward compatibility with version 1.0 of this protocol,
    // which did not perform a final HKDF round.
    this.unsafeSkipFinalHkdf = conf.authEngineVersion() == UNSAFE_SKIP_HKDF_VERSION;
  }

  @VisibleForTesting
  void setClientPrivateKey(byte[] privateKey) {
    this.clientPrivateKey = privateKey;
  }

  /**
   * This method will derive a key from a pre-shared secret, a random salt, and an arbitrary
   * transcript. It will then use that derived key to AES-GCM encrypt an ephemeral X25519 public
   * key.
   *
   * @param ephemeralX25519PublicKey Ephemeral X25519 Public Key to encrypt under a derived key.
   * @param transcript               Optional byte array representing a protocol transcript, which
   *                                 is mixed into the key derivation and included as AES-GCM
   *                                 associated authenticated data (AAD).
   * @return An encrypted ephemeral X25519 public key.
   * @throws GeneralSecurityException If HKDF key deriviation or AES-GCM encryption fails.
   */
  private AuthMessage encryptEphemeralPublicKey(
      byte[] ephemeralX25519PublicKey,
      byte[] transcript) throws GeneralSecurityException {
    // This non-secret salt is used in the HKDF key derivations and will be sent in plaintext as
    // part of the AES-GCM encrypted X25519 public key. It will be included as additional
    // associated data (AAD).
    byte[] nonSecretSalt = Random.randBytes(AES_GCM_KEY_SIZE_BYTES);
    // Mix in the app ID, salt, and transcript into HKDF and use it as AES-GCM AAD
    byte[] aadState = Bytes.concat(appId.getBytes(UTF_8), nonSecretSalt, transcript);
    // Use HKDF to derive an AES_GCM key from the pre-shared key, non-secret salt, and AAD state
    byte[] derivedKeyEncryptingKey = Hkdf.computeHkdf(
        MAC_ALGORITHM,
        preSharedSecret,
        nonSecretSalt,
        aadState,
        AES_GCM_KEY_SIZE_BYTES);
    // AES-GCM encrypt the X25519 public key and include the app ID, salt, and transcript as AAD
    byte[] aesGcmCiphertext = new AesGcmJce(derivedKeyEncryptingKey)
        .encrypt(ephemeralX25519PublicKey, aadState);
    return new AuthMessage(appId, nonSecretSalt, aesGcmCiphertext);
  }

  /**
   * This method will derive a key from a pre-shared secret, a random salt, and an arbitrary
   * transcript. It will then use that derived key to AES-GCM encrypt an ephemeral X25519
   * public key.
   *
   * @param encryptedPublicKey An X25519 public key to decrypt with a derived key
   * @param transcript         Optional byte array representing a protocol transcript, which is
   *                           mixed into the key derivation and included as AES-GCM associated
   *                           authenticated data (AAD).
   * @return A decrypted ephemeral public key
   * @throws GeneralSecurityException If decryption fails, notably if authenticated checks fails.
   */
  private byte[] decryptEphemeralPublicKey(
      AuthMessage encryptedPublicKey,
      byte[] transcript) throws GeneralSecurityException {
    Preconditions.checkArgument(appId.equals(encryptedPublicKey.appId));
    // Mix in the app ID, salt, and transcript into HKDF and use it as AES-GCM AAD
    byte[] aadState = Bytes.concat(appId.getBytes(UTF_8), encryptedPublicKey.salt, transcript);
    // Use HKDF to derive an AES_GCM key from the pre-shared key, non-secret salt, and AAD state
    byte[] derivedKeyEncryptingKey = Hkdf.computeHkdf(
        MAC_ALGORITHM,
        preSharedSecret,
        encryptedPublicKey.salt,
        aadState,
        AES_GCM_KEY_SIZE_BYTES);
    // If the AES-GCM payload is modified at all or if the AAD state does not match, decryption
    // will throw a GeneralSecurityException.
    return new AesGcmJce(derivedKeyEncryptingKey)
        .decrypt(encryptedPublicKey.ciphertext, aadState);
  }

  /**
   * Encrypt an ephemeral X25519 public key to be sent to the server as a challenge.
   *
   * @return An encrypted client ephemeral public key to be sent to the server.
   */
  AuthMessage challenge() throws GeneralSecurityException {
    setClientPrivateKey(X25519.generatePrivateKey());
    return encryptEphemeralPublicKey(
        X25519.publicFromPrivate(clientPrivateKey),
        EMPTY_TRANSCRIPT);
  }

  /**
   * Validates the client challenge by decrypting the ephemeral X25519 public key, computing a
   * shared secret from it, then encrypting a server ephemeral X25519 public key for the client.
   *
   * @param encryptedClientPublicKey The encrypted public key from the client to be decrypted.
   * @return An encrypted server ephemeral public key to be sent to the client.
   */
  AuthMessage response(AuthMessage encryptedClientPublicKey) throws GeneralSecurityException {
    Preconditions.checkArgument(appId.equals(encryptedClientPublicKey.appId));
    // Compute a shared secret given the client public key and the server private key
    byte[] clientPublicKey =
        decryptEphemeralPublicKey(encryptedClientPublicKey, EMPTY_TRANSCRIPT);
    // Generate an ephemeral X25519 private key.
    byte[] serverEphemeralPrivateKey = X25519.generatePrivateKey();
    // Encrypt the X25519 public key with a key derived from the preSharedSecret and transcript
    AuthMessage ephemeralServerPublicKey = encryptEphemeralPublicKey(
        X25519.publicFromPrivate(serverEphemeralPrivateKey),
        getTranscript(encryptedClientPublicKey));
    // Compute a shared secret given the client public key and the server private key
    byte[] sharedSecret =
        X25519.computeSharedSecret(serverEphemeralPrivateKey, clientPublicKey);
    byte[] challengeResponseTranscript =
        getTranscript(encryptedClientPublicKey, ephemeralServerPublicKey);
    this.sessionCipher =
        generateTransportCipher(sharedSecret, false, challengeResponseTranscript);
    return ephemeralServerPublicKey;
  }

  /**
   * Validates the server response and initializes the cipher to use for the session.
   *
   * @param encryptedClientPublicKey The encrypted ephemeral public key from the client.
   * @param encryptedServerPublicKey The encrypted ephemeral public key from the server.
   */
  void deriveSessionCipher(AuthMessage encryptedClientPublicKey,
                           AuthMessage encryptedServerPublicKey) throws GeneralSecurityException {
    Preconditions.checkArgument(appId.equals(encryptedClientPublicKey.appId));
    Preconditions.checkArgument(appId.equals(encryptedServerPublicKey.appId));
    // Compute a shared secret given the server public key and the client private key,
    // mixing in the protocol transcript.
    byte[] serverPublicKey = decryptEphemeralPublicKey(
        encryptedServerPublicKey,
        getTranscript(encryptedClientPublicKey));
    // Compute a shared secret given the client public key and the server private key
    byte[] sharedSecret = X25519.computeSharedSecret(clientPrivateKey, serverPublicKey);
    byte[] challengeResponseTranscript =
        getTranscript(encryptedClientPublicKey, encryptedServerPublicKey);
    this.sessionCipher =
        generateTransportCipher(sharedSecret, true, challengeResponseTranscript);
  }

  private TransportCipher generateTransportCipher(
      byte[] sharedSecret,
      boolean isClient,
      byte[] transcript) throws GeneralSecurityException {
    byte[] derivedKey = unsafeSkipFinalHkdf ? sharedSecret :  // This is for backwards compatibility
      Hkdf.computeHkdf(
        MAC_ALGORITHM,
        sharedSecret,
        transcript,
        DERIVED_KEY_INFO,
        AES_GCM_KEY_SIZE_BYTES);
    byte[] clientIv = Hkdf.computeHkdf(
        MAC_ALGORITHM,
        sharedSecret,
        transcript,  // Passing this as the HKDF salt
        INPUT_IV_INFO,  // This is the HKDF info field used to differentiate IV values
        AES_GCM_KEY_SIZE_BYTES);
    byte[] serverIv = Hkdf.computeHkdf(
        MAC_ALGORITHM,
        sharedSecret,
        transcript,  // Passing this as the HKDF salt
        OUTPUT_IV_INFO,  // This is the HKDF info field used to differentiate IV values
        AES_GCM_KEY_SIZE_BYTES);
    SecretKeySpec sessionKey = new SecretKeySpec(derivedKey, "AES");
    if (LEGACY_CIPHER_ALGORITHM.equalsIgnoreCase(conf.cipherTransformation())) {
      return new CtrTransportCipher(
          cryptoConf,
          sessionKey,
          isClient ? clientIv : serverIv,  // If it's the client, use the client IV first
          isClient ? serverIv : clientIv);
    } else if (CIPHER_ALGORITHM.equalsIgnoreCase(conf.cipherTransformation())) {
      return new GcmTransportCipher(sessionKey);
    } else {
      throw new IllegalArgumentException(
              String.format("Unsupported cipher mode: %s. %s and %s are supported.",
                      conf.cipherTransformation(), CIPHER_ALGORITHM, LEGACY_CIPHER_ALGORITHM));
    }
  }

  private byte[] getTranscript(AuthMessage... encryptedPublicKeys) {
    ByteBuf transcript = Unpooled.buffer(
        Arrays.stream(encryptedPublicKeys).mapToInt(k -> k.encodedLength()).sum());
    Arrays.stream(encryptedPublicKeys).forEachOrdered(k -> k.encode(transcript));
    return transcript.array();
  }

  TransportCipher sessionCipher() {
    Preconditions.checkState(sessionCipher != null);
    return sessionCipher;
  }

  @Override
  public void close() {

  }
}
