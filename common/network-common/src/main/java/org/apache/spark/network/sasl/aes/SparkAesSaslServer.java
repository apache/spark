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

import java.nio.ByteBuffer;
import java.util.Properties;
import javax.security.sasl.SaslException;

import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.commons.crypto.random.CryptoRandom;
import org.apache.commons.crypto.random.CryptoRandomFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.sasl.SparkSaslServer;
import org.apache.spark.network.util.TransportConf;

public class SparkAesSaslServer extends SparkSaslServer {
  private static final Logger logger = LoggerFactory.getLogger(SparkAesSaslServer.class);
  private SparkAesCipher cipher;
  private boolean isAuthenticated = false;

  public SparkAesSaslServer(
      String secretKeyId,
      SecretKeyHolder secretKeyHolder,
      boolean alwaysEncrypt){
    super(secretKeyId, secretKeyHolder, alwaysEncrypt);
  }

  @Override
  public byte[] wrap(byte[] data, int offset, int len) throws SaslException {
    return cipher.wrap(data, offset, len);
  }

  @Override
  public byte[] unwrap(byte[] data, int offset, int len) throws SaslException {
    return cipher.unwrap(data, offset, len);
  }

  @Override
  public boolean negotiate(ByteBuffer message, RpcResponseCallback callback, TransportConf conf) {
    if (!isAuthenticated) {
      isAuthenticated = true;
      return false;
    }

    // Receive initial option from client
    CipherOption cipherOption = CipherOption.decode(Unpooled.wrappedBuffer(message));
    String transformation = cipherOption.cipherSuite;
    Properties properties = new Properties();

    try {
      // Generate key and iv
      if (conf.saslEncryptionAesCipherKeySizeBits() % 8 != 0) {
        throw new IllegalArgumentException("The AES cipher key size in bits should be a multiple " +
          "of byte");
      }

      int keyLen = conf.saslEncryptionAesCipherKeySizeBits() / 8;
      int paramLen = CryptoCipherFactory.getCryptoCipher(transformation,properties).getBlockSize();
      byte[] inKey = new byte[keyLen];
      byte[] outKey = new byte[keyLen];
      byte[] inIv = new byte[paramLen];
      byte[] outIv = new byte[paramLen];

      // Get the 'CryptoRandom' instance.
      CryptoRandom random = CryptoRandomFactory.getCryptoRandom(properties);
      random.nextBytes(inKey);
      random.nextBytes(outKey);
      random.nextBytes(inIv);
      random.nextBytes(outIv);

      // Create new option for client. The key is encrypted
      cipherOption = new CipherOption(cipherOption.cipherSuite,
        super.wrap(inKey, 0, inKey.length), inIv,
        super.wrap(outKey, 0, outKey.length), outIv);

      // Enable AES on saslServer
      cipher = new SparkAesCipher(transformation, properties, inKey, outKey, inIv, outIv);

      // Send cipher option to client
      ByteBuf buf = Unpooled.buffer(cipherOption.encodedLength());
      cipherOption.encode(buf);
      callback.onSuccess(buf.nioBuffer());
    } catch (Exception e) {
      logger.error("AES negotiation exception: ", e);
      throw Throwables.propagate(e);
    }

    return true;
  }
}
