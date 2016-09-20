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
import java.nio.ByteBuffer;
import java.util.Properties;
import javax.security.sasl.SaslException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.sasl.SparkSaslClient;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.util.TransportConf;

public class SparkAesSaslClient extends SparkSaslClient {
  private static final Logger logger = LoggerFactory.getLogger(SparkAesSaslClient.class);
  private SparkAesCipher cipher;

  public SparkAesSaslClient(String secretKeyId, SecretKeyHolder secretKeyHolder, boolean encrypt){
    super(secretKeyId, secretKeyHolder, encrypt);
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
  public void negotiate(TransportClient client, TransportConf conf) throws IOException {
    super.negotiate(client, conf);

    // Create option for negotiation
    CipherOption cipherOption = new CipherOption(conf.saslEncryptionAesCipherTransformation());
    ByteBuf buf = Unpooled.buffer(cipherOption.encodedLength());
    cipherOption.encode(buf);

    // Send option to server and decode received negotiated option
    ByteBuffer response = client.sendRpcSync(buf.nioBuffer(), conf.saslRTTimeoutMs());
    cipherOption = CipherOption.decode(Unpooled.wrappedBuffer(response));

    // Decrypt key from option. Server's outKey is client's inKey, and vice versa.
    byte[] outKey = super.unwrap(cipherOption.inKey, 0, cipherOption.inKey.length);
    byte[] inKey = super.unwrap(cipherOption.outKey, 0, cipherOption.outKey.length);

    // Enable AES on SaslClient
    Properties properties = new Properties();

    cipher = new SparkAesCipher(cipherOption.cipherSuite, properties, inKey, outKey,
      cipherOption.outIv, cipherOption.inIv);

    logger.debug("AES enabled for SASL client encryption.");
  }
}
