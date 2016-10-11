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
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;

import org.apache.commons.crypto.stream.CryptoInputStream;
import org.apache.commons.crypto.stream.CryptoOutputStream;

/**
 * AES cipher for encryption and decryption.
 */
public class AesCipher {
  private final SecretKeySpec inKeySpec;
  private final IvParameterSpec inIvSpec;
  private final SecretKeySpec outKeySpec;
  private final IvParameterSpec outIvSpec;
  private Properties properties;

  private HashMap<ReadableByteChannel, CryptoInputStream> inputStreamMap;
  private HashMap<WritableByteChannel, CryptoOutputStream> outputStreamMap;

  public static final int STREAM_BUFFER_SIZE = 8192;
  public static final String TRANSFORM = "AES/CTR/NoPadding";

  public AesCipher(
      Properties properties,
      byte[] inKey,
      byte[] outKey,
      byte[] inIv,
      byte[] outIv) throws IOException {
    properties.setProperty(CryptoInputStream.STREAM_BUFFER_SIZE_KEY,
      String.valueOf(STREAM_BUFFER_SIZE));
    this.properties = properties;

    inputStreamMap = new HashMap<>();
    outputStreamMap= new HashMap<>();

    inKeySpec = new SecretKeySpec(inKey, "AES");
    inIvSpec = new IvParameterSpec(inIv);
    outKeySpec = new SecretKeySpec(outKey, "AES");
    outIvSpec = new IvParameterSpec(outIv);
  }

  /**
   * Create AES crypto output stream
   * @param ch The underlying channel to write out.
   * @return Return output crypto stream for encryption.
   * @throws IOException
   */
  public CryptoOutputStream CreateOutputStream(WritableByteChannel ch) throws IOException {
    if (!outputStreamMap.containsKey(ch)) {
      outputStreamMap.put(ch, new CryptoOutputStream(TRANSFORM, properties, ch, outKeySpec, outIvSpec));
    }

    return outputStreamMap.get(ch);
  }

  /**
   * Create AES crypto input stream
   * @param ch The underlying channel used to read data.
   * @return Return input crypto stream for decryption.
   * @throws IOException
   */
  public CryptoInputStream CreateInputStream(ReadableByteChannel ch) throws IOException {
    if (!inputStreamMap.containsKey(ch)) {
      inputStreamMap.put(ch, new CryptoInputStream(TRANSFORM, properties, ch, inKeySpec, inIvSpec));
    }

    return inputStreamMap.get(ch);
  }

}
