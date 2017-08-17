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

package org.apache.spark.network.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;

import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;

/**
 * Utility methods related to the hadoop security
 */
public class HadoopSecurityUtils {

  /** Creates an ClientToAMTokenIdentifier from the encoded Base-64 String */
  public static ClientToAMTokenIdentifier getIdentifier(String id) throws InvalidToken {
    byte[] tokenId = byteBufToByte(Base64.decode(
            Unpooled.wrappedBuffer(id.getBytes(StandardCharsets.UTF_8))));

    ClientToAMTokenIdentifier tokenIdentifier = new ClientToAMTokenIdentifier();
    try {
      tokenIdentifier.readFields(new DataInputStream(new ByteArrayInputStream(tokenId)));
    } catch (IOException e) {
      throw (InvalidToken) new InvalidToken(
              "Can't de-serialize tokenIdentifier").initCause(e);
    }
    return tokenIdentifier;
  }

  /** Returns an Base64-encoded secretKey created from the Identifier and the secretmanager */
  public static char[] getClientToAMSecretKey(ClientToAMTokenIdentifier tokenid,
                                              ClientToAMTokenSecretManager secretManager) throws InvalidToken {
    byte[] password =  secretManager.retrievePassword(tokenid);
    return Base64.encode(Unpooled.wrappedBuffer(password)).toString(StandardCharsets.UTF_8)
            .toCharArray();
  }
//
//  /** Decode a base64-encoded indentifier as a String. */
//  public static String decodeIdentifier(String identifier) {
//    Preconditions.checkNotNull(identifier, "User cannot be null if SASL is enabled");
//    return Base64.decode(Unpooled.wrappedBuffer(identifier.getBytes(StandardCharsets.UTF_8)))
//            .toString(StandardCharsets.UTF_8);
//  }

  /** Decode a base64-encoded MasterKey as a byte[] array. */
  public static byte[] decodeMasterKey(String masterKey) {
    ByteBuf masterKeyByteBuf = Base64.decode(Unpooled.wrappedBuffer(masterKey.getBytes(StandardCharsets.UTF_8)));
    return byteBufToByte(masterKeyByteBuf);
  }


  /** Convert an ByteBuf to a byte[] array. */
  private static byte[] byteBufToByte(ByteBuf byteBuf) {
    byte[] byteArray = new byte[byteBuf.readableBytes()];
    byteBuf.readBytes(byteArray);
    return byteArray;
  }

}
