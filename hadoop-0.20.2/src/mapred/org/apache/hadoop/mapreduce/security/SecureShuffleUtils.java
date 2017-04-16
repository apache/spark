/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.security;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;

import javax.crypto.SecretKey;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.record.Utils;

/**
 * 
 * utilities for generating kyes, hashes and verifying them for shuffle
 *
 */
public class SecureShuffleUtils {
  public static final String HTTP_HEADER_URL_HASH = "UrlHash";
  public static final String HTTP_HEADER_REPLY_URL_HASH = "ReplyHash";
  
  /**
   * Base64 encoded hash of msg
   * @param msg
   */
  public static String generateHash(byte[] msg, SecretKey key) {
    return new String(Base64.encodeBase64(generateByteHash(msg, key)));
  }
  
  /**
   * calculate hash of msg
   * @param msg
   * @return
   */
  private static byte[] generateByteHash(byte[] msg, SecretKey key) {
    return JobTokenSecretManager.computeHash(msg, key);
  }
  
  /**
   * verify that hash equals to HMacHash(msg)
   * @param newHash
   * @return true if is the same
   */
  private static boolean verifyHash(byte[] hash, byte[] msg, SecretKey key) {
    byte[] msg_hash = generateByteHash(msg, key);
    return Utils.compareBytes(msg_hash, 0, msg_hash.length, hash, 0, hash.length) == 0;
  }
  
  /**
   * Aux util to calculate hash of a String
   * @param enc_str
   * @param key
   * @return Base64 encodedHash
   * @throws IOException
   */
  public static String hashFromString(String enc_str, SecretKey key) 
  throws IOException {
    return generateHash(enc_str.getBytes(), key); 
  }
  
  /**
   * verify that base64Hash is same as HMacHash(msg)  
   * @param base64Hash (Base64 encoded hash)
   * @param msg
   * @throws IOException if not the same
   */
  public static void verifyReply(String base64Hash, String msg, SecretKey key)
  throws IOException {
    byte[] hash = Base64.decodeBase64(base64Hash.getBytes());
    
    boolean res = verifyHash(hash, msg.getBytes(), key);
    
    if(res != true) {
      throw new IOException("Verification of the hashReply failed");
    }
  }
  
  /**
   * Shuffle specific utils - build string for encoding from URL
   * @param url
   * @return string for encoding
   */
  public static String buildMsgFrom(URL url) {
    return buildMsgFrom(url.getPath(), url.getQuery(), url.getPort());
  }
  /**
   * Shuffle specific utils - build string for encoding from URL
   * @param request
   * @return string for encoding
   */
  public static String buildMsgFrom(HttpServletRequest request ) {
    return buildMsgFrom(request.getRequestURI(), request.getQueryString(),
        request.getLocalPort());
  }
  /**
   * Shuffle specific utils - build string for encoding from URL
   * @param uri_path
   * @param uri_query
   * @return string for encoding
   */
  private static String buildMsgFrom(String uri_path, String uri_query, int port) {
    return String.valueOf(port) + uri_path + "?" + uri_query;
  }
  
  
  /**
   * byte array to Hex String
   * @param ba
   * @return string with HEX value of the key
   */
  public static String toHex(byte[] ba) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    for(byte b: ba) {
      ps.printf("%x", b);
    }
    return baos.toString();
  }
}
