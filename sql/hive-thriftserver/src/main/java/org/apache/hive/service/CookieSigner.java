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

package org.apache.hive.service;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The cookie signer generates a signature based on SHA digest
 * and appends it to the cookie value generated at the
 * server side. It uses SHA digest algorithm to sign and verify signatures.
 */
public class CookieSigner {
  private static final String SIGNATURE = "&s=";
  private static final String SHA_STRING = "SHA";
  private byte[] secretBytes;
  private static final Log LOG = LogFactory.getLog(CookieSigner.class);

  /**
   * Constructor
   * @param secret Secret Bytes
   */
  public CookieSigner(byte[] secret) {
    if (secret == null) {
      throw new IllegalArgumentException(" NULL Secret Bytes");
    }
    this.secretBytes = secret.clone();
  }

  /**
   * Sign the cookie given the string token as input.
   * @param str Input token
   * @return Signed token that can be used to create a cookie
   */
  public String signCookie(String str) {
    if (str == null || str.isEmpty()) {
      throw new IllegalArgumentException("NULL or empty string to sign");
    }
    String signature = getSignature(str);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Signature generated for " + str + " is " + signature);
    }
    return str + SIGNATURE + signature;
  }

  /**
   * Verify a signed string and extracts the original string.
   * @param signedStr The already signed string
   * @return Raw Value of the string without the signature
   */
  public String verifyAndExtract(String signedStr) {
    int index = signedStr.lastIndexOf(SIGNATURE);
    if (index == -1) {
      throw new IllegalArgumentException("Invalid input sign: " + signedStr);
    }
    String originalSignature = signedStr.substring(index + SIGNATURE.length());
    String rawValue = signedStr.substring(0, index);
    String currentSignature = getSignature(rawValue);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Signature generated for " + rawValue + " inside verify is " + currentSignature);
    }
    if (!originalSignature.equals(currentSignature)) {
      throw new IllegalArgumentException("Invalid sign, original = " + originalSignature +
        " current = " + currentSignature);
    }
    return rawValue;
  }

  /**
   * Get the signature of the input string based on SHA digest algorithm.
   * @param str Input token
   * @return Signed String
   */
  private String getSignature(String str) {
    try {
      MessageDigest md = MessageDigest.getInstance(SHA_STRING);
      md.update(str.getBytes());
      md.update(secretBytes);
      byte[] digest = md.digest();
      return new Base64(0).encodeToString(digest);
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException("Invalid SHA digest String: " + SHA_STRING +
        " " + ex.getMessage(), ex);
    }
  }
}
