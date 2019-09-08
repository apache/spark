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

package org.apache.spark.sql.hive.thriftserver

import java.security.{MessageDigest, NoSuchAlgorithmException}

import org.apache.commons.codec.binary.Base64

import org.apache.spark.internal.Logging

/**
 * The cookie signer generates a signature based on SHA digest
 * and appends it to the cookie value generated at the
 * server side. It uses SHA digest algorithm to sign and verify signatures.
 */
object CookieSigner {
  private val SIGNATURE = "&s="
  private val SHA_STRING = "SHA"
}

class CookieSigner extends Logging {
  private var secretBytes: Array[Byte] = null

  def this(secret: Array[Byte]) {
    this()
    if (secret == null) {
      throw new IllegalArgumentException(" NULL Secret Bytes")
    }
    this.secretBytes = secret.clone
  }


  /**
   * Sign the cookie given the string token as input.
   *
   * @param str Input token
   * @return Signed token that can be used to create a cookie
   */
  def signCookie(str: String): String = {
    if (str == null || str.isEmpty) {
      throw new IllegalArgumentException("NULL or empty string to sign")
    }
    val signature = getSignature(str)
    logDebug("Signature generated for " + str + " is " + signature)
    str + CookieSigner.SIGNATURE + signature
  }

  /**
   * Verify a signed string and extracts the original string.
   *
   * @param signedStr The already signed string
   * @return Raw Value of the string without the signature
   */
  def verifyAndExtract(signedStr: String): String = {
    val index = signedStr.lastIndexOf(CookieSigner.SIGNATURE)
    if (index == -1) {
      throw new IllegalArgumentException("Invalid input sign: " + signedStr)
    }
    val originalSignature = signedStr.substring(index + CookieSigner.SIGNATURE.length)
    val rawValue = signedStr.substring(0, index)
    val currentSignature = getSignature(rawValue)
    logDebug("Signature generated for " + rawValue + " inside verify is " + currentSignature)
    if (!(originalSignature == currentSignature)) {
      throw new IllegalArgumentException("Invalid sign, original = " +
        originalSignature + " current = " + currentSignature)
    }
    rawValue
  }

  /**
   * Get the signature of the input string based on SHA digest algorithm.
   *
   * @param str Input token
   * @return Signed String
   */
  private def getSignature(str: String) = try {
    val md = MessageDigest.getInstance(CookieSigner.SHA_STRING)
    md.update(str.getBytes)
    md.update(secretBytes)
    val digest = md.digest
    new Base64(0).encodeToString(digest)
  } catch {
    case ex: NoSuchAlgorithmException =>
      throw new RuntimeException("Invalid SHA digest String: " +
        CookieSigner.SHA_STRING + " " + ex.getMessage, ex)
  }
}
