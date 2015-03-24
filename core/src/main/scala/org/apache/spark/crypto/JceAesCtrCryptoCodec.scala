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
package org.apache.spark.crypto

import java.io.IOException
import java.lang.String
import java.nio.ByteBuffer
import java.security.{GeneralSecurityException, SecureRandom}
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import com.google.common.base.Preconditions

import org.apache.spark.crypto.CommonConfigurationKeys
.SPARK_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT
import org.apache.spark.crypto.CommonConfigurationKeys
.SPARK_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY
import org.apache.spark.{SparkConf, Logging}

/**
 * Implement the AES-CTR crypto codec using JCE provider.
 * @param conf
 */
class JceAesCtrCryptoCodec(conf:SparkConf) extends AesCtrCryptoCodec with Logging {
  var provider: String = null
  var random: SecureRandom = null

  setConf(conf)

  def setConf(conf: SparkConf) {
    val secureRandomAlg: String = conf.get(SPARK_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY,
      SPARK_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT)
    try {
        random =  SecureRandom.getInstance(secureRandomAlg)
    }
    catch {
      case e: GeneralSecurityException => {
        logWarning(e.getMessage)
        random = new SecureRandom
      }
    }
  }

  def createEncryptor: Encryptor = {
    new JceAesCtrCipher(Cipher.ENCRYPT_MODE, provider)
  }

  def createDecryptor: Decryptor = {
    new JceAesCtrCipher(Cipher.DECRYPT_MODE, provider)
  }

  def generateSecureRandom(bytes: Array[Byte]) {
    random.nextBytes(bytes)
  }

  class JceAesCtrCipher(mode: Int, provider: String) extends Encryptor with Decryptor {

    var contextReset: Boolean = false

    val cipher: Cipher = if (provider == null || provider.isEmpty) {
      Cipher.getInstance(SUITE.name)
    }
    else {
      Cipher.getInstance(SUITE.name, provider)
    }

    def init(key: Array[Byte], iv: Array[Byte]) {
      Preconditions.checkNotNull(key)
      Preconditions.checkNotNull(iv)
      contextReset = false
      try {
        cipher.init(mode, new SecretKeySpec(key, "AES"), new IvParameterSpec(iv))
      }
      catch {
        case e: Exception => {
          throw new IOException(e)
        }
      }
    }

    /**
     * AES-CTR will consume all of the input data. It requires enough space in
     * the destination buffer to encrypt entire input buffer.
     */
    def encrypt(inBuffer: ByteBuffer, outBuffer: ByteBuffer) {
      process(inBuffer, outBuffer)
    }

    /**
     * AES-CTR will consume all of the input data. It requires enough space in
     * the destination buffer to decrypt entire input buffer.
     */
    def decrypt(inBuffer: ByteBuffer, outBuffer: ByteBuffer) {
      process(inBuffer, outBuffer)
    }

    def process(inBuffer: ByteBuffer, outBuffer: ByteBuffer) {
      try {
        val inputSize: Int = inBuffer.remaining
        val n: Int = cipher.update(inBuffer, outBuffer)
        if (n < inputSize) {
          contextReset = true
          cipher.doFinal(inBuffer, outBuffer)
        }
      }
      catch {
        case e: Exception => {
          throw new IOException(e)
        }
      }
    }

    def isContextReset: Boolean = {
      contextReset
    }
  }
}

