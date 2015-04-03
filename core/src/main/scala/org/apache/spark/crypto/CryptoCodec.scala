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

import java.lang.ClassCastException

import scala.reflect.runtime.universe

import org.apache.spark.crypto.CommonConfigurationKeys.SPARK_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT
import org.apache.spark.crypto.CommonConfigurationKeys.SPARK_SECURITY_CRYPTO_CIPHER_SUITE_KEY
import org.apache.spark.crypto.CommonConfigurationKeys.SPARK_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_KEY
import org.apache.spark.{SparkConf, Logging}
/**
 * Crypto codec class, encapsulates encryptor/decryptor pair.
 */
abstract case class CryptoCodec() {
  /**
   *
   * @return the CipherSuite for this codec.
   */
  def getCipherSuite(): CipherSuite

  /**
   * This interface is only for Counter (CTR) mode. Generally the Encryptor
   * or Decryptor calculates the IV and maintain encryption context internally.
   * For example a {@link javax.crypto.Cipher} will maintain its encryption
   * context internally when we do encryption/decryption using the
   * Cipher#update interface.
   * <p/>
   * The IV can be calculated by combining the initial IV and the counter with
   * a lossless operation (concatenation, addition, or XOR).
   * @see http://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Counter_.28CTR.29
   * @param initIV
   * @param counter
   * @param IV
   */
  def calculateIV(initIV: Array[Byte], counter: Long, IV: Array[Byte])

  /**
   * @return Encryptor the encryptor
   */
  def createEncryptor: Encryptor

  /**
   * @return Decryptor the decryptor
   */
  def createDecryptor: Decryptor

  /**
   * Generate a number of secure, random bytes suitable for cryptographic use.
   * This method needs to be thread-safe.
   * @param bytes byte array to populate with random data
   */
  def generateSecureRandom(bytes: Array[Byte])
}

object CryptoCodec extends Logging {
  def getInstance(conf: SparkConf): CryptoCodec = {
    var name: String = conf.get(SPARK_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
      SPARK_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT)
    getInstance(conf, CipherSuite.convert(name))
  }

  def getInstance(conf: SparkConf, cipherSuite: CipherSuite): CryptoCodec = {
    var klasses: List[String] = getCodecClasses(conf, cipherSuite)
    var codec: CryptoCodec = null
    for (klass <- klasses) {
      try {
        val m = universe.runtimeMirror(getClass.getClassLoader)
        var c: CryptoCodec = null
        if (klass.equals("org.apache.spark.crypto.JceAesCtrCryptoCodec")) {
          val classCryptoCodec = universe.typeOf[org.apache.spark.crypto.JceAesCtrCryptoCodec]
            .typeSymbol.asClass
          val cm = m.reflectClass(classCryptoCodec)
          val ctor = universe.typeOf[org.apache.spark.crypto.JceAesCtrCryptoCodec].declaration(
            universe.nme.CONSTRUCTOR).asMethod
          val ctorm = cm.reflectConstructor(ctor)
          val p = ctorm(conf)
          c = p.asInstanceOf[org.apache.spark.crypto.CryptoCodec]
        } else {
          val classCryptoCodec = universe.typeOf[org.apache.spark.crypto.OpensslAesCtrCryptoCodec]
            .typeSymbol.asClass
          val cm = m.reflectClass(classCryptoCodec)
          val ctor = universe.typeOf[org.apache.spark.crypto.OpensslAesCtrCryptoCodec]
            .declaration(
            universe.nme.CONSTRUCTOR).asMethod
          val ctorm = cm.reflectConstructor(ctor)
          val p = ctorm(conf)
          c = p.asInstanceOf[org.apache.spark.crypto.CryptoCodec]
        }

        if (c.getCipherSuite.name.equals(cipherSuite.name)) {
          if (codec == null) {
            logDebug(s"Using crypto codec $klass.getName.")
            codec = c
          }
        }
        else {
          logDebug(s"Crypto codec $klass.getName doesn't meet the cipher suite $cipherSuite" +
            s".getName.")
        }
      }
      catch {
        case e: Exception => {
          logDebug(s"Crypto codec $klass.getName is not available.")
        }
      }
    }
    codec
  }

  def getCodecClasses(conf: SparkConf, cipherSuite: CipherSuite): List[String] = {
    var result:List[String] = List()
    val configName: String = SPARK_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_KEY
    val codecString: String = conf.get(configName)
    if (codecString == null) {
      logDebug("No crypto codec classes with cipher suite configured.")
      null
    }

    var codecArray: Array[String] = codecString.trim.split(",")
    for (c <- codecArray) {
      try {
       result = c :: result
      }
      catch {
        case e: ClassCastException => {
          logDebug(s"Class $c is not a CryptoCodec.")
        }
        case e: ClassNotFoundException => {
          logDebug(s"Crypto codec $c not found.")
        }
      }
    }
    result
  }
}
