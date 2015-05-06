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

import java.security.NoSuchAlgorithmException
import javax.crypto.{KeyGenerator, SecretKey}

import org.apache.hadoop.security.Credentials

import org.apache.spark.crypto.CommonConfigurationKeys._
import org.apache.spark.SparkConf

/**
 * CryptoConf is a class for Crypto configuration
 */
private[spark] case class CryptoConf(enabled: Boolean = false) {

}

private[spark] object CryptoConf {
  def parse(sparkConf: SparkConf): CryptoConf = {
    val enabled = if (sparkConf != null) {
      sparkConf.getBoolean("spark.encrypted.shuffle", false)
    } else {
      false
    }
    new CryptoConf(enabled)
  }

  def initSparkShuffleCredentials(conf:SparkConf, credentials: Credentials) {
    if (credentials.getSecretKey(SPARK_SHUFFLE_TOKEN) == null) {
      var keyGen: KeyGenerator = null
      try {
        val SHUFFLE_KEY_LENGTH: Int = 64
        var keyLen: Int = if (conf.getBoolean(SPARK_ENCRYPTED_INTERMEDIATE_DATA,
          DEFAULT_SPARK_ENCRYPTED_INTERMEDIATE_DATA) == true) {
          conf.getInt(SPARK_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS,
            DEFAULT_SPARK_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS)
        } else {
          SHUFFLE_KEY_LENGTH
        }
        val SHUFFLE_KEYGEN_ALGORITHM = "HmacSHA1";
        keyGen = KeyGenerator.getInstance(SHUFFLE_KEYGEN_ALGORITHM)
        keyGen.init(keyLen)
      } catch {
        case e: NoSuchAlgorithmException => throw new RuntimeException("Error generating " +
          "shuffle secret key")
      }
      val shuffleKey: SecretKey = keyGen.generateKey
      credentials.addSecretKey(SPARK_SHUFFLE_TOKEN, shuffleKey.getEncoded)
    }
  }
}

