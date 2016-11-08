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
package org.apache.spark.security

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.security.CryptoStreamUtils._

class CryptoStreamUtilsSuite extends SparkFunSuite {
  val ugi = UserGroupInformation.createUserForTesting("testuser", Array("testgroup"))

  test("Crypto configuration conversion") {
    val sparkKey1 = s"${SPARK_IO_ENCRYPTION_COMMONS_CONFIG_PREFIX}a.b.c"
    val sparkVal1 = "val1"
    val cryptoKey1 = s"${COMMONS_CRYPTO_CONF_PREFIX}a.b.c"

    val sparkKey2 = SPARK_IO_ENCRYPTION_COMMONS_CONFIG_PREFIX.stripSuffix(".") + "A.b.c"
    val sparkVal2 = "val2"
    val cryptoKey2 = s"${COMMONS_CRYPTO_CONF_PREFIX}A.b.c"
    val conf = new SparkConf()
    conf.set(sparkKey1, sparkVal1)
    conf.set(sparkKey2, sparkVal2)
    val props = CryptoStreamUtils.toCryptoConf(conf)
    assert(props.getProperty(cryptoKey1) === sparkVal1)
    assert(!props.containsKey(cryptoKey2))
  }

  test("Shuffle encryption is disabled by default") {
    ugi.doAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        val credentials = UserGroupInformation.getCurrentUser.getCredentials()
        val conf = new SparkConf()
        initCredentials(conf, credentials)
        assert(credentials.getSecretKey(SPARK_IO_TOKEN) === null)
      }
    })
  }

  test("Shuffle encryption key length should be 128 by default") {
    ugi.doAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        val credentials = UserGroupInformation.getCurrentUser.getCredentials()
        val conf = new SparkConf()
        conf.set(IO_ENCRYPTION_ENABLED, true)
        initCredentials(conf, credentials)
        var key = credentials.getSecretKey(SPARK_IO_TOKEN)
        assert(key !== null)
        val actual = key.length * (java.lang.Byte.SIZE)
        assert(actual === 128)
      }
    })
  }

  test("Initial credentials with key length in 256") {
    ugi.doAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        val credentials = UserGroupInformation.getCurrentUser.getCredentials()
        val conf = new SparkConf()
        conf.set(IO_ENCRYPTION_KEY_SIZE_BITS, 256)
        conf.set(IO_ENCRYPTION_ENABLED, true)
        initCredentials(conf, credentials)
        var key = credentials.getSecretKey(SPARK_IO_TOKEN)
        assert(key !== null)
        val actual = key.length * (java.lang.Byte.SIZE)
        assert(actual === 256)
      }
    })
  }

  test("Initial credentials with invalid key length") {
    ugi.doAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        val credentials = UserGroupInformation.getCurrentUser.getCredentials()
        val conf = new SparkConf()
        conf.set(IO_ENCRYPTION_KEY_SIZE_BITS, 328)
        conf.set(IO_ENCRYPTION_ENABLED, true)
        val thrown = intercept[IllegalArgumentException] {
          initCredentials(conf, credentials)
        }
      }
    })
  }

  private[this] def initCredentials(conf: SparkConf, credentials: Credentials): Unit = {
    if (conf.get(IO_ENCRYPTION_ENABLED)) {
      SecurityManager.initIOEncryptionKey(conf, credentials)
    }
  }
}
