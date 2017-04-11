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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, FileOutputStream}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.{Arrays, Random, UUID}

import com.google.common.io.ByteStreams

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.network.util.CryptoUtils
import org.apache.spark.security.CryptoStreamUtils._
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.storage.TempShuffleBlockId

class CryptoStreamUtilsSuite extends SparkFunSuite {

  test("crypto configuration conversion") {
    val sparkKey1 = s"${SPARK_IO_ENCRYPTION_COMMONS_CONFIG_PREFIX}a.b.c"
    val sparkVal1 = "val1"
    val cryptoKey1 = s"${CryptoUtils.COMMONS_CRYPTO_CONFIG_PREFIX}a.b.c"

    val sparkKey2 = SPARK_IO_ENCRYPTION_COMMONS_CONFIG_PREFIX.stripSuffix(".") + "A.b.c"
    val sparkVal2 = "val2"
    val cryptoKey2 = s"${CryptoUtils.COMMONS_CRYPTO_CONFIG_PREFIX}A.b.c"
    val conf = new SparkConf()
    conf.set(sparkKey1, sparkVal1)
    conf.set(sparkKey2, sparkVal2)
    val props = CryptoStreamUtils.toCryptoConf(conf)
    assert(props.getProperty(cryptoKey1) === sparkVal1)
    assert(!props.containsKey(cryptoKey2))
  }

  test("shuffle encryption key length should be 128 by default") {
    val conf = createConf()
    var key = CryptoStreamUtils.createKey(conf)
    val actual = key.length * (java.lang.Byte.SIZE)
    assert(actual === 128)
  }

  test("create 256-bit key") {
    val conf = createConf(IO_ENCRYPTION_KEY_SIZE_BITS.key -> "256")
    var key = CryptoStreamUtils.createKey(conf)
    val actual = key.length * (java.lang.Byte.SIZE)
    assert(actual === 256)
  }

  test("create key with invalid length") {
    intercept[IllegalArgumentException] {
      val conf = createConf(IO_ENCRYPTION_KEY_SIZE_BITS.key -> "328")
      CryptoStreamUtils.createKey(conf)
    }
  }

  test("serializer manager integration") {
    val conf = createConf()
      .set("spark.shuffle.compress", "true")
      .set("spark.shuffle.spill.compress", "true")

    val plainStr = "hello world"
    val blockId = new TempShuffleBlockId(UUID.randomUUID())
    val key = Some(CryptoStreamUtils.createKey(conf))
    val serializerManager = new SerializerManager(new JavaSerializer(conf), conf,
      encryptionKey = key)

    val outputStream = new ByteArrayOutputStream()
    val wrappedOutputStream = serializerManager.wrapStream(blockId, outputStream)
    wrappedOutputStream.write(plainStr.getBytes(UTF_8))
    wrappedOutputStream.close()

    val encryptedBytes = outputStream.toByteArray
    val encryptedStr = new String(encryptedBytes, UTF_8)
    assert(plainStr !== encryptedStr)

    val inputStream = new ByteArrayInputStream(encryptedBytes)
    val wrappedInputStream = serializerManager.wrapStream(blockId, inputStream)
    val decryptedBytes = ByteStreams.toByteArray(wrappedInputStream)
    val decryptedStr = new String(decryptedBytes, UTF_8)
    assert(decryptedStr === plainStr)
  }

  test("encryption key propagation to executors") {
    val conf = createConf().setAppName("Crypto Test").setMaster("local-cluster[1,1,1024]")
    val sc = new SparkContext(conf)
    try {
      val content = "This is the content to be encrypted."
      val encrypted = sc.parallelize(Seq(1))
        .map { str =>
          val bytes = new ByteArrayOutputStream()
          val out = CryptoStreamUtils.createCryptoOutputStream(bytes, SparkEnv.get.conf,
            SparkEnv.get.securityManager.getIOEncryptionKey().get)
          out.write(content.getBytes(UTF_8))
          out.close()
          bytes.toByteArray()
        }.collect()(0)

      assert(content != encrypted)

      val in = CryptoStreamUtils.createCryptoInputStream(new ByteArrayInputStream(encrypted),
        sc.conf, SparkEnv.get.securityManager.getIOEncryptionKey().get)
      val decrypted = new String(ByteStreams.toByteArray(in), UTF_8)
      assert(content === decrypted)
    } finally {
      sc.stop()
    }
  }

  test("crypto stream wrappers") {
    val testData = new Array[Byte](128 * 1024)
    new Random().nextBytes(testData)

    val conf = createConf()
    val key = createKey(conf)
    val file = Files.createTempFile("crypto", ".test").toFile()

    val outStream = createCryptoOutputStream(new FileOutputStream(file), conf, key)
    try {
      ByteStreams.copy(new ByteArrayInputStream(testData), outStream)
    } finally {
      outStream.close()
    }

    val inStream = createCryptoInputStream(new FileInputStream(file), conf, key)
    try {
      val inStreamData = ByteStreams.toByteArray(inStream)
      assert(Arrays.equals(inStreamData, testData))
    } finally {
      inStream.close()
    }

    val outChannel = createWritableChannel(new FileOutputStream(file).getChannel(), conf, key)
    try {
      val inByteChannel = Channels.newChannel(new ByteArrayInputStream(testData))
      ByteStreams.copy(inByteChannel, outChannel)
    } finally {
      outChannel.close()
    }

    val inChannel = createReadableChannel(new FileInputStream(file).getChannel(), conf, key)
    try {
      val inChannelData = ByteStreams.toByteArray(Channels.newInputStream(inChannel))
      assert(Arrays.equals(inChannelData, testData))
    } finally {
      inChannel.close()
    }
  }

  private def createConf(extra: (String, String)*): SparkConf = {
    val conf = new SparkConf()
    extra.foreach { case (k, v) => conf.set(k, v) }
    conf.set(IO_ENCRYPTION_ENABLED, true)
    conf
  }

}
