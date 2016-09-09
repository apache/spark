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
package org.apache.spark.deploy.yarn

import java.io._
import java.nio.charset.StandardCharsets
import java.security.PrivilegedExceptionAction
import java.util.UUID

import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config._
import org.apache.spark.serializer._
import org.apache.spark.storage._

class IOEncryptionSuite extends SparkFunSuite with Matchers with BeforeAndAfterAll
  with BeforeAndAfterEach {
  private[this] val blockId = new TempShuffleBlockId(UUID.randomUUID())
  private[this] val conf = new SparkConf()
  private[this] val ugi = UserGroupInformation.createUserForTesting("testuser", Array("testgroup"))
  private[this] val serializer = new KryoSerializer(conf)

  override def beforeAll(): Unit = {
    System.setProperty("SPARK_YARN_MODE", "true")
    ugi.doAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        conf.set(IO_ENCRYPTION_ENABLED, true)
        val creds = new Credentials()
        SecurityManager.initIOEncryptionKey(conf, creds)
        SparkHadoopUtil.get.addCurrentUserCredentials(creds)
      }
    })
  }

  override def afterAll(): Unit = {
    SparkEnv.set(null)
    System.clearProperty("SPARK_YARN_MODE")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    conf.set("spark.shuffle.compress", false.toString)
    conf.set("spark.shuffle.spill.compress", false.toString)
  }

  test("IO encryption read and write") {
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        conf.set(IO_ENCRYPTION_ENABLED, true)
        conf.set("spark.shuffle.compress", false.toString)
        conf.set("spark.shuffle.spill.compress", false.toString)
        testYarnIOEncryptionWriteRead()
      }
    })
  }

  test("IO encryption read and write with shuffle compression enabled") {
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        conf.set(IO_ENCRYPTION_ENABLED, true)
        conf.set("spark.shuffle.compress", true.toString)
        conf.set("spark.shuffle.spill.compress", true.toString)
        testYarnIOEncryptionWriteRead()
      }
    })
  }

  private[this] def testYarnIOEncryptionWriteRead(): Unit = {
    val plainStr = "hello world"
    val outputStream = new ByteArrayOutputStream()
    val serializerManager = new SerializerManager(serializer, conf)
    val wrappedOutputStream = serializerManager.wrapStream(blockId, outputStream)
    wrappedOutputStream.write(plainStr.getBytes(StandardCharsets.UTF_8))
    wrappedOutputStream.close()

    val encryptedBytes = outputStream.toByteArray
    val encryptedStr = new String(encryptedBytes)
    assert(plainStr !== encryptedStr)

    val inputStream = new ByteArrayInputStream(encryptedBytes)
    val wrappedInputStream = serializerManager.wrapStream(blockId, inputStream)
    val decryptedBytes = new Array[Byte](1024)
    val len = wrappedInputStream.read(decryptedBytes)
    val decryptedStr = new String(decryptedBytes, 0, len, StandardCharsets.UTF_8)
    assert(decryptedStr === plainStr)
  }
}
