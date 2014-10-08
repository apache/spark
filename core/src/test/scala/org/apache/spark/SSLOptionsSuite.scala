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

package org.apache.spark

import java.io.{FileInputStream, FileOutputStream, PrintWriter, File}
import java.util.jar.{JarEntry, JarOutputStream}

import com.google.common.io.Files
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.commons.lang3.RandomUtils
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

class SSLOptionsSuite extends FunSuite with BeforeAndAfterAll {

  @transient var tmpDir: File = _

  override def beforeAll() {
    super.beforeAll()

    tmpDir = Files.createTempDir()
    tmpDir.deleteOnExit()
  }

  override def afterAll() {
    super.afterAll()
    Utils.deleteRecursively(tmpDir)
  }

  test("test loading existing property file") {
    val file = File.createTempFile("SSLOptionsSuite", "conf", tmpDir)
    FileUtils.write(file,
      """
        |ssl.some.property      someValue
      """.stripMargin)

    val props = SSLOptions.load(file)
    assert(props.get("ssl.some.property") === "someValue")
    assert(props.get("sslConfigurationFileLocation") === file.getAbsolutePath)
  }

  test("test loading not existing property file") {
    val file = File.createTempFile("SSLOptionsSuite", "conf", tmpDir)
    FileUtils.write(file,
      """
        |ssl.some.property      someValue
      """.stripMargin)

    val props = SSLOptions.load(new File(file.getParentFile, Random.nextString(10)))
    assert(props.get("ssl.some.property") === null)
    assert(props.get("sslConfigurationFileLocation") === null)
  }

  test("test loading existing property file by sprecifying it in system properties") {
    val file = File.createTempFile("SSLOptionsSuite", "conf", tmpDir)
    FileUtils.write(file,
      """
        |ssl.some.property      someValue
      """.stripMargin)

    System.setProperty("spark.ssl.configFile", file.getAbsolutePath)
    val props = SSLOptions.load(file)
    assert(props.get("ssl.some.property") === "someValue")
    assert(props.get("sslConfigurationFileLocation") === file.getAbsolutePath)
  }

  test("test resolving property file as spark conf ") {
    val file = File.createTempFile("SSLOptionsSuite", "conf", tmpDir)
    FileUtils.write(file,
    """
      |ssl.enabled               true
      |ssl.keyStore              keystore
      |ssl.keyStorePassword      password
      |ssl.keyPassword           password
      |ssl.trustStore            truststore
      |ssl.trustStorePassword    password
      |ssl.enabledAlgorithms     [TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA]
      |ssl.protocol              SSLv3
    """.stripMargin)

    System.setProperty("spark.ssl.configFile", file.getAbsolutePath)
    val opts = SSLOptions.parse(new SparkConf(), "ssl")

    assert(opts.enabled === true)
    assert(opts.trustStore.isDefined === true)
    assert(opts.trustStore.get.getName === "truststore")
    assert(opts.trustStore.get.getParent === file.getParent)
    assert(opts.keyStore.isDefined === true)
    assert(opts.keyStore.get.getName === "keystore")
    assert(opts.keyStore.get.getParent === file.getParent)
    assert(opts.trustStorePassword === Some("password"))
    assert(opts.keyStorePassword === Some("password"))
    assert(opts.keyPassword === Some("password"))
    assert(opts.protocol === Some("SSLv3"))
    assert(opts.enabledAlgorithms === Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"))
  }

}
