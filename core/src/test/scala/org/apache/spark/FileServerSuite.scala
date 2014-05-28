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

import java.io._
import java.util.jar.{JarEntry, JarOutputStream}

import com.google.common.io.Files
import org.scalatest.FunSuite

import org.apache.spark.SparkContext._
import org.apache.spark.util.Utils

class FileServerSuite extends FunSuite with LocalSparkContext {

  @transient var tmpDir: File = _
  @transient var tmpFile: File = _
  @transient var tmpJarUrl: String = _

  override def beforeEach() {
    super.beforeEach()
    resetSparkContext()
    System.setProperty("spark.authenticate", "false")
  }

  override def beforeAll() {
    super.beforeAll()

    tmpDir = Files.createTempDir()
    tmpDir.deleteOnExit()
    val testTempDir = new File(tmpDir, "test")
    testTempDir.mkdir()

    val textFile = new File(testTempDir, "FileServerSuite.txt")
    val pw = new PrintWriter(textFile)
    pw.println("100")
    pw.close()

    val jarFile = new File(testTempDir, "test.jar")
    val jarStream = new FileOutputStream(jarFile)
    val jar = new JarOutputStream(jarStream, new java.util.jar.Manifest())
    System.setProperty("spark.authenticate", "false")

    val jarEntry = new JarEntry(textFile.getName)
    jar.putNextEntry(jarEntry)

    val in = new FileInputStream(textFile)
    val buffer = new Array[Byte](10240)
    var nRead = 0
    while (nRead <= 0) {
      nRead = in.read(buffer, 0, buffer.length)
      jar.write(buffer, 0, nRead)
    }

    in.close()
    jar.close()
    jarStream.close()

    tmpFile = textFile
    tmpJarUrl = jarFile.toURI.toURL.toString
  }

  override def afterAll() {
    super.afterAll()
    Utils.deleteRecursively(tmpDir)
  }

  test("Distributing files locally") {
    sc = new SparkContext("local[4]", "test")
    sc.addFile(tmpFile.toString)
    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,2), (3,0))
    val result = sc.parallelize(testData).reduceByKey {
      val path = SparkFiles.get("FileServerSuite.txt")
      val in = new BufferedReader(new FileReader(path))
      val fileVal = in.readLine().toInt
      in.close()
      _ * fileVal + _ * fileVal
    }.collect()
    assert(result.toSet === Set((1,200), (2,300), (3,500)))
  }

  test("Distributing files locally security On") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("spark.authenticate", "true")
    sparkConf.set("spark.authenticate.secret", "good")
    sc = new SparkContext("local[4]", "test", sparkConf)

    sc.addFile(tmpFile.toString)
    assert(sc.env.securityManager.isAuthenticationEnabled() === true)
    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,2), (3,0))
    val result = sc.parallelize(testData).reduceByKey {
      val path = SparkFiles.get("FileServerSuite.txt")
      val in = new BufferedReader(new FileReader(path))
      val fileVal = in.readLine().toInt
      in.close()
      _ * fileVal + _ * fileVal
    }.collect()
    assert(result.toSet === Set((1,200), (2,300), (3,500)))
  }

  test("Distributing files locally using URL as input") {
    // addFile("file:///....")
    sc = new SparkContext("local[4]", "test")
    sc.addFile(new File(tmpFile.toString).toURI.toString)
    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,2), (3,0))
    val result = sc.parallelize(testData).reduceByKey {
      val path = SparkFiles.get("FileServerSuite.txt")
      val in = new BufferedReader(new FileReader(path))
      val fileVal = in.readLine().toInt
      in.close()
      _ * fileVal + _ * fileVal
    }.collect()
    assert(result.toSet === Set((1,200), (2,300), (3,500)))
  }

  test ("Dynamically adding JARS locally") {
    sc = new SparkContext("local[4]", "test")
    sc.addJar(tmpJarUrl)
    val testData = Array((1, 1))
    sc.parallelize(testData).foreach { x =>
      if (Thread.currentThread.getContextClassLoader.getResource("FileServerSuite.txt") == null) {
        throw new SparkException("jar not added")
      }
    }
  }

  test("Distributing files on a standalone cluster") {
    sc = new SparkContext("local-cluster[1,1,512]", "test")
    sc.addFile(tmpFile.toString)
    val testData = Array((1,1), (1,1), (2,1), (3,5), (2,2), (3,0))
    val result = sc.parallelize(testData).reduceByKey {
      val path = SparkFiles.get("FileServerSuite.txt")
      val in = new BufferedReader(new FileReader(path))
      val fileVal = in.readLine().toInt
      in.close()
      _ * fileVal + _ * fileVal
    }.collect()
    assert(result.toSet === Set((1,200), (2,300), (3,500)))
  }

  test ("Dynamically adding JARS on a standalone cluster") {
    sc = new SparkContext("local-cluster[1,1,512]", "test")
    sc.addJar(tmpJarUrl)
    val testData = Array((1,1))
    sc.parallelize(testData).foreach { x =>
      if (Thread.currentThread.getContextClassLoader.getResource("FileServerSuite.txt") == null) {
        throw new SparkException("jar not added")
      }
    }
  }

  test ("Dynamically adding JARS on a standalone cluster using local: URL") {
    sc = new SparkContext("local-cluster[1,1,512]", "test")
    sc.addJar(tmpJarUrl.replace("file", "local"))
    val testData = Array((1,1))
    sc.parallelize(testData).foreach { x =>
      if (Thread.currentThread.getContextClassLoader.getResource("FileServerSuite.txt") == null) {
        throw new SparkException("jar not added")
      }
    }
  }

}
