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

import SparkContext._
import com.google.common.io.Files
import org.scalatest.FunSuite

class FileServerSuite extends FunSuite with LocalSparkContext {

  @transient var tmpFile: File = _
  @transient var testJarFile: String = _


  override def beforeAll() {
    super.beforeAll()
    val buffer = new Array[Byte](10240)
    val tmpdir = new File(Files.createTempDir(), "test")
    tmpdir.mkdir()
    val tmpJarEntry = new File(tmpdir, "FileServerSuite2.txt")
    val pw = new PrintWriter(tmpJarEntry)
    pw.println("test String in the file named FileServerSuite2.txt")
    pw.close()
    // The ugliest code possible, was translated from java.
    val tmpFile2 = new File(tmpdir, "test.jar")
    val stream = new FileOutputStream(tmpFile2)
    val jar = new JarOutputStream(stream, new java.util.jar.Manifest())
    val jarAdd = new JarEntry(tmpJarEntry.getName)
    jarAdd.setTime(tmpJarEntry.lastModified)
    jar.putNextEntry(jarAdd)
    val in = new FileInputStream(tmpJarEntry)
    var nRead = 0
      while (nRead <= 0) {
      nRead = in.read(buffer, 0, buffer.length)
      jar.write(buffer, 0, nRead)
    }
    in.close()
    jar.close()
    stream.close()
    testJarFile = tmpFile2.toURI.toURL.toString
  }

  override def beforeEach() {
    super.beforeEach()
    // Create a sample text file
    val tmpdir = new File(Files.createTempDir(), "test")
    tmpdir.mkdir()
    tmpFile = new File(tmpdir, "FileServerSuite.txt")
    val pw = new PrintWriter(tmpFile)
    pw.println("100")
    pw.close()
  }

  override def afterEach() {
    super.afterEach()
    // Clean up downloaded file
    if (tmpFile.exists) {
      tmpFile.delete()
    }
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
    sc.addJar(testJarFile)
    val testData = Array((1, 1))
    sc.parallelize(testData).foreach { (x) =>
      if (Thread.currentThread.getContextClassLoader.getResource("FileServerSuite2.txt") == null) {
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
    sc.addJar(testJarFile)
    val testData = Array((1,1))
    sc.parallelize(testData).foreach { (x) =>
      if (Thread.currentThread.getContextClassLoader.getResource("FileServerSuite2.txt") == null) {
        throw new SparkException("jar not added")
      }
    }
  }

  test ("Dynamically adding JARS on a standalone cluster using local: URL") {
    sc = new SparkContext("local-cluster[1,1,512]", "test")
    sc.addJar(testJarFile.replace("file", "local"))
    val testData = Array((1,1))
    sc.parallelize(testData).foreach { (x) =>
      if (Thread.currentThread.getContextClassLoader.getResource("FileServerSuite2.txt") == null) {
        throw new SparkException("jar not added")
      }
    }
  }

}
