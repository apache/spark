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

package org.apache.spark.mllib.util


import java.io.{InputStreamReader, BufferedReader, DataOutputStream, FileOutputStream}
import java.nio.file.Files
import java.nio.file.{Path => JPath}
import java.nio.file.{Paths => JPaths}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text

import org.apache.spark.mllib.util.MLUtils._
import org.apache.spark.SparkContext

/**
 * Tests HDFS IO and local disk IO of [[smallTextFiles]] in MLutils. HDFS tests create a mock DFS in
 * memory, while local disk test create a temp directory. All these temporal storages are deleted
 * in the end.
 */

class SmallTextFilesSuite extends FunSuite with BeforeAndAfterAll {
  private var sc: SparkContext = _
  private var dfs: MiniDFSCluster = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
    sc.hadoopConfiguration.set("dfs.datanode.data.dir.perm", SmallTextFilesSuite.dirPermission())
    dfs = new MiniDFSCluster(sc.hadoopConfiguration, 4, true,
                             Array("/rack0", "/rack0", "/rack1", "/rack1"),
                             Array("host0", "host1", "host2", "host3"))
  }

  override def afterAll() {
    if (dfs != null) dfs.shutdown()
    sc.stop()
    System.clearProperty("spark.driver.port")
  }


  private def createHDFSFile(
      fs: FileSystem,
      inputDir: Path,
      fileName: String,
      contents: Array[Byte]) = {
    val out: DataOutputStream = fs.create(new Path(inputDir, fileName), true, 4096, 2, 512, null)
    out.write(contents, 0, contents.length)
    out.close()
    System.out.println("Wrote HDFS file")
  }

  /**
   * This code will test the behaviors on HDFS. There are three aspects to test:
   *    1) is all files are read.
   *    2) is the fileNames are read correctly.
   *    3) is the contents must be the same.
   */
  test("Small file input || HDFS IO") {
    val fs: FileSystem = dfs.getFileSystem
    val dir = "/foo/"
    val inputDir: Path = new Path(dir)

    SmallTextFilesSuite.fileNames.zip(SmallTextFilesSuite.filesContents).foreach {
      case (fname, contents) =>
        createHDFSFile(fs, inputDir, fname, contents)
    }

    println(s"name node is ${dfs.getNameNode.getNameNodeAddress.getHostName}")
    println(s"name node port is ${dfs.getNameNodePort}")

    val hdfsAddressDir =
      s"hdfs://${dfs.getNameNode.getNameNodeAddress.getHostName}:${dfs.getNameNodePort}$dir"
    println(s"HDFS address dir is $hdfsAddressDir")

    val res = smallTextFiles(sc, hdfsAddressDir).collect()

    assert(res.size === SmallTextFilesSuite.fileNames.size,
      "Number of files read out do not fit with the actual value")

    for ((fname, contents) <- res) {
      assert(SmallTextFilesSuite.fileNames.contains(fname),
        s"Missing file name $fname.")
      assert(contents.hashCode === SmallTextFilesSuite.hashCodeOfContents(fname),
        s"file $fname contents can not match")
    }
  }

  private def createNativeFile(inputDir: JPath, fileName: String, contents: Array[Byte]) = {
    val out = new DataOutputStream(new FileOutputStream(s"${inputDir.toString}/$fileName"))
    out.write(contents, 0, contents.length)
    out.close()
    println("Wrote native file")
  }

  /**
   * This code will test the behaviors on native file system. There are three aspects:
   *    1) is all files are read.
   *    2) is the fileNames are read correctly.
   *    3) is the contents must be the same.
   */
  test("Small file input || native disk IO") {

    sc.hadoopConfiguration.clear()

    val dir = Files.createTempDirectory("smallfiles")
    println(s"native disk address is ${dir.toString}")

    SmallTextFilesSuite.fileNames.zip(SmallTextFilesSuite.filesContents).foreach {
      case (fname, contents) =>
        createNativeFile(dir, fname, contents)
    }

    val res = smallTextFiles(sc, dir.toString).collect()

    assert(res.size === SmallTextFilesSuite.fileNames.size,
      "Number of files read out do not fit with the actual value")

    for ((fname, contents) <- res) {
      assert(SmallTextFilesSuite.fileNames.contains(fname),
        s"Missing file name $fname.")
      assert(contents.hashCode === SmallTextFilesSuite.hashCodeOfContents(fname),
        s"file $fname contents can not match")
    }

    SmallTextFilesSuite.fileNames.foreach { fname =>
      Files.deleteIfExists(JPaths.get(s"${dir.toString}/$fname"))
    }
    Files.deleteIfExists(dir)
  }
}

/**
 * Some final values are defined here. chineseWordsSpark is refer to the Chinese character version
 * of "Spark", we use UTF-8 to encode the bytes together, with a '\n' in the end. fileNames and
 * fileContents represent the test data that will be used later. hashCodeOfContents is a Map of
 * fileName to the hashcode of contents, which is used for the comparison of contents, i.e. the
 * "read in" contents should be same with the "read out" ones.
 */

object SmallTextFilesSuite {
  private val chineseWordsSpark = Array(
    0xe7.toByte, 0x81.toByte, 0xab.toByte,
    0xe8.toByte, 0x8a.toByte, 0xb1.toByte,
    '\n'.toByte)

  private val fileNames = Array("part-00000", "part-00001", "part-00002")

  private val filesContents = Array(7, 70, 700).map { upperBound =>
    Stream.continually(chineseWordsSpark.toList.toStream).flatten.take(upperBound).toArray
  }

  private val hashCodeOfContents = fileNames.zip(filesContents).map { case (fname, contents) =>
    fname -> new Text(contents).toString.hashCode
  }.toMap

  /**
   * Functions getUmask and getDirPermission are used for get the default directory permission on
   * current system, so as to give the MiniDFSCluster proper permissions. Or the test will be abort
   * due to the improper permission.
   */
  private def umask(): Int = {

    val COMMAND = "/bin/bash -c umask -S"
    val umask = try {
      val process = Runtime.getRuntime.exec(COMMAND)
      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
      reader.readLine()
    } catch {
      case _ : Exception => "0022"
    }

    assert(umask.size == 4, "failed because of improper umask value")

    println(s"current umask on your system is $umask")

    umask.zipWithIndex.map {
      case (c, 3) => c - '0'
      case (c, 2) => (c - '0') * 8
      case (c, 1) => (c - '0') * 64
      case (c, 0) => (c - '0') * 512
    }.sum
  }

  private def dirPermission(): String = {
    // 511 represents for 0777
    val permission = 511 ^ umask()

    def getOCT(num: Int): List[Int] = {
      if (num > 8) num % 8 :: getOCT(num / 8)
      else num :: Nil
    }

    val res = getOCT(permission).reverse.mkString
    println(s"default directory premission is $res")
    res
  }
}
