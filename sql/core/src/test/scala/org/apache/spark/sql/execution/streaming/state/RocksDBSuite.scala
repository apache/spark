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

package org.apache.spark.sql.execution.streaming.state

import java.io._
import java.nio.charset.Charset

import scala.language.implicitConversions

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark._
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.streaming.CreateAtomicTestManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

class RocksDBSuite extends SparkFunSuite {

  test("RocksDBFileManager: upload only new immutable files") {
    withTempDir { dir =>
      val dfsRootDir = dir.getAbsolutePath
      val verificationDir = Utils.createTempDir().getAbsolutePath // local dir to load checkpoints
      val fileManager = new RocksDBFileManager(
        dfsRootDir, Utils.createTempDir(), new Configuration)
      val sstDir = s"$dfsRootDir/SSTs"
      def numRemoteSSTFiles: Int = listFiles(sstDir).length
      val logDir = s"$dfsRootDir/logs"
      def numRemoteLogFiles: Int = listFiles(logDir).length

      // Verify behavior before any saved checkpoints
      assert(fileManager.getLatestVersion() === 0)

      // Try to load incorrect versions
      intercept[FileNotFoundException] {
        fileManager.loadCheckpointFromDfs(1, Utils.createTempDir())
      }

      // Save a version of checkpoint files
      val cpFiles1 = Seq(
        "sst-file1.sst" -> 10,
        "sst-file2.sst" -> 20,
        "other-file1" -> 100,
        "other-file2" -> 200,
        "archive/00001.log" -> 1000,
        "archive/00002.log" -> 2000
      )
      saveCheckpointFiles(fileManager, cpFiles1, version = 1, numKeys = 101)
      assert(fileManager.getLatestVersion() === 1)
      assert(numRemoteSSTFiles == 2) // 2 sst files copied
      assert(numRemoteLogFiles == 2) // 2 log files copied

      // Load back the checkpoint files into another local dir with existing files and verify
      generateFiles(verificationDir, Seq(
        "sst-file1.sst" -> 11, // files with same name but different sizes, should get overwritten
        "other-file1" -> 101,
        "archive/00001.log" -> 1001,
        "random-sst-file.sst" -> 100, // unnecessary files, should get deleted
        "random-other-file" -> 9,
        "00005.log" -> 101,
        "archive/00007.log" -> 101
      ))
      loadAndVerifyCheckpointFiles(fileManager, verificationDir, version = 1, cpFiles1, 101)

      // Save SAME version again with different checkpoint files and load back again to verify
      // whether files were overwritten.
      val cpFiles1_ = Seq(
        "sst-file1.sst" -> 10, // same SST file as before, should not get copied
        "sst-file2.sst" -> 25, // new SST file with same name as before, but different length
        "sst-file3.sst" -> 30, // new SST file
        "other-file1" -> 100, // same non-SST file as before, should not get copied
        "other-file2" -> 210, // new non-SST file with same name as before, but different length
        "other-file3" -> 300, // new non-SST file
        "archive/00001.log" -> 1000, // same log file as before, should not get copied
        "archive/00002.log" -> 2500, // new log file with same name as before, but different length
        "archive/00003.log" -> 3000 // new log file
      )
      saveCheckpointFiles(fileManager, cpFiles1_, version = 1, numKeys = 1001)
      assert(numRemoteSSTFiles === 4, "shouldn't copy same files again") // 2 old + 2 new SST files
      assert(numRemoteLogFiles === 4, "shouldn't copy same files again") // 2 old + 2 new log files
      loadAndVerifyCheckpointFiles(fileManager, verificationDir, version = 1, cpFiles1_, 1001)

      // Save another version and verify
      val cpFiles2 = Seq(
        "sst-file4.sst" -> 40,
        "other-file4" -> 400,
        "archive/00004.log" -> 4000
      )
      saveCheckpointFiles(fileManager, cpFiles2, version = 2, numKeys = 1501)
      assert(numRemoteSSTFiles === 5) // 1 new file over earlier 4 files
      assert(numRemoteLogFiles === 5) // 1 new file over earlier 4 files
      loadAndVerifyCheckpointFiles(fileManager, verificationDir, version = 2, cpFiles2, 1501)

      // Loading an older version should work
      loadAndVerifyCheckpointFiles(fileManager, verificationDir, version = 1, cpFiles1_, 1001)

      // Loading incorrect version should fail
      intercept[FileNotFoundException] {
        loadAndVerifyCheckpointFiles(fileManager, verificationDir, version = 3, Nil, 1001)
      }

      // Loading 0 should delete all files
      require(verificationDir.list().length > 0)
      loadAndVerifyCheckpointFiles(fileManager, verificationDir, version = 0, Nil, 0)
    }
  }

  test("RocksDBFileManager: error writing [version].zip cancels the output stream") {
    quietly {
      val hadoopConf = new Configuration()
      hadoopConf.set(
        SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key,
        classOf[CreateAtomicTestManager].getName)
      val dfsRootDir = Utils.createTempDir().getAbsolutePath
      val fileManager = new RocksDBFileManager(dfsRootDir, Utils.createTempDir(), hadoopConf)
      val cpFiles = Seq("sst-file1.sst" -> 10, "sst-file2.sst" -> 20, "other-file1" -> 100)
      CreateAtomicTestManager.shouldFailInCreateAtomic = true
      intercept[IOException] {
        saveCheckpointFiles(fileManager, cpFiles, version = 1, numKeys = 101)
      }
      assert(CreateAtomicTestManager.cancelCalledInCreateAtomic)
    }
  }

  test("checkpoint metadata serde roundtrip") {
    def checkJsonRoundtrip(metadata: RocksDBCheckpointMetadata, json: String): Unit = {
      assert(metadata.json == json)
      withTempDir { dir =>
        val file = new File(dir, "json")
        FileUtils.write(file, s"v1\n$json", Charset.defaultCharset)
        assert(metadata == RocksDBCheckpointMetadata.readFromFile(file))
      }
    }
    val sstFiles = Seq(RocksDBSstFile("00001.sst", "00001-uuid.sst", 12345678901234L))
    val logFiles = Seq(RocksDBLogFile("00001.log", "00001-uuid.log", 12345678901234L))

    // scalastyle:off line.size.limit
    // should always include sstFiles and numKeys
    checkJsonRoundtrip(
      RocksDBCheckpointMetadata(Seq.empty, 0L),
      """{"sstFiles":[],"numKeys":0}"""
    )
    // shouldn't include the "logFiles" field in json when it's empty
    checkJsonRoundtrip(
      RocksDBCheckpointMetadata(sstFiles, 12345678901234L),
      """{"sstFiles":[{"localFileName":"00001.sst","dfsSstFileName":"00001-uuid.sst","sizeBytes":12345678901234}],"numKeys":12345678901234}"""
    )
    checkJsonRoundtrip(
      RocksDBCheckpointMetadata(sstFiles, logFiles, 12345678901234L),
      """{"sstFiles":[{"localFileName":"00001.sst","dfsSstFileName":"00001-uuid.sst","sizeBytes":12345678901234}],"logFiles":[{"localFileName":"00001.log","dfsLogFileName":"00001-uuid.log","sizeBytes":12345678901234}],"numKeys":12345678901234}""")
    // scalastyle:on line.size.limit
  }

  def generateFiles(dir: String, fileToLengths: Seq[(String, Int)]): Unit = {
    fileToLengths.foreach { case (fileName, length) =>
      val file = new File(dir, fileName)
      FileUtils.write(file, "a" * length)
    }
  }

  def saveCheckpointFiles(
      fileManager: RocksDBFileManager,
      fileToLengths: Seq[(String, Int)],
      version: Int,
      numKeys: Int): Unit = {
    val checkpointDir = Utils.createTempDir().getAbsolutePath // local dir to create checkpoints
    generateFiles(checkpointDir, fileToLengths)
    fileManager.saveCheckpointToDfs(checkpointDir, version, numKeys)
  }

  def loadAndVerifyCheckpointFiles(
      fileManager: RocksDBFileManager,
      verificationDir: String,
      version: Int,
      expectedFiles: Seq[(String, Int)],
      expectedNumKeys: Int): Unit = {
    val metadata = fileManager.loadCheckpointFromDfs(version, verificationDir)
    val filesAndLengths =
      listFiles(verificationDir).map(f => f.getName -> f.length).toSet ++
      listFiles(verificationDir + "/archive").map(f => s"archive/${f.getName}" -> f.length()).toSet
    assert(filesAndLengths === expectedFiles.toSet)
    assert(metadata.numKeys === expectedNumKeys)
  }

  implicit def toFile(path: String): File = new File(path)

  def listFiles(file: File): Seq[File] = {
    if (!file.exists()) return Seq.empty
    file.listFiles.filter(file => !file.getName.endsWith("crc") && !file.isDirectory)
  }

  def listFiles(file: String): Seq[File] = listFiles(new File(file))
}
