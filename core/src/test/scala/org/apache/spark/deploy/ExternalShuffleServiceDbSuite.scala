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

package org.apache.spark.deploy

import java.io._
import java.nio.charset.StandardCharsets

import com.google.common.io.{CharStreams, Closeables, Files}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SecurityManager, ShuffleSuite, SparkConf, SparkException}
import org.apache.spark.network.shuffle.{ExternalShuffleBlockHandler, ExternalShuffleBlockResolver}
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.Utils


/**
 * This suite gets BlockData when the ExternalShuffleService is restarted
 * with #spark.shuffle.service.db.enabled = true or false
 * Note that failures in this suite may arise when#spark.shuffle.service.db.enabled = false
 */
class ExternalShuffleServiceDbSuite extends ShuffleSuite with BeforeAndAfterAll {
  val sortBlock0 = "Hello!"
  val sortBlock1 = "World!"
  val SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager"

  var sparkConf: SparkConf = _
  var dataContext: TestShuffleDataContext = _

  var securityManager: SecurityManager = _
  var externalShuffleService: ExternalShuffleService = _
  var bockHandler: ExternalShuffleBlockHandler = _
  var blockResolver: ExternalShuffleBlockResolver = _

  override def beforeAll() {
    super.beforeAll()
    sparkConf = new SparkConf()
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.local.dir", System.getProperty("java.io.tmpdir"))
    Utils.loadDefaultSparkProperties(sparkConf, null)
    securityManager = new SecurityManager(sparkConf)

    dataContext = new TestShuffleDataContext(2, 5)
    dataContext.create()
    // Write some sort data.
    dataContext.insertSortShuffleData(0, 0,
      Array[Array[Byte]](sortBlock0.getBytes(StandardCharsets.UTF_8),
        sortBlock1.getBytes(StandardCharsets.UTF_8)))
    registerExecutor()
  }

  override def afterAll() {
    try {
      dataContext.cleanup()
    } finally {
      super.afterAll()
    }
  }

  def registerExecutor(): Unit = {
    sparkConf.set("spark.shuffle.service.db.enabled", "true")
    externalShuffleService = new ExternalShuffleService(sparkConf, securityManager)

    // external Shuffle Service start
    externalShuffleService.start()
    bockHandler = externalShuffleService.getBlockHandler
    blockResolver = bockHandler.getBlockResolver
    blockResolver.registerExecutor("app0", "exec0", dataContext.createExecutorInfo(SORT_MANAGER))
    blockResolver.closeForTest()
    // external Shuffle Service stop
    externalShuffleService.stop()
  }

  // This test getBlockData will be passed when the external shuffle service is restarted.
  test("restart External Shuffle Service With InitRegisteredExecutorsDB") {
    sparkConf.set("spark.shuffle.service.db.enabled", "true")
    externalShuffleService = new ExternalShuffleService(sparkConf, securityManager)
    // externalShuffleService restart
    externalShuffleService.start()
    bockHandler = externalShuffleService.getBlockHandler
    blockResolver = bockHandler.getBlockResolver

    val block0Stream = blockResolver.getBlockData("app0", "exec0", 0, 0, 0).createInputStream
    val block0 = CharStreams.toString(new InputStreamReader(block0Stream, StandardCharsets.UTF_8))
    block0Stream.close()
    assert(sortBlock0 == block0)
    // pass
    blockResolver.closeForTest()
    // externalShuffleService stop
    externalShuffleService.stop()

  }

  // This test getBlockData will't be passed when the external shuffle service is restarted.
  test("restart External Shuffle Service Without InitRegisteredExecutorsDB") {
    sparkConf.set("spark.shuffle.service.db.enabled", "false")
    externalShuffleService = new ExternalShuffleService(sparkConf, securityManager)
    // externalShuffleService restart
    externalShuffleService.start()
    bockHandler = externalShuffleService.getBlockHandler
    blockResolver = bockHandler.getBlockResolver

    val error = intercept[RuntimeException] {
      val block0Stream = blockResolver.getBlockData("app0", "exec0", 0, 0, 0).createInputStream
      val block0 = CharStreams.toString(new InputStreamReader(block0Stream, StandardCharsets.UTF_8))
      block0Stream.close()
      assert(sortBlock0 == block0)
    }.getMessage

    assert(error.contains("not registered"))
    blockResolver.closeForTest()
    // externalShuffleService stop
    externalShuffleService.stop()
  }

  /**
   * Manages some sort-shuffle data, including the creation
   * and cleanup of directories that can be read by the
   *
   * Copy from org.apache.spark.network.shuffle.TestShuffleDataContext
   */
  class TestShuffleDataContext(val numLocalDirs: Int, val subDirsPerLocalDir: Int) {
    val localDirs: Array[String] = new Array[String](numLocalDirs)

    def create(): Unit = {
      for (i <- 0 to numLocalDirs - 1) {
        localDirs(i) = Files.createTempDir().getAbsolutePath()
        for (p <- 0 to subDirsPerLocalDir - 1) {
          new File(localDirs(i), "%02x".format(p)).mkdirs()
        }
      }
    }

    def cleanup(): Unit = {
      for (i <- 0 to numLocalDirs - 1) {
        try {
          JavaUtils.deleteRecursively(new File(localDirs(i)))
        }
        catch {
          case e: IOException =>
            logError("Unable to cleanup localDir = " + localDirs(i), e)
        }
      }
    }

    // Creates reducer blocks in a sort-based data format within our local dirs.
    def insertSortShuffleData(shuffleId: Int, mapId: Int, blocks: Array[Array[Byte]]): Unit = {
      val blockId = "shuffle_" + shuffleId + "_" + mapId + "_0"
      var dataStream: FileOutputStream = null
      var indexStream: DataOutputStream = null
      var suppressExceptionsDuringClose = true
      try {
        dataStream = new FileOutputStream(ExternalShuffleBlockResolver.getFileForTest(localDirs,
          subDirsPerLocalDir, blockId + ".data"))
        indexStream = new DataOutputStream(new FileOutputStream(ExternalShuffleBlockResolver
          .getFileForTest(localDirs, subDirsPerLocalDir, blockId + ".index")))
        var offset = 0
        indexStream.writeLong(offset)
        for (block <- blocks) {
          offset += block.length
          dataStream.write(block)
          indexStream.writeLong(offset)
        }
        suppressExceptionsDuringClose = false
      } finally {
        Closeables.close(dataStream, suppressExceptionsDuringClose)
        Closeables.close(indexStream, suppressExceptionsDuringClose)
      }
    }

    // Creates an ExecutorShuffleInfo object based on the given shuffle manager
    // which targets this context's directories.
    def createExecutorInfo(shuffleManager: String): ExecutorShuffleInfo = {
      new ExecutorShuffleInfo(localDirs, subDirsPerLocalDir, shuffleManager)
    }
  }
}
