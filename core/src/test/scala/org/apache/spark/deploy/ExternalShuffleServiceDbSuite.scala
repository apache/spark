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

import com.google.common.io.CharStreams

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.network.shuffle.{ExternalBlockHandler, ExternalShuffleBlockResolver}
import org.apache.spark.network.shuffle.TestShuffleDataContext
import org.apache.spark.network.shuffledb.DBBackend
import org.apache.spark.tags.ExtendedLevelDBTest
import org.apache.spark.util.Utils

/**
 * This suite gets BlockData when the ExternalShuffleService is restarted
 * with #spark.shuffle.service.db.enabled = true or false
 * Note that failures in this suite may arise when#spark.shuffle.service.db.enabled = false
 */
abstract class ExternalShuffleServiceDbSuite extends SparkFunSuite {
  val sortBlock0 = "Hello!"
  val sortBlock1 = "World!"
  val SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager"

  var sparkConf: SparkConf = _
  var dataContext: TestShuffleDataContext = _

  var securityManager: SecurityManager = _
  var externalShuffleService: ExternalShuffleService = _
  var blockHandler: ExternalBlockHandler = _
  var blockResolver: ExternalShuffleBlockResolver = _

  protected def shuffleDBBackend(): DBBackend

  override def beforeAll(): Unit = {
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

  override def afterAll(): Unit = {
    try {
      dataContext.cleanup()
    } finally {
      super.afterAll()
    }
  }

  def shuffleServiceConf: SparkConf = sparkConf.clone().set(SHUFFLE_SERVICE_PORT, 0)

  def registerExecutor(): Unit = {
    try {
      sparkConf.set("spark.shuffle.service.db.enabled", "true")
      sparkConf.set(SHUFFLE_SERVICE_DB_BACKEND.key, shuffleDBBackend().name())
      externalShuffleService = new ExternalShuffleService(shuffleServiceConf, securityManager)

      // external Shuffle Service start
      externalShuffleService.start()
      blockHandler = externalShuffleService.getBlockHandler
      blockResolver = blockHandler.getBlockResolver
      blockResolver.registerExecutor("app0", "exec0", dataContext.createExecutorInfo(SORT_MANAGER))
    } finally {
      blockHandler.close()
      // external Shuffle Service stop
      externalShuffleService.stop()
    }
  }

  // The beforeAll ensures the shuffle data was already written, and then
  // the shuffle service was stopped. Here we restart the shuffle service
  // and make we can read the shuffle data
  test("Recover shuffle data with spark.shuffle.service.db.enabled=true after " +
    "shuffle service restart") {
    try {
      sparkConf.set("spark.shuffle.service.db.enabled", "true")
      sparkConf.set(SHUFFLE_SERVICE_DB_BACKEND.key, shuffleDBBackend().name())
      externalShuffleService = new ExternalShuffleService(shuffleServiceConf, securityManager)
      // externalShuffleService restart
      externalShuffleService.start()
      blockHandler = externalShuffleService.getBlockHandler
      blockResolver = blockHandler.getBlockResolver

      val block0Stream = blockResolver.getBlockData("app0", "exec0", 0, 0, 0).createInputStream
      val block0 = CharStreams.toString(new InputStreamReader(block0Stream, StandardCharsets.UTF_8))
      block0Stream.close()
      assert(sortBlock0 == block0)
      // pass
    } finally {
      blockHandler.close()
      // externalShuffleService stop
      externalShuffleService.stop()
    }

  }

  // The beforeAll ensures the shuffle data was already written, and then
  // the shuffle service was stopped. Here we restart the shuffle service ,
  // but we can't read the shuffle data
  test("Can't recover shuffle data with spark.shuffle.service.db.enabled=false after" +
    " shuffle service restart") {
    try {
      sparkConf.set("spark.shuffle.service.db.enabled", "false")
      externalShuffleService = new ExternalShuffleService(shuffleServiceConf, securityManager)
      // externalShuffleService restart
      externalShuffleService.start()
      blockHandler = externalShuffleService.getBlockHandler
      blockResolver = blockHandler.getBlockResolver

      val error = intercept[RuntimeException] {
        blockResolver.getBlockData("app0", "exec0", 0, 0, 0).createInputStream
      }.getMessage

      assert(error.contains("not registered"))
    } finally {
      blockHandler.close()
      // externalShuffleService stop
      externalShuffleService.stop()
    }
  }
}

@ExtendedLevelDBTest
class ExternalShuffleServiceLevelDBSuite extends ExternalShuffleServiceDbSuite {
  override protected def shuffleDBBackend(): DBBackend = DBBackend.LEVELDB
}

class ExternalShuffleServiceRocksDBSuite extends ExternalShuffleServiceDbSuite {
  override protected def shuffleDBBackend(): DBBackend = DBBackend.ROCKSDB
}
