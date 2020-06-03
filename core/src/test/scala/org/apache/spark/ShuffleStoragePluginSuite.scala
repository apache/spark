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

import scala.compat.java8.OptionConverters._

import org.scalatest.concurrent.Eventually

import org.apache.spark.shuffle.io.plugin.{MockAsyncBackupShuffleDataIO, MockAsyncBackupShuffleOutputTracker}

class ShuffleStoragePluginSuite extends SortShuffleSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.set(
      org.apache.spark.internal.config.SHUFFLE_IO_PLUGIN_CLASS,
      classOf[MockAsyncBackupShuffleDataIO].getName())
  }

  test("Running shuffle should register metadata with the custom output tracker.") {
    sc = new SparkContext("local", "test", conf)
    val pairs = sc.parallelize(Seq((1, 1), (1, 2), (1, 3), (2, 1)), 4)
    val groups = pairs.groupByKey(4)
    assert(groups.collect.size === 2)
    assert(sc.shuffleDriverComponents.shuffleOutputTracker().asScala.isDefined)
    val outputTracker = sc
      .shuffleDriverComponents
      .shuffleOutputTracker()
      .get
      .asInstanceOf[MockAsyncBackupShuffleOutputTracker]
    val backupManager = outputTracker.backupManager
    val deps = groups.dependencies
    assert(deps.nonEmpty)
    assert(deps.forall(_.isInstanceOf[ShuffleDependency[_, _, _]]))
    val shuffleId = deps.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    val backupIds = outputTracker.getBackupIds(shuffleId)
    backupIds.foreach { backupId =>
      Eventually.eventually {
        assert(backupManager.getBlock(shuffleId, backupId) !== null)
      }
    }
  }
}
