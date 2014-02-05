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

import org.scalatest.FunSuite
import org.scalatest.concurrent.Timeouts._
import org.scalatest.time.{Millis, Span}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._
import org.scalatest.matchers.ShouldMatchers._

class BroadcastSuite extends FunSuite with LocalSparkContext {

  override def afterEach() {
    super.afterEach()
    System.clearProperty("spark.broadcast.factory")
  }

  test("Using HttpBroadcast locally") {
    System.setProperty("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
    sc = new SparkContext("local", "test")
    val list = List(1, 2, 3, 4)
    val listBroadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 2).map(x => (x, listBroadcast.value.sum))
    assert(results.collect.toSet === Set((1, 10), (2, 10)))
  }

  test("Accessing HttpBroadcast variables from multiple threads") {
    System.setProperty("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
    sc = new SparkContext("local[10]", "test")
    val list = List(1, 2, 3, 4)
    val listBroadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 10).map(x => (x, listBroadcast.value.sum))
    assert(results.collect.toSet === (1 to 10).map(x => (x, 10)).toSet)
  }

  test("Accessing HttpBroadcast variables in a local cluster") {
    System.setProperty("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
    val numSlaves = 4
    sc = new SparkContext("local-cluster[%d, 1, 512]".format(numSlaves), "test")
    val list = List(1, 2, 3, 4)
    val listBroadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to numSlaves).map(x => (x, listBroadcast.value.sum))
    assert(results.collect.toSet === (1 to numSlaves).map(x => (x, 10)).toSet)
  }

  test("Using TorrentBroadcast locally") {
    System.setProperty("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")
    sc = new SparkContext("local", "test")
    val list = List(1, 2, 3, 4)
    val listBroadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 2).map(x => (x, listBroadcast.value.sum))
    assert(results.collect.toSet === Set((1, 10), (2, 10)))
  }

  test("Accessing TorrentBroadcast variables from multiple threads") {
    System.setProperty("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")
    sc = new SparkContext("local[10]", "test")
    val list = List(1, 2, 3, 4)
    val listBroadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 10).map(x => (x, listBroadcast.value.sum))
    assert(results.collect.toSet === (1 to 10).map(x => (x, 10)).toSet)
  }

  test("Accessing TorrentBroadcast variables in a local cluster") {
    System.setProperty("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")
    val numSlaves = 4
    sc = new SparkContext("local-cluster[%d, 1, 512]".format(numSlaves), "test")
    val list = List(1, 2, 3, 4)
    val listBroadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to numSlaves).map(x => (x, listBroadcast.value.sum))
    assert(results.collect.toSet === (1 to numSlaves).map(x => (x, 10)).toSet)
  }

  def blocksExist(sc: SparkContext, numSlaves: Int) = {
    val rdd = sc.parallelize(1 to numSlaves, numSlaves)
    val workerBlocks = rdd.mapPartitions(_ => {
      val blocks = SparkEnv.get.blockManager.numberOfBlocksInMemory()
      Seq(blocks).iterator
    })
    val totalKnown = workerBlocks.reduce(_ + _) + sc.env.blockManager.numberOfBlocksInMemory()

    totalKnown > 0
  }

  def testUnpersist(bcFactory: String, removeSource: Boolean) {
    test("Broadcast unpersist(" + removeSource + ") with " + bcFactory) {
      val numSlaves = 2
      System.setProperty("spark.broadcast.factory", bcFactory)
      sc = new SparkContext("local-cluster[%d, 1, 512]".format(numSlaves), "test")
      val list = List(1, 2, 3, 4)

      assert(!blocksExist(sc, numSlaves))

      val listBroadcast = sc.broadcast(list, true)
      val results = sc.parallelize(1 to numSlaves).map(x => (x, listBroadcast.value.sum))
      assert(results.collect.toSet === (1 to numSlaves).map(x => (x, 10)).toSet)

      assert(blocksExist(sc, numSlaves))

      listBroadcast.unpersist(removeSource)

      eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
        blocksExist(sc, numSlaves) should be (false)
      }

      if (!removeSource) {
        val results = sc.parallelize(1 to numSlaves).map(x => (x, listBroadcast.value.sum))
        assert(results.collect.toSet === (1 to numSlaves).map(x => (x, 10)).toSet)
      }
    }
  }

  for (removeSource <- Seq(true, false)) {
    testUnpersist("org.apache.spark.broadcast.HttpBroadcastFactory", removeSource)
    testUnpersist("org.apache.spark.broadcast.TorrentBroadcastFactory", removeSource)
  }
}
