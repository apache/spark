/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.internal

import java.nio.charset.StandardCharsets
import java.util
import java.util.Random
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import com.google.common.primitives.Longs
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.remoteshuffle.common._
import org.apache.spark.shuffle._
import org.apache.spark.{MapOutputTrackerMaster, Partitioner, ShuffleDependency, SparkConf, SparkContext, SparkEnv}



class ShuffleWritePerfToolPartitioner(val partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val b = key.asInstanceOf[Array[Byte]]
    val prefix = Longs.fromBytes(0, b(0), b(1), b(2), b(3), b(4), b(5), b(6))
    (prefix % numPartitions).toInt
  }
}

/** *
 * This is a tool to write shuffle records for performance testing
 */
abstract class ShuffleWritePerfTool extends Logging {
  private val random = new Random

  // Successfully written records (by last mapper task attempt) in shuffle files
  private val successShuffleWrittenRecords = new AtomicLong

  // Threads for all map tasks
  private val allMapThreads = new util.ArrayList[Thread]
  private val mapThreadErrors = new AtomicLong

  protected val appId = "app_" + System.nanoTime
  protected val appAttempt = "exec1"
  protected val appShuffleId = new AppShuffleId(appId, appAttempt, 1)

  // Number of total map tasks
  var numMaps = 4
  // Number of records in each map tasks
  var numMapRecords = 300
  // Number of total partitions
  var numPartitions = 3

  // This tool generates a range of map tasks to simulate uploading data.
  // This field specifies the lower bound (inclusive) of the map id.
  private var startMapId = 0
  // This field specifies the upper bound (inclusive) of the map id.
  private var endMapId = numMaps - 1

  // Total number of test values to use. This tool wil generate a list of test values and use them
  // to fill shuffle data.
  private var numTestValues = 1000
  // Max length for test values to use. This tool wil generate a list of test values and use them
  private var testValueLen = 100

  protected var sparkConf: SparkConf = null
  protected var sparkContext: SparkContext = null
  protected var mapOutputTrackerMaster: MapOutputTrackerMaster = null
  protected var shuffleDependency: ShuffleDependency[Array[Byte], Array[Byte], Array[Byte]] = null

  private val taskAttemptIdSeed = new AtomicLong;

  def createShuffleWriter(
      shuffleId: Int,
      shuffleDependency: ShuffleDependency[Array[Byte], Array[Byte], Array[Byte]],
      appMapId: AppMapId,
      taskAttemptId: Long):
    ShuffleWriter[Array[Byte], Array[Byte]]

  def setup(): Unit = {
    this.endMapId = this.startMapId + numMaps - 1

    // Set up Spark environment
    sparkConf = new SparkConf().setAppName("testApp")
      .setMaster(s"local[2]")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.app.id", appId)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    sparkContext = new SparkContext(sparkConf)

    mapOutputTrackerMaster = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTrackerMaster.registerShuffle(appShuffleId.getShuffleId, numMaps)

    val rdd = sparkContext.parallelize(1 to numMaps, numMaps)
      .map(t => (t.toString.getBytes(StandardCharsets.UTF_8)
        -> t.toString.getBytes(StandardCharsets.UTF_8)))
      .partitionBy(new ShuffleWritePerfToolPartitioner(numPartitions))
    shuffleDependency = new ShuffleDependency[Array[Byte], Array[Byte], Array[Byte]](
      rdd, rdd.partitioner.get)
  }

  def cleanup(): Unit = {
    sparkContext.stop()
  }

  def run(): Unit = {
    // Generate test values to use
    val testValues = new util.ArrayList[Array[Byte]]
    while (testValues.size < numTestValues) {
      val ch = ('a' + random.nextInt(26)).toChar
      val str = StringUtils.repeat(ch, testValueLen)
      testValues.add(str.getBytes(StandardCharsets.UTF_8))
    }

    var mapId = startMapId
    while (mapId <= endMapId) {
      val appMapId = new AppMapId(appShuffleId.getAppId, appShuffleId.getAppAttempt,
        appShuffleId.getShuffleId, mapId)
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          val taskAttemptId = taskAttemptIdSeed.getAndIncrement
          simulateMapperTask(testValues, appMapId, taskAttemptId)
        }
      })
      thread.setName(String.format("[Map Thread %s]", appMapId))
      thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          logError(String.format("Mapper thread %s got exception", t.getName), e)
          e.printStackTrace()
          mapThreadErrors.incrementAndGet
        }
      })
      allMapThreads.add(thread)

      mapId += 1;
    }

    // Start map task threads
    allMapThreads.asScala.foreach((t: Thread) => t.start())
    // Wait for map tasks to finish
    allMapThreads.asScala.foreach((t: Thread) => t.join())

    if (mapThreadErrors.get > 0) {
      throw new RuntimeException("Number of errors in map threads: " + mapThreadErrors)
    }

    logInfo("Test run finished successfully")
  }

  private def simulateMapperTask(testValues: util.List[Array[Byte]], appMapId: AppMapId,
                                 taskAttemptId: Long): Unit = {
    val shuffleWriter = createShuffleWriter(appShuffleId.getShuffleId,
      shuffleDependency, appMapId, taskAttemptId)

    logInfo(s"Map $appMapId attempt $taskAttemptId started, writer: $shuffleWriter")

    val recordIterator = Iterator.tabulate(numMapRecords) { t =>
      val index1 = t % testValues.size()
      val index2 = (t + 1) % testValues.size()
      (testValues.get(index1), testValues.get(index2))
    }

    shuffleWriter.write(recordIterator)
    successShuffleWrittenRecords.addAndGet(numMapRecords)

    val mapStatus = shuffleWriter.stop(true)
    mapOutputTrackerMaster
      .registerMapOutput(appShuffleId.getShuffleId, appMapId.getMapId.intValue(), mapStatus.get)

    // TODO simulate broken map tasks without proper closing
    logInfo(s"Map $appMapId attempt $taskAttemptId finished")
  }
}
