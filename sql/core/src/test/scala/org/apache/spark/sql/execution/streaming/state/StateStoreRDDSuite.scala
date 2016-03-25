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

import java.io.File
import java.nio.file.Files

import scala.util.Random

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.LocalSparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.Utils

class StateStoreRDDSuite extends SparkFunSuite with BeforeAndAfter with BeforeAndAfterAll {

  private val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
  private var tempDir = Files.createTempDirectory("StateStoreRDDSuite").toString
  private val keySchema = StructType(Seq(StructField("key", StringType, true)))
  private val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  import StateStoreSuite._

  after {
    StateStore.stop()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    Utils.deleteRecursively(new File(tempDir))
  }

  test("versioning and immutability") {
    quietly {
      withSpark(new SparkContext(sparkConf)) { sc =>
        implicit val sqlContet = new SQLContext(sc)
        val path = Utils.createDirectory(tempDir, Random.nextString(10)).toString
        val increment = (store: StateStore, iter: Iterator[String]) => {
          iter.foreach { s =>
            store.update(
              stringToRow(s), oldRow => {
                val oldValue = oldRow.map(rowToInt).getOrElse(0)
                intToRow(oldValue + 1)
              })
          }
          store.commit()
          store.iterator().map(rowsToStringInt)
        }
        val opId = 0
        val rdd1 = makeRDD(sc, Seq("a", "b", "a")).mapPartitionWithStateStore(
          increment, path, opId, storeVersion = 0, keySchema, valueSchema)
        assert(rdd1.collect().toSet === Set("a" -> 2, "b" -> 1))

        // Generate next version of stores
        val rdd2 = makeRDD(sc, Seq("a", "c")).mapPartitionWithStateStore(
          increment, path, opId, storeVersion = 1, keySchema, valueSchema)
        assert(rdd2.collect().toSet === Set("a" -> 3, "b" -> 1, "c" -> 1))

        // Make sure the previous RDD still has the same data.
        assert(rdd1.collect().toSet === Set("a" -> 2, "b" -> 1))
      }
    }
  }

  test("recovering from files") {
    quietly {
      val opId = 0
      val path = Utils.createDirectory(tempDir, Random.nextString(10)).toString

      def makeStoreRDD(
          sc: SparkContext,
          seq: Seq[String],
          storeVersion: Int): RDD[(String, Int)] = {
        implicit val sqlContext = new SQLContext(sc)
        makeRDD(sc, Seq("a")).mapPartitionWithStateStore(
          increment, path, opId, storeVersion, keySchema, valueSchema)
      }

      // Generate RDDs and state store data
      withSpark(new SparkContext(sparkConf)) { sc =>
        for (i <- 1 to 20) {
          require(makeStoreRDD(sc, Seq("a"), i - 1).collect().toSet === Set("a" -> i))
        }
      }

      // With a new context, try using the earlier state store data
      withSpark(new SparkContext(sparkConf)) { sc =>
        assert(makeStoreRDD(sc, Seq("a"), 20).collect().toSet === Set("a" -> 21))
      }
    }
  }

  test("preferred locations using StateStoreCoordinator") {
    quietly {
      val opId = 0
      val path = Utils.createDirectory(tempDir, Random.nextString(10)).toString

      withSpark(new SparkContext(sparkConf)) { sc =>
        implicit val sqlContext = new SQLContext(sc)
        val coordinatorRef = sqlContext.streams.stateStoreCoordinator
        coordinatorRef.reportActiveInstance(StateStoreId(path, opId, 0), "host1", "exec1")
        coordinatorRef.reportActiveInstance(StateStoreId(path, opId, 1), "host2", "exec2")
        assert(
          coordinatorRef.getLocation(StateStoreId(path, opId, 0)) ===
            Some(ExecutorCacheTaskLocation("host1", "exec1").toString))

        val rdd = makeRDD(sc, Seq("a", "b", "a")).mapPartitionWithStateStore(
          increment, path, opId, storeVersion = 0, keySchema, valueSchema)
        require(rdd.partitions.length === 2)

        assert(
          rdd.preferredLocations(rdd.partitions(0)) ===
            Seq(ExecutorCacheTaskLocation("host1", "exec1").toString))

        assert(
          rdd.preferredLocations(rdd.partitions(1)) ===
            Seq(ExecutorCacheTaskLocation("host2", "exec2").toString))

        rdd.collect()
      }
    }
  }

  test("distributed test") {
    quietly {
      withSpark(new SparkContext(sparkConf.setMaster("local-cluster[2, 1, 1024]"))) { sc =>
        implicit val sqlContet = new SQLContext(sc)
        val path = Utils.createDirectory(tempDir, Random.nextString(10)).toString
        val increment = (store: StateStore, iter: Iterator[String]) => {
          iter.foreach { s =>
            store.update(
              stringToRow(s), oldRow => {
                val oldValue = oldRow.map(rowToInt).getOrElse(0)
                intToRow(oldValue + 1)
              })
          }
          store.commit()
          store.iterator().map(rowsToStringInt)
        }
        val opId = 0
        val rdd1 = makeRDD(sc, Seq("a", "b", "a")).mapPartitionWithStateStore(
          increment, path, opId, storeVersion = 0, keySchema, valueSchema)
        assert(rdd1.collect().toSet === Set("a" -> 2, "b" -> 1))

        // Generate next version of stores
        val rdd2 = makeRDD(sc, Seq("a", "c")).mapPartitionWithStateStore(
          increment, path, opId, storeVersion = 1, keySchema, valueSchema)
        assert(rdd2.collect().toSet === Set("a" -> 3, "b" -> 1, "c" -> 1))

        // Make sure the previous RDD still has the same data.
        assert(rdd1.collect().toSet === Set("a" -> 2, "b" -> 1))
      }
    }
  }

  private def makeRDD(sc: SparkContext, seq: Seq[String]): RDD[String] = {
    sc.makeRDD(seq, 2).groupBy(x => x).flatMap(_._2)
  }

  private val increment = (store: StateStore, iter: Iterator[String]) => {
    iter.foreach { s =>
      store.update(
        stringToRow(s), oldRow => {
          val oldValue = oldRow.map(rowToInt).getOrElse(0)
          intToRow(oldValue + 1)
        })
    }
    store.commit()
    store.iterator().map(rowsToStringInt)
  }
}
