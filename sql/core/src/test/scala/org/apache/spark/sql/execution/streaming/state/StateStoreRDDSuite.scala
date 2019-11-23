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
import java.util.UUID

import scala.util.Random

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.LocalSparkSession._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.{CompletionIterator, Utils}

class StateStoreRDDSuite extends SparkFunSuite with BeforeAndAfter with BeforeAndAfterAll {

  import StateStoreTestsHelper._

  private val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
  private val tempDir = Files.createTempDirectory("StateStoreRDDSuite").toString
  private val keySchema = StructType(Seq(StructField("key", StringType, true)))
  private val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  after {
    StateStore.stop()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      Utils.deleteRecursively(new File(tempDir))
    }
  }

  test("versioning and immutability") {
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val path = Utils.createDirectory(tempDir, Random.nextFloat.toString).toString
      val rdd1 = makeRDD(spark.sparkContext, Seq("a", "b", "a")).mapPartitionsWithStateStore(
            spark.sqlContext, operatorStateInfo(path, version = 0), keySchema, valueSchema, None)(
            increment)
      assert(rdd1.collect().toSet === Set("a" -> 2, "b" -> 1))

      // Generate next version of stores
      val rdd2 = makeRDD(spark.sparkContext, Seq("a", "c")).mapPartitionsWithStateStore(
        spark.sqlContext, operatorStateInfo(path, version = 1), keySchema, valueSchema, None)(
        increment)
      assert(rdd2.collect().toSet === Set("a" -> 3, "b" -> 1, "c" -> 1))

      // Make sure the previous RDD still has the same data.
      assert(rdd1.collect().toSet === Set("a" -> 2, "b" -> 1))
    }
  }

  test("recovering from files") {
    val path = Utils.createDirectory(tempDir, Random.nextFloat.toString).toString

    def makeStoreRDD(
        spark: SparkSession,
        seq: Seq[String],
        storeVersion: Int): RDD[(String, Int)] = {
      implicit val sqlContext = spark.sqlContext
      makeRDD(spark.sparkContext, Seq("a")).mapPartitionsWithStateStore(
        sqlContext, operatorStateInfo(path, version = storeVersion),
        keySchema, valueSchema, None)(increment)
    }

    // Generate RDDs and state store data
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      for (i <- 1 to 20) {
        require(makeStoreRDD(spark, Seq("a"), i - 1).collect().toSet === Set("a" -> i))
      }
    }

    // With a new context, try using the earlier state store data
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      assert(makeStoreRDD(spark, Seq("a"), 20).collect().toSet === Set("a" -> 21))
    }
  }

  test("usage with iterators - only gets and only puts") {
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      implicit val sqlContext = spark.sqlContext
      val path = Utils.createDirectory(tempDir, Random.nextFloat.toString).toString
      val opId = 0

      // Returns an iterator of the incremented value made into the store
      def iteratorOfPuts(store: StateStore, iter: Iterator[String]): Iterator[(String, Int)] = {
        val resIterator = iter.map { s =>
          val key = stringToRow(s)
          val oldValue = Option(store.get(key)).map(rowToInt).getOrElse(0)
          val newValue = oldValue + 1
          store.put(key, intToRow(newValue))
          (s, newValue)
        }
        CompletionIterator[(String, Int), Iterator[(String, Int)]](resIterator, {
          store.commit()
        })
      }

      def iteratorOfGets(
          store: StateStore,
          iter: Iterator[String]): Iterator[(String, Option[Int])] = {
        iter.map { s =>
          val key = stringToRow(s)
          val value = Option(store.get(key)).map(rowToInt)
          (s, value)
        }
      }

      val rddOfGets1 = makeRDD(spark.sparkContext, Seq("a", "b", "c")).mapPartitionsWithStateStore(
        spark.sqlContext, operatorStateInfo(path, version = 0), keySchema, valueSchema, None)(
        iteratorOfGets)
      assert(rddOfGets1.collect().toSet === Set("a" -> None, "b" -> None, "c" -> None))

      val rddOfPuts = makeRDD(spark.sparkContext, Seq("a", "b", "a")).mapPartitionsWithStateStore(
        sqlContext, operatorStateInfo(path, version = 0), keySchema, valueSchema, None)(
        iteratorOfPuts)
      assert(rddOfPuts.collect().toSet === Set("a" -> 1, "a" -> 2, "b" -> 1))

      val rddOfGets2 = makeRDD(spark.sparkContext, Seq("a", "b", "c")).mapPartitionsWithStateStore(
        sqlContext, operatorStateInfo(path, version = 1), keySchema, valueSchema, None)(
        iteratorOfGets)
      assert(rddOfGets2.collect().toSet === Set("a" -> Some(2), "b" -> Some(1), "c" -> None))
    }
  }

  test("preferred locations using StateStoreCoordinator") {
    quietly {
      val queryRunId = UUID.randomUUID
      val opId = 0
      val path = Utils.createDirectory(tempDir, Random.nextFloat.toString).toString

      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        implicit val sqlContext = spark.sqlContext
        val coordinatorRef = sqlContext.streams.stateStoreCoordinator
        val storeProviderId1 = StateStoreProviderId(StateStoreId(path, opId, 0), queryRunId)
        val storeProviderId2 = StateStoreProviderId(StateStoreId(path, opId, 1), queryRunId)
        coordinatorRef.reportActiveInstance(storeProviderId1, "host1", "exec1")
        coordinatorRef.reportActiveInstance(storeProviderId2, "host2", "exec2")

        require(
          coordinatorRef.getLocation(storeProviderId1) ===
            Some(ExecutorCacheTaskLocation("host1", "exec1").toString))

        val rdd = makeRDD(spark.sparkContext, Seq("a", "b", "a")).mapPartitionsWithStateStore(
          sqlContext, operatorStateInfo(path, queryRunId = queryRunId),
          keySchema, valueSchema, None)(increment)
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

      withSparkSession(
        SparkSession.builder
          .config(sparkConf.setMaster("local-cluster[2, 1, 1024]"))
          .getOrCreate()) { spark =>
        implicit val sqlContext = spark.sqlContext
        val path = Utils.createDirectory(tempDir, Random.nextFloat.toString).toString
        val opId = 0
        val rdd1 = makeRDD(spark.sparkContext, Seq("a", "b", "a")).mapPartitionsWithStateStore(
          sqlContext, operatorStateInfo(path, version = 0), keySchema, valueSchema, None)(increment)
        assert(rdd1.collect().toSet === Set("a" -> 2, "b" -> 1))

        // Generate next version of stores
        val rdd2 = makeRDD(spark.sparkContext, Seq("a", "c")).mapPartitionsWithStateStore(
          sqlContext, operatorStateInfo(path, version = 1), keySchema, valueSchema, None)(increment)
        assert(rdd2.collect().toSet === Set("a" -> 3, "b" -> 1, "c" -> 1))

        // Make sure the previous RDD still has the same data.
        assert(rdd1.collect().toSet === Set("a" -> 2, "b" -> 1))
      }
    }
  }

  private def makeRDD(sc: SparkContext, seq: Seq[String]): RDD[String] = {
    sc.makeRDD(seq, 2).groupBy(x => x).flatMap(_._2)
  }

  private def operatorStateInfo(
      path: String,
      queryRunId: UUID = UUID.randomUUID,
      version: Int = 0): StatefulOperatorStateInfo = {
    StatefulOperatorStateInfo(path, queryRunId, operatorId = 0, version, numPartitions = 5)
  }

  private val increment = (store: StateStore, iter: Iterator[String]) => {
    iter.foreach { s =>
      val key = stringToRow(s)
      val oldValue = Option(store.get(key)).map(rowToInt).getOrElse(0)
      store.put(key, intToRow(oldValue + 1))
    }
    store.commit()
    store.iterator().map(rowsToStringInt)
  }
}
