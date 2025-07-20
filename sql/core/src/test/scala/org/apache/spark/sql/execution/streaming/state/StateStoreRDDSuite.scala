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

import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.LocalSparkSession._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.{CompletionIterator, Utils}

@ExtendedSQLTest
class StateStoreRDDSuite extends SparkFunSuite with BeforeAndAfter {

  import StateStoreTestsHelper._

  private val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
  private val tempDir = Files.createTempDirectory("StateStoreRDDSuite").toString
  private val keySchema = StateStoreTestsHelper.keySchema
  private val valueSchema = StateStoreTestsHelper.valueSchema

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

  test("SPARK-51955: ReadStateStore reuse and upgrade to WriteStore") {
    withSparkSession(SparkSession.builder()
      .config(sparkConf)
      .config(SQLConf.STATE_STORE_PROVIDER_CLASS.key, classOf[RocksDBStateStoreProvider].getName)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "1")
      .getOrCreate()) { spark =>
      implicit val sqlContext = spark.sqlContext
      val path = Utils.createDirectory(tempDir, Random.nextFloat().toString).toString

      // Create initial data in the state store (version 0)
      val initialData = makeRDD(spark.sparkContext, Seq(("a", 0), ("b", 0)))
      val setupRDD = initialData.mapPartitionsWithStateStore(
        sqlContext,
        operatorStateInfo(path, version = 0),
        keySchema,
        valueSchema,
        NoPrefixKeyStateEncoderSpec(keySchema)
      ) { (store, iter) =>
        // Set initial values: a->1, b->2
        iter.foreach { case (s, i) =>
          val key = dataToKeyRow(s, i)
          store.put(key, dataToValueRow(if (s == "a") 1 else 2))
        }
        store.commit()
        Iterator.empty
      }
      setupRDD.count() // Force evaluation

      // Create input data for our chained operations
      val inputData = makeRDD(spark.sparkContext, Seq(("a", 0), ("b", 0), ("c", 0)))

      var mappedReadStore: ReadStateStore = null
      var mappedWriteStore: StateStore = null

      // Chain operations: first read with ReadStateStore, then write with StateStore
      val chainedResults = inputData
        // First pass: read-only state store access
        .mapPartitionsWithReadStateStore(
          operatorStateInfo(path, version = 1),
          keySchema,
          valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema),
          spark.sessionState,
          Some(castToImpl(spark).streams.stateStoreCoordinator)
        ) { (readStore, iter) =>
          mappedReadStore = readStore

          // Read values and store them for later verification
          val inputItems = iter.toSeq // Materialize the input data

          val readValues = inputItems.map { case (s, i) =>
            val key = dataToKeyRow(s, i)
            val value = Option(readStore.get(key)).map(valueRowToData)
            ((s, i), value)
          }

          // Also capture all state store entries
          val allValues = readStore.iterator().map(rowPairToDataPair).toSeq

          // Return everything as a single tuple - only create one element in the iterator
          Iterator((readValues, allValues, inputItems))
        }
        // Second pass: use StateStore to write updates (should reuse the read store)
        .mapPartitionsWithStateStore(
          operatorStateInfo(path, version = 1),
          keySchema,
          valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema),
          spark.sessionState,
          Some(castToImpl(spark).streams.stateStoreCoordinator)
        ) { (writeStore, writeIter) =>
          if (writeIter.hasNext) {
            val (readValues, allStoreValues, originalItems) = writeIter.next()
            mappedWriteStore = writeStore
            // Get all existing values from the write store to verify reuse
            val storeValues = writeStore.iterator().map(rowPairToDataPair).toSeq

            // Update values for a and c from the original items
            originalItems.filter(p => p._1 == "a" || p._1 == "c").foreach { case (s, i) =>
              val key = dataToKeyRow(s, i)
              val oldValue = Option(writeStore.get(key)).map(valueRowToData).getOrElse(0)
              val newValue = oldValue + 10 // Add 10 to existing values
              writeStore.put(key, dataToValueRow(newValue))
            }
            writeStore.commit()

            // Return all collected information for verification
            Iterator((readValues, allStoreValues, storeValues))
          } else {
            Iterator.empty
          }
        }

      // Collect the results
      val (readValues, initialStoreState, writeStoreValues) = chainedResults.collect().head

      // Verify read results
      assert(readValues.toSet === Set(
        ("a", 0) -> Some(1),
        ("b", 0) -> Some(2),
        ("c", 0) -> None
      ))

      // Verify store state matches expected values
      assert(initialStoreState.toSet === Set((("a", 0), 1), (("b", 0), 2)))

      // Verify the existing values in the write store (should be the same as initial state)
      assert(writeStoreValues.toSet === Set((("a", 0), 1), (("b", 0), 2)))

      // Verify that the same store was used for both read and write operations
      assert(mappedReadStore == mappedWriteStore,
        "StateStoreThreadLocalTracker should indicate the read store was reused")

      // Create another ReadStateStoreRDD to verify the final state (version 2)
      val verifyData = makeRDD(spark.sparkContext, Seq(("a", 0), ("b", 0), ("c", 0)))
      val verifyRDD = verifyData.mapPartitionsWithReadStateStore(
        operatorStateInfo(path, version = 2),
        keySchema,
        valueSchema,
        NoPrefixKeyStateEncoderSpec(keySchema),
        spark.sessionState,
        Some(castToImpl(spark).streams.stateStoreCoordinator)
      ) { (store, iter) =>
        iter.map { case (s, i) =>
          val key = dataToKeyRow(s, i)
          val value = Option(store.get(key)).map(valueRowToData)
          ((s, i), value)
        }
      }

      // Verify the final state has the expected values
      // a: 1 + 10 = 11, b: 2 (unchanged), c: 0 + 10 = 10
      val finalResults = verifyRDD.collect().toSet
      assert(finalResults === Set(
        ("a", 0) -> Some(11),
        ("b", 0) -> Some(2),
        ("c", 0) -> Some(10)
      ))
    }
  }

  test("versioning and immutability") {
    withSparkSession(SparkSession.builder().config(sparkConf).getOrCreate()) { spark =>
      val path = Utils.createDirectory(tempDir, Random.nextFloat().toString).toString
      val rdd1 = makeRDD(spark.sparkContext, Seq(("a", 0), ("b", 0), ("a", 0)))
        .mapPartitionsWithStateStore(spark.sqlContext, operatorStateInfo(path, version = 0),
          keySchema, valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema))(increment)
      assert(rdd1.collect().toSet === Set(("a", 0) -> 2, ("b", 0) -> 1))

      // Generate next version of stores
      val rdd2 = makeRDD(spark.sparkContext, Seq(("a", 0), ("c", 0)))
        .mapPartitionsWithStateStore(spark.sqlContext, operatorStateInfo(path, version = 1),
          keySchema, valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema))(increment)
      assert(rdd2.collect().toSet === Set(("a", 0) -> 3, ("b", 0) -> 1, ("c", 0) -> 1))

      // Make sure the previous RDD still has the same data.
      assert(rdd1.collect().toSet === Set(("a", 0) -> 2, ("b", 0) -> 1))
    }
  }

  test("recovering from files") {
    val path = Utils.createDirectory(tempDir, Random.nextFloat().toString).toString

    def makeStoreRDD(
        spark: SparkSession,
        seq: Seq[(String, Int)],
        storeVersion: Int): RDD[((String, Int), Int)] = {
      implicit val sqlContext = spark.sqlContext
      makeRDD(spark.sparkContext, Seq(("a", 0))).mapPartitionsWithStateStore(
        sqlContext, operatorStateInfo(path, version = storeVersion),
        keySchema, valueSchema,
        NoPrefixKeyStateEncoderSpec(keySchema))(increment)
    }

    // Generate RDDs and state store data
    withSparkSession(SparkSession.builder().config(sparkConf).getOrCreate()) { spark =>
      for (i <- 1 to 20) {
        require(makeStoreRDD(spark, Seq(("a", 0)), i - 1).collect().toSet === Set(("a", 0) -> i))
      }
    }

    // With a new context, try using the earlier state store data
    withSparkSession(SparkSession.builder().config(sparkConf).getOrCreate()) { spark =>
      assert(makeStoreRDD(spark, Seq(("a", 0)), 20).collect().toSet === Set(("a", 0) -> 21))
    }
  }

  test("usage with iterators - only gets and only puts") {
    withSparkSession(SparkSession.builder().config(sparkConf).getOrCreate()) { spark =>
      implicit val sqlContext = spark.sqlContext
      val path = Utils.createDirectory(tempDir, Random.nextFloat().toString).toString
      val opId = 0

      // Returns an iterator of the incremented value made into the store
      def iteratorOfPuts(
          store: StateStore,
          iter: Iterator[(String, Int)]): Iterator[((String, Int), Int)] = {
        val resIterator = iter.map { case (s, i) =>
          val key = dataToKeyRow(s, i)
          val oldValue = Option(store.get(key)).map(valueRowToData).getOrElse(0)
          val newValue = oldValue + 1
          store.put(key, dataToValueRow(newValue))
          ((s, i), newValue)
        }
        CompletionIterator[((String, Int), Int), Iterator[((String, Int), Int)]](resIterator, {
          store.commit()
        })
      }

      def iteratorOfGets(
          store: StateStore,
          iter: Iterator[(String, Int)]): Iterator[((String, Int), Option[Int])] = {
        iter.map { case (s, i) =>
          val key = dataToKeyRow(s, i)
          val value = Option(store.get(key)).map(valueRowToData)
          ((s, i), value)
        }
      }

      val rddOfGets1 = makeRDD(spark.sparkContext, Seq(("a", 0), ("b", 0), ("c", 0)))
        .mapPartitionsWithStateStore(spark.sqlContext, operatorStateInfo(path, version = 0),
          keySchema, valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema))(iteratorOfGets)
      assert(rddOfGets1.collect().toSet ===
        Set(("a", 0) -> None, ("b", 0) -> None, ("c", 0) -> None))

      val rddOfPuts = makeRDD(spark.sparkContext, Seq(("a", 0), ("b", 0), ("a", 0)))
        .mapPartitionsWithStateStore(sqlContext, operatorStateInfo(path, version = 0),
          keySchema, valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema))(iteratorOfPuts)
      assert(rddOfPuts.collect().toSet ===
        Set(("a", 0) -> 1, ("a", 0) -> 2, ("b", 0) -> 1))

      val rddOfGets2 = makeRDD(spark.sparkContext, Seq(("a", 0), ("b", 0), ("c", 0)))
        .mapPartitionsWithStateStore(sqlContext, operatorStateInfo(path, version = 1),
          keySchema, valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema))(iteratorOfGets)
      assert(rddOfGets2.collect().toSet ===
        Set(("a", 0) -> Some(2), ("b", 0) -> Some(1), ("c", 0) -> None))
    }
  }

  test("preferred locations using StateStoreCoordinator") {
    quietly {
      val queryRunId = UUID.randomUUID
      val opId = 0
      val path = Utils.createDirectory(tempDir, Random.nextFloat().toString).toString

      withSparkSession(SparkSession.builder().config(sparkConf).getOrCreate()) { spark =>
        implicit val sqlContext = spark.sqlContext
        val coordinatorRef = castToImpl(spark).streams.stateStoreCoordinator
        val storeProviderId1 = StateStoreProviderId(StateStoreId(path, opId, 0), queryRunId)
        val storeProviderId2 = StateStoreProviderId(StateStoreId(path, opId, 1), queryRunId)
        coordinatorRef.reportActiveInstance(storeProviderId1, "host1", "exec1", Seq.empty)
        coordinatorRef.reportActiveInstance(storeProviderId2, "host2", "exec2", Seq.empty)

        require(
          coordinatorRef.getLocation(storeProviderId1) ===
            Some(ExecutorCacheTaskLocation("host1", "exec1").toString))

        val rdd = makeRDD(spark.sparkContext, Seq(("a", 0), ("b", 0), ("a", 0)))
          .mapPartitionsWithStateStore(sqlContext, operatorStateInfo(path, queryRunId = queryRunId),
          keySchema, valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema))(increment)
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
        SparkSession.builder()
          .config(sparkConf.setMaster("local-cluster[2, 1, 1024]"))
          .getOrCreate()) { spark =>
        implicit val sqlContext = spark.sqlContext
        val path = Utils.createDirectory(tempDir, Random.nextFloat().toString).toString
        val opId = 0
        val rdd1 = makeRDD(spark.sparkContext, Seq(("a", 0), ("b", 0), ("a", 0)))
          .mapPartitionsWithStateStore(sqlContext, operatorStateInfo(path, version = 0),
            keySchema, valueSchema,
            NoPrefixKeyStateEncoderSpec(keySchema))(increment)
        assert(rdd1.collect().toSet === Set(("a", 0) -> 2, ("b", 0) -> 1))

        // Generate next version of stores
        val rdd2 = makeRDD(spark.sparkContext, Seq(("a", 0), ("c", 0)))
          .mapPartitionsWithStateStore(sqlContext, operatorStateInfo(path, version = 1),
            keySchema, valueSchema,
            NoPrefixKeyStateEncoderSpec(keySchema))(increment)
        assert(rdd2.collect().toSet === Set(("a", 0) -> 3, ("b", 0) -> 1, ("c", 0) -> 1))

        // Make sure the previous RDD still has the same data.
        assert(rdd1.collect().toSet === Set(("a", 0) -> 2, ("b", 0) -> 1))
      }
    }
  }

  test("SPARK-51823: unload on commit") {
    withSparkSession(
      SparkSession.builder()
        .config(sparkConf)
        .config(SQLConf.STATE_STORE_UNLOAD_ON_COMMIT.key, true)
        .getOrCreate()) { spark =>
      val path = Utils.createDirectory(tempDir, Random.nextFloat().toString).toString
      val rdd1 = makeRDD(spark.sparkContext, Seq(("a", 0), ("b", 0), ("a", 0)))
        .mapPartitionsWithStateStore(spark.sqlContext, operatorStateInfo(path, version = 0),
          keySchema, valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema))(increment)

      assert(rdd1.collect().toSet === Set(("a", 0) -> 2, ("b", 0) -> 1))

      // Generate next version of stores
      val rdd2 = makeRDD(spark.sparkContext, Seq(("a", 0), ("c", 0)))
        .mapPartitionsWithStateStore(spark.sqlContext, operatorStateInfo(path, version = 1),
          keySchema, valueSchema,
          NoPrefixKeyStateEncoderSpec(keySchema))(increment)
      assert(rdd2.collect().toSet === Set(("a", 0) -> 3, ("b", 0) -> 1, ("c", 0) -> 1))

      // Make sure the previous RDD still has the same data.
      assert(rdd1.collect().toSet === Set(("a", 0) -> 2, ("b", 0) -> 1))
    }
  }

  private def makeRDD(sc: SparkContext, seq: Seq[(String, Int)]): RDD[(String, Int)] = {
    sc.makeRDD(seq, 2).groupBy(x => x).flatMap(_._2)
  }

  private def operatorStateInfo(
      path: String,
      queryRunId: UUID = UUID.randomUUID,
      version: Int = 0): StatefulOperatorStateInfo = {
    StatefulOperatorStateInfo(path, queryRunId, operatorId = 0, version, numPartitions = 5, None)
  }

  private val increment = (store: StateStore, iter: Iterator[(String, Int)]) => {
    iter.foreach { case (s, i) =>
      val key = dataToKeyRow(s, i)
      val oldValue = Option(store.get(key)).map(valueRowToData).getOrElse(0)
      store.put(key, dataToValueRow(oldValue + 1))
    }
    store.commit()
    store.iterator().map(rowPairToDataPair)
  }
}
