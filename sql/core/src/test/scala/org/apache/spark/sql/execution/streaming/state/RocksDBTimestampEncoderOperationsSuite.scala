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

import java.util.UUID

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.runtime.StreamExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

@ExtendedSQLTest
class RocksDBTimestampEncoderOperationsSuite extends SharedSparkSession
  with BeforeAndAfterEach with Matchers {

  // Test schemas
  private val keySchema = StructType(Seq(
    StructField("key", StringType, nullable = true),
    StructField("partitionId", IntegerType, nullable = true)
  ))
  private val valueSchema = StructType(Seq(StructField("value", IntegerType, nullable = true)))

  // Column family names for testing
  private val testColFamily = "test_cf"

  override def beforeEach(): Unit = {
    super.beforeEach()
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
    spark.streams.stateStoreCoordinator // initialize the lazy coordinator
  }

  override def afterEach(): Unit = {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
    super.afterEach()
  }

  private def newDir(): String = Utils.createTempDir().getCanonicalPath

  // TODO: Address the new state format with Avro and enable the test with Avro encoding
  Seq("unsaferow").foreach { encoding =>
    Seq("prefix", "postfix").foreach { encoderType =>
      test(s"Event time as $encoderType: basic put and get operations (encoding = $encoding)") {
        tryWithProviderResource(
          newStoreProviderWithTimestampEncoder(
            encoderType = encoderType, dataEncoding = encoding)) { provider =>
          val store = provider.getStore(0)

          try {
            // Test put and get
            val keyWithTimestamp1 = keyAndTimestampToRow("key1", 1, 1000L)
            val value1 = valueToRow(100)

            store.put(keyWithTimestamp1, value1)
            val retrievedValue = store.get(keyWithTimestamp1)

            assert(retrievedValue != null)
            assert(retrievedValue.getInt(0) === 100)

            // Test get with different event time should return null
            val keyWithTimestamp2 = keyAndTimestampToRow("key1", 1, 2000L)
            assert(store.get(keyWithTimestamp2) === null)

            // Test with different key should return null
            val keyWithTimestamp3 = keyAndTimestampToRow("key2", 1, 1000L)
            assert(store.get(keyWithTimestamp3) === null)
          } finally {
            store.abort()
          }
        }
      }

      test(s"Event time as $encoderType: remove operations (encoding = $encoding)") {
        tryWithProviderResource(
          newStoreProviderWithTimestampEncoder(
            encoderType = encoderType, dataEncoding = encoding)) { provider =>
          val store = provider.getStore(0)

          try {
            val keyWithTimestamp1 = keyAndTimestampToRow("key1", 1, 1000L)
            val value1 = valueToRow(100)

            // Put and verify
            store.put(keyWithTimestamp1, value1)
            assert(store.get(keyWithTimestamp1) != null)

            // Remove and verify
            store.remove(keyWithTimestamp1)
            assert(store.get(keyWithTimestamp1) === null)

            // Removing non-existent key should not throw error
            store.remove(keyAndTimestampToRow("nonexistent", 1, 2000L))
          } finally {
            store.abort()
          }
        }
      }

      test(s"Event time as $encoderType: multiple values per key (encoding = $encoding)") {
        tryWithProviderResource(
          newStoreProviderWithTimestampEncoder(
            encoderType = encoderType,
            useMultipleValuesPerKey = true,
            dataEncoding = encoding)
        ) { provider =>
          val store = provider.getStore(0)

          try {
            val keyWithTimestamp1 = keyAndTimestampToRow("key1", 1, 1000L)
            val values = Array(valueToRow(100), valueToRow(200), valueToRow(300))

            // Test putList
            store.putList(keyWithTimestamp1, values)

            // Test valuesIterator
            val retrievedValues =
              store.valuesIterator(keyWithTimestamp1).map(_.copy()).toList
            assert(retrievedValues.length === 3)
            assert(
              retrievedValues.map(_.getInt(0)).sorted === Array(
                100,
                200,
                300
              ).sorted
            )

            // Test with different event time should return empty iterator
            val keyWithTimestamp2 = keyAndTimestampToRow("key1", 1, 2000L)
            val emptyIterator = store.valuesIterator(keyWithTimestamp2)
            assert(!emptyIterator.hasNext)
          } finally {
            store.abort()
          }
        }
      }

      test(s"Event time as $encoderType: merge operations (encoding = $encoding)") {
        tryWithProviderResource(
          newStoreProviderWithTimestampEncoder(
            encoderType = encoderType,
            useMultipleValuesPerKey = true,
            dataEncoding = encoding)
        ) { provider =>
          val store = provider.getStore(0)

          try {
            val keyWithTimestamp1 = keyAndTimestampToRow("key1", 1, 1000L)
            val value1 = valueToRow(100)
            val value2 = valueToRow(200)

            // Test merge single values
            store.merge(keyWithTimestamp1, value1)
            store.merge(keyWithTimestamp1, value2)

            val retrievedValues =
              store.valuesIterator(keyWithTimestamp1).map(_.copy()).toList
            assert(retrievedValues.length === 2)
            assert(retrievedValues.map(_.getInt(0)).toSet === Set(100, 200))

            // Test mergeList
            val additionalValues = Array(valueToRow(300), valueToRow(400))
            store.mergeList(keyWithTimestamp1, additionalValues)

            val allValues = store.valuesIterator(keyWithTimestamp1).map(_.copy()).toList
            assert(allValues.length === 4)
            assert(allValues.map(_.getInt(0)).toSet === Set(100, 200, 300, 400))
          } finally {
            store.abort()
          }
        }
      }

      test(s"Event time as $encoderType: null value validation (encoding = $encoding)") {
        tryWithProviderResource(
          newStoreProviderWithTimestampEncoder(
            encoderType = encoderType, dataEncoding = encoding)) { provider =>
          val store = provider.getStore(0)

          try {
            val keyWithTimestamp = keyAndTimestampToRow("key1", 1, 1000L)

            // Test null value should throw exception
            intercept[IllegalArgumentException] {
              store.put(keyWithTimestamp, null)
            }
          } finally {
            store.abort()
          }
        }
      }
    }
  }

  // TODO: Address the new state format with Avro and enable the test with Avro encoding
  Seq("unsaferow").foreach { encoding =>
    test(s"Event time as prefix: iterator operations (encoding = $encoding)") {
      tryWithProviderResource(
        newStoreProviderWithTimestampEncoder(
          encoderType = "prefix", dataEncoding = encoding)) { provider =>
        val store = provider.getStore(0)

        try {
          val entries = Map(
            keyAndTimestampToRow("key1", 1, 2000L) -> valueToRow(100),
            keyAndTimestampToRow("key2", 1, 1000L) -> valueToRow(200),
            keyAndTimestampToRow("key1", 2, -3000L) -> valueToRow(300)
          )

          // Put all entries (in non-sorted order)
          entries.foreach { case (keyAndTimestampRow, value) =>
            store.put(keyAndTimestampRow, value)
          }

          // Test iterator - should return all entries ordered by event time
          val iterator = store.iterator()
          val results = iterator.map { pair =>
            val keyString = pair.key.getString(0)
            val partitionId = pair.key.getInt(1)
            // The timestamp will be placed at the end of the key row.
            val timestamp = pair.key.getLong(2)
            val value = pair.value.getInt(0)
            (keyString, partitionId, timestamp, value)
          }.toList

          iterator.close()

          assert(results.length === 3)

          // Verify results are ordered by event time (ascending)
          val eventTimes = results.map(_._3)
          assert(
            eventTimes === Seq(-3000L, 1000L, 2000L),
            "Results should be ordered by event time"
          )

          // Verify all expected entries are present
          val retrievedEntries = results.map {
            case (key, partId, time, value) =>
              ((key, partId, time), value)
          }.toMap
          assert(retrievedEntries(("key1", 2, -3000L)) === 300)
          assert(retrievedEntries(("key2", 1, 1000L)) === 200)
          assert(retrievedEntries(("key1", 1, 2000L)) === 100)
        } finally {
          store.abort()
        }
      }
    }

    test(s"Event time as prefix: iterator with multiple values (encoding = $encoding)") {
      tryWithProviderResource(
        newStoreProviderWithTimestampEncoder(
          encoderType = "prefix",
          useMultipleValuesPerKey = true,
          dataEncoding = encoding)
      ) { provider =>
        val store = provider.getStore(0)

        try {
          val keyWithTimestamp1 = keyAndTimestampToRow("key1", 1, 1000L)
          val values1 = Array(valueToRow(100), valueToRow(101))
          val keyWithTimestamp2 = keyAndTimestampToRow("key2", 1, 1000L)
          val values2 = Array(valueToRow(200))

          store.putList(keyWithTimestamp1, values1)
          store.putList(keyWithTimestamp2, values2)

          // Test iteratorWithMultiValues
          val iterator = store.iteratorWithMultiValues()
          val retrievedValues = iterator.map { pair =>
            pair.value.getInt(0)
          }.toList

          iterator.close()

          assert(
            retrievedValues.length === 3
          ) // 2 values from key1 + 1 value from key2
          assert(retrievedValues.toSet === Set(100, 101, 200))
        } finally {
          store.abort()
        }
      }
    }

    test(
      s"Event time as postfix: prefix scan operations (encoding = $encoding)"
    ) {
      tryWithProviderResource(
        newStoreProviderWithTimestampEncoder(encoderType = "postfix", dataEncoding = encoding)
      ) { provider =>
        val store = provider.getStore(0)

        try {
          // Put entries with the same complete key but different event times
          // Prefix scan should find all entries with the same key across different event times

          // Insert in non-sorted order to verify that prefix scan returns them sorted by time
          store.put(keyAndTimestampToRow("key1", 1, 2000L), valueToRow(102))
          store.put(keyAndTimestampToRow("key1", 1, -3000L), valueToRow(100))
          store.put(keyAndTimestampToRow("key1", 1, 1000L), valueToRow(101))

          // Different key (key2, 1) - should not be returned
          store.put(keyAndTimestampToRow("key2", 1, 1500L), valueToRow(200))

          // Test prefixScan - pass the complete key to find all event times for that key
          val iterator = store.prefixScan(keyToRow("key1", 1))

          val results = iterator.map { pair =>
            val keyStr = pair.key.getString(0)
            val partitionId = pair.key.getInt(1)
            // The timestamp will be placed at the end of the key row.
            val timestamp = pair.key.getLong(2)
            val value = pair.value.getInt(0)
            (keyStr, partitionId, timestamp, value)
          }.toList
          iterator.close()

          // Should return all entries with the same complete key but different event times
          assert(results.length === 3)

          // Verify results are ordered by event time (ascending)
          val eventTimes = results.map(_._3)
          assert(
            eventTimes === Seq(-3000L, 1000L, 2000L),
            "Results should be ordered by event time"
          )

          // Verify all expected entries are present
          assert(results(0) === (("key1", 1, -3000L, 100)))
          assert(results(1) === (("key1", 1, 1000L, 101)))
          assert(results(2) === (("key1", 1, 2000L, 102)))

          // Should not contain key2
          assert(!results.exists(_._1 == "key2"))
        } finally {
          store.abort()
        }
      }
    }

    test(s"Event time as postfix: prefix scan with multiple values (encoding = $encoding)") {
      tryWithProviderResource(
        newStoreProviderWithTimestampEncoder(
          encoderType = "postfix",
          useMultipleValuesPerKey = true,
          dataEncoding = encoding
        )
      ) { provider =>
        val store = provider.getStore(0)

        try {
          // Put multiple values for the same key at different event times

          // Insert in non-sorted order to verify ordering by event time
          val values2 = Array(valueToRow(200), valueToRow(201))
          store.putList(keyAndTimestampToRow("key1", 1, 1000L), values2)

          val values1 = Array(valueToRow(100), valueToRow(101))
          store.putList(keyAndTimestampToRow("key1", 1, -3000L), values1)

          val values3 = Array(valueToRow(300), valueToRow(301))
          store.putList(keyAndTimestampToRow("key1", 1, 2000L), values3)

          // Different key - should not be returned
          val values4 = Array(valueToRow(400))
          store.putList(keyAndTimestampToRow("key2", 1, 1500L), values4)

          // Test prefixScanWithMultiValues - pass complete key to find all event times
          val key1 = keyToRow("key1", 1)
          val iterator = store.prefixScanWithMultiValues(key1)

          val results = iterator.map { pair =>
            val keyStr = pair.key.getString(0)
            val partitionId = pair.key.getInt(1)
            // The timestamp will be placed at the end of the key row.
            val timestamp = pair.key.getLong(2)
            val value = pair.value.getInt(0)
            (keyStr, partitionId, timestamp, value)
          }.toList
          iterator.close()

          // Should return all individual values for key1 across different event times
          assert(results.length === 6) // 2 values at each of 3 event times

          // Verify results are ordered by event time (ascending)
          // Group by event time to verify ordering
          val eventTimes = results.map(_._3)
          val distinctEventTimes = eventTimes.distinct
          assert(
            distinctEventTimes === Seq(-3000L, 1000L, 2000L),
            "Results should be ordered by event time"
          )

          // Verify the first 2 results are from time -3000L
          assert(results.take(2).forall(_._3 === -3000L))
          assert(results.take(2).map(_._4).toSet === Set(100, 101))

          // Verify the next 2 results are from time 1000L
          assert(results.slice(2, 4).forall(_._3 === 1000L))
          assert(results.slice(2, 4).map(_._4).toSet === Set(200, 201))

          // Verify the last 2 results are from time 2000L
          assert(results.slice(4, 6).forall(_._3 === 2000L))
          assert(results.slice(4, 6).map(_._4).toSet === Set(300, 301))

          // Should not contain key2
          assert(!results.exists(_._1 == "key2"))
        } finally {
          store.abort()
        }
      }
    }
  }

  // Helper methods to create test data
  private val keyProjection = UnsafeProjection.create(keySchema)
  private val keyAndTimestampProjection = UnsafeProjection.create(
    TimestampKeyStateEncoder.finalKeySchema(keySchema)
  )

  private def keyToRow(key: String, partitionId: Int): UnsafeRow = {
    keyProjection.apply(InternalRow(UTF8String.fromString(key), partitionId)).copy()
  }

  private def keyAndTimestampToRow(key: String, partitionId: Int, timestamp: Long): UnsafeRow = {
    keyAndTimestampProjection.apply(
      InternalRow(UTF8String.fromString(key), partitionId, timestamp)).copy()
  }

  private def valueToRow(value: Int): UnsafeRow = {
    UnsafeProjection.create(valueSchema).apply(InternalRow(value)).copy()
  }

  // Helper to create a new store provider with timestamp encoder
  private def newStoreProviderWithTimestampEncoder(
      encoderType: String, // "prefix" or "postfix"
      useColumnFamilies: Boolean = true,
      useMultipleValuesPerKey: Boolean = false,
      dataEncoding: String = "unsaferow"): RocksDBStateStoreProvider = {

    val keyStateEncoderSpec = encoderType match {
      case "prefix" => TimestampAsPrefixKeyStateEncoderSpec(
        TimestampKeyStateEncoder.finalKeySchema(keySchema))
      case "postfix" => TimestampAsPostfixKeyStateEncoderSpec(
        TimestampKeyStateEncoder.finalKeySchema(keySchema))
      case _ => throw new IllegalArgumentException(s"Unknown encoder type: $encoderType")
    }

    // Create a copy of SQLConf and set data encoding format on it
    val sqlConf = SQLConf.get.clone()
    sqlConf.setConfString(
      "spark.sql.streaming.stateStore.encodingFormat",
      dataEncoding)

    val provider = new RocksDBStateStoreProvider()
    val stateStoreId = StateStoreId(newDir(), Random.nextInt(), 0)
    val conf = new Configuration
    conf.set(StreamExecution.RUN_ID_KEY, UUID.randomUUID().toString)

    val testProvider = new TestStateSchemaProvider()
    testProvider.captureSchema(
      testColFamily,
      keySchema,
      valueSchema
    )

    val storeConf = new StateStoreConf(sqlConf)
    provider.init(
      stateStoreId,
      keySchema,
      valueSchema,
      keyStateEncoderSpec,
      useColumnFamilies,
      storeConf,
      conf,
      useMultipleValuesPerKey,
    Some(testProvider))

    provider
  }

  // Helper method for resource management
  private def tryWithProviderResource[T](provider: RocksDBStateStoreProvider)
      (f: RocksDBStateStoreProvider => T): T = {
    try {
      f(provider)
    } finally {
      provider.close()
    }
  }
}
