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
class RocksDBEventTimeAwareStateOperationsSuite extends SharedSparkSession
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

  // Helper methods to create test data
  private def stringToRow(s: String): UnsafeRow = {
    UnsafeProjection.create(Array[DataType](StringType))
      .apply(InternalRow(UTF8String.fromString(s)))
  }

  private def intToRow(i: Int): UnsafeRow = {
    UnsafeProjection.create(Array[DataType](IntegerType)).apply(InternalRow(i))
  }

  private def keyToRow(key: String, partitionId: Int): UnsafeRow = {
    UnsafeProjection.create(keySchema)
      .apply(InternalRow(UTF8String.fromString(key), partitionId))
  }

  private def valueToRow(value: Int): UnsafeRow = {
    UnsafeProjection.create(valueSchema).apply(InternalRow(value))
  }

  // Helper to create a new store provider with event time encoder
  private def newStoreProviderWithEventTime(
      encoderType: String = "prefix", // "prefix" or "postfix"
      useColumnFamilies: Boolean = true,
      useMultipleValuesPerKey: Boolean = false,
      dataEncoding: String = "unsaferow"): RocksDBStateStoreProvider = {

    val keyStateEncoderSpec = encoderType match {
      case "prefix" => EventTimeAsPrefixStateEncoderSpec(keySchema)
      case "postfix" => EventTimeAsPostfixStateEncoderSpec(keySchema)
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

    val storeConf = new StateStoreConf(sqlConf)
    provider.init(
      stateStoreId,
      keySchema,
      valueSchema,
      keyStateEncoderSpec,
      useColumnFamilies,
      storeConf,
      conf,
      useMultipleValuesPerKey)

    provider
  }

  // Helper to get event time aware state operations
  // Returns both the store and operations so caller can manage store lifecycle
  private def getEventTimeAwareOps(
      provider: RocksDBStateStoreProvider,
      version: Long = 0,
      columnFamily: String = testColFamily,
      encoderType: String = "prefix",
      useMultipleValuesPerKey: Boolean = false): (StateStore, EventTimeAwareStateOperations) = {

    val keyStateEncoderSpec = encoderType match {
      case "prefix" => EventTimeAsPrefixStateEncoderSpec(keySchema)
      case "postfix" => EventTimeAsPostfixStateEncoderSpec(keySchema)
      case _ => throw new IllegalArgumentException(s"Unknown encoder type: $encoderType")
    }

    val store = provider.getStore(version).asInstanceOf[provider.RocksDBStateStore]

    // Create column family for testing
    store.createColFamilyIfAbsent(
      columnFamily,
      keySchema,
      valueSchema,
      keyStateEncoderSpec,
      useMultipleValuesPerKey = useMultipleValuesPerKey)

    val operations = store.initiateEventTimeAwareStateOperations(columnFamily)
    (store, operations)
  }

  private def newDir(): String = Utils.createTempDir().getCanonicalPath

  Seq("unsaferow", "avro").foreach { encoding =>
    Seq("prefix", "postfix").foreach { encoderType =>
      test(s"Event time as $encoderType: basic put and get operations (encoding = $encoding)") {
        tryWithProviderResource(
          newStoreProviderWithEventTime(
            encoderType = "prefix", dataEncoding = encoding)) { provider =>
          val (store, operations) = getEventTimeAwareOps(provider, encoderType = "prefix")
          try {
            val key1 = keyToRow("key1", 1)
            val value1 = valueToRow(100)
            val eventTime1 = 1000L

            // Test put and get
            operations.put(key1, eventTime1, value1)
            val retrievedValue = operations.get(key1, eventTime1)

            assert(retrievedValue != null)
            assert(retrievedValue.getInt(0) === 100)

            // Test get with different event time should return null
            val nonExistentValue = operations.get(key1, 2000L)
            assert(nonExistentValue === null)

            // Test with different key should return null
            val key2 = keyToRow("key2", 1)
            val nonExistentValue2 = operations.get(key2, eventTime1)
            assert(nonExistentValue2 === null)
          } finally {
            store.abort()
          }
        }
      }

      test(s"Event time as $encoderType: remove operations (encoding = $encoding)") {
        tryWithProviderResource(
          newStoreProviderWithEventTime(
            encoderType = encoderType, dataEncoding = encoding)) { provider =>
          val (store, operations) = getEventTimeAwareOps(provider)
          try {
            val key1 = keyToRow("key1", 1)
            val value1 = valueToRow(100)
            val eventTime1 = 1000L

            // Put and verify
            operations.put(key1, eventTime1, value1)
            assert(operations.get(key1, eventTime1) != null)

            // Remove and verify
            operations.remove(key1, eventTime1)
            assert(operations.get(key1, eventTime1) === null)

            // Removing non-existent key should not throw error
            operations.remove(keyToRow("nonexistent", 1), 2000L)
          } finally {
            store.abort()
          }
        }
      }

      test(s"Event time as $encoderType: multiple values per key (encoding = $encoding)") {
        tryWithProviderResource(
          newStoreProviderWithEventTime(
            encoderType = encoderType,
            useMultipleValuesPerKey = true,
            dataEncoding = encoding)
        ) { provider =>
          val (store, operations) =
            getEventTimeAwareOps(provider, useMultipleValuesPerKey = true)
          try {
            val key1 = keyToRow("key1", 1)
            val values = Array(valueToRow(100), valueToRow(200), valueToRow(300))
            val eventTime1 = 1000L

            // Test putList
            operations.putList(key1, eventTime1, values)

            // Test valuesIterator
            val retrievedValues =
              operations.valuesIterator(key1, eventTime1).toList
            assert(retrievedValues.length === 3)
            assert(
              retrievedValues.map(_.getInt(0)).sorted === Array(
                100,
                200,
                300
              ).sorted
            )

            // Test with different event time should return empty iterator
            val emptyIterator = operations.valuesIterator(key1, 2000L)
            assert(!emptyIterator.hasNext)
          } finally {
            store.abort()
          }
        }
      }

      test(s"Event time as $encoderType: merge operations (encoding = $encoding)") {
        tryWithProviderResource(
          newStoreProviderWithEventTime(
            encoderType = encoderType,
            useMultipleValuesPerKey = true,
            dataEncoding = encoding)
        ) { provider =>
          val (store, operations) =
            getEventTimeAwareOps(provider, useMultipleValuesPerKey = true)
          try {
            val key1 = keyToRow("key1", 1)
            val value1 = valueToRow(100)
            val value2 = valueToRow(200)
            val eventTime1 = 1000L

            // Test merge single values
            operations.merge(key1, eventTime1, value1)
            operations.merge(key1, eventTime1, value2)

            val retrievedValues =
              operations.valuesIterator(key1, eventTime1).toList
            assert(retrievedValues.length === 2)
            assert(retrievedValues.map(_.getInt(0)).toSet === Set(100, 200))

            // Test mergeList
            val additionalValues = Array(valueToRow(300), valueToRow(400))
            operations.mergeList(key1, eventTime1, additionalValues)

            val allValues = operations.valuesIterator(key1, eventTime1).toList
            assert(allValues.length === 4)
            assert(allValues.map(_.getInt(0)).toSet === Set(100, 200, 300, 400))
          } finally {
            store.abort()
          }
        }
      }

      test(s"Event time as $encoderType: null key validation (encoding = $encoding)") {
        tryWithProviderResource(
          newStoreProviderWithEventTime(
            encoderType = encoderType, dataEncoding = encoding)) { provider =>
          val (store, operations) = getEventTimeAwareOps(provider)
          try {
            val value = valueToRow(100)
            val eventTime = 1000L

            // Test null key should throw exception
            intercept[IllegalStateException] {
              operations.put(null, eventTime, value)
            }

            intercept[IllegalStateException] {
              operations.get(null, eventTime)
            }

            intercept[IllegalStateException] {
              operations.remove(null, eventTime)
            }
          } finally {
            store.abort()
          }
        }
      }

      test(s"Event time as $encoderType: null value validation (encoding = $encoding)") {
        tryWithProviderResource(
          newStoreProviderWithEventTime(
            encoderType = encoderType, dataEncoding = encoding)) { provider =>
          val (store, operations) = getEventTimeAwareOps(provider)
          try {
            val key = keyToRow("key1", 1)
            val eventTime = 1000L

            // Test null value should throw exception
            intercept[IllegalArgumentException] {
              operations.put(key, eventTime, null)
            }
          } finally {
            store.abort()
          }
        }
      }
    }
  }

  Seq("unsaferow", "avro").foreach { encoding =>
    test(s"Event time as prefix: iterator operations (encoding = $encoding)") {
      tryWithProviderResource(
        newStoreProviderWithEventTime(
          encoderType = "prefix", dataEncoding = encoding)) { provider =>
        val operations = getEventTimeAwareOps(provider)

        val entries = Map(
          (keyToRow("key1", 1), 2000L) -> valueToRow(100),
          (keyToRow("key2", 1), 1500L) -> valueToRow(200),
          (keyToRow("key1", 2), 1000L) -> valueToRow(300)
        )

        // Put all entries (in non-sorted order)
        entries.foreach { case ((key, eventTime), value) =>
          operations.put(key, eventTime, value)
        }

        // Test iterator - should return all entries ordered by event time
        val iterator = operations.iterator()
        val results = iterator.map { pair =>
          val keyString = pair.key.getString(0)
          val partitionId = pair.key.getInt(1)
          val eventTime = pair.eventTime
          val value = pair.value.getInt(0)
          (keyString, partitionId, eventTime, value)
        }.toList

        iterator.close()

        assert(results.length === 3)

        // Verify results are ordered by event time (ascending)
        val eventTimes = results.map(_._3)
        assert(
          eventTimes === Seq(1000L, 1500L, 2000L),
          "Results should be ordered by event time"
        )

        // Verify all expected entries are present
        val retrievedEntries = results.map {
          case (key, partId, time, value) =>
            ((key, partId, time), value)
        }.toMap
        assert(retrievedEntries((("key1", 2, 1000L))) === 300)
        assert(retrievedEntries((("key2", 1, 1500L))) === 200)
        assert(retrievedEntries((("key1", 1, 2000L))) === 100)
      }
    }

    test(s"Event time as prefix: iterator with multiple values (encoding = $encoding)") {
      tryWithProviderResource(
        newStoreProviderWithEventTime(
          encoderType = "prefix",
          useMultipleValuesPerKey = true,
          dataEncoding = encoding)
      ) { provider =>
        val operations =
          getEventTimeAwareOps(provider, useMultipleValuesPerKey = true)

        val key1 = keyToRow("key1", 1)
        val values1 = Array(valueToRow(100), valueToRow(101))
        val key2 = keyToRow("key2", 1)
        val values2 = Array(valueToRow(200))
        val eventTime = 1000L

        operations.putList(key1, eventTime, values1)
        operations.putList(key2, eventTime, values2)

        // Test iteratorWithMultiValues
        val iterator = operations.iteratorWithMultiValues()
        val retrievedValues = iterator.map { pair =>
          pair.value.getInt(0)
        }.toList

        iterator.close()

        assert(
          retrievedValues.length === 3
        ) // 2 values from key1 + 1 value from key2
        assert(retrievedValues.toSet === Set(100, 101, 200))
      }
    }

    test(
      s"Event time as postfix: prefix scan operations (encoding = $encoding)"
    ) {
      tryWithProviderResource(
        newStoreProviderWithEventTime(encoderType = "postfix", dataEncoding = encoding)
      ) { provider =>
        val operations =
          getEventTimeAwareOps(provider, encoderType = "postfix")

        // Put entries with the same complete key but different event times
        // Prefix scan should find all entries with the same key across different event times
        val key1 = keyToRow("key1", 1)
        val key2 = keyToRow("key2", 1) // Different key

        // Insert in non-sorted order to verify that prefix scan returns them sorted by time
        operations.put(key1, 3000L, valueToRow(102))
        operations.put(key1, 1000L, valueToRow(100))
        operations.put(key1, 2000L, valueToRow(101))

        // Different key (key2, 1) - should not be returned
        operations.put(key2, 1500L, valueToRow(200))

        // Test prefixScan - pass the complete key to find all event times for that key
        val iterator = operations.prefixScan(key1)

        val results = iterator.map { pair =>
          val keyStr = pair.key.getString(0)
          val partitionId = pair.key.getInt(1)
          val eventTime = pair.eventTime
          val value = pair.value.getInt(0)
          (keyStr, partitionId, eventTime, value)
        }.toList
        iterator.close()

        // Should return all entries with the same complete key but different event times
        assert(results.length === 3)

        // Verify results are ordered by event time (ascending)
        val eventTimes = results.map(_._3)
        assert(
          eventTimes === Seq(1000L, 2000L, 3000L),
          "Results should be ordered by event time"
        )

        // Verify all expected entries are present
        assert(results(0) === (("key1", 1, 1000L, 100)))
        assert(results(1) === (("key1", 1, 2000L, 101)))
        assert(results(2) === (("key1", 1, 3000L, 102)))

        // Should not contain key2
        assert(!results.exists(_._1 == "key2"))
      }
    }

    test(s"Event time as postfix: prefix scan with multiple values (encoding = $encoding)") {
      tryWithProviderResource(
        newStoreProviderWithEventTime(
          encoderType = "postfix",
          useMultipleValuesPerKey = true,
          dataEncoding = encoding
        )
      ) { provider =>
        val operations = getEventTimeAwareOps(
          provider,
          encoderType = "postfix",
          useMultipleValuesPerKey = true
        )

        // Put multiple values for the same key at different event times
        val key1 = keyToRow("key1", 1)

        // Insert in non-sorted order to verify ordering by event time
        val values2 = Array(valueToRow(200), valueToRow(201))
        operations.putList(key1, 3000L, values2)

        val values1 = Array(valueToRow(100), valueToRow(101))
        operations.putList(key1, 1000L, values1)

        val values3 = Array(valueToRow(300), valueToRow(301))
        operations.putList(key1, 2000L, values3)

        // Different key - should not be returned
        val key2 = keyToRow("key2", 1)
        val values4 = Array(valueToRow(400))
        operations.putList(key2, 1500L, values4)

        // Test prefixScanWithMultiValues - pass complete key to find all event times
        val iterator = operations.prefixScanWithMultiValues(key1)

        val results = iterator.map { pair =>
          val keyStr = pair.key.getString(0)
          val partitionId = pair.key.getInt(1)
          val eventTime = pair.eventTime
          val value = pair.value.getInt(0)
          (keyStr, partitionId, eventTime, value)
        }.toList
        iterator.close()

        // Should return all individual values for key1 across different event times
        assert(results.length === 6) // 2 values at each of 3 event times

        // Verify results are ordered by event time (ascending)
        // Group by event time to verify ordering
        val eventTimes = results.map(_._3)
        val distinctEventTimes = eventTimes.distinct
        assert(
          distinctEventTimes === Seq(1000L, 2000L, 3000L),
          "Results should be ordered by event time"
        )

        // Verify the first 2 results are from time 1000L
        assert(results.take(2).forall(_._3 === 1000L))
        assert(results.take(2).map(_._4).toSet === Set(100, 101))

        // Verify the next 2 results are from time 2000L
        assert(results.slice(2, 4).forall(_._3 === 2000L))
        assert(results.slice(2, 4).map(_._4).toSet === Set(300, 301))

        // Verify the last 2 results are from time 3000L
        assert(results.slice(4, 6).forall(_._3 === 3000L))
        assert(results.slice(4, 6).map(_._4).toSet === Set(200, 201))

        // Should not contain key2
        assert(!results.exists(_._1 == "key2"))
      }
    }
  }

  test("EventTimeAwareStateOperations - error conditions") {
    // Test with non-event-time encoder should fail
    val provider = new RocksDBStateStoreProvider()
    val stateStoreId = StateStoreId(newDir(), Random.nextInt(), 0)
    val conf = new Configuration
    conf.set(StreamExecution.RUN_ID_KEY, UUID.randomUUID().toString)

    val storeConf = new StateStoreConf(SQLConf.get)
    provider.init(
      stateStoreId,
      keySchema,
      valueSchema,
      NoPrefixKeyStateEncoderSpec(
        keySchema
      ), // This doesn't support event time
      useColumnFamilies = true,
      storeConf,
      conf,
      useMultipleValuesPerKey = false
    )

    tryWithProviderResource(provider) { provider =>
      val store =
        provider.getStore(0).asInstanceOf[provider.RocksDBStateStore]

      store.createColFamilyIfAbsent(
        testColFamily,
        keySchema,
        valueSchema,
        NoPrefixKeyStateEncoderSpec(keySchema),
        useMultipleValuesPerKey = false
      )

      // Should throw exception when trying to create event time operations
      intercept[IllegalArgumentException] {
        store.initiateEventTimeAwareStateOperations(testColFamily)
      }
    }
  }

  test("EventTimeAwareStateOperations - column family name property") {
    tryWithProviderResource(newStoreProviderWithEventTime()) { provider =>
      val operations =
        getEventTimeAwareOps(provider, columnFamily = "custom_cf")

      assert(operations.columnFamilyName === "custom_cf")
    }
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
