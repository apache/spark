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

package org.apache.spark.sql.execution.streaming

import java.io.IOException
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSeq, MutableProjection, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{PartialMerge, Sum}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingAggregationStateManager
import org.apache.spark.sql.execution.streaming.runtime.StreamExecution
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}
import org.apache.spark.util.CompletionIterator

class StatefulStreamlineAggregationProcessorSuite extends SparkFunSuite with SQLConfHelper {

  private class PutFailingStore extends MemoryStateStore {
    var reads = 0
    var writes = 0
    val writtenKeys = new ArrayBuffer[Int]
    var failure: Option[IOException] = None
    var writeBeforeFailure = false
    var commits = 0

    override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
      reads += 1
      super.get(key, colFamilyName)
    }

    override def put(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
      writes += 1
      if (key.numFields > 0) {
        writtenKeys += key.getInt(0)
      }
      if (failure.isEmpty || writeBeforeFailure) {
        super.put(key, value, colFamilyName)
      }
      failure.foreach(throw _)
    }

    override def commit(): Long = {
      commits += 1
      super.commit()
    }
  }

  private class ProcessorFixture(version: Int, store: StateStore, global: Boolean = false) {
    val key = AttributeReference("key", IntegerType, nullable = false)()
    private val value = AttributeReference("value", LongType)()
    private val sum = Sum(value)
    private val groupingAttributes = if (global) Seq.empty else Seq(key)
    private val inputAttributes = groupingAttributes ++ sum.inputAggBufferAttributes
    val stateManager = StreamingAggregationStateManager.createStateManager(
      groupingAttributes, inputAttributes, version)
    val updatedRows = new SQLMetric("sum")
    val processor = new StatefulStreamlineAggregationProcessor(
      partIndex = 0,
      groupingExpressions = groupingAttributes,
      originalInputAttributes = inputAttributes,
      aggregateExpressions = Seq(sum.toAggregateExpression().copy(mode = PartialMerge)),
      aggregateAttributes = sum.aggBufferAttributes,
      initialInputBufferOffset = groupingAttributes.length,
      resultExpressions = groupingAttributes ++ sum.aggBufferAttributes,
      newMutableProjection = (expressions, attributes) =>
        MutableProjection.create(expressions, attributes),
      stateManager = stateManager,
      stateStore = store,
      numUpdatedStateRows = updatedRows) {
      // The literal constant must be available while the superclass constructs its cache.
      override protected final val flushThresholdNumKeys = 2
    }

    def process(key: Int, value: Long): Long =
      processor.process(InternalRow(key, value)).getLong(1)

    def storedTotal(key: Int): Long = {
      val keyRow = UnsafeProjection.create(Array[DataType](IntegerType))(InternalRow(key))
      stateManager.get(store, keyRow).getLong(1)
    }
  }

  private def assertWriteFailure(cause: Throwable)(f: => Any): Unit = {
    val error = intercept[IllegalStateException](f)
    assert(error.getMessage === "Failed to write an aggregation update to the state store")
    assert(error.getCause eq cause)
  }

  StreamingAggregationStateManager.supportedVersions.foreach { version =>
    test(s"healthy eviction and reload preserve aggregates - state format $version") {
      val store = new PutFailingStore
      val fixture = new ProcessorFixture(version, store)
      assert(fixture.process(0, 5) === 5L)
      assert(fixture.process(0, 10) === 15L)
      assert(fixture.process(1, 7) === 7L)
      assert(store.reads === 2)
      assert(store.writtenKeys.isEmpty)

      assert(fixture.process(2, 3) === 3L)
      assert(store.writtenKeys.toSeq === Seq(0))
      val readsBeforeReload = store.reads
      assert(fixture.process(0, 20) === 35L)
      assert(store.reads === readsBeforeReload + 1)
      fixture.processor.flushDirtyWrites()
      assert(fixture.storedTotal(0) === 35L)
      assert(fixture.storedTotal(1) === 7L)
      assert(fixture.storedTotal(2) === 3L)
      assert(fixture.updatedRows.value === store.writes.toLong)
      val writesAfterFlush = store.writes
      fixture.processor.flushDirtyWrites()
      assert(store.writes === writesAfterFlush)
    }

    Seq(false, true).foreach { writeBeforeFailure =>
      test(s"eviction fails the current input before output - state format $version, " +
        s"partial write $writeBeforeFailure") {
        val store = new PutFailingStore
        val fixture = new ProcessorFixture(version, store)
        assert(fixture.process(0, 5) === 5L)
        assert(fixture.process(0, 10) === 15L)
        assert(fixture.process(1, 7) === 7L)
        assert(store.reads === 2)
        assert(store.writtenKeys.isEmpty)
        val firstFailure = new IOException("eviction write failed")
        store.failure = Some(firstFailure)
        store.writeBeforeFailure = writeBeforeFailure

        assertWriteFailure(firstFailure) { fixture.process(2, 3) }
        assert(store.writtenKeys.toSeq === Seq(0))
        assert(store.reads === 3)
        assert(fixture.updatedRows.value === 0L)
        store.failure = Some(new IOException("must not replace the first failure"))

        // A failed processor must reject even an input that cannot be projected.
        assertWriteFailure(firstFailure) { fixture.processor.process(null) }
        assertWriteFailure(firstFailure) { fixture.process(0, 20) }
        assertWriteFailure(firstFailure) { fixture.process(3, 20) }
        assertWriteFailure(firstFailure) { fixture.processor.flushDirtyWrites() }
        assertWriteFailure(firstFailure) { fixture.processor.flushDirtyWrites() }
        assert(store.reads === 3)
        assert(store.writtenKeys.toSeq === Seq(0))
        assert(fixture.updatedRows.value === 0L)
        assert(store.commits === 0)
      }
    }

    test(s"flush-only failure remains sticky - state format $version") {
      val store = new PutFailingStore
      val fixture = new ProcessorFixture(version, store)
      assert(fixture.process(0, 5) === 5L)
      assert(store.writes === 0)
      val failure = new IOException("flush write failed")
      store.failure = Some(failure)
      assertWriteFailure(failure) { fixture.processor.flushDirtyWrites() }
      assertWriteFailure(failure) { fixture.process(1, 7) }
      assertWriteFailure(failure) { fixture.processor.flushDirtyWrites() }
      assert(store.reads === 1)
      assert(store.writtenKeys.toSeq === Seq(0))
      assert(fixture.updatedRows.value === 0L)
    }

    test(s"empty global aggregation preserves initialized and stored values - " +
      s"state format $version") {
      val store = new PutFailingStore
      val fixture = new ProcessorFixture(version, store, global = true)
      assert(fixture.processor.processEmptyGroupingKey().isNullAt(0))
      assert(store.reads === 1)
      assert(fixture.processor.process(InternalRow(5L)).getLong(0) === 5L)
      fixture.processor.flushDirtyWrites()
      assert(store.writes === 1)
      assert(fixture.processor.processEmptyGroupingKey().getLong(0) === 5L)
      fixture.processor.flushDirtyWrites()
      assert(store.reads === 2)
      assert(store.writes === 2)
      assert(fixture.updatedRows.value === 2L)
    }

    test(s"empty global aggregation rejects a failed flush - state format $version") {
      val store = new PutFailingStore
      val fixture = new ProcessorFixture(version, store, global = true)
      assert(fixture.processor.process(InternalRow(5L)).getLong(0) === 5L)
      val failure = new IOException("global aggregation write failed")
      store.failure = Some(failure)
      assertWriteFailure(failure) { fixture.processor.flushDirtyWrites() }
      assertWriteFailure(failure) { fixture.processor.processEmptyGroupingKey() }
      assertWriteFailure(failure) { fixture.processor.process(InternalRow(10L)) }
      assertWriteFailure(failure) { fixture.processor.flushDirtyWrites() }
      assert(store.reads === 1)
      assert(store.writes === 1)
      assert(fixture.updatedRows.value === 0L)
    }

    Seq(false, true).foreach { failFlush =>
      test(s"completion flushes before commit - state format $version, failure $failFlush") {
        val store = new PutFailingStore
        val fixture = new ProcessorFixture(version, store)
        val input = Iterator(InternalRow(0, 5L))
        val aggregated = CompletionIterator[UnsafeRow, Iterator[UnsafeRow]](
          input.map(fixture.processor.process), fixture.processor.flushDirtyWrites())
        val output = CompletionIterator[UnsafeRow, Iterator[UnsafeRow]](
          aggregated, { fixture.stateManager.commit(store); () })
        assert(output.next().getLong(1) === 5L)
        assert(store.writes === 0)
        assert(store.commits === 0)

        if (failFlush) {
          val failure = new IOException("completion write failed")
          store.failure = Some(failure)
          assertWriteFailure(failure) { output.hasNext }
          assert(store.commits === 0)
        } else {
          assert(!output.hasNext)
          assert(!output.hasNext)
          assert(store.commits === 1)
        }
        assert(store.writtenKeys.toSeq === Seq(0))
      }
    }

    test(s"abort failed writes and replay committed state - state format $version") {
      withSQLConf(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "1") {
        withTempDir { dir =>
          val injection = FailureInjectionFileSystem.registerTempPath(dir.getCanonicalPath)
          val hadoopConf = new Configuration()
          hadoopConf.set(StreamExecution.RUN_ID_KEY, UUID.randomUUID().toString)
          hadoopConf.set(SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key,
            classOf[FailureInjectionCheckpointFileManager].getName)
          val id = StateStoreId(dir.getCanonicalPath, 0, 0)
          val schemaFixture = new ProcessorFixture(version, new MemoryStateStore)

          def openProvider(): HDFSBackedStateStoreProvider = {
            val provider = new HDFSBackedStateStoreProvider
            val manager = schemaFixture.stateManager
            val keySchema = Seq(schemaFixture.key).toStructType
            provider.init(id, keySchema, manager.getStateValueSchema,
              NoPrefixKeyStateEncoderSpec(keySchema), useColumnFamilies = false,
              new StateStoreConf(conf), hadoopConf)
            provider
          }

          var provider: HDFSBackedStateStoreProvider = null
          var activeStore: StateStore = null
          try {
            provider = openProvider()
            activeStore = provider.getStore(0)
            val initial = new ProcessorFixture(version, activeStore)
            assert(initial.process(0, 4) === 4L)
            initial.processor.flushDirtyWrites()
            assert(activeStore.commit() === 1L)
            activeStore = provider.getStore(1)
            val failed = new ProcessorFixture(version, activeStore)
            assert(failed.process(0, 5) === 9L)
            assert(failed.process(0, 10) === 19L)
            assert(failed.process(1, 7) === 7L)
            // HDFS puts update the local map before creating the next delta file.
            injection.failureCreateAtomicRegex = Seq(".*2\\.delta")
            val failure = intercept[IllegalStateException] { failed.process(2, 3) }
            assert(failure.getCause.isInstanceOf[IOException])
            assert(failure.getCause.getMessage === "Fake File System Create Atomic Failure")
            assert(!activeStore.hasCommitted)
            val uncommittedDelta = new Path(id.storeCheckpointLocation(), "2.delta")
            assert(!uncommittedDelta.getFileSystem(hadoopConf).exists(uncommittedDelta))
            injection.failureCreateAtomicRegex = Seq.empty
            activeStore.abort()
            activeStore = null
            provider.close()
            provider = null

            provider = openProvider()
            activeStore = provider.getStore(1)
            val replay = new ProcessorFixture(version, activeStore)
            assert(replay.storedTotal(0) === 4L)
            assert(replay.process(0, 5) === 9L)
            assert(replay.process(0, 10) === 19L)
            assert(replay.process(1, 7) === 7L)
            assert(replay.process(2, 3) === 3L)
            assert(replay.process(0, 20) === 39L)
            replay.processor.flushDirtyWrites()
            assert(activeStore.commit() === 2L)
            activeStore = null
            provider.close()
            provider = null

            provider = openProvider()
            activeStore = provider.getStore(2)
            val recovered = new ProcessorFixture(version, activeStore)
            assert(recovered.storedTotal(0) === 39L)
            assert(recovered.storedTotal(1) === 7L)
            assert(recovered.storedTotal(2) === 3L)
          } finally {
            injection.failureCreateAtomicRegex = Seq.empty
            try {
              if (activeStore != null && !activeStore.hasCommitted) activeStore.abort()
            } finally {
              try {
                if (provider != null) provider.close()
              } finally {
                FailureInjectionFileSystem.removePathFromTempToInjectionState(dir.getCanonicalPath)
              }
            }
          }
        }
      }
    }
  }
}
