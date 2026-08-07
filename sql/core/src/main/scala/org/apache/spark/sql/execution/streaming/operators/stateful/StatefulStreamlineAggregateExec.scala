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

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalNotification}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.WidenStatefulOpNullability
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, MutableProjection, NamedExpression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.{Append, Complete, Update}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.operators.stateful.{StatefulOperatorCustomMetric, StatefulOperatorCustomSumMetric, StatefulOperatorStateInfo, StatefulOperatorsUtils, StateStoreWriter, StreamingAggregationStateManager, WatermarkSupport}
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, StateSchemaCompatibilityChecker, StateSchemaValidationResult, StateStore, StateStoreColFamilySchema, StateStoreOps}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CompletionIterator

/**
 * The physical plan of streaming aggregation which "streamlines" the process of aggregation.
 * (Here the term "streamline" represents the loop of "read-process-output" for each input.)
 *
 * Refer to the classdoc of [[StatefulStreamlineAggregationProcessor]] for more details.
 *
 * For producing result table as output, it follows the semantic of output mode.
 *
 * - Append mode: accumulated result for the grouping key will be produced once the watermark
 *   passes and there will be no further update against the grouping key.
 * - Update mode: each input will produce intermediate accumulated result as an output.
 *   Note that this is different from streaming aggregation in microbatch mode
 *   ([[StateStoreSaveExec]]) which produces the final intermediate accumulated result for
 *   each grouping key only in this microbatch.
 * - Complete mode: the entire result table will be produced per each microbatch.
 */
case class StatefulStreamlineAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    numShufflePartitions: Option[Int],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    isFinalAggregate: Boolean,
    outputMode: Option[OutputMode] = None,
    stateFormatVersion: Int,
    child: SparkPlan,
    stateInfo: Option[StatefulOperatorStateInfo] = None,
    eventTimeWatermarkForLateEvents: Option[Long] = None,
    eventTimeWatermarkForEviction: Option[Long] = None)
  extends BaseAggregateExec with StateStoreWriter with WatermarkSupport {

  override val isStreaming: Boolean = true

  override def shortName: String =
    StatefulOperatorsUtils.STATEFUL_STREAMLINE_AGGREGATE_EXEC_OP_NAME

  override def keyExpressions: Seq[Attribute] = groupingExpressions.map(_.toAttribute)

  // SPARK-57003 component (b): widen StatefulStreamlineAggregateExec output.
  override def output: Seq[Attribute] =
    WidenStatefulOpNullability.widenOutputForStatefulOp(
      resultExpressions.map(_.toAttribute))

  override def customStatefulOperatorMetrics: Seq[StatefulOperatorCustomMetric] = {
    Seq(
      StatefulOperatorCustomSumMetric(
        "numRowsReadDuringEviction", "number of state rows read during state eviction"
      ),
      StatefulOperatorCustomSumMetric(
        "numRowsIncrementallyRemoved", "number of state rows removed during incremental eviction"
      )
    )
  }

  private[sql] val stateManager = StreamingAggregationStateManager.createStateManager(
    keyExpressions, child.output, stateFormatVersion)

  // SPARK-57003 component (a): widen state schemas to nullable at construction. Both the
  // schema-check site (`validateAndMaybeEvolveStateSchema`) and the runtime
  // `mapPartitionsWithStateStore` site read these.
  private val stateKeySchema: StructType =
    WidenStatefulOpNullability.widenStateSchema(keyExpressions.toStructType)
  private val stateValueSchema: StructType =
    WidenStatefulOpNullability.widenStateSchema(stateManager.getStateValueSchema)

  private val incrementalCleanupFactor = session.sessionState.conf.getConf(
    SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR)

  private def doIncrementalCleanup = incrementalCleanupFactor > 0

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration, batchId: Long, stateSchemaVersion: Int):
    List[StateSchemaValidationResult] = {
    val newStateSchema = List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME,
      0, stateKeySchema, 0, stateValueSchema))
    List(StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema(getStateInfo,
      hadoopConf, newStateSchema, session.sessionState, stateSchemaVersion))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    val numOutputRows = longMetric("numOutputRows")
    val numUpdatedStateRows = longMetric("numUpdatedStateRows")
    val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
    val numRowsReadDuringEviction = longMetric("numRowsReadDuringEviction")
    val numRemovedStateRows = longMetric("numRemovedStateRows")
    val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
    val commitTimeMs = longMetric("commitTimeMs")
    val numRowsIncrementallyRemoved = longMetric("numRowsIncrementallyRemoved")

    assert(outputMode.nonEmpty,
      "Incorrect planning in IncrementalExecution, outputMode has not been set")

    child.execute().mapPartitionsWithStateStore(
      getStateInfo,
      stateKeySchema,
      stateValueSchema,
      NoPrefixKeyStateEncoderSpec(stateKeySchema),
      session.sessionState,
      Some(session.streams.stateStoreCoordinator)) { (store, iter) =>

      // It's feasible to overload the method to provide a partition index, but now it's too
      // many...
      val partIdx = TaskContext.get().partitionId()

      // Filter late date using watermark if specified
      val baseIterator = watermarkPredicateForDataForLateEvents match {
        case Some(predicate) => applyRemovingRowsOlderThanWatermark(iter, predicate)
        case None => iter
      }

      val aggProcessor = new StatefulStreamlineAggregationProcessor(
        partIdx,
        groupingExpressions,
        inputAttributes,
        aggregateExpressions,
        aggregateAttributes,
        initialInputBufferOffset,
        resultExpressions,
        (expressions, inputSchema) =>
          MutableProjection.create(expressions, inputSchema),
        stateManager,
        store,
        numUpdatedStateRows)

      // Each output row from aggIter references the same underlying row.
      // It is the caller's responsibility to ensure that each row is consumed before the next
      // one is produced.
      //
      // flushDirtyWrites is aggIter's completion action, so it runs when aggIter is exhausted and
      // BEFORE store.commit() in every mode below: Complete drains aggIter (line ~191) ahead of the
      // commit iterator; Append drains it (~222) ahead of its own; Update chains it inside the
      // outer CompletionIterator whose completion commits, and draining the outer drains aggIter
      // first. This ordering is what lets flushDirtyWrites surface a state-write failure (see its
      // rethrow) in time to fail the task before the batch commits -- a partial write can never be
      // committed.
      var tmpRow: UnsafeRow = null
      val aggIter = CompletionIterator[UnsafeRow, Iterator[UnsafeRow]](
        baseIterator.map { row =>
          allUpdatesTimeMs += timeTakenMs {
            tmpRow = aggProcessor.process(row)
          }
          tmpRow
        },
        aggProcessor.flushDirtyWrites() // Lazily evaluated
      )

      // Remaining logic performs the same thing with StateStoreSaveExec. It's mostly duplicated,
      // with slight modification to cover the usage of aggregation iterator.

      outputMode match {
        // Update and output all rows in the StateStore.
        case Some(Complete) =>
          // consume iterator fully to process all inputs and save the result into state store.
          aggIter.foreach(_ => ())

          // SPARK-45582 - Ensure that store instance is not used after commit is called
          // to invoke the iterator.
          val rangeIter = stateManager.values(store)

          CompletionIterator[UnsafeRow, Iterator[UnsafeRow]](
            rangeIter.map { valueRow =>
              numOutputRows += 1
              valueRow
            }, {
              allRemovalsTimeMs += 0
              commitTimeMs += timeTakenMs {
                store.commit()
              }
              setStoreMetrics(store)
              setOperatorMetrics()
            }
          )

        // Update and output only rows being evicted from the StateStore
        // Assumption: watermark predicates must be non-empty if append mode is allowed
        case Some(Append) =>
          assert(watermarkPredicateForDataForLateEvents.isDefined,
            "Watermark needs to be defined for streaming aggregation query in append mode")

          assert(watermarkPredicateForKeysForEviction.isDefined,
            "Watermark needs to be defined for streaming aggregation query in append mode")

          allUpdatesTimeMs += timeTakenMs {
            // consume iterator fully to process all inputs and save the result into state store.
            while (aggIter.hasNext) {
              aggIter.next()
            }
          }

          val removalStartTimeNs = System.nanoTime
          val evictionIterator =
            stateManager.evictionIterator(store, eventTimeWatermarkForEviction)

          CompletionIterator[UnsafeRow, Iterator[UnsafeRow]](
            evictionIterator.map(_.value), {
            numRowsReadDuringEviction += evictionIterator.numRowsReadDuringEvictionSoFar
            numRemovedStateRows += evictionIterator.numRowsRemovedSoFar
            numOutputRows += evictionIterator.numRowsRemovedSoFar

            // Note: Due to the iterator lazy exec, this metric also captures the time taken
            // by the consumer operators in addition to the processing in this operator.
            allRemovalsTimeMs += NANOSECONDS.toMillis(System.nanoTime - removalStartTimeNs)
            commitTimeMs += timeTakenMs {
              store.commit()
            }
            setStoreMetrics(store)
            setOperatorMetrics()
          })

        // Update and output modified rows from the StateStore.
        case Some(Update) =>
          /**
           * When doing incremental cleanup, we have to be careful what watermark to use. Because
           * the late events timestamp is less than the eviction timestamp, within a batch it is
           * possible for us to receive events whose timestamps is less than the eviction
           * timestamp. Thus, it is possible to evict a record at timestamp t, such that
           * t < evictionTimestamp, and, within the same batch, receive a record at timestamp t.
           *
           * Thus, when using incremental eviction, we have to make sure to clean up records
           * up to the timestamp before which we will _never_ receive new records. This would be
           * the event time watermark for late events.
           */
          val incrementalAwareEvictionWatermark = if (doIncrementalCleanup) {
            eventTimeWatermarkForLateEvents
          } else {
            eventTimeWatermarkForEviction
          }

          // Only create it if we are doing incremental cleanup. If we instantiate it and are
          // not doing incremental cleanup, then the iterator will not iterate through records
          // inserted into the store during the batch.
          val incrementalEvictionIter = if (doIncrementalCleanup) {
            Some(stateManager.evictionIterator(store, incrementalAwareEvictionWatermark))
          } else {
            None
          }

          val updateIter = aggIter.map { row =>
            incrementalEvictionIter.foreach { evictionIter =>
              allRemovalsTimeMs += timeTakenMs {
                var numRemovalsCurrRecord = 0
                // NOTE: EvictionIterator removes (and counts) a row inside hasNext, not next(), so
                //  a bare hasNext already deletes and bumps the metrics. To stop at exactly
                //  incrementalCleanupFactor removals per input we must re-check the count before
                //  each hasNext rather than draining the iterator.
                while (numRemovalsCurrRecord < incrementalCleanupFactor
                  && evictionIter.hasNext) {
                  // The removal happens inside of the iterator; in Update mode,
                  // we don't need the result.
                  evictionIter.next()
                  numRemovalsCurrRecord += 1
                }
              }
            }
            numOutputRows += 1
            row
          }

          CompletionIterator[UnsafeRow, Iterator[UnsafeRow]](updateIter, {
            // Anything removed so far must have been part of incremental eviction
            incrementalEvictionIter.foreach { iter =>
              numRowsIncrementallyRemoved += iter.numRowsRemovedSoFar
            }

            // If the incremental eviction iterator is defined, we'll finish eviction here
            // if any records remain. If it's not, we'll construct an eviction iterator
            // and also use it up here.
            val evictionIter = incrementalEvictionIter.getOrElse(
              stateManager.evictionIterator(store, eventTimeWatermarkForEviction))

            allRemovalsTimeMs += timeTakenMs {
              while (evictionIter.hasNext) {
                evictionIter.next()
              }
            }

            numRowsReadDuringEviction += evictionIter.numRowsReadDuringEvictionSoFar
            numRemovedStateRows += evictionIter.numRowsRemovedSoFar

            commitTimeMs += timeTakenMs {
              store.commit()
            }
            setStoreMetrics(store)
            setOperatorMetrics()
          })

        case _ => throw QueryExecutionErrors.unsupportedOutputModeForStreamingOperationError(
          outputMode.get, "streaming aggregations")
      }
    }
  }

  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = {
    (outputMode.contains(Append) || outputMode.contains(Update)) &&
      eventTimeWatermarkForEviction.isDefined &&
      newInputWatermark > eventTimeWatermarkForEviction.get
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  // FIXME: How we can prevent this to be called?
  override def toSortAggregate: SortAggregateExec = {
    throw new IllegalStateException("This class cannot be replaced with SortAggregate!")
  }
}

/**
 * This class is an implementation of GenericBufferAggregationIterator which performs the
 * aggregation against state store instead of maintaining aggregation hash table.
 *
 * For each input, do the following
 * - Read the previous value for grouping key in state store
 * - Merge the input and previous value (if any)
 * - Produce the merged result
 * - Store the new value to the dirty writes.
 *
 * This class maintains dirty writes which represent the cache of state store, performing both
 * caching and deferred writes. Note that there is a size limit of dirty writes which should be
 * considered carefully in both 1) memory usage and 2) latency spike on flushing writes.
 *
 * TODO: dirty writes can be implemented via LRU, which will optimize the ability of cache and
 *       also less spike on flushing writes (as evicted entry would be small portion of LRU).
 */
class StatefulStreamlineAggregationProcessor(
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    originalInputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    stateManager: StreamingAggregationStateManager,
    stateStore: StateStore,
    numUpdatedStateRows: SQLMetric)
  extends GenericBufferAggregationIterator(
    partIndex,
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) {

  // The value for unsafe buffer is just an conservative arbitrary value - it should be probably
  // safer to increase the value.
  // The value for safe buffer is picked from the default value of fallback threshold of object
  // hash aggregate, 128.
  // That said, it's conservative, but should we still make this be configurable?
  // NOTE: We need to consider the latency spike on flushing, so the number should not be too high
  // even though there is more available memory to cache more entries.
  protected val flushThresholdNumKeys: Int = if (useUnsafeBuffer) 1000 else 100

  // Holds the first failure thrown while a removal listener writes an entry to the state store.
  // Guava logs and SWALLOWS any exception a removal listener throws (see the
  // CacheBuilder.removalListener scaladoc), so the put below cannot fail the task on its own --
  // without this capture a failed write would be silently dropped and the batch would still commit,
  // losing that key's update. flushDirtyWrites rethrows it so the task fails instead of committing
  // partial state.
  private var writeFailure: Option[Throwable] = None

  // The writes which did not go into state store yet. We also leverage this dirty writes to the
  // cache of state store.
  private val dirtyWrites: LoadingCache[UnsafeRow, UnsafeRowReference] = CacheBuilder.newBuilder()
    .maximumSize(flushThresholdNumKeys)
    .removalListener((notification: RemovalNotification[UnsafeRow, UnsafeRowReference]) => {
      // Skip once a write has already failed: the task is going to fail anyway, and further puts
      // into a broken store are pointless.
      if (writeFailure.isEmpty) {
        try {
          val value = notification.getValue
          assert(value.getRow != null, "dirty writes should contain the valid row to update, " +
            "but found null.")
          stateManager.put(stateStore, value.getRow)
          numUpdatedStateRows += 1
        } catch {
          case NonFatal(e) => writeFailure = Some(e)
        }
      }
    })
    .build(new CacheLoader[UnsafeRow, UnsafeRowReference] {
      override def load(key: UnsafeRow): UnsafeRowReference = {
        val newRef = new UnsafeRowReference
        val existingValueInState = stateManager.get(stateStore, key)
        // NOTE: newRef.getRow could be still null after this line
        newRef.putRow(existingValueInState)
        newRef
      }
    })

  def hasNext: Boolean =
    throw new UnsupportedOperationException(
      "hasNext is not supported for StatefulStreamlineAggregationProcessor")

  def next(): UnsafeRow =
    throw new UnsupportedOperationException(
      "next is not supported for StatefulStreamlineAggregationProcessor")

  /**
   * Flush the dirty writes to state store. This should be called at the end of each microbatch.
   *
   * Rethrows the first state-store write failure a removal listener captured, so that a failed put
   * fails the task rather than committing state that is missing an update.
   */
  def flushDirtyWrites(): Unit = {
    dirtyWrites.invalidateAll()
    writeFailure.foreach { e =>
      throw new IllegalStateException(
        "Failed to write an aggregation update to the state store", e)
    }
  }

  def process(newInput: InternalRow): UnsafeRow = {
    // groupingProjection hands back the same UnsafeRow on every call, overwriting its bytes, so the
    // key has to be copied before the cache can store it. Without the copy, one shared row becomes
    // the key of every entry: each entry is then only findable through the hash captured when it
    // was inserted, and the equality check that should discriminate between keys degenerates to a
    // reference match against that one row.
    val groupingKey = groupingProjection.apply(newInput).copy()
    val buffer = newAggregationBuffer()

    val existingValueRef = dirtyWrites.get(groupingKey)

    if (existingValueRef.getRow != null) {
      processRow(buffer, existingValueRef.getRow)
    }

    processRow(buffer, newInput)

    val output = generateOutput(groupingKey, buffer)
    existingValueRef.putRow(output.copy())
    output
  }
}

class UnsafeRowReference {
  private var row: UnsafeRow = _

  def getRow: UnsafeRow = row

  def putRow(r: UnsafeRow): Unit = {
    row = r
  }
}
