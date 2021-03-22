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

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.streaming.{OutputMode, StateOperatorProgress}
import org.apache.spark.sql.types._
import org.apache.spark.util.{CompletionIterator, NextIterator, SerializableConfiguration, Utils}


/** Used to identify the state store for a given operator. */
case class StatefulOperatorStateInfo(
    checkpointLocation: String,
    queryRunId: UUID,
    operatorId: Long,
    storeVersion: Long,
    numPartitions: Int) {
  override def toString(): String = {
    s"state info [ checkpoint = $checkpointLocation, runId = $queryRunId, " +
      s"opId = $operatorId, ver = $storeVersion, numPartitions = $numPartitions]"
  }
}

/**
 * An operator that reads or writes state from the [[StateStore]].
 * The [[StatefulOperatorStateInfo]] should be filled in by `prepareForExecution` in
 * [[IncrementalExecution]].
 */
trait StatefulOperator extends SparkPlan {
  def stateInfo: Option[StatefulOperatorStateInfo]

  protected def getStateInfo: StatefulOperatorStateInfo = {
    stateInfo.getOrElse {
      throw new IllegalStateException("State location not present for execution")
    }
  }
}

/** An operator that reads from a StateStore. */
trait StateStoreReader extends StatefulOperator {
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))
}

/** An operator that writes to a StateStore. */
trait StateStoreWriter extends StatefulOperator { self: SparkPlan =>

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numRowsDroppedByWatermark" -> SQLMetrics.createMetric(sparkContext,
      "number of rows which are dropped by watermark"),
    "numTotalStateRows" -> SQLMetrics.createMetric(sparkContext, "number of total state rows"),
    "numUpdatedStateRows" -> SQLMetrics.createMetric(sparkContext, "number of updated state rows"),
    "allUpdatesTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "time to update"),
    "allRemovalsTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "time to remove"),
    "commitTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "time to commit changes"),
    "stateMemory" -> SQLMetrics.createSizeMetric(sparkContext, "memory used by state")
  ) ++ stateStoreCustomMetrics

  /**
   * Get the progress made by this stateful operator after execution. This should be called in
   * the driver after this SparkPlan has been executed and metrics have been updated.
   */
  def getProgress(): StateOperatorProgress = {
    val customMetrics = stateStoreCustomMetrics
      .map(entry => entry._1 -> longMetric(entry._1).value)

    val javaConvertedCustomMetrics: java.util.HashMap[String, java.lang.Long] =
      new java.util.HashMap(customMetrics.mapValues(long2Long).toMap.asJava)

    new StateOperatorProgress(
      numRowsTotal = longMetric("numTotalStateRows").value,
      numRowsUpdated = longMetric("numUpdatedStateRows").value,
      memoryUsedBytes = longMetric("stateMemory").value,
      numRowsDroppedByWatermark = longMetric("numRowsDroppedByWatermark").value,
      javaConvertedCustomMetrics
    )
  }

  /** Records the duration of running `body` for the next query progress update. */
  protected def timeTakenMs(body: => Unit): Long = Utils.timeTakenMs(body)._2

  /**
   * Set the SQL metrics related to the state store.
   * This should be called in that task after the store has been updated.
   */
  protected def setStoreMetrics(store: StateStore): Unit = {
    val storeMetrics = store.metrics
    longMetric("numTotalStateRows") += storeMetrics.numKeys
    longMetric("stateMemory") += storeMetrics.memoryUsedBytes
    storeMetrics.customMetrics.foreach { case (metric, value) =>
      longMetric(metric.name) += value
    }
  }

  private def stateStoreCustomMetrics: Map[String, SQLMetric] = {
    val provider = StateStoreProvider.create(sqlContext.conf.stateStoreProviderClass)
    provider.supportedCustomMetrics.map {
      case StateStoreCustomSumMetric(name, desc) =>
        name -> SQLMetrics.createMetric(sparkContext, desc)
      case StateStoreCustomSizeMetric(name, desc) =>
        name -> SQLMetrics.createSizeMetric(sparkContext, desc)
      case StateStoreCustomTimingMetric(name, desc) =>
        name -> SQLMetrics.createTimingMetric(sparkContext, desc)
    }.toMap
  }

  protected def applyRemovingRowsOlderThanWatermark(
      iter: Iterator[InternalRow],
      predicateDropRowByWatermark: BasePredicate): Iterator[InternalRow] = {
    iter.filterNot { row =>
      val shouldDrop = predicateDropRowByWatermark.eval(row)
      if (shouldDrop) longMetric("numRowsDroppedByWatermark") += 1
      shouldDrop
    }
  }

  /**
   * Should the MicroBatchExecution run another batch based on this stateful operator and the
   * current updated metadata.
   */
  def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = false
}

/** An operator that supports watermark. */
trait WatermarkSupport extends UnaryExecNode {

  /** The keys that may have a watermark attribute. */
  def keyExpressions: Seq[Attribute]

  /** The watermark value. */
  def eventTimeWatermark: Option[Long]

  /** Generate an expression that matches data older than the watermark */
  lazy val watermarkExpression: Option[Expression] = {
    WatermarkSupport.watermarkExpression(
      child.output.find(_.metadata.contains(EventTimeWatermark.delayKey)),
      eventTimeWatermark)
  }

  /** Predicate based on keys that matches data older than the watermark */
  lazy val watermarkPredicateForKeys: Option[BasePredicate] = watermarkExpression.flatMap { e =>
    if (keyExpressions.exists(_.metadata.contains(EventTimeWatermark.delayKey))) {
      Some(Predicate.create(e, keyExpressions))
    } else {
      None
    }
  }

  /** Predicate based on the child output that matches data older than the watermark. */
  lazy val watermarkPredicateForData: Option[BasePredicate] =
    watermarkExpression.map(Predicate.create(_, child.output))

  protected def removeKeysOlderThanWatermark(store: StateStore): Unit = {
    if (watermarkPredicateForKeys.nonEmpty) {
      store.getRange(None, None).foreach { rowPair =>
        if (watermarkPredicateForKeys.get.eval(rowPair.key)) {
          store.remove(rowPair.key)
        }
      }
    }
  }

  protected def removeKeysOlderThanWatermark(
      storeManager: StreamingAggregationStateManager,
      store: StateStore): Unit = {
    if (watermarkPredicateForKeys.nonEmpty) {
      storeManager.keys(store).foreach { keyRow =>
        if (watermarkPredicateForKeys.get.eval(keyRow)) {
          storeManager.remove(store, keyRow)
        }
      }
    }
  }
}

object WatermarkSupport {

  /** Generate an expression on given attributes that matches data older than the watermark */
  def watermarkExpression(
      optionalWatermarkExpression: Option[Expression],
      optionalWatermarkMs: Option[Long]): Option[Expression] = {
    if (optionalWatermarkExpression.isEmpty || optionalWatermarkMs.isEmpty) return None

    val watermarkAttribute = optionalWatermarkExpression.get
    // If we are evicting based on a window, use the end of the window.  Otherwise just
    // use the attribute itself.
    val evictionExpression =
      if (watermarkAttribute.dataType.isInstanceOf[StructType]) {
        LessThanOrEqual(
          GetStructField(watermarkAttribute, 1),
          Literal(optionalWatermarkMs.get * 1000))
      } else {
        LessThanOrEqual(
          watermarkAttribute,
          Literal(optionalWatermarkMs.get * 1000))
      }
    Some(evictionExpression)
  }
}

/**
 * For each input tuple, the key is calculated and the value from the [[StateStore]] is added
 * to the stream (in addition to the input tuple) if present.
 */
case class StateStoreRestoreExec(
    keyExpressions: Seq[Attribute],
    stateInfo: Option[StatefulOperatorStateInfo],
    stateFormatVersion: Int,
    child: SparkPlan)
  extends UnaryExecNode with StateStoreReader {

  private[sql] val stateManager = StreamingAggregationStateManager.createStateManager(
    keyExpressions, child.output, stateFormatVersion)

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    child.execute().mapPartitionsWithReadStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      stateManager.getStateValueSchema,
      indexOrdinal = None,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator)) { case (store, iter) =>
        val hasInput = iter.hasNext
        if (!hasInput && keyExpressions.isEmpty) {
          // If our `keyExpressions` are empty, we're getting a global aggregation. In that case
          // the `HashAggregateExec` will output a 0 value for the partial merge. We need to
          // restore the value, so that we don't overwrite our state with a 0 value, but rather
          // merge the 0 with existing state.
          store.iterator().map(_.value)
        } else {
          iter.flatMap { row =>
            val key = stateManager.getKey(row.asInstanceOf[UnsafeRow])
            val restoredRow = stateManager.get(store, key)
            numOutputRows += 1
            Option(restoredRow).toSeq :+ row
          }
        }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    if (keyExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(keyExpressions, stateInfo.map(_.numPartitions)) :: Nil
    }
  }
}

/**
 * For each input tuple, the key is calculated and the tuple is `put` into the [[StateStore]].
 */
case class StateStoreSaveExec(
    keyExpressions: Seq[Attribute],
    stateInfo: Option[StatefulOperatorStateInfo] = None,
    outputMode: Option[OutputMode] = None,
    eventTimeWatermark: Option[Long] = None,
    stateFormatVersion: Int,
    child: SparkPlan)
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport {

  private[sql] val stateManager = StreamingAggregationStateManager.createStateManager(
    keyExpressions, child.output, stateFormatVersion)

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver
    assert(outputMode.nonEmpty,
      "Incorrect planning in IncrementalExecution, outputMode has not been set")

    child.execute().mapPartitionsWithStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      stateManager.getStateValueSchema,
      indexOrdinal = None,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator)) { (store, iter) =>
        val numOutputRows = longMetric("numOutputRows")
        val numUpdatedStateRows = longMetric("numUpdatedStateRows")
        val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
        val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
        val commitTimeMs = longMetric("commitTimeMs")

        outputMode match {
          // Update and output all rows in the StateStore.
          case Some(Complete) =>
            allUpdatesTimeMs += timeTakenMs {
              while (iter.hasNext) {
                val row = iter.next().asInstanceOf[UnsafeRow]
                stateManager.put(store, row)
                numUpdatedStateRows += 1
              }
            }
            allRemovalsTimeMs += 0
            commitTimeMs += timeTakenMs {
              stateManager.commit(store)
            }
            setStoreMetrics(store)
            stateManager.values(store).map { valueRow =>
              numOutputRows += 1
              valueRow
            }

          // Update and output only rows being evicted from the StateStore
          // Assumption: watermark predicates must be non-empty if append mode is allowed
          case Some(Append) =>
            allUpdatesTimeMs += timeTakenMs {
              val filteredIter = applyRemovingRowsOlderThanWatermark(iter,
                watermarkPredicateForData.get)
              while (filteredIter.hasNext) {
                val row = filteredIter.next().asInstanceOf[UnsafeRow]
                stateManager.put(store, row)
                numUpdatedStateRows += 1
              }
            }

            val removalStartTimeNs = System.nanoTime
            val rangeIter = stateManager.iterator(store)

            new NextIterator[InternalRow] {
              override protected def getNext(): InternalRow = {
                var removedValueRow: InternalRow = null
                while(rangeIter.hasNext && removedValueRow == null) {
                  val rowPair = rangeIter.next()
                  if (watermarkPredicateForKeys.get.eval(rowPair.key)) {
                    stateManager.remove(store, rowPair.key)
                    removedValueRow = rowPair.value
                  }
                }
                if (removedValueRow == null) {
                  finished = true
                  null
                } else {
                  numOutputRows += 1
                  removedValueRow
                }
              }

              override protected def close(): Unit = {
                allRemovalsTimeMs += NANOSECONDS.toMillis(System.nanoTime - removalStartTimeNs)
                commitTimeMs += timeTakenMs { stateManager.commit(store) }
                setStoreMetrics(store)
              }
            }

          // Update and output modified rows from the StateStore.
          case Some(Update) =>

            new NextIterator[InternalRow] {
              // Filter late date using watermark if specified
              private[this] val baseIterator = watermarkPredicateForData match {
                case Some(predicate) => applyRemovingRowsOlderThanWatermark(iter, predicate)
                case None => iter
              }
              private val updatesStartTimeNs = System.nanoTime

              override protected def getNext(): InternalRow = {
                if (baseIterator.hasNext) {
                  val row = baseIterator.next().asInstanceOf[UnsafeRow]
                  stateManager.put(store, row)
                  numOutputRows += 1
                  numUpdatedStateRows += 1
                  row
                } else {
                  finished = true
                  null
                }
              }

              override protected def close(): Unit = {
                allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)

                // Remove old aggregates if watermark specified
                allRemovalsTimeMs += timeTakenMs {
                  removeKeysOlderThanWatermark(stateManager, store)
                }
                commitTimeMs += timeTakenMs { stateManager.commit(store) }
                setStoreMetrics(store)
              }
            }

          case _ => throw new UnsupportedOperationException(s"Invalid output mode: $outputMode")
        }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    if (keyExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(keyExpressions, stateInfo.map(_.numPartitions)) :: Nil
    }
  }

  override def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = {
    (outputMode.contains(Append) || outputMode.contains(Update)) &&
      eventTimeWatermark.isDefined &&
      newMetadata.batchWatermarkMs > eventTimeWatermark.get
  }
}

/**
 * This class sorts input rows and existing sessions in state and provides output rows as
 * sorted by "group keys + start time of session window".
 *
 * Refer [[MergingSortWithSessionWindowStateIterator]] for more details.
 */
case class SessionWindowStateStoreRestoreExec(
    keyWithoutSessionExpressions: Seq[Attribute],
    sessionExpression: Attribute,
    stateInfo: Option[StatefulOperatorStateInfo],
    eventTimeWatermark: Option[Long],
    child: SparkPlan)
  extends UnaryExecNode with StateStoreReader with WatermarkSupport {

  import StreamingSessionWindowHelper._

  override def keyExpressions: Seq[Attribute] = keyWithoutSessionExpressions

  private val storeConf = new StateStoreConf(sqlContext.conf)
  private val hadoopConfBcast = sparkContext.broadcast(
    new SerializableConfiguration(SessionState.newHadoopConf(
      sparkContext.hadoopConfiguration, sqlContext.conf)))

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    assert(keyExpressions.nonEmpty, "Grouping key must be specified when using sessionWindow")

    val stateVersion = conf.getConf(SQLConf.STREAMING_SESSION_WINDOW_STATE_FORMAT_VERSION)
    val stateStoreCoord = sqlContext.sessionState.streamingQueryManager.stateStoreCoordinator

    child.execute().mapPartitionsWithStateStoreAwareRDD(
      getStateInfo,
      StreamingSessionWindowStateManager.allStateStoreNames(stateVersion),
      stateStoreCoord) { case (partitionId, iter) =>

      // We need to filter out outdated inputs
      val filteredIterator = watermarkPredicateForData match {
        case Some(predicate) => iter.filter((row: InternalRow) => !predicate.eval(row))
        case None => iter
      }

      val stateStoreManager = StreamingSessionWindowStateManager.createStateManager(
        keyExpressions, sessionExpression, child.output, child.output, stateInfo, storeConf,
        hadoopConfBcast.value.value, partitionId, stateVersion)

      new MergingSortWithSessionWindowStateIterator(
        filteredIterator,
        stateStoreManager,
        keyWithoutSessionExpressions,
        sessionExpression,
        child.output).map { row =>
        numOutputRows += 1
        row
      }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = {
    (keyWithoutSessionExpressions ++ Seq(sessionExpression)).map(SortOrder(_, Ascending))
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    if (keyWithoutSessionExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(keyWithoutSessionExpressions, stateInfo.map(_.numPartitions)) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    Seq((keyWithoutSessionExpressions ++ Seq(sessionExpression)).map(SortOrder(_, Ascending)))
  }
}

/**
 * For each input tuple, the key is calculated and the tuple is `put` into the [[StateStore]].
 */
case class SessionWindowStateStoreSaveExec(
    keyExpressions: Seq[Attribute],
    sessionExpression: Attribute,
    stateInfo: Option[StatefulOperatorStateInfo] = None,
    outputMode: Option[OutputMode] = None,
    eventTimeWatermark: Option[Long] = None,
    child: SparkPlan)
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport {

  import StreamingSessionWindowHelper._

  private val storeConf = new StateStoreConf(sqlContext.conf)
  private val hadoopConfBcast = sparkContext.broadcast(
    new SerializableConfiguration(SessionState.newHadoopConf(
      sparkContext.hadoopConfiguration, sqlContext.conf)))

  private val childOutputSchema = child.output.toStructType
  private val childEncoder = RowEncoder(childOutputSchema).resolveAndBind().createDeserializer()

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver
    assert(outputMode.nonEmpty,
      "Incorrect planning in IncrementalExecution, outputMode has not been set")
    assert(keyExpressions.nonEmpty,
      "Grouping key must be specified when using sessionWindow")

    val stateVersion = conf.getConf(SQLConf.STREAMING_SESSION_WINDOW_STATE_FORMAT_VERSION)
    val stateStoreCoord = sqlContext.sessionState.streamingQueryManager.stateStoreCoordinator

    child.execute().mapPartitionsWithStateStoreAwareRDD(
      getStateInfo,
      StreamingSessionWindowStateManager.allStateStoreNames(stateVersion),
      stateStoreCoord) { case (partitionId, iter) =>

      val stateStoreManager = StreamingSessionWindowStateManager.createStateManager(
        keyExpressions, sessionExpression, child.output, child.output, stateInfo, storeConf,
        hadoopConfBcast.value.value, partitionId, stateVersion)

      val numOutputRows = longMetric("numOutputRows")
      val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
      val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
      val commitTimeMs = longMetric("commitTimeMs")

      outputMode match {
        // Update and output all rows in the StateStore.
        case Some(Complete) =>
          allUpdatesTimeMs += timeTakenMs {
            putToStore(iter, stateStoreManager, false)
          }
          allRemovalsTimeMs += 0
          updateStateMetrics(stateStoreManager)

          val output = stateStoreManager.getStates()
          numOutputRows += output.size

          commitTimeMs += timeTakenMs {
            stateStoreManager.commit()
          }

          output.toIterator

        // Update and output only rows being evicted from the StateStore
        // Assumption: watermark predicates must be non-empty if append mode is allowed
        case Some(Append) =>
          allUpdatesTimeMs += timeTakenMs {
            putToStore(iter, stateStoreManager, true)
          }

          val removalStartTimeNs = System.nanoTime

          new NextIterator[InternalRow] {
            private val evictedIter = stateStoreManager.removeByValueCondition(
              watermarkPredicateForData.get.eval)

            override protected def getNext(): InternalRow = {
              if (evictedIter.hasNext) {
                numOutputRows += 1
                evictedIter.next()
              } else {
                finished = true
                null
              }
            }

            override protected def close(): Unit = {
              allRemovalsTimeMs += NANOSECONDS.toMillis(System.nanoTime - removalStartTimeNs)
              commitTimeMs += timeTakenMs { stateStoreManager.commit() }
              updateStateMetrics(stateStoreManager)
            }
          }

        case Some(Update) =>
          val iterPutToStore = iteratorPutToStore(iter, stateStoreManager, true, true)
          new NextIterator[InternalRow] {
            private val updatesStartTimeNs = System.nanoTime

            override protected def getNext(): InternalRow = {
              if (iterPutToStore.hasNext) {
                iterPutToStore.next()
              } else {
                finished = true
                null
              }
            }

            override protected def close(): Unit = {
              allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)

              allRemovalsTimeMs += timeTakenMs {
                watermarkPredicateForData.foreach { pred =>
                  stateStoreManager.removeByValueCondition(pred.eval)
                }
              }
              commitTimeMs += timeTakenMs { stateStoreManager.commit() }
              updateStateMetrics(stateStoreManager)
            }
          }

        case _ => throw new UnsupportedOperationException(s"Invalid output mode: $outputMode")
      }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    if (keyExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(keyExpressions, stateInfo.map(_.numPartitions)) :: Nil
    }
  }

  override def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = {
    (outputMode.contains(Append) || outputMode.contains(Update)) &&
      eventTimeWatermark.isDefined &&
      newMetadata.batchWatermarkMs > eventTimeWatermark.get
  }

  private def updateStateMetrics(stateStoreManager: StreamingSessionWindowStateManager): Unit = {
    val updater = (storeMetrics: StateStoreMetrics) => {
      longMetric("numTotalStateRows") += storeMetrics.numKeys
      longMetric("stateMemory") += storeMetrics.memoryUsedBytes

      storeMetrics.customMetrics.foreach { case (metric, value) =>
        longMetric(metric.name) += value
      }
    }
    stateStoreManager.updateMetrics(updater)
  }

  private def iteratorPutToStore(
      baseIter: Iterator[InternalRow],
      stateManager: StreamingSessionWindowStateManager,
      needFilter: Boolean,
      returnOnlyUpdatedRows: Boolean): Iterator[InternalRow] = {
    val numUpdatedStateRows = longMetric("numUpdatedStateRows")
    val iter = if (needFilter) {
      baseIter.filter(row => !watermarkPredicateForData.get.eval(row))
    } else {
      baseIter
    }

    new NextIterator[InternalRow] {
      var curKey: UnsafeRow = null
      val curValuesOnKey = new ArrayBuffer[UnsafeRow]()

      private def applyChangesOnKey(): Unit = {
        if (curValuesOnKey.nonEmpty) {
          val updatedRows = if (needFilter) {
            replaceModifyWindow(curKey, curValuesOnKey.toSeq, stateManager)
          } else {
            stateManager.putStates(curKey, curValuesOnKey.toSeq)
            curValuesOnKey.length
          }

          numUpdatedStateRows += updatedRows
          curValuesOnKey.clear
        }
      }

      @tailrec
      override protected def getNext(): InternalRow = {
        if (!iter.hasNext) {
          applyChangesOnKey()
          finished = true
          return null
        }

        val row = iter.next().asInstanceOf[UnsafeRow]
        val key = stateManager.getKey(row)

        if (curKey == null || curKey != key) {
          // new group appears
          applyChangesOnKey()
          curKey = key.copy()
        }

        // must copy the row, for this row is a reference in iterator and
        // will change when iter.next
        curValuesOnKey += row.copy

        if (!returnOnlyUpdatedRows) {
          row
        } else {
          val startTime = stateManager.getStartTime(row)
          val stateRow = stateManager.getState(key, startTime)
          if (stateRow == null || !stateRow.equals(row)) {
            row
          } else {
            // current row isn't the "updated" row, continue to the next row
            getNext()
          }
        }
      }

      override protected def close(): Unit = {}
    }
  }

  private def putToStore(
      baseIter: Iterator[InternalRow],
      stateManager: StreamingSessionWindowStateManager,
      needFilter: Boolean) {
    val iterPutToStore = iteratorPutToStore(baseIter, stateManager, needFilter, false)
    while (iterPutToStore.hasNext) {
      iterPutToStore.next()
    }
  }

  /**
   * add to store, only update the changed window elements
   */
  private def replaceModifyWindow(
      key: UnsafeRow,
      values: Seq[UnsafeRow],
      stateManager: StreamingSessionWindowStateManager): Long = {
    val savedStates = stateManager.getStates(key)
    if (savedStates.isEmpty) {
      stateManager.putStates(key, values.toSeq)
      values.length
    } else {
      // get origin state from state store by specified key
      // filter out the window which have overlapped with new state
      val newStates = removeOverlapWindow(values.toSeq, savedStates)
      stateManager.putStates(key, newStates)
      newStates.length
    }
  }

  /**
   * Filter out the windows which belong to old state and have overlapped with the windows
   * in new state.
   *
   * 1. merge old windows(state) and new windows(state) into an array, and sorted it by startTime
   *    and endTime of window, see [[WindowOrdering]]
   *
   * 2. use a new array as result to store the valid window, go through the sorted array
   *    generated by step 1
   *    1) if the window belongs to new windows, put it into the result array directly
   *    2) if the window belongs to old windows, check whether it have overlapped with adjacent
   *       windows and latest window in result array. If no overlap, put it into result, otherwise
   *       filter it out
   *
   * 3. return the new array generated by step 2
   *
   * Note:
   *    1) old windows array and new windows array have not overlap elements individually
   *    2) The reason to check overlap with latest window in result array is that
   *       the window no overlap with adjacent windows in sorted array, however, may have overlapped
   *       with window in result array.
   *    e.g:
   *       latest window in result array is [12:00:00, 12:00:25], and sorted array contains windows:
   *       [12:00:01, 12:00:06], [12:00:08, 12:00:13], [12:00:15, 12:00:20]. For window
   *       [12:00:08, 12:00:13] have overlapped with [12:00:00, 12:00:25], so can't put it into
   *       result array
   */
  private def removeOverlapWindow(
      newWindows: Seq[UnsafeRow],
      oldWindows: Seq[UnsafeRow]): Seq[UnsafeRow] = {
    assert(newWindows.nonEmpty && oldWindows.nonEmpty,
      "new windows & old windows must be nonEmpty")

    val buffer = new ArrayBuffer[WindowRecord]()
    newWindows.foreach { window =>
      val times = extractTimePair(window)
      buffer += WindowRecord(times._1, times._2, true, window)
    }
    oldWindows.foreach { window =>
      val times = extractTimePair(window)
      buffer += WindowRecord(times._1, times._2, false, window)
    }

    // sort the buffer by startTime and endTime
    val sorted = buffer.sorted(WindowOrdering)
    val result = new ArrayBuffer[UnsafeRow]

    // the latest record in result buffer
    var latestValidRecord: WindowRecord = null

    // filter out the old windows that have overlap with new windows
    (0 to sorted.size - 1).foreach { i =>
      val record = sorted(i)
      if (record.isNew) { // record belong to new windows
        latestValidRecord = record
        result += record.row
      } else if (checkNoOverlap(i, latestValidRecord, sorted)) {
        // record belong to old windows and no overlap
        latestValidRecord = record
        result += record.row
      }
    }
    result.toSeq
  }

  private def checkNoOverlap(
      index: Int,
      latestValidRecord: WindowRecord,
      sorted: ArrayBuffer[WindowRecord]): Boolean = {
    val current = sorted(index)
    if (0 < index && index < sorted.size - 1) {
      checkNoOverlapBetweenRecord(current, sorted(index - 1)) &&
        checkNoOverlapBetweenRecord(current, sorted(index + 1)) &&
        (latestValidRecord == null || checkNoOverlapBetweenRecord(current, latestValidRecord))
    } else if (index == 0) { // first record
      checkNoOverlapBetweenRecord(current, sorted(index + 1))
    } else { // last record
      checkNoOverlapBetweenRecord(current, sorted(index - 1)) &&
        (latestValidRecord == null || checkNoOverlapBetweenRecord(current, latestValidRecord))
    }
  }

  private def checkNoOverlapBetweenRecord(a: WindowRecord, b: WindowRecord): Boolean = {
    (a.end < b.start || a.start > b.end)
  }

  // extract session_window (start, end) from UnsafeRow
  private def extractTimePair(ele: InternalRow): (Long, Long) = {
    val window = childEncoder(ele).getAs[Row]("session_window")
    (window.getAs[Timestamp]("start").getTime, window.getAs[Timestamp]("end").getTime)
  }

  /**
   * @param start  window's startTime which the row belong to
   * @param end    window's endTime which the row belong to
   * @param isNew  whether the row is belong to new windows
   */
  case class WindowRecord(start: Long, end: Long, isNew: Boolean, row: UnsafeRow)

  object WindowOrdering extends Ordering[WindowRecord] {
    def compare(a: WindowRecord, b: WindowRecord): Int = {
      var res = a.start compare b.start
      if (res == 0) {
        res = a.end compare b.end
      }
      res
    }
  }
}

/** Physical operator for executing streaming Deduplicate. */
case class StreamingDeduplicateExec(
    keyExpressions: Seq[Attribute],
    child: SparkPlan,
    stateInfo: Option[StatefulOperatorStateInfo] = None,
    eventTimeWatermark: Option[Long] = None)
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport {

  /** Distribute by grouping attributes */
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(keyExpressions, stateInfo.map(_.numPartitions)) :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    child.execute().mapPartitionsWithStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      child.output.toStructType,
      indexOrdinal = None,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator),
      // We won't check value row in state store since the value StreamingDeduplicateExec.EMPTY_ROW
      // is unrelated to the output schema.
      Map(StateStoreConf.FORMAT_VALIDATION_CHECK_VALUE_CONFIG -> "false")) { (store, iter) =>
      val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)
      val numOutputRows = longMetric("numOutputRows")
      val numTotalStateRows = longMetric("numTotalStateRows")
      val numUpdatedStateRows = longMetric("numUpdatedStateRows")
      val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
      val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
      val commitTimeMs = longMetric("commitTimeMs")

      val baseIterator = watermarkPredicateForData match {
        case Some(predicate) => applyRemovingRowsOlderThanWatermark(iter, predicate)
        case None => iter
      }

      val updatesStartTimeNs = System.nanoTime

      val result = baseIterator.filter { r =>
        val row = r.asInstanceOf[UnsafeRow]
        val key = getKey(row)
        val value = store.get(key)
        if (value == null) {
          store.put(key, StreamingDeduplicateExec.EMPTY_ROW)
          numUpdatedStateRows += 1
          numOutputRows += 1
          true
        } else {
          // Drop duplicated rows
          false
        }
      }

      CompletionIterator[InternalRow, Iterator[InternalRow]](result, {
        allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)
        allRemovalsTimeMs += timeTakenMs { removeKeysOlderThanWatermark(store) }
        commitTimeMs += timeTakenMs { store.commit() }
        setStoreMetrics(store)
      })
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = {
    eventTimeWatermark.isDefined && newMetadata.batchWatermarkMs > eventTimeWatermark.get
  }
}

object StreamingDeduplicateExec {
  private val EMPTY_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))
}
