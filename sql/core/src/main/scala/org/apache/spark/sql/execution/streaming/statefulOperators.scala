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

import java.util.UUID
import java.util.concurrent.TimeUnit._

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{OutputMode, StateOperatorProgress}
import org.apache.spark.sql.types._
import org.apache.spark.util.{CompletionIterator, NextIterator, Utils}


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

/**
 * Custom stateful operator metric definition to allow operators to expose their own custom metrics.
 * Also provides [[SQLMetric]] instance to show the metric in UI and accumulate it at the query
 * level.
 */
trait StatefulOperatorCustomMetric {
  def name: String
  def desc: String
  def createSQLMetric(sparkContext: SparkContext): SQLMetric
}

/** Custom stateful operator metric for simple "count" gauge */
case class StatefulOperatorCustomSumMetric(name: String, desc: String)
  extends StatefulOperatorCustomMetric {
  override def createSQLMetric(sparkContext: SparkContext): SQLMetric =
    SQLMetrics.createMetric(sparkContext, desc)
}

/** An operator that reads from a StateStore. */
trait StateStoreReader extends StatefulOperator {
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))
}

/** An operator that writes to a StateStore. */
trait StateStoreWriter extends StatefulOperator { self: SparkPlan =>

  override lazy val metrics = statefulOperatorCustomMetrics ++ Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numRowsDroppedByWatermark" -> SQLMetrics.createMetric(sparkContext,
      "number of rows which are dropped by watermark"),
    "numTotalStateRows" -> SQLMetrics.createMetric(sparkContext, "number of total state rows"),
    "numUpdatedStateRows" -> SQLMetrics.createMetric(sparkContext, "number of updated state rows"),
    "allUpdatesTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "time to update"),
    "numRemovedStateRows" -> SQLMetrics.createMetric(sparkContext, "number of removed state rows"),
    "allRemovalsTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "time to remove"),
    "commitTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "time to commit changes"),
    "stateMemory" -> SQLMetrics.createSizeMetric(sparkContext, "memory used by state"),
    "numShufflePartitions" -> SQLMetrics.createMetric(sparkContext, "number of shuffle partitions"),
    "numStateStoreInstances" -> SQLMetrics.createMetric(sparkContext,
      "number of state store instances")
  ) ++ stateStoreCustomMetrics

  /**
   * Get the progress made by this stateful operator after execution. This should be called in
   * the driver after this SparkPlan has been executed and metrics have been updated.
   */
  def getProgress(): StateOperatorProgress = {
    val customMetrics = (stateStoreCustomMetrics ++ statefulOperatorCustomMetrics)
      .map(entry => entry._1 -> longMetric(entry._1).value)

    val javaConvertedCustomMetrics: java.util.HashMap[String, java.lang.Long] =
      new java.util.HashMap(customMetrics.mapValues(long2Long).toMap.asJava)

    new StateOperatorProgress(
      operatorName = shortName,
      numRowsTotal = longMetric("numTotalStateRows").value,
      numRowsUpdated = longMetric("numUpdatedStateRows").value,
      allUpdatesTimeMs = longMetric("allUpdatesTimeMs").value,
      numRowsRemoved = longMetric("numRemovedStateRows").value,
      allRemovalsTimeMs = longMetric("allRemovalsTimeMs").value,
      commitTimeMs = longMetric("commitTimeMs").value,
      memoryUsedBytes = longMetric("stateMemory").value,
      numRowsDroppedByWatermark = longMetric("numRowsDroppedByWatermark").value,
      numShufflePartitions = longMetric("numShufflePartitions").value,
      numStateStoreInstances = longMetric("numStateStoreInstances").value,
      javaConvertedCustomMetrics
    )
  }

  /** Records the duration of running `body` for the next query progress update. */
  protected def timeTakenMs(body: => Unit): Long = Utils.timeTakenMs(body)._2

  /** Set the operator level metrics */
  protected def setOperatorMetrics(numStateStoreInstances: Int = 1): Unit = {
    assert(numStateStoreInstances >= 1, s"invalid number of stores: $numStateStoreInstances")
    // Shuffle partitions capture the number of tasks that have this stateful operator instance.
    // For each task instance this number is incremented by one.
    longMetric("numShufflePartitions") += 1
    longMetric("numStateStoreInstances") += numStateStoreInstances
  }

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
    val provider = StateStoreProvider.create(conf.stateStoreProviderClass)
    provider.supportedCustomMetrics.map {
      metric => (metric.name, metric.createSQLMetric(sparkContext))
    }.toMap
  }

  /**
   * Set of stateful operator custom metrics. These are captured as part of the generic
   * key-value map [[StateOperatorProgress.customMetrics]].
   * Stateful operators can extend this method to provide their own unique custom metrics.
   */
  protected def customStatefulOperatorMetrics: Seq[StatefulOperatorCustomMetric] = Nil

  private def statefulOperatorCustomMetrics: Map[String, SQLMetric] = {
    customStatefulOperatorMetrics.map {
      metric => (metric.name, metric.createSQLMetric(sparkContext))
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

  /** Name to output in [[StreamingOperatorProgress]] to identify operator type */
  protected def shortName: String = "defaultName"

  /**
   * Should the MicroBatchExecution run another batch based on this stateful operator and the
   * current updated metadata.
   */
  def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = false
}

/** An operator that supports watermark. */
trait WatermarkSupport extends SparkPlan {

  def child: SparkPlan

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
      val numRemovedStateRows = longMetric("numRemovedStateRows")
      store.iterator().foreach { rowPair =>
        if (watermarkPredicateForKeys.get.eval(rowPair.key)) {
          store.remove(rowPair.key)
          numRemovedStateRows += 1
        }
      }
    }
  }

  protected def removeKeysOlderThanWatermark(
      storeManager: StreamingAggregationStateManager,
      store: StateStore): Unit = {
    if (watermarkPredicateForKeys.nonEmpty) {
      val numRemovedStateRows = longMetric("numRemovedStateRows")
      storeManager.keys(store).foreach { keyRow =>
        if (watermarkPredicateForKeys.get.eval(keyRow)) {
          storeManager.remove(store, keyRow)
          numRemovedStateRows += 1
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
      numColsPrefixKey = 0,
      session.sessionState,
      Some(session.streams.stateStoreCoordinator)) { case (store, iter) =>
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
            val outputRows = Option(restoredRow).toSeq :+ row
            numOutputRows += outputRows.size
            outputRows
          }
        }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    // NOTE: Please read through the NOTE on the classdoc of StatefulOpClusteredDistribution
    // before making any changes.
    // TODO(SPARK-38204)
    if (keyExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(keyExpressions,
        requiredNumPartitions = stateInfo.map(_.numPartitions)) :: Nil
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): StateStoreRestoreExec =
    copy(child = newChild)
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
      numColsPrefixKey = 0,
      session.sessionState,
      Some(session.streams.stateStoreCoordinator)) { (store, iter) =>
        val numOutputRows = longMetric("numOutputRows")
        val numUpdatedStateRows = longMetric("numUpdatedStateRows")
        val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
        val numRemovedStateRows = longMetric("numRemovedStateRows")
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
            setOperatorMetrics()
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
                    numRemovedStateRows += 1
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
                // Note: Due to the iterator lazy exec, this metric also captures the time taken
                // by the consumer operators in addition to the processing in this operator.
                allRemovalsTimeMs += NANOSECONDS.toMillis(System.nanoTime - removalStartTimeNs)
                commitTimeMs += timeTakenMs { stateManager.commit(store) }
                setStoreMetrics(store)
                setOperatorMetrics()
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
                setOperatorMetrics()
              }
            }

          case _ => throw QueryExecutionErrors.invalidStreamingOutputModeError(outputMode)
        }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    // NOTE: Please read through the NOTE on the classdoc of StatefulOpClusteredDistribution
    // before making any changes.
    // TODO(SPARK-38204)
    if (keyExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(keyExpressions,
        requiredNumPartitions = stateInfo.map(_.numPartitions)) :: Nil
    }
  }

  override def shortName: String = "stateStoreSave"

  override def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = {
    (outputMode.contains(Append) || outputMode.contains(Update)) &&
      eventTimeWatermark.isDefined &&
      newMetadata.batchWatermarkMs > eventTimeWatermark.get
  }

  override protected def withNewChildInternal(newChild: SparkPlan): StateStoreSaveExec =
    copy(child = newChild)
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
    stateFormatVersion: Int,
    child: SparkPlan)
  extends UnaryExecNode with StateStoreReader with WatermarkSupport {

  override def keyExpressions: Seq[Attribute] = keyWithoutSessionExpressions

  assert(keyExpressions.nonEmpty, "Grouping key must be specified when using sessionWindow")

  private val stateManager = StreamingSessionWindowStateManager.createStateManager(
    keyWithoutSessionExpressions, sessionExpression, child.output, stateFormatVersion)

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    child.execute().mapPartitionsWithReadStateStore(
      getStateInfo,
      stateManager.getStateKeySchema,
      stateManager.getStateValueSchema,
      numColsPrefixKey = stateManager.getNumColsForPrefixKey,
      session.sessionState,
      Some(session.streams.stateStoreCoordinator)) { case (store, iter) =>

      // We need to filter out outdated inputs
      val filteredIterator = watermarkPredicateForData match {
        case Some(predicate) => iter.filter((row: InternalRow) => !predicate.eval(row))
        case None => iter
      }

      new MergingSortWithSessionWindowStateIterator(
        filteredIterator,
        stateManager,
        store,
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
    // NOTE: Please read through the NOTE on the classdoc of StatefulOpClusteredDistribution
    // before making any changes.
    // TODO(SPARK-38204)
    ClusteredDistribution(keyWithoutSessionExpressions,
      requiredNumPartitions = stateInfo.map(_.numPartitions)) :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    Seq((keyWithoutSessionExpressions ++ Seq(sessionExpression)).map(SortOrder(_, Ascending)))
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

/**
 * This class replaces existing sessions for the grouping key with new sessions in state store.
 * All inputs are valid on storing into state store; don't filter out via watermark while storing.
 * Refer the method doc of [[StreamingSessionWindowStateManager.updateSessions]] for more details.
 *
 * This class will provide the output according to the output mode.
 * Update mode is not supported as the semantic is not feasible for session window.
 */
case class SessionWindowStateStoreSaveExec(
    keyWithoutSessionExpressions: Seq[Attribute],
    sessionExpression: Attribute,
    stateInfo: Option[StatefulOperatorStateInfo] = None,
    outputMode: Option[OutputMode] = None,
    eventTimeWatermark: Option[Long] = None,
    stateFormatVersion: Int,
    child: SparkPlan)
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport {

  override def keyExpressions: Seq[Attribute] = keyWithoutSessionExpressions

  private val stateManager = StreamingSessionWindowStateManager.createStateManager(
    keyWithoutSessionExpressions, sessionExpression, child.output, stateFormatVersion)

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver
    assert(outputMode.nonEmpty,
      "Incorrect planning in IncrementalExecution, outputMode has not been set")
    assert(keyExpressions.nonEmpty,
      "Grouping key must be specified when using sessionWindow")

    child.execute().mapPartitionsWithStateStore(
      getStateInfo,
      stateManager.getStateKeySchema,
      stateManager.getStateValueSchema,
      numColsPrefixKey = stateManager.getNumColsForPrefixKey,
      session.sessionState,
      Some(session.streams.stateStoreCoordinator)) { case (store, iter) =>

      val numOutputRows = longMetric("numOutputRows")
      val numRemovedStateRows = longMetric("numRemovedStateRows")
      val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
      val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
      val commitTimeMs = longMetric("commitTimeMs")

      outputMode match {
        // Update and output all rows in the StateStore.
        case Some(Complete) =>
          allUpdatesTimeMs += timeTakenMs {
            putToStore(iter, store)
          }
          commitTimeMs += timeTakenMs {
            stateManager.commit(store)
          }
          setStoreMetrics(store)
          stateManager.iterator(store).map { row =>
            numOutputRows += 1
            row
          }

        // Update and output only rows being evicted from the StateStore
        // Assumption: watermark predicates must be non-empty if append mode is allowed
        case Some(Append) =>
          allUpdatesTimeMs += timeTakenMs {
            putToStore(iter, store)
          }

          val removalStartTimeNs = System.nanoTime
          new NextIterator[InternalRow] {
            private val removedIter = stateManager.removeByValueCondition(
              store, watermarkPredicateForData.get.eval)

            override protected def getNext(): InternalRow = {
              if (!removedIter.hasNext) {
                finished = true
                null
              } else {
                numRemovedStateRows += 1
                numOutputRows += 1
                removedIter.next()
              }
            }

            override protected def close(): Unit = {
              allRemovalsTimeMs += NANOSECONDS.toMillis(System.nanoTime - removalStartTimeNs)
              commitTimeMs += timeTakenMs { store.commit() }
              setStoreMetrics(store)
              setOperatorMetrics()
            }
          }

        case _ => throw QueryExecutionErrors.invalidStreamingOutputModeError(outputMode)
      }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    // NOTE: Please read through the NOTE on the classdoc of StatefulOpClusteredDistribution
    // before making any changes.
    // TODO(SPARK-38204)
    ClusteredDistribution(keyExpressions,
      requiredNumPartitions = stateInfo.map(_.numPartitions)) :: Nil
  }

  override def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = {
    (outputMode.contains(Append) || outputMode.contains(Update)) &&
      eventTimeWatermark.isDefined &&
      newMetadata.batchWatermarkMs > eventTimeWatermark.get
  }

  private def putToStore(iter: Iterator[InternalRow], store: StateStore): Unit = {
    val numUpdatedStateRows = longMetric("numUpdatedStateRows")
    val numRemovedStateRows = longMetric("numRemovedStateRows")

    var curKey: UnsafeRow = null
    val curValuesOnKey = new mutable.ArrayBuffer[UnsafeRow]()

    def applyChangesOnKey(): Unit = {
      if (curValuesOnKey.nonEmpty) {
        val (upserted, deleted) = stateManager.updateSessions(store, curKey, curValuesOnKey.toSeq)
        numUpdatedStateRows += upserted
        numRemovedStateRows += deleted
        curValuesOnKey.clear
      }
    }

    while (iter.hasNext) {
      val row = iter.next().asInstanceOf[UnsafeRow]
      val key = stateManager.extractKeyWithoutSession(row)

      if (curKey == null || curKey != key) {
        // new group appears
        applyChangesOnKey()
        curKey = key.copy()
      }

      // must copy the row, for this row is a reference in iterator and
      // will change when iter.next
      curValuesOnKey += row.copy
    }

    applyChangesOnKey()
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}


/** Physical operator for executing streaming Deduplicate. */
case class StreamingDeduplicateExec(
    keyExpressions: Seq[Attribute],
    child: SparkPlan,
    stateInfo: Option[StatefulOperatorStateInfo] = None,
    eventTimeWatermark: Option[Long] = None)
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport {

  /** Distribute by grouping attributes */
  override def requiredChildDistribution: Seq[Distribution] = {
    // NOTE: Please read through the NOTE on the classdoc of StatefulOpClusteredDistribution
    // before making any changes.
    // TODO(SPARK-38204)
    ClusteredDistribution(keyExpressions,
      requiredNumPartitions = stateInfo.map(_.numPartitions)) :: Nil
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    child.execute().mapPartitionsWithStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      child.output.toStructType,
      numColsPrefixKey = 0,
      session.sessionState,
      Some(session.streams.stateStoreCoordinator),
      // We won't check value row in state store since the value StreamingDeduplicateExec.EMPTY_ROW
      // is unrelated to the output schema.
      Map(StateStoreConf.FORMAT_VALIDATION_CHECK_VALUE_CONFIG -> "false")) { (store, iter) =>
      val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)
      val numOutputRows = longMetric("numOutputRows")
      val numUpdatedStateRows = longMetric("numUpdatedStateRows")
      val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
      val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
      val commitTimeMs = longMetric("commitTimeMs")
      val numDroppedDuplicateRows = longMetric("numDroppedDuplicateRows")

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
          numDroppedDuplicateRows += 1
          false
        }
      }

      CompletionIterator[InternalRow, Iterator[InternalRow]](result, {
        allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)
        allRemovalsTimeMs += timeTakenMs { removeKeysOlderThanWatermark(store) }
        commitTimeMs += timeTakenMs { store.commit() }
        setStoreMetrics(store)
        setOperatorMetrics()
      })
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def customStatefulOperatorMetrics: Seq[StatefulOperatorCustomMetric] = {
    Seq(StatefulOperatorCustomSumMetric("numDroppedDuplicateRows", "number of duplicates dropped"))
  }

  override def shortName: String = "dedupe"

  override def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = {
    eventTimeWatermark.isDefined && newMetadata.batchWatermarkMs > eventTimeWatermark.get
  }

  override protected def withNewChildInternal(newChild: SparkPlan): StreamingDeduplicateExec =
    copy(child = newChild)
}

object StreamingDeduplicateExec {
  private val EMPTY_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))
}
