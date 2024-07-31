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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python.PythonSQLMetrics
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SQLConf
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

  def getStateInfo: StatefulOperatorStateInfo = {
    stateInfo.getOrElse {
      throw new IllegalStateException("State location not present for execution")
    }
  }

  def metadataFilePath(): Path = {
    val stateCheckpointPath =
      new Path(getStateInfo.checkpointLocation, getStateInfo.operatorId.toString)
    new Path(new Path(stateCheckpointPath, "_metadata"), "metadata")
  }

  // Function used to record state schema for the first time and validate it against proposed
  // schema changes in the future. Runs as part of a planning rule on the driver.
  // Returns the schema file path for operators that write this to the metadata file,
  // otherwise None
  def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration, batchId: Long, stateSchemaVersion: Int):
    List[StateSchemaValidationResult]
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
trait StateStoreWriter extends StatefulOperator with PythonSQLMetrics { self: SparkPlan =>

  /**
   * Produce the output watermark for given input watermark (ms).
   *
   * In most cases, this is same as the criteria of state eviction, as most stateful operators
   * produce the output from two different kinds:
   *
   * 1. without buffering
   * 2. with buffering (state)
   *
   * The state eviction happens when event time exceeds a "certain threshold of timestamp", which
   * denotes a lower bound of event time values for output (output watermark).
   *
   * The default implementation provides the input watermark as it is. Most built-in operators
   * will evict based on min input watermark and ensure it will be minimum of the event time value
   * for the output so far (including output from eviction). Operators which behave differently
   * (e.g. different criteria on eviction) must override this method.
   *
   * Note that the default behavior wil advance the watermark aggressively to simplify the logic,
   * but it does not break the semantic of output watermark, which is following:
   *
   * An operator guarantees that it will not emit record with an event timestamp lower than its
   * output watermark.
   *
   * For example, for 5 minutes time window aggregation, the advancement of watermark can happen
   * "before" the window has been evicted and produced as output. Say, suppose there's an window
   * in state: [0, 5) and input watermark = 3. Although there is no output for this operator, this
   * operator will produce an output watermark as 3. It's still respecting the guarantee, as the
   * operator will produce the window [0, 5) only when the output watermark is equal or greater
   * than 5, and the downstream operator will process the input data, "and then" advance the
   * watermark. Hence this window is considered as "non-late" record.
   */
  def produceOutputWatermark(inputWatermarkMs: Long): Option[Long] = Some(inputWatermarkMs)

  def operatorStateMetadataVersion: Int = 1

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
    "numStateStoreInstances" -> SQLMetrics.createMetric(sparkContext,
      "number of state store instances")
  ) ++ stateStoreCustomMetrics ++ pythonMetrics

  def stateSchemaFilePath(storeName: Option[String] = None): Path = {
    def stateInfo = getStateInfo
    val stateCheckpointPath =
      new Path(getStateInfo.checkpointLocation,
        s"${stateInfo.operatorId.toString}")
    storeName match {
      case Some(storeName) =>
        val storeNamePath = new Path(stateCheckpointPath, storeName)
        new Path(new Path(storeNamePath, "_metadata"), "schema")
      case None =>
        new Path(new Path(stateCheckpointPath, "_metadata"), "schema")
    }
  }

  /**
   * Get the progress made by this stateful operator after execution. This should be called in
   * the driver after this SparkPlan has been executed and metrics have been updated.
   */
  def getProgress(): StateOperatorProgress = {
    val customMetrics = (stateStoreCustomMetrics ++ statefulOperatorCustomMetrics)
      .map(entry => entry._1 -> longMetric(entry._1).value)

    val javaConvertedCustomMetrics: java.util.HashMap[String, java.lang.Long] =
      new java.util.HashMap(customMetrics.transform((_, v) => long2Long(v)).asJava)

    // We now don't report number of shuffle partitions inside the state operator. Instead,
    // it will be filled when the stream query progress is reported
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
      numShufflePartitions = stateInfo.map(_.numPartitions.toLong).getOrElse(-1L),
      numStateStoreInstances = longMetric("numStateStoreInstances").value,
      javaConvertedCustomMetrics
    )
  }

  /** Records the duration of running `body` for the next query progress update. */
  protected def timeTakenMs(body: => Unit): Long = Utils.timeTakenMs(body)._2

  /** Metadata of this stateful operator and its states stores. */
  def operatorStateMetadata(
      stateSchemaPaths: List[String] = List.empty): OperatorStateMetadata = {
    val info = getStateInfo
    val operatorInfo = OperatorInfoV1(info.operatorId, shortName)
    val stateStoreInfo =
      Array(StateStoreMetadataV1(StateStoreId.DEFAULT_STORE_NAME, 0, info.numPartitions))
    OperatorStateMetadataV1(operatorInfo, stateStoreInfo)
  }

  /** Set the operator level metrics */
  protected def setOperatorMetrics(numStateStoreInstances: Int = 1): Unit = {
    assert(numStateStoreInstances >= 1, s"invalid number of stores: $numStateStoreInstances")
    // Shuffle partitions capture the number of tasks that have this stateful operator instance.
    // For each task instance this number is incremented by one.
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
  def shortName: String = "defaultName"

  def validateNewMetadata(
      oldMetadata: OperatorStateMetadata,
      newMetadata: OperatorStateMetadata): Unit = {}

  /**
   * Should the MicroBatchExecution run another batch based on this stateful operator and the
   * new input watermark.
   */
  def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = false
}

/** An operator that supports watermark. */
trait WatermarkSupport extends SparkPlan {

  def child: SparkPlan

  /** The keys that may have a watermark attribute. */
  def keyExpressions: Seq[Attribute]

  /**
   * The watermark value for filtering late events/records. This should be the previous
   * batch state eviction watermark.
   */
  def eventTimeWatermarkForLateEvents: Option[Long]
  /**
   * The watermark value for closing aggregates and evicting state.
   * It is different from the late events filtering watermark (consider chained aggregators
   * agg1 -> agg2: agg1 evicts state which will be effectively late against the eviction watermark
   * but should not be late for agg2 input late record filtering watermark. Thus agg1 and agg2 use
   * the current batch watermark for state eviction but the previous batch watermark for late
   * record filtering.
   */
  def eventTimeWatermarkForEviction: Option[Long]

  /** Generate an expression that matches data older than late event filtering watermark */
  lazy val watermarkExpressionForLateEvents: Option[Expression] =
    watermarkExpression(eventTimeWatermarkForLateEvents)
  /** Generate an expression that matches data older than the state eviction watermark */
  lazy val watermarkExpressionForEviction: Option[Expression] =
    watermarkExpression(eventTimeWatermarkForEviction)

  lazy val allowMultipleStatefulOperators: Boolean =
    conf.getConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE)

  /** Generate an expression that matches data older than the watermark */
  private def watermarkExpression(watermark: Option[Long]): Option[Expression] = {
    WatermarkSupport.watermarkExpression(
      WatermarkSupport.findEventTimeColumn(child.output,
        allowMultipleEventTimeColumns = !allowMultipleStatefulOperators), watermark)
  }

  /** Predicate based on keys that matches data older than the late event filtering watermark */
  lazy val watermarkPredicateForKeysForLateEvents: Option[BasePredicate] =
    watermarkPredicateForKeys(watermarkExpressionForLateEvents)

  /** Generate an expression that matches data older than the state eviction watermark */
  lazy val watermarkPredicateForKeysForEviction: Option[BasePredicate] =
    watermarkPredicateForKeys(watermarkExpressionForEviction)

  private def watermarkPredicateForKeys(
      watermarkExpression: Option[Expression]): Option[BasePredicate] = {
    watermarkExpression.flatMap { e =>
      if (keyExpressions.exists(_.metadata.contains(EventTimeWatermark.delayKey))) {
        Some(Predicate.create(e, keyExpressions))
      } else {
        None
      }
    }
  }

  /**
   * Predicate based on the child output that matches data older than the watermark for late events
   * filtering.
   */
  lazy val watermarkPredicateForDataForLateEvents: Option[BasePredicate] =
    watermarkPredicateForData(watermarkExpressionForLateEvents)

  lazy val watermarkPredicateForDataForEviction: Option[BasePredicate] =
    watermarkPredicateForData(watermarkExpressionForEviction)

  private def watermarkPredicateForData(
    watermarkExpression: Option[Expression]): Option[BasePredicate] = {
    watermarkExpression.map(Predicate.create(_, child.output))
  }

  protected def removeKeysOlderThanWatermark(store: StateStore): Unit = {
    if (watermarkPredicateForKeysForEviction.nonEmpty) {
      val numRemovedStateRows = longMetric("numRemovedStateRows")
      store.iterator().foreach { rowPair =>
        if (watermarkPredicateForKeysForEviction.get.eval(rowPair.key)) {
          store.remove(rowPair.key)
          numRemovedStateRows += 1
        }
      }
    }
  }

  protected def removeKeysOlderThanWatermark(
      storeManager: StreamingAggregationStateManager,
      store: StateStore): Unit = {
    if (watermarkPredicateForKeysForEviction.nonEmpty) {
      val numRemovedStateRows = longMetric("numRemovedStateRows")
      storeManager.keys(store).foreach { keyRow =>
        if (watermarkPredicateForKeysForEviction.get.eval(keyRow)) {
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
    // If we are evicting based on a window, use the end of the window. Otherwise just
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

  /**
   * Find the column which is marked as "event time" column.
   *
   * If there are multiple event time columns in given column list, the behavior depends on the
   * parameter `allowMultipleEventTimeColumns`. If it's set to true, the first occurred column will
   * be returned. If not, this method will throw an AnalysisException as it is not allowed to have
   * multiple event time columns.
   */
  def findEventTimeColumn(
      attrs: Seq[Attribute],
      allowMultipleEventTimeColumns: Boolean): Option[Attribute] = {
    val eventTimeCols = attrs.filter(_.metadata.contains(EventTimeWatermark.delayKey))
    if (!allowMultipleEventTimeColumns) {
      // There is a case projection leads the same column (same exprId) to appear more than one
      // time. Allowing them does not hurt the correctness of state row eviction, hence let's start
      // with allowing them.
      val eventTimeColsSet = eventTimeCols.map(_.exprId).toSet
      if (eventTimeColsSet.size > 1) {
        throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3077",
          messageParameters = Map("eventTimeCols" -> eventTimeCols.mkString("(", ",", ")")))
      }

      // With above check, even there are multiple columns in eventTimeCols, all columns must be
      // the same.
    } else {
      // This is for compatibility with previous behavior - we allow multiple distinct event time
      // columns and pick up the first occurrence. This is incorrect if non-first occurrence is
      // not smaller than the first one, but allow this as "escape hatch" in case we break the
      // existing query.
    }
    // pick the first element if exists
    eventTimeCols.headOption
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

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration, batchId: Long, stateSchemaVersion: Int):
    List[StateSchemaValidationResult] = {
    val newStateSchema = List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME,
      keyExpressions.toStructType, stateManager.getStateValueSchema))
    List(StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema(getStateInfo,
      hadoopConf, newStateSchema, session.sessionState, stateSchemaVersion))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    child.execute().mapPartitionsWithReadStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      stateManager.getStateValueSchema,
      NoPrefixKeyStateEncoderSpec(keyExpressions.toStructType),
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
    if (keyExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      StatefulOperatorPartitioning.getCompatibleDistribution(
        keyExpressions, getStateInfo, conf) :: Nil
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
    eventTimeWatermarkForLateEvents: Option[Long] = None,
    eventTimeWatermarkForEviction: Option[Long] = None,
    stateFormatVersion: Int,
    child: SparkPlan)
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport {

  private[sql] val stateManager = StreamingAggregationStateManager.createStateManager(
    keyExpressions, child.output, stateFormatVersion)

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration,
      batchId: Long,
      stateSchemaVersion: Int): List[StateSchemaValidationResult] = {
    val newStateSchema = List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME,
      keyExpressions.toStructType, stateManager.getStateValueSchema))
    List(StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema(getStateInfo,
      hadoopConf, newStateSchema, session.sessionState, stateSchemaVersion))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver
    assert(outputMode.nonEmpty,
      "Incorrect planning in IncrementalExecution, outputMode has not been set")

    child.execute().mapPartitionsWithStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      stateManager.getStateValueSchema,
      NoPrefixKeyStateEncoderSpec(keyExpressions.toStructType),
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

            // SPARK-45582 - Ensure that store instance is not used after commit is called
            // to invoke the iterator.
            val rangeIter = stateManager.values(store)

            new NextIterator[InternalRow] {
              override protected def getNext(): InternalRow = {
                if (rangeIter.hasNext) {
                  val valueRow = rangeIter.next()
                  numOutputRows += 1
                  valueRow
                } else {
                  finished = true
                  null
                }
              }

              override protected def close(): Unit = {
                allRemovalsTimeMs += 0
                commitTimeMs += timeTakenMs {
                  stateManager.commit(store)
                }
                setStoreMetrics(store)
                setOperatorMetrics()
              }
            }

          // Update and output only rows being evicted from the StateStore
          // Assumption: watermark predicates must be non-empty if append mode is allowed
          case Some(Append) =>
            assert(watermarkPredicateForDataForLateEvents.isDefined,
              "Watermark needs to be defined for streaming aggregation query in append mode")

            assert(watermarkPredicateForKeysForEviction.isDefined,
              "Watermark needs to be defined for streaming aggregation query in append mode")

            allUpdatesTimeMs += timeTakenMs {
              val filteredIter = applyRemovingRowsOlderThanWatermark(iter,
                watermarkPredicateForDataForLateEvents.get)
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
                  if (watermarkPredicateForKeysForEviction.get.eval(rowPair.key)) {
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
              private[this] val baseIterator = watermarkPredicateForDataForLateEvents match {
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
    if (keyExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      StatefulOperatorPartitioning.getCompatibleDistribution(
        keyExpressions, getStateInfo, conf) :: Nil
    }
  }

  override def shortName: String = "stateStoreSave"

  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = {
    (outputMode.contains(Append) || outputMode.contains(Update)) &&
      eventTimeWatermarkForEviction.isDefined &&
      newInputWatermark > eventTimeWatermarkForEviction.get
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
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    stateFormatVersion: Int,
    child: SparkPlan)
  extends UnaryExecNode with StateStoreReader with WatermarkSupport {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numRowsDroppedByWatermark" -> SQLMetrics.createMetric(sparkContext,
      "number of rows which are dropped by watermark")
  )

  override def keyExpressions: Seq[Attribute] = keyWithoutSessionExpressions

  assert(keyExpressions.nonEmpty, "Grouping key must be specified when using sessionWindow")

  private val stateManager = StreamingSessionWindowStateManager.createStateManager(
    keyWithoutSessionExpressions, sessionExpression, child.output, stateFormatVersion)

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration, batchId: Long, stateSchemaVersion: Int):
    List[StateSchemaValidationResult] = {
    val newStateSchema = List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME,
      stateManager.getStateKeySchema, stateManager.getStateValueSchema))
    List(StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema(getStateInfo, hadoopConf,
      newStateSchema, session.sessionState, stateSchemaVersion))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    child.execute().mapPartitionsWithReadStateStore(
      getStateInfo,
      stateManager.getStateKeySchema,
      stateManager.getStateValueSchema,
      PrefixKeyScanStateEncoderSpec(stateManager.getStateKeySchema,
        stateManager.getNumColsForPrefixKey),
      session.sessionState,
      Some(session.streams.stateStoreCoordinator)) { case (store, iter) =>

      // We need to filter out outdated inputs
      val filteredIterator = watermarkPredicateForDataForLateEvents match {
        case Some(predicate) => iter.filter((row: InternalRow) => {
          val shouldKeep = !predicate.eval(row)
          if (!shouldKeep) longMetric("numRowsDroppedByWatermark") += 1
          shouldKeep
        })
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
    StatefulOperatorPartitioning.getCompatibleDistribution(
      keyWithoutSessionExpressions, getStateInfo, conf) :: Nil
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
    eventTimeWatermarkForLateEvents: Option[Long] = None,
    eventTimeWatermarkForEviction: Option[Long] = None,
    stateFormatVersion: Int,
    child: SparkPlan)
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport {

  override def keyExpressions: Seq[Attribute] = keyWithoutSessionExpressions

  override def shortName: String = "sessionWindowStateStoreSaveExec"

  private val stateManager = StreamingSessionWindowStateManager.createStateManager(
    keyWithoutSessionExpressions, sessionExpression, child.output, stateFormatVersion)

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration, batchId: Long, stateSchemaVersion: Int):
    List[StateSchemaValidationResult] = {
    val newStateSchema = List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME,
      stateManager.getStateKeySchema, stateManager.getStateValueSchema))
    List(StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema(getStateInfo, hadoopConf,
      newStateSchema, session.sessionState, stateSchemaVersion))
  }

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
      PrefixKeyScanStateEncoderSpec(stateManager.getStateKeySchema,
        stateManager.getNumColsForPrefixKey),
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

          // SPARK-45582 - Ensure that store instance is not used after commit is called
          // to invoke the iterator.
          val rangeIter = stateManager.iterator(store)

          new NextIterator[InternalRow] {
            override protected def getNext(): InternalRow = {
              if (rangeIter.hasNext) {
                val valueRow = rangeIter.next()
                numOutputRows += 1
                valueRow
              } else {
                finished = true
                null
              }
            }

            override protected def close(): Unit = {
              commitTimeMs += timeTakenMs {
                stateManager.commit(store)
              }
              setStoreMetrics(store)
            }
          }

        // Update and output only rows being evicted from the StateStore
        // Assumption: watermark predicates must be non-empty if append mode is allowed
        case Some(Append) =>
          assert(watermarkPredicateForDataForEviction.isDefined,
              "Watermark needs to be defined for session window query in append mode")

          allUpdatesTimeMs += timeTakenMs {
            putToStore(iter, store)
          }

          val removalStartTimeNs = System.nanoTime
          new NextIterator[InternalRow] {
            private val removedIter = stateManager.removeByValueCondition(
              store, watermarkPredicateForDataForEviction.get.eval)

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
    StatefulOperatorPartitioning.getCompatibleDistribution(
      keyWithoutSessionExpressions, getStateInfo, conf) :: Nil
  }

  override def operatorStateMetadata(
      stateSchemaPaths: List[String] = List.empty): OperatorStateMetadata = {
    val info = getStateInfo
    val operatorInfo = OperatorInfoV1(info.operatorId, shortName)
    val stateStoreInfo = Array(StateStoreMetadataV1(
      StateStoreId.DEFAULT_STORE_NAME, stateManager.getNumColsForPrefixKey, info.numPartitions))
    OperatorStateMetadataV1(operatorInfo, stateStoreInfo)
  }

  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = {
    (outputMode.contains(Append) || outputMode.contains(Update)) &&
      eventTimeWatermarkForEviction.isDefined &&
      newInputWatermark > eventTimeWatermarkForEviction.get
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
        curValuesOnKey.clear()
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

  /**
   * The class overrides this method since dropping late events are happening in the upstream node
   * [[SessionWindowStateStoreRestoreExec]], and this class has responsibility to report the number
   * of dropped late events as a part of StateOperatorProgress.
   *
   * This method should be called in the driver after this SparkPlan has been executed and metrics
   * have been updated.
   */
  override def getProgress(): StateOperatorProgress = {
    val stateOpProgress = super.getProgress()

    // This should be safe, since the method is called in the driver after the plan has been
    // executed and metrics have been updated.
    val numRowsDroppedByWatermark = child.collectFirst {
      case s: SessionWindowStateStoreRestoreExec =>
        s.longMetric("numRowsDroppedByWatermark").value
    }.getOrElse(0L)

    stateOpProgress.copy(
      newNumRowsUpdated = stateOpProgress.numRowsUpdated,
      newNumRowsDroppedByWatermark = numRowsDroppedByWatermark)
  }
}

abstract class BaseStreamingDeduplicateExec
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport {

  def keyExpressions: Seq[Attribute]
  def child: SparkPlan
  def stateInfo: Option[StatefulOperatorStateInfo]
  def eventTimeWatermarkForLateEvents: Option[Long]
  def eventTimeWatermarkForEviction: Option[Long]

  /** Distribute by grouping attributes */
  override def requiredChildDistribution: Seq[Distribution] = {
    StatefulOperatorPartitioning.getCompatibleDistribution(
      keyExpressions, getStateInfo, conf) :: Nil
  }

  protected val schemaForValueRow: StructType
  protected val extraOptionOnStateStore: Map[String, String]

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    child.execute().mapPartitionsWithStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      schemaForValueRow,
      NoPrefixKeyStateEncoderSpec(keyExpressions.toStructType),
      session.sessionState,
      Some(session.streams.stateStoreCoordinator),
      extraOptions = extraOptionOnStateStore) { (store, iter) =>
      val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)
      val numOutputRows = longMetric("numOutputRows")
      val numUpdatedStateRows = longMetric("numUpdatedStateRows")
      val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
      val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
      val commitTimeMs = longMetric("commitTimeMs")
      val numDroppedDuplicateRows = longMetric("numDroppedDuplicateRows")

      val baseIterator = watermarkPredicateForDataForLateEvents match {
        case Some(predicate) => applyRemovingRowsOlderThanWatermark(iter, predicate)
        case None => iter
      }

      val reusedDupInfoRow = initializeReusedDupInfoRow()

      val updatesStartTimeNs = System.nanoTime

      val result = baseIterator.filter { r =>
        val row = r.asInstanceOf[UnsafeRow]
        val key = getKey(row)
        val value = store.get(key)
        if (value == null) {
          putDupInfoIntoState(store, row, key, reusedDupInfoRow)
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
        allRemovalsTimeMs += timeTakenMs { evictDupInfoFromState(store) }
        commitTimeMs += timeTakenMs { store.commit() }
        setStoreMetrics(store)
        setOperatorMetrics()
      })
    }
  }

  protected def initializeReusedDupInfoRow(): Option[UnsafeRow]

  protected def putDupInfoIntoState(
      store: StateStore,
      data: UnsafeRow,
      key: UnsafeRow,
      reusedDupInfoRow: Option[UnsafeRow]): Unit

  protected def evictDupInfoFromState(store: StateStore): Unit

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def customStatefulOperatorMetrics: Seq[StatefulOperatorCustomMetric] = {
    Seq(StatefulOperatorCustomSumMetric("numDroppedDuplicateRows", "number of duplicates dropped"))
  }

  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = {
    eventTimeWatermarkForEviction.isDefined &&
      newInputWatermark > eventTimeWatermarkForEviction.get
  }
}

/** Physical operator for executing streaming Deduplicate. */
case class StreamingDeduplicateExec(
    keyExpressions: Seq[Attribute],
    child: SparkPlan,
    stateInfo: Option[StatefulOperatorStateInfo] = None,
    eventTimeWatermarkForLateEvents: Option[Long] = None,
    eventTimeWatermarkForEviction: Option[Long] = None)
  extends BaseStreamingDeduplicateExec {

  protected val schemaForValueRow: StructType =
    StructType(Array(StructField("__dummy__", NullType)))

  // We won't check value row in state store since the value StreamingDeduplicateExec.EMPTY_ROW
  // is unrelated to the output schema.
  protected val extraOptionOnStateStore: Map[String, String] =
    Map(StateStoreConf.FORMAT_VALIDATION_CHECK_VALUE_CONFIG -> "false")

  protected def initializeReusedDupInfoRow(): Option[UnsafeRow] = None

  protected def putDupInfoIntoState(
      store: StateStore,
      data: UnsafeRow,
      key: UnsafeRow,
      reusedDupInfoRow: Option[UnsafeRow]): Unit = {
    store.put(key, StreamingDeduplicateExec.EMPTY_ROW)
  }

  protected def evictDupInfoFromState(store: StateStore): Unit = {
    removeKeysOlderThanWatermark(store)
  }

  override def shortName: String = "dedupe"

  override protected def withNewChildInternal(newChild: SparkPlan): StreamingDeduplicateExec =
    copy(child = newChild)

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration, batchId: Long, stateSchemaVersion: Int):
    List[StateSchemaValidationResult] = {
    val newStateSchema = List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME,
      keyExpressions.toStructType, schemaForValueRow))
    List(StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema(getStateInfo, hadoopConf,
      newStateSchema, session.sessionState, stateSchemaVersion,
      extraOptions = extraOptionOnStateStore))
  }
}

object StreamingDeduplicateExec {
  private val EMPTY_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))
}

case class StreamingDeduplicateWithinWatermarkExec(
    keyExpressions: Seq[Attribute],
    child: SparkPlan,
    stateInfo: Option[StatefulOperatorStateInfo] = None,
    eventTimeWatermarkForLateEvents: Option[Long] = None,
    eventTimeWatermarkForEviction: Option[Long] = None)
  extends BaseStreamingDeduplicateExec {

  protected val schemaForValueRow: StructType = StructType(
    Array(StructField("expiresAtMicros", LongType, nullable = false)))

  protected val extraOptionOnStateStore: Map[String, String] = Map.empty

  // Below three variables are defined as lazy, as evaluating these variables does not work with
  // canonicalized plan. Specifically, attributes in child won't have an event time column in
  // the canonicalized plan. These variables are NOT referenced in canonicalized plan, hence
  // defining these variables as lazy would avoid such error.
  private lazy val eventTimeCol: Attribute = WatermarkSupport.findEventTimeColumn(child.output,
    allowMultipleEventTimeColumns = false).get
  private lazy val delayThresholdMs = eventTimeCol.metadata.getLong(EventTimeWatermark.delayKey)
  private lazy val eventTimeColOrdinal: Int = child.output.indexOf(eventTimeCol)

  protected def initializeReusedDupInfoRow(): Option[UnsafeRow] = {
    val timeoutToUnsafeRow = UnsafeProjection.create(schemaForValueRow)
    val timeoutRow = timeoutToUnsafeRow(new SpecificInternalRow(schemaForValueRow))
    Some(timeoutRow)
  }

  protected def putDupInfoIntoState(
      store: StateStore,
      data: UnsafeRow,
      key: UnsafeRow,
      reusedDupInfoRow: Option[UnsafeRow]): Unit = {
    assert(reusedDupInfoRow.isDefined, "This should have reused row.")
    val timeoutRow = reusedDupInfoRow.get

    // We expect data type of event time column to be TimestampType or TimestampNTZType which both
    // are internally represented as Long.
    val timestamp = data.getLong(eventTimeColOrdinal)
    // The unit of timestamp in Spark is microseconds, convert the delay threshold to micros.
    val expiresAt = timestamp + DateTimeUtils.millisToMicros(delayThresholdMs)

    timeoutRow.setLong(0, expiresAt)
    store.put(key, timeoutRow)
  }

  protected def evictDupInfoFromState(store: StateStore): Unit = {
    val numRemovedStateRows = longMetric("numRemovedStateRows")

    // Convert watermark value to micros.
    val watermarkForEviction = DateTimeUtils.millisToMicros(eventTimeWatermarkForEviction.get)
    store.iterator().foreach { rowPair =>
      val valueRow = rowPair.value

      val expiresAt = valueRow.getLong(0)
      if (watermarkForEviction >= expiresAt) {
        store.remove(rowPair.key)
        numRemovedStateRows += 1
      }
    }
  }

  override def shortName: String = "dedupeWithinWatermark"

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration, batchId: Long, stateSchemaVersion: Int):
    List[StateSchemaValidationResult] = {
    val newStateSchema = List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME,
      keyExpressions.toStructType, schemaForValueRow))
    List(StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema(getStateInfo, hadoopConf,
      newStateSchema, session.sessionState, stateSchemaVersion,
      extraOptions = extraOptionOnStateStore))
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): StreamingDeduplicateWithinWatermarkExec = copy(child = newChild)
}
