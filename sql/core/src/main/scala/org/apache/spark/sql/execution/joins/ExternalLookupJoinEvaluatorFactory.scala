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

package org.apache.spark.sql.execution.joins

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.mutable

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, SparkException,
  SparkIllegalStateException, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, CollationKey, Expression,
  GenericInternalRow, JoinedRow, Predicate, SpecializedGetters, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.optimizer.NormalizeFloatingNumbers
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{ExternalLookupReader, ExternalLookupReaderFactory,
  ExternalLookupResultCursor}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.sql.types.{ArrayType, DataType, Decimal, DecimalType, MapType,
  StructType}
import org.apache.spark.util.Utils

/**
 * Creates the partition evaluators of an [[ExternalLookupJoinExec]]. The factory is sent to the
 * tasks, so it holds only what they need; the plan node and its scan stay on the driver.
 *
 * An evaluator joins its partition one batch of up to `maxParameterRowsPerRequest` stream rows at
 * a time. For each batch it sends one request with the distinct parameters that can match, and
 * streams the connector's result rows into output rows. Parameters are deduplicated per request,
 * that is within one batch: a key repeated in a later batch is requested again.
 *
 * Spark does not trust the connector with the selected lookup predicates: it evaluates
 * `lookupKeyCheck` on every row that the connector returns, against the request row that the row
 * answers, and drops a row that fails it, before it joins the row. The stream rows that share a
 * request row have Spark-equal parameters, so one check serves all of them.
 *
 * The join condition comes in two parts: `streamCondition` reads only stream columns and is
 * evaluated on each stream row before the request, and `residualCondition` is evaluated on the
 * joined rows. A stream row that fails `streamCondition` cannot match, so its parameters are not
 * sent.
 */
class ExternalLookupJoinEvaluatorFactory(
    tableName: String,
    lookupParameters: Seq[Expression],
    nullSafeParameters: Seq[Boolean],
    lookupKeyCheck: Option[Expression],
    streamOutput: Seq[Attribute],
    lookupInput: Seq[Attribute],
    output: Seq[Attribute],
    joinType: JoinType,
    streamCondition: Option[Expression],
    residualCondition: Option[Expression],
    readerFactory: ExternalLookupReaderFactory,
    maxParameterRowsPerRequest: Int,
    maxBatchBytes: Long,
    metrics: ExternalLookupJoinMetrics,
    readerMetrics: ExternalLookupReaderMetrics)
  extends PartitionEvaluatorFactory[InternalRow, InternalRow] {

  require(lookupParameters.length == nullSafeParameters.length,
    "Every external lookup parameter must define its null semantics")

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new ExternalLookupJoinEvaluator

  private class ExternalLookupJoinEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      assert(inputs.length == 1)
      new PartitionJoinIterator(inputs.head, partitionIndex)
    }
  }

  /** Joins one partition batch by batch, and closes its reader after the last batch. */
  private class PartitionJoinIterator(input: Iterator[InternalRow], partitionIndex: Int)
    extends Iterator[InternalRow] {
    private val reader = new LookupReader
    private val parameters = new LookupParameters(partitionIndex)
    private val resultRows = new LookupResultRows
    private val joinOutput = new JoinOutput(partitionIndex)
    private var batch: Iterator[InternalRow] = Iterator.empty
    private var inputExhausted = false

    override def hasNext: Boolean = {
      tryAdvance()
      batch.hasNext
    }

    override def next(): InternalRow = {
      tryAdvance()
      if (!batch.hasNext) {
        throw new SparkIllegalStateException(
          errorClass = "INTERNAL_ERROR",
          messageParameters = Map("message" -> "External lookup join exhausted"))
      }
      batch.next()
    }

    /** Reads more only when nothing is buffered; safe to call repeatedly. */
    private def tryAdvance(): Unit = {
      while (!batch.hasNext && !inputExhausted) {
        val rows = readBatch()
        if (rows.nonEmpty) {
          batch = new BatchJoinIterator(rows, parameters, resultRows, joinOutput, reader)
        }
        inputExhausted = !input.hasNext
      }
      if (inputExhausted && !batch.hasNext) reader.close()
    }

    private lazy val toUnsafe = UnsafeProjection.create(streamOutput, streamOutput)

    // An array, because a batch's rows are read by position for every key, match and output row.
    // The batch holds its rows until its request is read, so it stops once their copies reach
    // maxBatchBytes, as well as at maxParameterRowsPerRequest rows. It always takes one row.
    private def readBatch(): Array[InternalRow] = {
      val rows = mutable.ArrayBuffer.empty[InternalRow]
      var bytes = 0L
      while (rows.length < maxParameterRowsPerRequest && bytes < maxBatchBytes && input.hasNext) {
        val row = input.next() match {
          case unsafe: UnsafeRow => unsafe.copy()
          case other => toUnsafe(other).copy()
        }
        bytes += row.getSizeInBytes
        rows += row
      }
      rows.toArray
    }
  }

  /** The stream rows that share a key, and the one request row sent for them. */
  private case class KeyEntry(requestRow: UnsafeRow, rowIndexes: mutable.ArrayBuffer[Int])

  /**
   * Joins one batch of stream rows. Rows whose parameters are equal under Spark equality share
   * one request row, and every result row for a request row that satisfies the selected lookup
   * predicates joins all the stream rows sharing it. A stream row is finished, which may emit its
   * unmatched output row, once no more result rows can match it.
   *
   * Output rows are projected into a reused row on `next()`, so the consumer copies the rows it
   * retains, as with other join iterators.
   */
  private class BatchJoinIterator(
      rows: Array[InternalRow],
      parameters: LookupParameters,
      resultRows: LookupResultRows,
      joinOutput: JoinOutput,
      reader: LookupReader) extends Iterator[InternalRow] {
    metrics.inputRows += rows.length
    metrics.inputBatches += 1

    private val matched = Array.fill(rows.length)(false)
    private val finished = mutable.BitSet.empty
    // Output rows not yet returned: a stream row index and the lookup row it is joined with.
    private val pending = mutable.Queue.empty[(Int, InternalRow)]

    // One entry for each distinct key that can match, in the order of first occurrence. A row
    // that can never match, because it fails the stream condition or has a null parameter, is
    // finished right away, and its key is not requested. The stream condition is checked before
    // the key is computed, so that it can guard the parameter expressions.
    private val keyEntries = mutable.LinkedHashMap.empty[UnsafeRow, KeyEntry]
    rows.indices.foreach { index =>
      lazy val key = parameters.key(rows(index))
      if (!joinOutput.canMatch(rows(index)) || parameters.matchesNothing(key)) {
        finish(index)
      } else {
        keyEntries.get(key) match {
          case Some(entry) => entry.rowIndexes += index
          case None => keyEntries.put(
            key.copy(),
            KeyEntry(parameters.requestRow(rows(index)), mutable.ArrayBuffer(index)))
        }
      }
    }
    private val requestedKeys = keyEntries.keys.toArray

    // No connector request, and no reader, when no key can match. The connector gets copies of
    // the request rows, so that the ones that Spark checks the results against stay its own.
    private var cursor: ExternalLookupResultCursor =
      if (keyEntries.isEmpty) EmptyLookupCursor
      else connectorCall(reader.lookup(keyEntries.values.map(_.requestRow.copy()).toArray))

    override def hasNext: Boolean = {
      tryFillPending()
      pending.nonEmpty
    }

    override def next(): InternalRow = {
      tryFillPending()
      if (pending.isEmpty) throw new NoSuchElementException("external lookup result")
      val (index, lookupRow) = pending.dequeue()
      joinOutput.project(rows(index), lookupRow)
    }

    /** Reads more only when nothing is buffered; safe to call repeatedly. */
    private def tryFillPending(): Unit = {
      try {
        while (pending.isEmpty && advanceCursor()) {
          reader.resultRowRead()
          // A close() during either call fails the read before Spark copies or joins the row.
          val start = System.nanoTime()
          val ordinal = cursor.inputOrdinal()
          reader.ensureOpen()
          val connectorRow = cursor.get()
          metrics.lookupTime += System.nanoTime() - start
          reader.ensureOpen()
          if (connectorRow == null) {
            throw QueryExecutionErrors.invalidDataSourceExternalLookupError(
              tableName, "NULL_RESULT_ROW")
          }
          val key = requestedKey(ordinal)
          val resultRow = resultRows.validatedCopy(connectorRow)
          if (parameters.selectedPredicatesHold(keyEntries(key).requestRow, resultRow)) {
            joinResultRow(key, resultRow)
          }
        }
        if (pending.isEmpty) finishCursor(completed = true)
      } catch {
        case error: Throwable =>
          Utils.tryWithSafeFinally(throw error) {
            finishCursor(completed = false)
          }
      }
    }

    /**
     * Advances the connector's cursor, unless the reader was closed meanwhile. A close() while
     * next() ran fails the read once next() returns: Spark then reads no row from the closed
     * cursor, and does not take a missing row for a complete result.
     */
    private def advanceCursor(): Boolean = (cursor ne EmptyLookupCursor) && {
      reader.ensureOpen()
      val advanced = connectorCall(cursor.next())
      reader.ensureOpen()
      advanced
    }

    /**
     * Runs a connector call and adds its time to the lookup time metric, which counts only the
     * time in the connector, not that of the work above the join while a cursor is open.
     */
    private def connectorCall[T](call: => T): T = {
      val start = System.nanoTime()
      try call finally metrics.lookupTime += System.nanoTime() - start
    }

    private def requestedKey(ordinal: Int): UnsafeRow = {
      if (ordinal < 0 || ordinal >= requestedKeys.length) {
        throw QueryExecutionErrors.invalidDataSourceExternalLookupError(
          tableName,
          "INVALID_INPUT_ORDINAL",
          Map("ordinal" -> ordinal.toString, "numParameterRows" -> requestedKeys.length.toString))
      }
      requestedKeys(ordinal)
    }

    private def joinResultRow(key: UnsafeRow, lookupRow: InternalRow): Unit =
      keyEntries(key).rowIndexes.foreach(joinMatch(_, lookupRow))

    private def joinMatch(index: Int, lookupRow: InternalRow): Unit = {
      if (joinOutput.matches(rows(index), lookupRow)) {
        matched(index) = true
        joinOutput.matched(lookupRow).foreach(emit(index, _))
      }
    }

    /**
     * Closes the cursor once. After a complete read, finishes every stream row that was waiting for
     * its results. After a failure or a close, the results may be partial, so it finishes none:
     * a row without a match could be a row whose match was never read.
     */
    private def finishCursor(completed: Boolean): Unit = if (cursor ne EmptyLookupCursor) {
      cursor = EmptyLookupCursor
      try {
        connectorCall(reader.closeCursor())
      } finally {
        reader.updateMetrics()
      }
      if (completed) keyEntries.values.foreach(_.rowIndexes.foreach(finish))
    }

    private def finish(index: Int): Unit = if (!finished.contains(index)) {
      finished += index
      if (!matched(index)) joinOutput.unmatched.foreach(emit(index, _))
    }

    private def emit(index: Int, lookupRow: InternalRow): Unit = {
      pending.enqueue(index -> lookupRow)
      metrics.outputRows += 1
    }
  }

  /** The cursor of a batch that sends no request, or whose cursor is finished. */
  private object EmptyLookupCursor extends ExternalLookupResultCursor {
    override def next(): Boolean = false
    override def inputOrdinal(): Int = throw new NoSuchElementException("external lookup result")
    override def get(): InternalRow = throw new NoSuchElementException("external lookup result")
    override def close(): Unit = {}
  }

  /** A check of one value slot: a field of a row, or an element of an array. */
  private type SlotCheck = (SpecializedGetters, Int) => Unit

  /**
   * Validates each row that the connector returns, and copies it once into Spark's own row before
   * Spark checks, joins or keeps it. A connector may reuse its row and its buffers, byte arrays
   * included, so a row that Spark still holds must not share them. A value that breaks its type
   * fails the task, at any nested level: a null in a field, array element or map value that the
   * table or the read schema declares NOT NULL, or a decimal that does not fit its precision and
   * scale. Spark already relied on them while it optimized the query. The checks are built once
   * from the read schema, and a field whose type has nothing to check costs nothing per row.
   */
  private class LookupResultRows {
    private val copy = UnsafeProjection.create(lookupInput, lookupInput)
    private val fieldChecks = lookupInput.zipWithIndex.flatMap { case (field, ordinal) =>
      slotCheck(field.dataType, field.nullable, Seq(field.name)).map(ordinal -> _)
    }

    /** The row, validated and copied into a row that Spark owns. */
    def validatedCopy(resultRow: InternalRow): UnsafeRow = {
      fieldChecks.foreach { case (ordinal, check) => check(resultRow, ordinal) }
      copy(resultRow).copy()
    }

    /** The check of a slot of this type, at this field path, or None if it has nothing to check. */
    private def slotCheck(
        dataType: DataType,
        nullable: Boolean,
        path: Seq[String]): Option[SlotCheck] = {
      val valueCheck = nonNullValueCheck(dataType, path)
      if (nullable && valueCheck.isEmpty) {
        None
      } else {
        Some { (values, ordinal) =>
          if (!values.isNullAt(ordinal)) {
            valueCheck.foreach(_(values, ordinal))
          } else if (!nullable) {
            throw QueryExecutionErrors.invalidDataSourceExternalLookupError(
              tableName,
              "NULL_IN_NOT_NULL_FIELD",
              Map("field" -> QueryExecutionErrors.toSQLId(path)))
          }
        }
      }
    }

    /** The check of a value that is not null, or None if its type has nothing to check. */
    private def nonNullValueCheck(dataType: DataType, path: Seq[String]): Option[SlotCheck] =
      dataType match {
        case decimalType: DecimalType => Some { (values, ordinal) =>
          val value = values.getDecimal(ordinal, decimalType.precision, decimalType.scale)
          if (!fits(value, decimalType)) {
            throw QueryExecutionErrors.invalidDataSourceExternalLookupError(
              tableName,
              "DECIMAL_OUT_OF_RANGE",
              Map(
                "field" -> QueryExecutionErrors.toSQLId(path),
                "type" -> QueryExecutionErrors.toSQLType(decimalType)))
          }
        }
        case structType: StructType =>
          val checks = structType.fields.toSeq.zipWithIndex.flatMap { case (field, ordinal) =>
            slotCheck(field.dataType, field.nullable, path :+ field.name).map(ordinal -> _)
          }
          Option.when(checks.nonEmpty) { (values, ordinal) =>
            val struct = values.getStruct(ordinal, structType.length)
            checks.foreach { case (fieldOrdinal, check) => check(struct, fieldOrdinal) }
          }
        case arrayType: ArrayType =>
          slotCheck(arrayType.elementType, arrayType.containsNull, path :+ "element").map {
            check => (values, ordinal) => eachElement(values.getArray(ordinal), check)
          }
        case mapType: MapType =>
          // A map key is never null, whatever the value's nullability.
          val keyCheck = slotCheck(mapType.keyType, nullable = false, path :+ "key")
          val valueCheck =
            slotCheck(mapType.valueType, mapType.valueContainsNull, path :+ "value")
          Option.when(keyCheck.nonEmpty || valueCheck.nonEmpty) { (values, ordinal) =>
            val map = values.getMap(ordinal)
            keyCheck.foreach(eachElement(map.keyArray(), _))
            valueCheck.foreach(eachElement(map.valueArray(), _))
          }
        case _ => None
      }

    private def eachElement(array: ArrayData, check: SlotCheck): Unit = {
      var index = 0
      while (index < array.numElements()) {
        check(array, index)
        index += 1
      }
    }

    /** Whether the decimal has its type's precision and scale without rounding. */
    private def fits(value: Decimal, decimalType: DecimalType): Boolean = {
      val rescaled = value.clone()
      rescaled.changePrecision(decimalType.precision, decimalType.scale) && rescaled == value
    }
  }

  /**
   * Evaluates the lookup parameters of stream rows: the request row that is sent to the
   * connector, and the key that identifies equal parameters under Spark equality, which
   * normalizes floating-point values and collations. Also checks the rows that the connector
   * returns against the selected lookup predicates.
   */
  private class LookupParameters(partitionIndex: Int) {
    private val requestProjection = UnsafeProjection.create(lookupParameters, streamOutput)
    private val keyProjection = UnsafeProjection.create(lookupParameters.map { parameter =>
      NormalizeFloatingNumbers.normalize(CollationKey.injectCollationKey(parameter))
    }, streamOutput)
    private val keyCheck = lookupKeyCheck.map(Predicate.create(_))
    private val requestAndResult = new JoinedRow()
    requestProjection.initialize(partitionIndex)
    keyProjection.initialize(partitionIndex)
    keyCheck.foreach(_.initialize(partitionIndex))

    def requestRow(streamRow: InternalRow): UnsafeRow = requestProjection(streamRow).copy()

    /** The key of a stream row, in a reused row: copy it to retain it. */
    def key(streamRow: InternalRow): UnsafeRow = keyProjection(streamRow)

    /** Whether a row that the connector returns satisfies the selected lookup predicates. */
    def selectedPredicatesHold(requestRow: UnsafeRow, resultRow: InternalRow): Boolean =
      keyCheck.forall(_.eval(requestAndResult(requestRow, resultRow)))

    /**
     * A null parameter that is not null-safe matches no lookup row, so it is not requested.
     * The key has the request row's nulls.
     */
    def matchesNothing(key: InternalRow): Boolean =
      nullSafeParameters.indices.exists { index =>
        !nullSafeParameters(index) && key.isNullAt(index)
      }
  }

  /**
   * Evaluates the stream condition on stream rows and the residual condition on joined rows, and
   * builds output rows per join type.
   */
  private class JoinOutput(partitionIndex: Int) {
    // Value rows follow the read schema, and the residual and output are bound to its fields.
    private val joinedLookupInput =
      if (joinType == LeftOuter) lookupInput.map(_.withNullability(true)) else lookupInput
    private val joinedInput = streamOutput ++ joinedLookupInput
    private val joinedRow = new JoinedRow()
    private val streamPredicate = streamCondition.map(Predicate.create(_, streamOutput))
    private val residual = residualCondition.map(Predicate.create(_, joinedInput))
    private val projection = UnsafeProjection.create(output, joinedInput)
    private val nullLookupRow = new GenericInternalRow(lookupInput.length)
    streamPredicate.foreach(_.initialize(partitionIndex))
    residual.foreach(_.initialize(partitionIndex))
    projection.initialize(partitionIndex)

    /** Whether a stream row passes the stream condition; a row that fails it matches nothing. */
    def canMatch(streamRow: InternalRow): Boolean = streamPredicate.forall(_.eval(streamRow))

    def matches(streamRow: InternalRow, lookupRow: InternalRow): Boolean =
      residual.forall(_.eval(joinedRow(streamRow, lookupRow)))

    /** The lookup row that the output for a match joins, if the join type emits one. */
    def matched(lookupRow: InternalRow): Option[InternalRow] = joinType match {
      case Inner | LeftOuter => Some(lookupRow)
      case _ => None
    }

    /** The lookup row that the output for a stream row without a match joins, if any. */
    def unmatched: Option[InternalRow] = joinType match {
      case LeftOuter => Some(nullLookupRow)
      case _ => None
    }

    /** The output row, projected into a reused row. */
    def project(streamRow: InternalRow, lookupRow: InternalRow): InternalRow =
      projection(joinedRow(streamRow, lookupRow))
  }

  /**
   * The connector reader of one partition and its active cursor. The reader is opened by the
   * first request, and closed once, after the last batch or when the task completes. A thread
   * other than the task's may still drive the join then. Closing may overlap the one connector
   * call in progress and Spark's read of the current row, and it reads the metrics of the task's
   * readers while that call runs. Once the join has seen the close, it starts no request, cursor
   * advance, row read or new reader. Requests and cursor calls take no lock, so closing does not
   * wait for them. Metrics reads do: closing keeps the reader's final metrics under the lock of
   * the task's `TaskLookupReaders`, so it can wait for a metrics read in progress, which calls
   * `currentMetricsValues()` on the task's readers.
   */
  private class LookupReader {
    private val taskReaders = readerMetrics.forCurrentTask()
    private val closed = new AtomicBoolean(false)
    // Each is taken by exactly one of the join and close(), so it is closed once.
    private val openReader = new AtomicReference[ExternalLookupReader]()
    private val activeCursor = new AtomicReference[ExternalLookupResultCursor]()
    private var resultRowsSinceMetricsUpdate = 0
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

    /** Sends one request and returns the cursor over its result rows. */
    def lookup(requestRows: Array[UnsafeRow]): ExternalLookupResultCursor = {
      ensureOpen()
      metrics.requests += 1
      metrics.parameterRows += requestRows.length
      val reader = Option(openReader.get()).getOrElse {
        // A close() since the check above took the reader. It marks the reader closed first, so
        // this check sees it, and no second reader is created.
        ensureOpen()
        createReader()
      }
      val cursor = Option(reader.lookupBatch(requestRows.asInstanceOf[Array[InternalRow]]))
        .getOrElse {
          throw QueryExecutionErrors.invalidDataSourceExternalLookupError(
            tableName, "NULL_RESULT_CURSOR")
        }
      activeCursor.set(cursor)
      // A close() while the request ran did not find this cursor, so it is closed here.
      if (closed.get()) {
        closeCursor()
        ensureOpen()
      }
      cursor
    }

    private def createReader(): ExternalLookupReader = {
      val reader = Option(readerFactory.createReader()).getOrElse {
        throw QueryExecutionErrors.invalidDataSourceExternalLookupError(
          tableName, "NULL_LOOKUP_READER")
      }
      taskReaders.opened(reader)
      openReader.set(reader)
      // A close() while the reader was created did not find it, so it is closed here.
      if (closed.get() && openReader.compareAndSet(reader, null)) {
        ExternalLookupJoinExec.closeResources((), taskReaders.closing(reader), reader.close())
      }
      ensureOpen()
      reader
    }

    /** Fails once the reader is closed, before any further connector call. */
    def ensureOpen(): Unit = if (closed.get()) {
      throw SparkException.internalError("External lookup reader used after it was closed")
    }

    /** Samples the connector metrics every [[CustomMetrics.NUM_ROWS_PER_UPDATE]] result rows. */
    def resultRowRead(): Unit = {
      resultRowsSinceMetricsUpdate += 1
      if (resultRowsSinceMetricsUpdate >= CustomMetrics.NUM_ROWS_PER_UPDATE) updateMetrics()
    }

    def updateMetrics(): Unit = {
      if (openReader.get() != null) taskReaders.report()
      resultRowsSinceMetricsUpdate = 0
    }

    def closeCursor(): Unit = Option(activeCursor.getAndSet(null)).foreach(_.close())

    def close(): Unit = if (closed.compareAndSet(false, true)) {
      val reader = openReader.getAndSet(null)
      ExternalLookupJoinExec.closeResources(
        closeCursor(),
        if (reader != null) taskReaders.closing(reader),
        if (reader != null) reader.close())
    }
  }
}

/**
 * Connector metrics of all lookup readers in a task. A task that computes several input
 * partitions, for example under a coalesce, opens one reader per partition; their values are
 * merged, as `DataSourceRDD` merges its readers', instead of each reader overwriting the last.
 */
private class ExternalLookupReaderMetrics(customMetrics: Map[String, SQLMetric])
  extends Serializable {
  @transient private lazy val readersByTask = new ConcurrentHashMap[Long, TaskLookupReaders]()

  def forCurrentTask(): TaskLookupReaders = Option(TaskContext.get()) match {
    case Some(context) =>
      val taskAttemptId = context.taskAttemptId()
      val readers = new TaskLookupReaders(customMetrics)
      Option(readersByTask.putIfAbsent(taskAttemptId, readers)).getOrElse {
        // Registered outside the map update: a completed task runs the listener right away.
        context.addTaskCompletionListener[Unit](_ => readersByTask.remove(taskAttemptId))
        readers
      }
    case None => new TaskLookupReaders(customMetrics)
  }
}

private class TaskLookupReaders(customMetrics: Map[String, SQLMetric]) {
  private val openReaders = mutable.ArrayBuffer.empty[ExternalLookupReader]
  private val closedMetrics = mutable.HashMap.empty[String, CustomTaskMetric]

  def opened(reader: ExternalLookupReader): Unit = synchronized {
    openReaders += reader
  }

  /** Keeps a reader's final values, which it may no longer report once it is closed. */
  def closing(reader: ExternalLookupReader): Unit = synchronized {
    reader.currentMetricsValues().foreach { metric =>
      closedMetrics.update(
        metric.name(), closedMetrics.get(metric.name()).fold(metric)(_.mergeWith(metric)))
    }
    openReaders -= reader
    report()
  }

  def report(): Unit = synchronized {
    (openReaders.flatMap(_.currentMetricsValues()) ++ closedMetrics.values)
      .groupMapReduce(_.name())(identity)(_.mergeWith(_))
      .values
      .foreach { metric =>
        customMetrics.get(ExternalLookupJoinExec.sourceMetricKey(metric.name()))
          .foreach(_.set(metric.value()))
      }
  }
}
