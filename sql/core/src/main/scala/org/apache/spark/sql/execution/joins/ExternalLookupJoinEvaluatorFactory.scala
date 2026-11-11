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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow,
  JoinedRow, Predicate, SpecializedGetters, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupFilter, ExternalLookupProject,
  ExternalLookupTransform}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{ExternalLookupReader, ExternalLookupReaderFactory,
  ExternalLookupResultCursor}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.sql.types.{ArrayType, DataType, Decimal, DecimalType, MapType,
  StructType, UserDefinedType}
import org.apache.spark.util.Utils

/**
 * Creates the partition evaluators of an [[ExternalLookupJoinExec]]. The factory is sent to the
 * tasks, so it holds only what they need; the plan node and its scan stay on the driver.
 *
 * An evaluator joins its partition one batch of up to `maxParameterRowsPerRequest` stream rows at
 * a time, or, with `oneKeyPerBatch`, of up to
 * [[ExternalLookupJoinExec.MAX_PARAMETER_ROWS_PER_REQUEST]] stream rows that share one key, whose
 * request holds one parameter row, whatever the source's limit. For each batch it sends
 * one request with the distinct parameters that can match, and streams the connector's result
 * rows into output rows. Parameters are deduplicated per request, that is within one batch: a key
 * repeated in a later batch is requested again.
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
    lookupTransforms: Seq[ExternalLookupTransform],
    transformedLookup: Seq[Attribute],
    output: Seq[Attribute],
    joinType: JoinType,
    streamCondition: Option[Expression],
    residualCondition: Option[Expression],
    readerFactory: ExternalLookupReaderFactory,
    maxParameterRowsPerRequest: Int,
    oneKeyPerBatch: Boolean,
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
    private val transforms = new LookupTransforms(partitionIndex)
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
        val next = readBatch()
        if (next.rows.nonEmpty) {
          batch = new BatchJoinIterator(
            next, parameters, resultRows, transforms, joinOutput, reader)
        }
        inputExhausted = !hasInput
      }
      if (inputExhausted && !batch.hasNext) reader.close()
    }

    private lazy val toUnsafe = UnsafeProjection.create(streamOutput, streamOutput)

    // An inner or left semi join emits nothing for a stream row that cannot match, because it
    // fails the stream condition or has a null parameter that is not null-safe, as an ordinary
    // inner join filters such a row out before it joins. So the row takes no place in a batch and
    // is not copied: it counts only as an input row. A left outer or left anti join emits it, so a
    // batch keeps it.
    private val keepsRowsThatCannotMatch = joinType != Inner && joinType != LeftSemi

    // When a request holds one parameter row because of a selected range, a batch takes stream
    // rows of one key. It ends before the first row with another key, which starts the next batch.
    // A row that cannot match has no key, and any batch that keeps it takes it.
    private var heldRow: UnsafeRow = null
    private var heldRequestRow: UnsafeRow = null

    // The request row of the row that nextKeptRow returned last, or null when that row cannot
    // match. It is a reused row that the next request row overwrites, unless the row was held.
    private var keptRequestRow: UnsafeRow = null

    // A batch of one key holds one parameter row in its request, whatever the source's limit, so
    // only Spark's own cap and maxBatchBytes end it before another key does.
    private val maxBatchRows =
      if (oneKeyPerBatch) ExternalLookupJoinExec.MAX_PARAMETER_ROWS_PER_REQUEST
      else maxParameterRowsPerRequest

    private def hasInput: Boolean = heldRow != null || input.hasNext

    /** The next row that a batch keeps, copied, or null after a row that it leaves out. */
    private def nextKeptRow(): UnsafeRow = if (heldRow != null) {
      val row = heldRow
      keptRequestRow = heldRequestRow
      heldRow = null
      heldRequestRow = null
      row
    } else {
      val next = input.next()
      keptRequestRow = requestRowOf(next)
      if (keepsRowsThatCannotMatch || keptRequestRow != null) {
        next match {
          case unsafe: UnsafeRow => unsafe.copy()
          case other => toUnsafe(other).copy()
        }
      } else {
        metrics.inputRows += 1
        null
      }
    }

    /**
     * The request row of a stream row that can match, in a reused row, or null. The stream
     * condition is checked before the parameters are evaluated, so that it can guard their
     * expressions.
     */
    private def requestRowOf(row: InternalRow): UnsafeRow =
      if (!joinOutput.canMatch(row)) null else {
        val requestRow = parameters.requestRow(row)
        if (parameters.matchesNothing(requestRow)) null else requestRow
      }

    // An array, because a batch's rows are read by position for every key, match and output row.
    // The batch holds its rows until its request is read, so it stops once their copies reach
    // maxBatchBytes, keeping the row that reaches it, and at maxBatchRows rows. A batch of one key
    // also stops at another key. It always keeps one row, when one remains. It decides once for
    // each row whether the row can match, and groups the rows that can by their request row, a
    // copy of which each group keeps.
    private def readBatch(): StreamBatch = {
      val rows = mutable.ArrayBuffer.empty[InternalRow]
      val keyEntries = mutable.LinkedHashMap.empty[UnsafeRow, KeyEntry]
      val rowsThatCannotMatch = mutable.ArrayBuffer.empty[Int]
      var bytes = 0L
      var keyChanged = false
      while (!keyChanged && rows.length < maxBatchRows && bytes < maxBatchBytes && hasInput) {
        val row = nextKeptRow()
        if (row != null) {
          val requestRow = keptRequestRow
          if (oneKeyPerBatch && requestRow != null && keyEntries.nonEmpty &&
              !keyEntries.contains(requestRow)) {
            heldRow = row
            heldRequestRow = requestRow.copy()
            keyChanged = true
          } else {
            val index = rows.length
            if (requestRow == null) {
              rowsThatCannotMatch += index
            } else {
              keyEntries.get(requestRow) match {
                case Some(entry) => entry.rowIndexes += index
                case None =>
                  val retained = requestRow.copy()
                  keyEntries.put(retained, KeyEntry(retained, mutable.ArrayBuffer(index)))
              }
            }
            bytes += row.getSizeInBytes
            rows += row
          }
        }
      }
      StreamBatch(rows.toArray, keyEntries.values.toArray, rowsThatCannotMatch.toArray)
    }
  }

  /** The stream rows that share a key, and the one request row sent for them. */
  private case class KeyEntry(requestRow: UnsafeRow, rowIndexes: mutable.ArrayBuffer[Int])

  /**
   * The stream rows of a batch, with one entry for each distinct request row that can match, in
   * the order of first occurrence, and the positions of the rows that a left outer or left anti
   * join keeps although they cannot match, because they fail the stream condition or have a null
   * parameter that is not null-safe.
   */
  private case class StreamBatch(
      rows: Array[InternalRow],
      keyEntries: Array[KeyEntry],
      rowsThatCannotMatch: Array[Int])

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
      batch: StreamBatch,
      parameters: LookupParameters,
      resultRows: LookupResultRows,
      transforms: LookupTransforms,
      joinOutput: JoinOutput,
      reader: LookupReader) extends Iterator[InternalRow] {
    private val rows = batch.rows
    metrics.inputRows += rows.length
    metrics.inputBatches += 1

    private val matched = Array.fill(rows.length)(false)
    private val finished = mutable.BitSet.empty
    // Output rows not yet returned: a stream row index and the lookup row it is joined with.
    private val pending = mutable.Queue.empty[(Int, InternalRow)]

    // A row that can never match is finished right away, and its parameters are not requested.
    batch.rowsThatCannotMatch.foreach(finish)
    private val keyEntries = batch.keyEntries

    // No connector request, and no reader, when no key can match.
    private var cursor: ExternalLookupResultCursor =
      if (keyEntries.isEmpty) EmptyLookupCursor
      else connectorCall(reader.lookup(keyEntries.map(_.requestRow)))

    // A semi or anti join decides a stream row at its first match. Once every requested row has
    // matched, no later result row can change the output, so the join reads no further.
    private var undecidedRows = keyEntries.iterator.map(_.rowIndexes.length).sum

    private def decided: Boolean = joinOutput.answeredByFirstMatch && undecidedRows == 0

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
        while (pending.isEmpty && !decided && advanceCursor()) {
          reader.resultRowRead()
          // The join checks for a close() before each connector call, so it makes none after it
          // has seen one.
          val start = System.nanoTime()
          reader.ensureOpen()
          val ordinal = cursor.inputOrdinal()
          reader.ensureOpen()
          val connectorRow = cursor.get()
          metrics.lookupTime += System.nanoTime() - start
          if (connectorRow == null) {
            throw QueryExecutionErrors.invalidDataSourceExternalLookupError(
              tableName, "NULL_RESULT", Map("method" -> "ExternalLookupResultCursor.get()"))
          }
          val entry = requestedEntry(ordinal)
          reader.ensureOpen()
          val resultRow = resultRows.validated(connectorRow)
          if (parameters.selectedPredicatesHold(entry.requestRow, resultRow)) {
            transforms(resultRow).foreach { lookupRow =>
              entry.rowIndexes.foreach { index =>
                if (joinMatch(index, lookupRow)) undecidedRows -= 1
              }
            }
          }
        }
        // A decided batch closes its cursor before the end. Its rows have all matched, so none is
        // waiting for its results.
        if (pending.isEmpty) finishCursor(completed = !decided)
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

    private def requestedEntry(ordinal: Int): KeyEntry = {
      if (ordinal < 0 || ordinal >= keyEntries.length) {
        throw QueryExecutionErrors.invalidDataSourceExternalLookupError(
          tableName,
          "INVALID_INPUT_ORDINAL",
          Map("ordinal" -> ordinal.toString, "numParameterRows" -> keyEntries.length.toString))
      }
      keyEntries(ordinal)
    }

    /** Joins a lookup row with a stream row, and returns whether it is the row's first match. */
    private def joinMatch(index: Int, lookupRow: InternalRow): Boolean = {
      // A semi or anti join knows a stream row's answer at its first match, so it evaluates the
      // residual condition no further for that row, as the other join operators do. It reads on
      // while another stream row of the batch has not matched.
      if (!(joinOutput.answeredByFirstMatch && matched(index)) &&
          joinOutput.matches(rows(index), lookupRow)) {
        val firstMatch = !matched(index)
        matched(index) = true
        joinOutput.matched(lookupRow, firstMatch).foreach(emit(index, _))
        firstMatch
      } else {
        false
      }
    }

    /**
     * Closes the cursor once. After a complete read, finishes every stream row that was waiting for
     * its results. After a failure or a close, the results may be partial, so it finishes none:
     * a row without a match could be a row whose match was never read. Nor does a semi or anti join
     * that closes the cursor once every stream row has matched. Once the reader is closed, its
     * close owns the cursor and the reader's metrics, so the join makes no further connector call.
     */
    private def finishCursor(completed: Boolean): Unit = if (cursor ne EmptyLookupCursor) {
      cursor = EmptyLookupCursor
      if (!reader.isClosed) {
        try {
          connectorCall(reader.closeCursor())
        } finally {
          reader.updateMetrics()
        }
      }
      if (completed) keyEntries.foreach(_.rowIndexes.foreach(finish))
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
   * Validates each row that the connector returns, and writes it into a row of Spark's own, which
   * is reused, before Spark checks it. A connector may reuse its row and its buffers, byte arrays
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

    /** The row, validated and written into a reused row that Spark owns. */
    def validated(resultRow: InternalRow): UnsafeRow = {
      fieldChecks.foreach { case (ordinal, check) => check(resultRow, ordinal) }
      copy(resultRow)
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
        // A value of a user-defined type is stored as a value of its underlying type.
        case udt: UserDefinedType[_] => nonNullValueCheck(udt.sqlType, path)
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
    private def fits(value: Decimal, decimalType: DecimalType): Boolean =
      if (value.precision == decimalType.precision && value.scale == decimalType.scale) {
        // changePrecision takes such a value as it is, but an UnsafeRow and a columnar row or
        // array record the type's precision for a compact decimal without checking its digits.
        value.toJavaBigDecimal.precision <= decimalType.precision
      } else {
        val rescaled = value.clone()
        rescaled.changePrecision(decimalType.precision, decimalType.scale) && rescaled == value
      }
  }

  /**
   * Evaluates the lookup parameters of stream rows into the request rows that are sent to the
   * connector. Spark offers lookup keys only on types whose values are equal under Spark equality
   * exactly when their binary forms are (see ExternalLookupKeyTypes), so equal request rows have
   * equal bytes. Also checks the rows that the connector returns against the selected lookup
   * predicates.
   */
  private class LookupParameters(partitionIndex: Int) {
    private val requestProjection = UnsafeProjection.create(lookupParameters, streamOutput)
    private val keyCheck = lookupKeyCheck.map(Predicate.create(_))
    private val requestAndResult = new JoinedRow()
    requestProjection.initialize(partitionIndex)
    keyCheck.foreach(_.initialize(partitionIndex))

    /** The request row of a stream row, in a reused row: copy it to retain it. */
    def requestRow(streamRow: InternalRow): UnsafeRow = requestProjection(streamRow)

    /** Whether a row that the connector returns satisfies the selected lookup predicates. */
    def selectedPredicatesHold(requestRow: UnsafeRow, resultRow: InternalRow): Boolean =
      keyCheck.forall(_.eval(requestAndResult(requestRow, resultRow)))

    /** A null parameter that is not null-safe matches no lookup row, so it is not requested. */
    def matchesNothing(requestRow: InternalRow): Boolean =
      nullSafeParameters.indices.exists { index =>
        !nullSafeParameters(index) && requestRow.isNullAt(index)
      }
  }

  /**
   * Applies the optimized lookup-side projections and filters to a result row, in evaluation
   * order. A row that a filter rejects joins no stream row. Several stream rows can join the
   * result, so it is retained: validation and each projection reuse their output rows, so the row
   * that passes every stage is copied, once.
   */
  private class LookupTransforms(partitionIndex: Int) {
    private val stages = lookupTransforms.map {
      case ExternalLookupProject(projectList, input) =>
        Left(UnsafeProjection.create(projectList, input))
      case ExternalLookupFilter(condition, input) =>
        Right(Predicate.create(condition, input))
    }
    stages.foreach {
      case Left(projection) => projection.initialize(partitionIndex)
      case Right(predicate) => predicate.initialize(partitionIndex)
    }

    def apply(resultRow: InternalRow): Option[InternalRow] =
      stages.foldLeft(Option(resultRow)) {
        case (Some(row), Left(projection)) => Some(projection(row))
        case (Some(row), Right(predicate)) if predicate.eval(row) => Some(row)
        case _ => None
      }.map(_.copy())
  }

  /**
   * Evaluates the stream condition on stream rows and the residual condition on joined rows, and
   * builds output rows per join type.
   */
  private class JoinOutput(partitionIndex: Int) {
    // Lookup rows follow the last lookup-side stage, or the read schema without one, and the
    // residual and output are bound to their fields.
    private val joinedLookupInput = if (joinType == LeftOuter) {
      transformedLookup.map(_.withNullability(true))
    } else {
      transformedLookup
    }
    private val joinedInput = streamOutput ++ joinedLookupInput
    private val joinedRow = new JoinedRow()
    private val streamPredicate = streamCondition.map(Predicate.create(_, streamOutput))
    private val residual = residualCondition.map(Predicate.create(_, joinedInput))
    private val projection = UnsafeProjection.create(output, joinedInput)
    private val nullLookupRow = new GenericInternalRow(transformedLookup.length)
    streamPredicate.foreach(_.initialize(partitionIndex))
    residual.foreach(_.initialize(partitionIndex))
    projection.initialize(partitionIndex)

    /** Whether a stream row passes the stream condition; a row that fails it matches nothing. */
    def canMatch(streamRow: InternalRow): Boolean = streamPredicate.forall(_.eval(streamRow))

    /** Whether the first match decides a stream row's output, as in a semi or anti join. */
    val answeredByFirstMatch: Boolean = joinType == LeftSemi || joinType == LeftAnti

    def matches(streamRow: InternalRow, lookupRow: InternalRow): Boolean =
      residual.forall(_.eval(joinedRow(streamRow, lookupRow)))

    /**
     * The lookup row that the output for a match joins, if the join type emits one; a semi join
     * emits only its first match.
     */
    def matched(lookupRow: InternalRow, firstMatch: Boolean): Option[InternalRow] =
      joinType match {
        case Inner | LeftOuter => Some(lookupRow)
        case LeftSemi if firstMatch => Some(nullLookupRow)
        case _ => None
      }

    /** The lookup row that the output for a stream row without a match joins, if any. */
    def unmatched: Option[InternalRow] = joinType match {
      case LeftOuter | LeftAnti => Some(nullLookupRow)
      case _ => None
    }

    /** The output row, projected into a reused row. */
    def project(streamRow: InternalRow, lookupRow: InternalRow): InternalRow =
      projection(joinedRow(streamRow, lookupRow))
  }

  /**
   * The connector reader of one partition and its active cursor. The reader is opened by the
   * first request, and closed once, after the last batch or when the task completes, as
   * `DataSourceRDD` closes its readers, by the task-end rule of `ExternalLookupReader`. A thread
   * other than the task's may still drive the join then. The close does not wait for it: it closes
   * the open cursor that it finds, then the reader. That thread checks for the close before each
   * connector call, so it makes no further one, but the one call that has passed its check may
   * still run. So a cursor that a request publishes after the close took the empty slot, or
   * obtains after the close returned, is left to the reader's close, and a finished cursor that
   * the thread took before the close may be closed after the reader.
   */
  private class LookupReader {
    private val taskReaders = readerMetrics.forCurrentTask()
    private val closed = new AtomicBoolean(false)
    // At most one of the join and close() takes each, so Spark closes each at most once. A cursor
    // that neither takes is left to the reader's close.
    private val openReader = new AtomicReference[ExternalLookupReader]()
    private val activeCursor = new AtomicReference[ExternalLookupResultCursor]()
    private var resultRowsSinceMetricsUpdate = 0
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

    /** Sends one request and returns the cursor over its result rows. */
    def lookup(requestRows: Array[UnsafeRow]): ExternalLookupResultCursor = {
      ensureOpen()
      metrics.requests += 1
      metrics.parameterRows += requestRows.length
      // close() marks the reader closed before it takes the reader, so a reader that it took
      // cannot be created again here.
      val reader = Option(openReader.get()).getOrElse { ensureOpen(); createReader() }
      ensureOpen()
      val cursor = Option(reader.lookupBatch(requestRows.asInstanceOf[Array[InternalRow]]))
        .getOrElse {
          throw QueryExecutionErrors.invalidDataSourceExternalLookupError(
            tableName, "NULL_RESULT", Map("method" -> "ExternalLookupReader.lookupBatch()"))
        }
      activeCursor.set(cursor)
      ensureOpen()
      cursor
    }

    private def createReader(): ExternalLookupReader = {
      val reader = Option(readerFactory.createReader()).getOrElse {
        throw QueryExecutionErrors.invalidDataSourceExternalLookupError(
          tableName, "NULL_RESULT", Map("method" -> "ExternalLookupReaderFactory.createReader()"))
      }
      taskReaders.opened(reader)
      openReader.set(reader)
      // A close() while the reader was created did not find it, so it is closed here.
      if (closed.get() && openReader.compareAndSet(reader, null)) {
        ExternalLookupJoinExec.closeResources((), taskReaders.closing(reader), reader.close())
      }
      reader
    }

    /** Fails once the reader is closed, before any further connector call. */
    def ensureOpen(): Unit = if (closed.get()) {
      throw SparkException.internalError("External lookup reader used after it was closed")
    }

    def isClosed: Boolean = closed.get()

    /** Samples the connector metrics every [[CustomMetrics.NUM_ROWS_PER_UPDATE]] result rows. */
    def resultRowRead(): Unit = {
      resultRowsSinceMetricsUpdate += 1
      if (resultRowsSinceMetricsUpdate >= CustomMetrics.NUM_ROWS_PER_UPDATE) updateMetrics()
    }

    /** Samples the connector metrics, unless the reader is closed, whose close keeps them. */
    def updateMetrics(): Unit = {
      if (!closed.get() && openReader.get() != null) taskReaders.report()
      resultRowsSinceMetricsUpdate = 0
    }

    def closeCursor(): Unit = Option(activeCursor.getAndSet(null)).foreach(_.close())

    def close(): Unit = if (closed.compareAndSet(false, true)) {
      val cursor = activeCursor.getAndSet(null)
      val reader = openReader.getAndSet(null)
      ExternalLookupJoinExec.closeResources(
        Option(cursor).foreach(_.close()),
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
  def closing(reader: ExternalLookupReader): Unit = {
    val finalValues = reader.currentMetricsValues()
    synchronized {
      finalValues.foreach { metric =>
        closedMetrics.update(
          metric.name(), closedMetrics.get(metric.name()).fold(metric)(_.mergeWith(metric)))
      }
      openReaders -= reader
    }
    report()
  }

  /**
   * Sets the connector metrics to the merged values of the task's readers. The readers are called
   * outside the lock, so a close never waits for a metrics read in progress, as in
   * `DataSourceRDD`.
   */
  def report(): Unit = {
    val currentValues = synchronized(openReaders.toList).flatMap(_.currentMetricsValues())
    synchronized(currentValues ++ closedMetrics.values)
      .groupMapReduce(_.name())(identity)(_.mergeWith(_))
      .values
      .foreach { metric =>
        customMetrics.get(ExternalLookupJoinExec.sourceMetricKey(metric.name()))
          .foreach(_.set(metric.value()))
      }
  }
}
