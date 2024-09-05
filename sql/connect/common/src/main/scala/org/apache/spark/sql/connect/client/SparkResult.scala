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
package org.apache.spark.sql.connect.client

import java.lang.ref.Cleaner
import java.util.Objects

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.{ArrowMessage, ArrowRecordBatch}
import org.apache.arrow.vector.types.pojo

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.ExecutePlanResponse.ObservedMetrics
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{ProductEncoder, UnboundRowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.connect.client.arrow.{AbstractMessageIterator, ArrowDeserializingIterator, ConcatenatingArrowStreamReader, MessageIterator}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, LiteralValueProtoConverter}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ArrowUtils

private[sql] class SparkResult[T](
    responses: CloseableIterator[proto.ExecutePlanResponse],
    allocator: BufferAllocator,
    encoder: AgnosticEncoder[T],
    timeZoneId: String,
    setObservationMetricsOpt: Option[(Long, Row) => Unit] = None)
    extends AutoCloseable { self =>

  case class StageInfo(
      stageId: Long,
      numTasks: Long,
      completedTasks: Long = 0,
      inputBytesRead: Long = 0,
      completed: Boolean = false)

  object StageInfo {
    def apply(stageInfo: proto.ExecutePlanResponse.ExecutionProgress.StageInfo): StageInfo = {
      StageInfo(
        stageInfo.getStageId,
        stageInfo.getNumTasks,
        stageInfo.getNumCompletedTasks,
        stageInfo.getInputBytesRead,
        stageInfo.getDone)
    }
  }

  object Progress {
    def apply(progress: proto.ExecutePlanResponse.ExecutionProgress): Progress = {
      Progress(
        progress.getStagesList.asScala.map(StageInfo(_)).toSeq,
        progress.getNumInflightTasks)
    }
  }

  /**
   * Progress of the query execution. This information can be accessed from the iterator.
   */
  case class Progress(stages: Seq[StageInfo], inflight: Long)

  var progress: Progress = new Progress(Seq.empty, 0)
  private[this] var opId: String = _
  private[this] var numRecords: Int = 0
  private[this] var structType: StructType = _
  private[this] var arrowSchema: pojo.Schema = _
  private[this] var nextResultIndex: Int = 0
  private val resultMap = mutable.Map.empty[Int, (Long, Seq[ArrowMessage])]
  private val observedMetrics = mutable.Map.empty[String, Row]
  private val cleanable =
    SparkResult.cleaner.register(this, new SparkResultCloseable(resultMap, responses))

  /**
   * Update RowEncoder and recursively update the fields of the ProductEncoder if found.
   */
  private def createEncoder[E](
      enc: AgnosticEncoder[E],
      dataType: DataType): AgnosticEncoder[E] = {
    enc match {
      case UnboundRowEncoder =>
        // Replace the row encoder with the encoder inferred from the schema.
        RowEncoder
          .encoderFor(dataType.asInstanceOf[StructType])
          .asInstanceOf[AgnosticEncoder[E]]
      case ProductEncoder(clsTag, fields, outer) if ProductEncoder.isTuple(clsTag) =>
        // Recursively continue updating the tuple product encoder
        val schema = dataType.asInstanceOf[StructType]
        assert(fields.length <= schema.fields.length)
        val updatedFields = fields.zipWithIndex.map { case (f, id) =>
          f.copy(enc = createEncoder(f.enc, schema.fields(id).dataType))
        }
        ProductEncoder(clsTag, updatedFields, outer)
      case _ =>
        enc
    }
  }

  private def processResponses(
      stopOnOperationId: Boolean = false,
      stopOnSchema: Boolean = false,
      stopOnArrowSchema: Boolean = false,
      stopOnFirstNonEmptyResponse: Boolean = false): Boolean = {
    var nonEmpty = false
    var stop = false
    while (!stop && responses.hasNext) {
      val response = responses.next()

      // Collect metrics for this response
      observedMetrics ++= processObservedMetrics(response.getObservedMetricsList)

      // Save and validate operationId
      if (opId == null) {
        opId = response.getOperationId
      }
      if (opId != response.getOperationId) {
        // backwards compatibility:
        // response from an old server without operationId field would have getOperationId == "".
        throw new IllegalStateException(
          "Received response with wrong operationId. " +
            s"Expected '$opId' but received '${response.getOperationId}'.")
      }
      stop |= stopOnOperationId

      // Update the execution status. This information can now be accessed directly from
      // the iterator.
      if (response.hasExecutionProgress) {
        progress = Progress(response.getExecutionProgress)
      }

      if (response.hasSchema) {
        // The original schema should arrive before ArrowBatches.
        structType =
          DataTypeProtoConverter.toCatalystType(response.getSchema).asInstanceOf[StructType]
        stop |= stopOnSchema
      }
      if (response.hasArrowBatch) {
        val ipcStreamBytes = response.getArrowBatch.getData
        val expectedNumRows = response.getArrowBatch.getRowCount
        val reader = new MessageIterator(ipcStreamBytes.newInput(), allocator)
        if (arrowSchema == null) {
          arrowSchema = reader.schema
          stop |= stopOnArrowSchema
        } else if (arrowSchema != reader.schema) {
          throw new IllegalStateException(
            s"""Schema Mismatch between expected and received schema:
               |=== Expected Schema ===
               |$arrowSchema
               |=== Received Schema ===
               |${reader.schema}
               |""".stripMargin)
        }
        if (structType == null) {
          // If the schema is not available yet, fallback to the arrow schema.
          structType = ArrowUtils.fromArrowSchema(reader.schema)
        }
        if (response.getArrowBatch.hasStartOffset) {
          val expectedStartOffset = response.getArrowBatch.getStartOffset
          if (numRecords != expectedStartOffset) {
            throw new IllegalStateException(
              s"Expected arrow batch to start at row offset $numRecords in results, " +
                s"but received arrow batch starting at offset $expectedStartOffset.")
          }
        }
        var numRecordsInBatch = 0
        val messages = Seq.newBuilder[ArrowMessage]
        while (reader.hasNext) {
          val message = reader.next()
          message match {
            case batch: ArrowRecordBatch =>
              numRecordsInBatch += batch.getLength
            case _ =>
          }
          messages += message
        }
        if (numRecordsInBatch != expectedNumRows) {
          throw new IllegalStateException(
            s"Expected $expectedNumRows rows in arrow batch but got $numRecordsInBatch.")
        }
        // Skip the entire result if it is empty.
        if (numRecordsInBatch > 0) {
          numRecords += numRecordsInBatch
          resultMap.put(nextResultIndex, (reader.bytesRead, messages.result()))
          nextResultIndex += 1
          nonEmpty |= true
          stop |= stopOnFirstNonEmptyResponse
        }
      }
    }
    nonEmpty
  }

  private def processObservedMetrics(
      metrics: java.util.List[ObservedMetrics]): Iterable[(String, Row)] = {
    metrics.asScala.map { metric =>
      assert(metric.getKeysCount == metric.getValuesCount)
      var schema = new StructType()
      val values = mutable.ArrayBuilder.make[Any]
      values.sizeHint(metric.getKeysCount)
      (0 until metric.getKeysCount).foreach { i =>
        val key = metric.getKeys(i)
        val value = LiteralValueProtoConverter.toCatalystValue(metric.getValues(i))
        schema = schema.add(key, LiteralValueProtoConverter.toDataType(value.getClass))
        values += value
      }
      val row = new GenericRowWithSchema(values.result(), schema)
      // If the metrics is registered by an Observation object, attach them and unblock any
      // blocked thread.
      setObservationMetricsOpt.foreach { setObservationMetrics =>
        setObservationMetrics(metric.getPlanId, row)
      }
      metric.getName -> row
    }
  }

  /**
   * Returns the number of elements in the result.
   */
  def length: Int = {
    // We need to process all responses to make sure numRecords is correct.
    processResponses()
    numRecords
  }

  /**
   * @return
   *   the schema of the result.
   */
  def schema: StructType = {
    if (structType == null) {
      processResponses(stopOnSchema = true)
    }
    structType
  }

  /**
   * @return
   *   the operationId of the result.
   */
  def operationId: String = {
    if (opId == null) {
      processResponses(stopOnOperationId = true)
    }
    opId
  }

  /**
   * Create an Array with the contents of the result.
   */
  def toArray: Array[T] = {
    val result = encoder.clsTag.newArray(length)
    val rows = iterator
    try {
      var i = 0
      while (rows.hasNext) {
        result(i) = rows.next()
        assert(i < numRecords)
        i += 1
      }
    } finally {
      rows.close()
    }
    result
  }

  /**
   * Returns all observed metrics in the result.
   */
  def getObservedMetrics: Map[String, Row] = {
    // We need to process all responses to get all metrics.
    processResponses()
    observedMetrics.toMap
  }

  /**
   * Returns an iterator over the contents of the result.
   */
  def iterator: CloseableIterator[T] =
    buildIterator(destructive = false)

  /**
   * Returns an destructive iterator over the contents of the result.
   */
  def destructiveIterator: CloseableIterator[T] =
    buildIterator(destructive = true)

  private def buildIterator(destructive: Boolean): CloseableIterator[T] = {
    new CloseableIterator[T] {
      private[this] var iter: CloseableIterator[T] = _

      private def initialize(): Unit = {
        if (iter == null) {
          iter = new ArrowDeserializingIterator(
            createEncoder(encoder, schema),
            new ConcatenatingArrowStreamReader(
              allocator,
              Iterator.single(new ResultMessageIterator(destructive)),
              destructive),
            timeZoneId)
        }
      }

      override def hasNext: Boolean = {
        initialize()
        iter.hasNext
      }

      override def next(): T = {
        initialize()
        iter.next()
      }

      override def close(): Unit = {
        if (iter != null) {
          iter.close()
        }
      }
    }
  }

  /**
   * Close this result, freeing any underlying resources.
   */
  override def close(): Unit = cleanable.clean()

  private class ResultMessageIterator(destructive: Boolean) extends AbstractMessageIterator {
    private[this] var totalBytesRead = 0L
    private[this] var nextResultIndex = 0
    private[this] var current: Iterator[ArrowMessage] = Iterator.empty

    override def bytesRead: Long = totalBytesRead

    override def schema: pojo.Schema = {
      if (arrowSchema == null) {
        // We need a schema to proceed. Spark Connect will always
        // return a result (with a schema) even if the result is empty.
        processResponses(stopOnArrowSchema = true)
        Objects.requireNonNull(arrowSchema)
      }
      arrowSchema
    }

    override def hasNext: Boolean = {
      if (current.hasNext) {
        return true
      }
      val hasNextResult = if (!resultMap.contains(nextResultIndex)) {
        self.processResponses(stopOnFirstNonEmptyResponse = true)
      } else {
        true
      }
      if (hasNextResult) {
        val Some((sizeInBytes, messages)) = if (destructive) {
          resultMap.remove(nextResultIndex)
        } else {
          resultMap.get(nextResultIndex)
        }
        totalBytesRead += sizeInBytes
        current = messages.iterator
        nextResultIndex += 1
      }
      hasNextResult
    }

    override def next(): ArrowMessage = {
      if (!hasNext) {
        throw new NoSuchElementException()
      }
      current.next()
    }
  }
}

private object SparkResult {
  private val cleaner: Cleaner = Cleaner.create()
}

private[client] class SparkResultCloseable(
    resultMap: mutable.Map[Int, (Long, Seq[ArrowMessage])],
    responses: CloseableIterator[proto.ExecutePlanResponse])
    extends AutoCloseable
    with Runnable {
  override def close(): Unit = {
    resultMap.values.foreach(_._2.foreach(_.close()))
    responses.close()
  }

  override def run(): Unit = {
    close()
  }
}
