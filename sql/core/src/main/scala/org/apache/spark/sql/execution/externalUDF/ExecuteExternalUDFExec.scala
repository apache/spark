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

package org.apache.spark.sql.execution.externalUDF

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.protobuf.UnsafeByteOperations
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression,
  ExternalUserDefinedFunction, JoinedRow, MutableProjection, NamedArgumentExpression,
  UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.execution.externalUDF.ExecuteExternalUDFExec._
import org.apache.spark.sql.execution.python.HybridRowQueue
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.udf.worker.{DataRequest, DataResponse, UDFWorkerSpecification}

/**
 * :: Experimental ::
 * Physical plan node that evaluates one scalar UDF in an external worker process.
 * Like [[org.apache.spark.sql.execution.python.ArrowEvalPythonExec]], this node projects UDF
 * arguments, queues original rows in [[HybridRowQueue]], and joins each response to its row.
 * It sends projected Arrow batches through an external-UDF dispatcher session instead of the
 * legacy PySpark runner.
 *
 * The dispatcher is intentionally not configured until a worker implementation is added, so
 * worker creation currently fails before any process is started.
 *
 * @param udf UDF expression evaluated by the worker session.
 * @param resultAttr Output attribute for the UDF expression.
 * @param child Child plan providing input rows.
 */
@Experimental
case class ExecuteExternalUDFExec(
    udf: ExternalUserDefinedFunction,
    resultAttr: Attribute,
    child: SparkPlan)
  extends ExternalUDFExec {

  override def workerSpec: UDFWorkerSpecification = udf.workerSpec

  // External worker outputs are always nullable.
  assert(resultAttr.nullable, "The external UDF result attribute must be nullable")

  override def output: Seq[Attribute] = child.output :+ resultAttr

  override def producedAttributes: AttributeSet = AttributeSet(Seq(resultAttr))

  override protected def doExecute(): RDD[InternalRow] = {
    // TODO(SPARK-59745): Preserve named argument metadata in unified Python UDF execution.
    if (udf.children.exists(_.isInstanceOf[NamedArgumentExpression])) {
      throw QueryCompilationErrors.namedArgumentsNotSupported(
        udf.name.getOrElse(udf.prettyName))
    }
    val argumentExpressions: Seq[Expression] = udf.children
    val inputSchema = StructType(
      argumentExpressions.zipWithIndex.map { case (expression, index) =>
        StructField(s"_$index", expression.dataType, expression.nullable)
      })
    val outputSchema = StructType(Seq(StructField("_0", udf.dataType, nullable = true)))
    val timeZoneId = conf.sessionLocalTimeZone
    val largeVarTypes = conf.arrowUseLargeVarTypes
    val maxRecordsPerBatch = conf.arrowMaxRecordsPerBatch
    val maxBytesPerBatch = conf.arrowMaxBytesPerBatch.toInt
    val preparedInit = prepareInit(inputSchema, outputSchema, timeZoneId, largeVarTypes)

    child.execute().mapPartitionsInternal { rows =>
      val context = TaskContext.get()
      val projection = MutableProjection.create(argumentExpressions, child.output)
      projection.initialize(context.partitionId())

      val queue = HybridRowQueue(context.taskMemoryManager(), child.output.length)
      context.addTaskCompletionListener[Unit](_ => queue.close())

      var inputRowCount = 0L
      var outputRowCount = 0L
      val projectedRows = rows.map { row =>
        queue.add(row.asInstanceOf[UnsafeRow])
        inputRowCount += 1
        projection(row)
      }

      withUDFWorkerSession(context) { session =>
        session.init(PythonInitAdapter.build(preparedInit, context))

        val batches = ArrowConverters.toBatchIterator(
          projectedRows,
          inputSchema,
          maxRecordsPerBatch,
          maxBytesPerBatch,
          timeZoneId,
          true,
          largeVarTypes,
          context)
        val requests = batches.map { batch =>
          DataRequest.newBuilder()
            .setData(UnsafeByteOperations.unsafeWrap(batch))
            .build()
        }
        val udfResults = new ArrowResponseRowIterator(
          session.process(requests),
          outputSchema,
          timeZoneId,
          largeVarTypes,
          context)

        val joined = new JoinedRow
        val resultProjection = UnsafeProjection.create(output, output)
        resultProjection.initialize(context.partitionId())
        var cardinalityVerified = false

        new Iterator[InternalRow] {
          override def hasNext: Boolean = {
            val hasNextResult = udfResults.hasNext
            if (!hasNextResult && !cardinalityVerified) {
              cardinalityVerified = true
              if (inputRowCount != outputRowCount) {
                throw cardinalityMismatch(inputRowCount, outputRowCount)
              }
            }
            hasNextResult
          }

          override def next(): InternalRow = {
            if (!hasNext) Iterator.empty.next()
            if (queue.getNumElementsQueued() == 0L) {
              val outputRowsLowerBound = outputRowCount + udfResults.bufferedRowCount
              throw cardinalityMismatch(inputRowCount, s"at least $outputRowsLowerBound")
            }
            val udfResult = udfResults.next()
            outputRowCount += 1
            resultProjection(joined(queue.remove(), udfResult))
          }
        }
      }
    }
  }

  private def prepareInit(
      inputSchema: StructType,
      outputSchema: StructType,
      timeZoneId: String,
      largeVarTypes: Boolean): PythonInitAdapter.PreparedInit = {
    // TODO(SPARK-59364): Make this node language-agnostic. Its dependency on the
    // Python-specific Init builder is temporary and tracked by this Spark ticket.
    PythonInitAdapter.prepare(
      workerSpec,
      udf,
      serializeArrowSchema(inputSchema, timeZoneId, largeVarTypes),
      serializeArrowSchema(outputSchema, timeZoneId, largeVarTypes),
      timeZoneId,
      PythonInitAdapter.sessionConf(conf, inputSchema))
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ExecuteExternalUDFExec =
    copy(child = newChild)
}

object ExecuteExternalUDFExec {
  private def serializeArrowSchema(
      schema: StructType,
      timeZoneId: String,
      largeVarTypes: Boolean): Array[Byte] = {
    val arrowSchema = ArrowUtils.toArrowSchema(
      schema,
      timeZoneId,
      true,
      largeVarTypes)
    val buffer = new ByteArrayOutputStream()
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(buffer)), arrowSchema)
    buffer.toByteArray
  }

  // Keep this condition and its parameter names aligned with verify_result_row_count in
  // python/pyspark/eval_handlers/verification.py. PySpark and the JVM use separate error catalogs,
  // so the condition must be registered in both.
  private def cardinalityMismatch(inputRows: Long, outputRows: Long): SparkException = {
    cardinalityMismatch(inputRows, outputRows.toString)
  }

  private def cardinalityMismatch(inputRows: Long, outputRows: String): SparkException = {
    new SparkException(
      errorClass = "RESULT_ROWS_MISMATCH",
      messageParameters = Map(
        "output_length" -> outputRows,
        "input_length" -> inputRows.toString),
      cause = null)
  }

  /** Decodes Arrow record-batch responses using the schema supplied in Init. */
  private class ArrowResponseRowIterator(
      responses: Iterator[DataResponse],
      expectedSchema: StructType,
      timeZoneId: String,
      largeVarTypes: Boolean,
      context: TaskContext)
    extends Iterator[InternalRow] {

    private var allocator: Option[BufferAllocator] = None
    private var root: Option[VectorSchemaRoot] = None
    private var columnarBatch: Option[ColumnarBatch] = None
    private var rows: Iterator[InternalRow] = Iterator.empty
    private var initialized = false
    private var exhausted = false
    private var closed = false
    private var rowsRemainingInBatch = 0L

    context.addTaskCompletionListener[Unit](_ => close())

    def bufferedRowCount: Long = rowsRemainingInBatch

    override def hasNext: Boolean = {
      try {
        val (activeAllocator, activeRoot, activeColumnarBatch) = initialize()
        while (!rows.hasNext && !exhausted) {
          if (responses.hasNext) {
            val batch = ArrowConverters.loadBatch(
              responses.next().getData.newInput(),
              activeAllocator)
            try {
              new VectorLoader(activeRoot).load(batch)
            } finally {
              batch.close()
            }
            activeColumnarBatch.setNumRows(activeRoot.getRowCount)
            rows = activeColumnarBatch.rowIterator().asScala
            rowsRemainingInBatch = activeRoot.getRowCount
          } else {
            exhausted = true
            close()
          }
        }
        rows.hasNext
      } catch {
        case NonFatal(error) =>
          close()
          throw error
      }
    }

    override def next(): InternalRow = {
      if (!hasNext) Iterator.empty.next()
      val row = rows.next()
      rowsRemainingInBatch -= 1
      row
    }

    private def initialize(): (BufferAllocator, VectorSchemaRoot, ColumnarBatch) = {
      if (!initialized) {
        initialized = true
        val newAllocator = ArrowUtils.rootAllocator.newChildAllocator(
          "externalUdfArrowResponse",
          0,
          Long.MaxValue)
        allocator = Some(newAllocator)
        val newRoot = VectorSchemaRoot.create(
          ArrowUtils.toArrowSchema(
            expectedSchema,
            timeZoneId,
            true,
            largeVarTypes),
          newAllocator)
        root = Some(newRoot)
        val columns: Array[ColumnVector] =
          newRoot.getFieldVectors.asScala.iterator.map { vector =>
            new ArrowColumnVector(vector): ColumnVector
          }.toArray
        columnarBatch = Some(new ColumnarBatch(columns))
      }
      (allocator, root, columnarBatch) match {
        case (Some(activeAllocator), Some(activeRoot), Some(activeColumnarBatch)) =>
          (activeAllocator, activeRoot, activeColumnarBatch)
        case _ =>
          throw SparkException.internalError(
            "The Arrow response row iterator was not initialized correctly.")
      }
    }

    private def close(): Unit = {
      if (!closed) {
        closed = true
        try {
          root.foreach(_.close())
        } finally {
          allocator.foreach(_.close())
        }
      }
    }
  }
}
