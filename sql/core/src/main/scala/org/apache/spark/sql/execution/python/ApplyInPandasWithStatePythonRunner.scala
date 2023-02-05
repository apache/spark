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

package org.apache.spark.sql.execution.python

import java.io._

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.api.python._
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.python.PythonSQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.ApplyInPandasWithStatePythonRunner.{InType, OutType, OutTypeForState, STATE_METADATA_SCHEMA_FROM_PYTHON_WORKER}
import org.apache.spark.sql.execution.python.ApplyInPandasWithStateWriter.STATE_METADATA_SCHEMA
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}


/**
 * A variant implementation of [[ArrowPythonRunner]] to serve the operation
 * applyInPandasWithState.
 *
 * Unlike normal ArrowPythonRunner which both input and output (executor <-> python worker)
 * are InternalRow, applyInPandasWithState has side data (state information) in both input
 * and output along with data, which requires different struct on Arrow RecordBatch.
 */
class ApplyInPandasWithStatePythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    inputSchema: StructType,
    override protected val timeZoneId: String,
    initialWorkerConf: Map[String, String],
    stateEncoder: ExpressionEncoder[Row],
    keySchema: StructType,
    outputSchema: StructType,
    stateValueSchema: StructType,
    val pythonMetrics: Map[String, SQLMetric])
  extends BasePythonRunner[InType, OutType](funcs, evalType, argOffsets)
  with PythonArrowInput[InType]
  with PythonArrowOutput[OutType] {

  private val sqlConf = SQLConf.get

  override protected val schema: StructType = inputSchema.add("__state", STATE_METADATA_SCHEMA)

  override val simplifiedTraceback: Boolean = sqlConf.pysparkSimplifiedTraceback

  override val bufferSize: Int = {
    val configuredSize = sqlConf.pandasUDFBufferSize
    if (configuredSize < 4) {
      logWarning("Pandas execution requires more than 4 bytes. Please configure bigger value " +
        s"for the configuration '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'. " +
        "Force using the value '4'.")
      4
    } else {
      configuredSize
    }
  }

  private val arrowMaxRecordsPerBatch = sqlConf.arrowMaxRecordsPerBatch

  // applyInPandasWithState has its own mechanism to construct the Arrow RecordBatch instance.
  // Configurations are both applied to executor and Python worker, set them to the worker conf
  // to let Python worker read the config properly.
  override protected val workerConf: Map[String, String] = initialWorkerConf +
    (SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> arrowMaxRecordsPerBatch.toString)

  private val stateRowDeserializer = stateEncoder.createDeserializer()

  /**
   * This method sends out the additional metadata before sending out actual data.
   *
   * Specifically, this class overrides this method to also write the schema for state value.
   */
  override protected def handleMetadataBeforeExec(stream: DataOutputStream): Unit = {
    super.handleMetadataBeforeExec(stream)
    // Also write the schema for state value
    PythonRDD.writeUTF(stateValueSchema.json, stream)
  }

  /**
   * Read the (key, state, values) from input iterator and construct Arrow RecordBatches, and
   * write constructed RecordBatches to the writer.
   *
   * See [[ApplyInPandasWithStateWriter]] for more details.
   */
  protected def writeIteratorToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[InType]): Unit = {
    val w = new ApplyInPandasWithStateWriter(root, writer, arrowMaxRecordsPerBatch)

    while (inputIterator.hasNext) {
      val startData = dataOut.size()
      val (keyRow, groupState, dataIter) = inputIterator.next()
      assert(dataIter.hasNext, "should have at least one data row!")
      w.startNewGroup(keyRow, groupState)

      while (dataIter.hasNext) {
        val dataRow = dataIter.next()
        w.writeRow(dataRow)
      }

      w.finalizeGroup()
      val deltaData = dataOut.size() - startData
      pythonMetrics("pythonDataSent") += deltaData
    }

    w.finalizeData()
  }

  /**
   * Deserialize ColumnarBatch received from the Python worker to produce the output. Schema info
   * for given ColumnarBatch is also provided as well.
   */
  protected def deserializeColumnarBatch(batch: ColumnarBatch, schema: StructType): OutType = {
    // This should at least have one row for state. Also, we ensure that all columns across
    // data and state metadata have same number of rows, which is required by Arrow record
    // batch.
    assert(batch.numRows() > 0)
    assert(schema.length == 2)

    def getColumnarBatchForStructTypeColumn(
        batch: ColumnarBatch,
        ordinal: Int,
        expectedType: StructType): ColumnarBatch = {
      //  UDF returns a StructType column in ColumnarBatch, select the children here
      val structVector = batch.column(ordinal).asInstanceOf[ArrowColumnVector]
      val dataType = schema(ordinal).dataType.asInstanceOf[StructType]
      assert(dataType.sameType(expectedType),
        s"Schema equality check failure! type from Arrow: $dataType, expected type: $expectedType")

      val outputVectors = dataType.indices.map(structVector.getChild)
      val flattenedBatch = new ColumnarBatch(outputVectors.toArray)
      flattenedBatch.setNumRows(batch.numRows())

      flattenedBatch
    }

    def constructIterForData(batch: ColumnarBatch): Iterator[InternalRow] = {
      val dataBatch = getColumnarBatchForStructTypeColumn(batch, 0, outputSchema)
      dataBatch.rowIterator.asScala.flatMap { row =>
        if (row.isNullAt(0)) {
          // The entire row in record batch seems to be for state metadata.
          None
        } else {
          Some(row)
        }
      }
    }

    def constructIterForState(batch: ColumnarBatch): Iterator[OutTypeForState] = {
      val stateMetadataBatch = getColumnarBatchForStructTypeColumn(batch, 1,
        STATE_METADATA_SCHEMA_FROM_PYTHON_WORKER)

      stateMetadataBatch.rowIterator().asScala.flatMap { row =>
        implicit val formats = org.json4s.DefaultFormats

        if (row.isNullAt(0)) {
          // The entire row in record batch seems to be for data.
          None
        } else {
          // NOTE: See ApplyInPandasWithStatePythonRunner.STATE_METADATA_SCHEMA_FROM_PYTHON_WORKER
          // for the schema.
          val propertiesAsJson = parse(row.getUTF8String(0).toString)
          val keyRowAsUnsafeAsBinary = row.getBinary(1)
          val keyRowAsUnsafe = new UnsafeRow(keySchema.fields.length)
          keyRowAsUnsafe.pointTo(keyRowAsUnsafeAsBinary, keyRowAsUnsafeAsBinary.length)
          val maybeObjectRow = if (row.isNullAt(2)) {
            None
          } else {
            val pickledStateValue = row.getBinary(2)
            Some(PythonSQLUtils.toJVMRow(pickledStateValue, stateValueSchema,
              stateRowDeserializer))
          }
          val oldTimeoutTimestamp = row.getLong(3)

          Some((keyRowAsUnsafe, GroupStateImpl.fromJson(maybeObjectRow, propertiesAsJson),
            oldTimeoutTimestamp))
        }
      }
    }

    (constructIterForState(batch), constructIterForData(batch))
  }
}

object ApplyInPandasWithStatePythonRunner {
  type InType = (UnsafeRow, GroupStateImpl[Row], Iterator[InternalRow])
  type OutTypeForState = (UnsafeRow, GroupStateImpl[Row], Long)
  type OutType = (Iterator[OutTypeForState], Iterator[InternalRow])

  val STATE_METADATA_SCHEMA_FROM_PYTHON_WORKER: StructType = StructType(
    Array(
      StructField("properties", StringType),
      StructField("keyRowAsUnsafe", BinaryType),
      StructField("object", BinaryType),
      StructField("oldTimeoutTimestamp", LongType)
    )
  )
}
