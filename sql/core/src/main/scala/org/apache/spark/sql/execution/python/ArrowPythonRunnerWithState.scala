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
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.{SparkEnv, TaskContext}

import org.apache.spark.api.python._
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.python.PythonSQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * [[ArrowPythonRunner]] with [[org.apache.spark.sql.streaming.GroupState]].
 */
class ArrowPythonRunnerWithState(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    inputSchema: StructType,
    timeZoneId: String,
    workerConf: Map[String, String],
    stateEncoder: ExpressionEncoder[Row],
    keySchema: StructType,
    valueSchema: StructType,
    stateSchema: StructType)
  extends BasePythonRunner[
    (UnsafeRow, GroupStateImpl[Row], Iterator[InternalRow]),
    (UnsafeRow, GroupStateImpl[Row], Iterator[InternalRow])](
    funcs, evalType, argOffsets) {

  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")

  val schemaWithState = inputSchema.add("!__state__!",
    StructType(
      Array(
        StructField("properties", StringType),
        StructField("keyRowAsUnsafe", BinaryType),
        StructField("object", BinaryType)
      )
    )
  )

  val stateRowSerializer = stateEncoder.createSerializer()
  val stateRowDeserializer = stateEncoder.createDeserializer()

  protected def handleMetadataBeforeExec(stream: DataOutputStream): Unit = {
    // Write config for the worker as a number of key -> value pairs of strings
    stream.writeInt(workerConf.size)
    for ((k, v) <- workerConf) {
      PythonRDD.writeUTF(k, stream)
      PythonRDD.writeUTF(v, stream)
    }
    PythonRDD.writeUTF(stateSchema.json, stream)
  }

  protected override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[(UnsafeRow, GroupStateImpl[Row], Iterator[InternalRow])],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        handleMetadataBeforeExec(dataOut)
        PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
      }

      private def buildStateInfoRow(
          keyRow: UnsafeRow,
          groupState: GroupStateImpl[Row]): InternalRow = {
        val stateUnderlyingRow = new GenericInternalRow(
          Array[Any](
            UTF8String.fromString(groupState.json()),
            keyRow.getBytes,
            groupState.getOption.map(PythonSQLUtils.toPyRow).orNull
          )
        )
        new GenericInternalRow(Array[Any](stateUnderlyingRow))
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        val arrowSchema = ArrowUtils.toArrowSchema(schemaWithState, timeZoneId)
        val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for $pythonExec", 0, Long.MaxValue)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)

        Utils.tryWithSafeFinally {
          val nullDataRow = new GenericInternalRow(Array.fill(inputSchema.length)(null: Any))
          val nullStateInfoRow = new GenericInternalRow(Array.fill(1)(null: Any))

          val arrowWriter = ArrowWriter.create(root)
          val writer = new ArrowStreamWriter(root, null, dataOut)
          writer.start()

          val joinedRow = new JoinedRow
          while (inputIterator.hasNext) {
            val (keyRow, groupState, dataIter) = inputIterator.next()

            // Provide state info row in the first row
            val stateInfoRow = buildStateInfoRow(keyRow, groupState)
            joinedRow.withLeft(nullDataRow).withRight(stateInfoRow)
            arrowWriter.write(joinedRow)

            // Continue providing remaining data rows
            while (dataIter.hasNext) {
              val dataRow = dataIter.next()
              joinedRow.withLeft(dataRow).withRight(nullStateInfoRow)
              arrowWriter.write(joinedRow)
            }

            arrowWriter.finish()
            writer.writeBatch()
            arrowWriter.reset()
          }
          // end writes footer to the output stream and doesn't clean any resources.
          // It could throw exception if the output stream is closed, so it should be
          // in the try block.
          writer.end()
        } {
          // If we close root and allocator in TaskCompletionListener, there could be a race
          // condition where the writer thread keeps writing to the VectorSchemaRoot while
          // it's being closed by the TaskCompletion listener.
          // Closing root and allocator here is cleaner because root and allocator is owned
          // by the writer thread and is only visible to the writer thread.
          //
          // If the writer thread is interrupted by TaskCompletionListener, it should either
          // (1) in the try block, in which case it will get an InterruptedException when
          // performing io, and goes into the finally block or (2) in the finally block,
          // in which case it will ignore the interruption and close the resources.
          root.close()
          allocator.close()
        }
      }
    }
  }

  protected def newReaderIterator(
      stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[(UnsafeRow, GroupStateImpl[Row], Iterator[InternalRow])] = {

    new ReaderIterator(
      stream, writerThread, startTime, env, worker, pid, releasedOrClosed, context) {

      private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdin reader for $pythonExec", 0, Long.MaxValue)

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var schema: StructType = _
      private var vectors: Array[ColumnVector] = _
      private var unsafeProjForData: UnsafeProjection = _

      context.addTaskCompletionListener[Unit] { _ =>
        if (reader != null) {
          reader.close(false)
        }
        allocator.close()
      }

      private var batchLoaded = true

      protected override def read(): (UnsafeRow, GroupStateImpl[Row], Iterator[InternalRow]) = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          if (reader != null && batchLoaded) {
            batchLoaded = reader.loadNextBatch()
            if (batchLoaded) {
              val batch = new ColumnarBatch(vectors)
              batch.setNumRows(root.getRowCount)
              deserializeColumnarBatch(batch)
            } else {
              reader.close(false)
              allocator.close()
              // Reach end of stream. Call `read()` again to read control data.
              read()
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                reader = new ArrowStreamReader(stream, allocator)
                root = reader.getVectorSchemaRoot()
                // FIXME: should we validate schema here with value schema and state schema?
                schema = ArrowUtils.fromArrowSchema(root.getSchema())

                val dataAttributes = schema(0).dataType.asInstanceOf[StructType].toAttributes

                unsafeProjForData = UnsafeProjection.create(dataAttributes, dataAttributes)

                vectors = root.getFieldVectors().asScala.map { vector =>
                  new ArrowColumnVector(vector)
                }.toArray[ColumnVector]
                read()
              case SpecialLengths.TIMING_DATA =>
                handleTimingData()
                read()
              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException()
              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection()
                null
            }
          }
        } catch handleException
      }

      private def deserializeColumnarBatch(
          batch: ColumnarBatch): (UnsafeRow, GroupStateImpl[Row], Iterator[InternalRow]) = {
        // this should at least have one row for state
        assert(batch.numRows() > 0)
        assert(schema.length == 2)

        val structVectorForState = batch.column(1).asInstanceOf[ArrowColumnVector]
        val outputVectorsForState = schema(1).dataType.asInstanceOf[StructType]
          .indices.map(structVectorForState.getChild)
        val flattenedBatchForState = new ColumnarBatch(outputVectorsForState.toArray)
        flattenedBatchForState.setNumRows(1)

        val rowForStateInfo = flattenedBatchForState.getRow(0)

        //  UDF returns a StructType column in ColumnarBatch, select the children here
        val structVector = batch.column(0).asInstanceOf[ArrowColumnVector]
        val outputVectors = schema(0).dataType.asInstanceOf[StructType]
          .indices.map(structVector.getChild)
        val flattenedBatch = new ColumnarBatch(outputVectors.toArray)
        flattenedBatch.setNumRows(batch.numRows())

        val rowIterator = flattenedBatch.rowIterator.asScala
        // drop first row as it's reserved for state
        assert(rowIterator.hasNext)
        rowIterator.next()

        // FIXME: we rely on known schema for state info, but would we want to access this by
        //  column name?
        // Received state information does not need schemas - this class already knows them.
        /*
        Array(
          StructField("properties", StringType),
          StructField("keyRowAsUnsafe", BinaryType),
          StructField("object", BinaryType)
        )
        */
        implicit val formats = org.json4s.DefaultFormats

        val propertiesAsJson = parse(rowForStateInfo.getUTF8String(0).toString)
        val keyRowAsUnsafeAsBinary = rowForStateInfo.getBinary(1)
        val keyRowAsUnsafe = new UnsafeRow(keySchema.fields.length)
        keyRowAsUnsafe.pointTo(keyRowAsUnsafeAsBinary, keyRowAsUnsafeAsBinary.length)
        val maybeObjectRow = if (rowForStateInfo.isNullAt(2)) {
          None
        } else {
          val pickledRow = rowForStateInfo.getBinary(2)
          Some(PythonSQLUtils.toJVMRow(pickledRow, stateSchema, stateRowDeserializer))
        }

        val newGroupState = GroupStateImpl.fromJson(maybeObjectRow, propertiesAsJson)

        (keyRowAsUnsafe, newGroupState, rowIterator.map(unsafeProjForData))
      }
    }
  }
}
