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
import java.net._

import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark._
import org.apache.spark.api.python._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils


class InterleavedArrowPythonRunner(
                         funcs: Seq[ChainedPythonFunctions],
                         evalType: Int,
                         argOffsets: Array[Array[Int]],
                         leftSchema: StructType,
                         rightSchema: StructType,
                         timeZoneId: String,
                         conf: Map[String, String])
  extends BaseArrowPythonRunner[(Iterator[InternalRow], Iterator[InternalRow])](
    funcs, evalType, argOffsets) {

  protected override def newWriterThread(
                                          env: SparkEnv,
                                          worker: Socket,
                                          inputIterator: Iterator[(Iterator[InternalRow], Iterator[InternalRow])],
                                          partitionIndex: Int,
                                          context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {

        // Write config for the worker as a number of key -> value pairs of strings
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }

        PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        val leftArrowSchema = ArrowUtils.toArrowSchema(leftSchema, timeZoneId)
        val rightArrowSchema = ArrowUtils.toArrowSchema(rightSchema, timeZoneId)
        val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for $pythonExec", 0, Long.MaxValue)
        val leftRoot = VectorSchemaRoot.create(leftArrowSchema, allocator)
        val rightRoot = VectorSchemaRoot.create(rightArrowSchema, allocator)

        Utils.tryWithSafeFinally {
          val leftArrowWriter = ArrowWriter.create(leftRoot)
          val rightArrowWriter = ArrowWriter.create(rightRoot)
          val writer = InterleavedArrowWriter(leftRoot, rightRoot, dataOut)
          writer.start()

          while (inputIterator.hasNext) {

            val (nextLeft, nextRight) = inputIterator.next()

            while (nextLeft.hasNext) {
              leftArrowWriter.write(nextLeft.next())
            }
            while (nextRight.hasNext) {
              rightArrowWriter.write(nextRight.next())
            }
            leftArrowWriter.finish()
            rightArrowWriter.finish()
            writer.writeBatch()
            leftArrowWriter.reset()
            rightArrowWriter.reset()
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
          leftRoot.close()
          rightRoot.close()
          allocator.close()
        }
      }
    }
  }
}
