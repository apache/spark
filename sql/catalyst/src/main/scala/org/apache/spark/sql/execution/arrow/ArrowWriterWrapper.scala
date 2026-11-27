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

package org.apache.spark.sql.execution.arrow

import java.io.DataOutputStream

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.sql.execution.arrow.{ArrowWriter => SparkArrowWriter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils


case class ArrowWriterWrapper(
    var streamWriter: ArrowStreamWriter,
    var arrowWriter: SparkArrowWriter,
    var root: VectorSchemaRoot,
    var allocator: BufferAllocator,
    var unloader: VectorUnloader) {
  @volatile var isClosed = false

  /*
  * Idempotent method to release the resources. Access to any member object is invalid
  * after calling close().
  */
  def close(): Unit = synchronized {
    if (!isClosed) {
      root.close()
      allocator.close()
      isClosed = true
      // Set members to null to enable GC as the TaskCompletionListener holds a reference to
      // the ArrowWriterWrapper.
      streamWriter = null
      arrowWriter = null
      root = null
      allocator = null
      unloader = null
    }
  }
}

object ArrowWriterWrapper {
  /**
   * Owns the Arrow stream for the current group. Cogrouped input writes its left and right
   * streams in sequence, so there is at most one open stream per input writer.
   */
  class InputWriter(context: TaskContext) {
    private val lock = new Object
    private var inputWriteInProgress = false
    private var writer: ArrowWriterWrapper = null

    // A helper thread can still be writing when the task completes. Do not wait for upstream
    // input here, or write an Arrow footer to an output stream that may already be closed.
    context.addTaskCompletionListener[Unit] { _ =>
      lock.synchronized {
        if (!inputWriteInProgress) {
          closeWriter()
        }
      }
    }

    private def closeWriter(): Unit = {
      if (writer != null) {
        writer.close()
        writer = null
      }
    }

    def withInputWrite[T](write: => T): T = {
      lock.synchronized {
        if (context.isCompleted()) {
          throw new TaskKilledException("Task completed before writing Python Arrow input")
        }
        inputWriteInProgress = true
      }
      Utils.tryWithSafeFinally {
        write
      } {
        lock.synchronized {
          inputWriteInProgress = false
          if (context.isCompleted()) {
            closeWriter()
          }
        }
      }
    }

    def createAndStartArrowWriter(
        schema: StructType,
        timeZoneId: String,
        allocatorOwner: String,
        largeVarTypes: Boolean,
        dataOut: DataOutputStream): ArrowWriterWrapper = {
      assert(inputWriteInProgress)
      assert(writer == null || writer.isClosed)
      val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId, largeVarTypes)
      val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdout writer for $allocatorOwner", 0, Long.MaxValue)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      // Retain the wrapper before allocating vectors or writing the schema, so task completion
      // can clean up even if initialization fails. An active write defers cleanup to its finally.
      writer = ArrowWriterWrapper(null, null, root, allocator, null)
      writer.arrowWriter = SparkArrowWriter.create(root)
      writer.streamWriter = new ArrowStreamWriter(root, null, dataOut)
      writer.streamWriter.start()
      writer
    }
  }
}
