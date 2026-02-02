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

import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.arrow.{ArrowWriter => SparkArrowWriter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils


case class ArrowWriterWrapper(
    var streamWriter: ArrowStreamWriter,
    var arrowWriter: SparkArrowWriter,
    var root: VectorSchemaRoot,
    var allocator: BufferAllocator,
    var unloader: VectorUnloader,
    context: TaskContext) {
  @volatile var isClosed = false

  // Register a listener to close the ArrowStreamWriter when the task completes. This
  // ensures that the ArrowStreamWriter is closed even if the task fails or is interrupted.
  context.addTaskCompletionListener[Unit](_ => {
    this.close();
  })

  /*
  * Idempotent method to release the resources. Access to any member object is invalid
  * after calling close().
  */
  def close(): Unit = {
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
  def createAndStartArrowWriter(
      schema: StructType,
      timeZoneId: String,
      allocatorOwner: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean,
      dataOut: DataOutputStream,
      context: TaskContext): ArrowWriterWrapper = {
    val arrowSchema =
      ArrowUtils.toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes)
    val allocator = ArrowUtils.rootAllocator.newChildAllocator(
      s"stdout writer for $allocatorOwner", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = SparkArrowWriter.create(root)

    val streamWriter = new ArrowStreamWriter(root, null, dataOut)
    streamWriter.start()
    // Unloader will be set by the caller after creation
    ArrowWriterWrapper(streamWriter, arrowWriter, root, allocator, null, context)
  }
}
