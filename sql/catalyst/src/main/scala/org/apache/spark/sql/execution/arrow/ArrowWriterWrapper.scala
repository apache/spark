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
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.sql.execution.arrow.{ArrowWriter => SparkArrowWriter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils


/**
 * The Arrow writer of one group that the grouped and cogrouped Python runners write. The runner
 * closes it at the end of the group, and in a task completion listener, registered once for all
 * its groups, if the task completes before the end of the group.
 */
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
  /**
   * Creates the Arrow stream of a group and writes its schema. If that fails, it closes what it
   * allocated before rethrowing, since the caller does not get the wrapper to close.
   */
  def createAndStartArrowWriter(
      schema: StructType,
      timeZoneId: String,
      allocatorOwner: String,
      largeVarTypes: Boolean,
      dataOut: DataOutputStream): ArrowWriterWrapper = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId, largeVarTypes)
    val allocator = ArrowUtils.rootAllocator.newChildAllocator(
      s"stdout writer for $allocatorOwner", 0, Long.MaxValue)
    var root: VectorSchemaRoot = null
    try {
      root = VectorSchemaRoot.create(arrowSchema, allocator)
      val arrowWriter = SparkArrowWriter.create(root)
      val streamWriter = new ArrowStreamWriter(root, null, dataOut)
      streamWriter.start()
      // Unloader will be set by the caller after creation
      ArrowWriterWrapper(streamWriter, arrowWriter, root, allocator, null)
    } catch {
      case t: Throwable =>
        AutoCloseables.close(t, root, allocator)
        throw t
    }
  }
}
