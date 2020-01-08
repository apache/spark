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

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{NextIterator, Utils}

class PythonForeachWriter(func: PythonFunction, schema: StructType)
  extends ForeachWriter[UnsafeRow] {

  private lazy val context = TaskContext.get()
  private lazy val buffer = new PythonForeachWriter.UnsafeRowBuffer(
    context.taskMemoryManager, new File(Utils.getLocalDir(SparkEnv.get.conf)), schema.fields.length)
  private lazy val inputRowIterator = buffer.iterator

  private lazy val inputByteIterator = {
    EvaluatePython.registerPicklers()
    val objIterator = inputRowIterator.map { row => EvaluatePython.toJava(row, schema) }
    new SerDeUtil.AutoBatchedPickler(objIterator)
  }

  private lazy val pythonRunner = {
    PythonRunner(func)
  }

  private lazy val outputIterator =
    pythonRunner.compute(inputByteIterator, context.partitionId(), context)

  override def open(partitionId: Long, version: Long): Boolean = {
    outputIterator  // initialize everything
    TaskContext.get.addTaskCompletionListener[Unit] { _ => buffer.close() }
    true
  }

  override def process(value: UnsafeRow): Unit = {
    buffer.add(value)
  }

  override def close(errorOrNull: Throwable): Unit = {
    buffer.allRowsAdded()
    if (outputIterator.hasNext) outputIterator.next() // to throw python exception if there was one
  }
}

object PythonForeachWriter {

  /**
   * A buffer that is designed for the sole purpose of buffering UnsafeRows in PythonForeachWriter.
   * It is designed to be used with only 1 writer thread (i.e. JVM task thread) and only 1 reader
   * thread (i.e. PythonRunner writing thread that reads from the buffer and writes to the Python
   * worker stdin). Adds to the buffer are non-blocking, and reads through the buffer's iterator
   * are blocking, that is, it blocks until new data is available or all data has been added.
   *
   * Internally, it uses a [[HybridRowQueue]] to buffer the rows in a practically unlimited queue
   * across memory and local disk. However, HybridRowQueue is designed to be used only with
   * EvalPythonExec where the reader is always behind the writer, that is, the reader does not
   * try to read n+1 rows if the writer has only written n rows at any point of time. This
   * assumption is not true for PythonForeachWriter where rows may be added at a different rate as
   * they are consumed by the python worker. Hence, to maintain the invariant of the reader being
   * behind the writer while using HybridRowQueue, the buffer does the following
   * - Keeps a count of the rows in the HybridRowQueue
   * - Blocks the buffer's consuming iterator when the count is 0 so that the reader does not
   *   try to read more rows than what has been written.
   *
   * The implementation of the blocking iterator (ReentrantLock, Condition, etc.) has been borrowed
   * from that of ArrayBlockingQueue.
   */
  class UnsafeRowBuffer(taskMemoryManager: TaskMemoryManager, tempDir: File, numFields: Int)
      extends Logging {
    private val queue = HybridRowQueue(taskMemoryManager, tempDir, numFields)
    private val lock = new ReentrantLock()
    private val unblockRemove = lock.newCondition()

    // All of these are guarded by `lock`
    private var count = 0L
    private var allAdded = false
    private var exception: Throwable = null

    val iterator = new NextIterator[UnsafeRow] {
      override protected def getNext(): UnsafeRow = {
        val row = remove()
        if (row == null) finished = true
        row
      }
      override protected def close(): Unit = { }
    }

    def add(row: UnsafeRow): Unit = withLock {
      assert(queue.add(row), s"Failed to add row to HybridRowQueue while sending data to Python" +
        s"[count = $count, allAdded = $allAdded, exception = $exception]")
      count += 1
      unblockRemove.signal()
      logTrace(s"Added $row, $count left")
    }

    private def remove(): UnsafeRow = withLock {
      while (count == 0 && !allAdded && exception == null) {
        unblockRemove.await(100, TimeUnit.MILLISECONDS)
      }

      // If there was any error in the adding thread, then rethrow it in the removing thread
      if (exception != null) throw exception

      if (count > 0) {
        val row = queue.remove()
        assert(row != null, "HybridRowQueue.remove() returned null " +
          s"[count = $count, allAdded = $allAdded, exception = $exception]")
        count -= 1
        logTrace(s"Removed $row, $count left")
        row
      } else {
        null
      }
    }

    def allRowsAdded(): Unit = withLock {
      allAdded = true
      unblockRemove.signal()
    }

    def close(): Unit = { queue.close() }

    private def withLock[T](f: => T): T = {
      lock.lockInterruptibly()
      try { f } catch {
        case e: Throwable =>
          if (exception == null) exception = e
          throw e
      } finally { lock.unlock() }
    }
  }
}

