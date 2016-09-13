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
import scala.collection.mutable.ArrayBuffer

import net.razorvine.pickle.{Pickler, Unpickler}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonRunner}
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.{CompletionIterator, Utils}


/**
 * A RowQueue is an FIFO queue for UnsafeRow.
 */
private[python] trait RowQueue {
  /**
   * Add a row to the end of it, returns true iff the row has added into it.
   */
  def add(row: UnsafeRow): Boolean

  /**
   * Retrieve and remove the first row, returns null if it's empty.
   */
  def remove(): UnsafeRow

  /**
   * Cleanup all the resources.
   */
  def close(): Unit
}

/**
 * A RowQueue that is based on in-memory page. UnsafeRows are appended into it until it's full.
 * Another thread could read from it at the same time (behind the writer).
 */
private[python] case class InMemoryRowQueue(page: MemoryBlock, fields: Int) extends RowQueue {
  private val base: AnyRef = page.getBaseObject
  private var last = page.getBaseOffset  // for writing
  private var first = page.getBaseOffset  // for reading
  private val resultRow = new UnsafeRow(fields)

  def add(row: UnsafeRow): Boolean = {
    if (last + 4 + row.getSizeInBytes > page.getBaseOffset + page.size) {
      if (last + 4 <= page.getBaseOffset + page.size) {
        Platform.putInt(base, last, -1)
      }
      return false
    }
    Platform.putInt(base, last, row.getSizeInBytes)
    Platform.copyMemory(row.getBaseObject, row.getBaseOffset, base, last + 4, row.getSizeInBytes)
    last += 4 + row.getSizeInBytes
    true
  }

  def remove(): UnsafeRow = {
    if (first + 4 > page.getBaseOffset + page.size || Platform.getInt(base, first) < 0) {
      null
    } else {
      val size = Platform.getInt(base, first)
      resultRow.pointTo(base, first + 4, size)
      first += 4 + size
      resultRow
    }
  }

  def close(): Unit = {
    // caller should override close() to free page
  }
}

/**
 * A RowQueue that is based on file in disk. It will stop to push once someone start to read
 * from it.
 */
private[python] case class DiskRowQueue(path: String, fields: Int) extends RowQueue {
  private var fout = new FileOutputStream(path)
  private var out = new DataOutputStream(new BufferedOutputStream(fout))
  private var length = 0L

  private var fin: FileInputStream = _
  private var in: DataInputStream = _
  private val resultRow = new UnsafeRow(fields)

  def add(row: UnsafeRow): Boolean = {
    synchronized {
      if (out == null) {
        // Another thread is reading, stop writing this one
        return false
      }
    }
    out.writeInt(row.getSizeInBytes)
    out.write(row.getBytes)
    length += 4 + row.getSizeInBytes
    true
  }

  def remove(): UnsafeRow = {
    synchronized {
      if (out != null) {
        out.flush()
        out.close()
        out = null
        fout.close()
        fout = null

        fin = new FileInputStream(path)
        in = new DataInputStream(new BufferedInputStream(fin))
      }
    }

    if (length > 0) {
      val size = in.readInt()
      assert(4 + size <= length, s"require ${4 + size} bytes for next row")
      val bytes = new Array[Byte](size)
      in.readFully(bytes)
      length -= 4 + size
      resultRow.pointTo(bytes, size)
      resultRow
    } else {
      null
    }
  }

  def close(): Unit = {
    synchronized {
      if (fout != null) {
        fout.close()
        fout = null
      }
      if (fin != null) {
        fin.close()
        fin = null
      }
    }
    val file = new File(path)
    if (file.exists()) {
      file.delete()
    }
  }
}

/**
 * A RowQueue that has a list of RowQueues, which could be in memory or disk.
 *
 * HybridRowQueue could be safely appended in one thread, and pulled in another thread in the same
 * time.
 */
private[python] case class HybridRowQueue(
    memManager: TaskMemoryManager,
    dir: File, fields: Int)
  extends MemoryConsumer(memManager) with RowQueue {

  // Each buffer should have at least one row
  private val queues = new java.util.LinkedList[RowQueue]()

  private var writing: RowQueue = _
  private var reading: RowQueue = _

  private[python] def numQueues(): Int = queues.size()

  def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger == this) {
      // When it's triggered by itself, it should write upcoming rows into disk instead of copying
      // the rows already in the queue.
      return 0L
    }
    var released = 0L
    synchronized {
      // poll out all the buffers and add them back in the same order to make sure that the rows
      // are in correct order.
      val n = queues.size()
      var i = 0
      while (i < n) {
        i += 1
        val queue = queues.remove()
        val newQueue = if (i < n && queue.isInstanceOf[InMemoryRowQueue]) {
          val diskQueue = createDiskQueue()
          var r = queue.remove()
          while (r != null) {
            diskQueue.add(r)
            r = queue.remove()
          }
          released += queue.asInstanceOf[InMemoryRowQueue].page.size()
          queue.close()
          diskQueue
        } else {
          queue
        }
        queues.add(newQueue)
      }
    }
    released
  }

  private def createDiskQueue(): RowQueue = {
    DiskRowQueue(File.createTempFile("buffer", "", dir).getAbsolutePath, fields)
  }

  private def createNewQueue(required: Long): RowQueue = {
    val buffer = try {
      val page = allocatePage(required)
      new InMemoryRowQueue(page, fields) {
        override def close(): Unit = {
          freePage(page)
        }
      }
    } catch {
      case _: OutOfMemoryError =>
        createDiskQueue()
    }
    synchronized {
      queues.add(buffer)
    }
    buffer
  }

  def add(row: UnsafeRow): Boolean = {
    if (writing == null || !writing.add(row)) {
      writing = createNewQueue(4 + row.getSizeInBytes)
      assert(writing.add(row), s"failed to push a row into $writing")
    }
    true
  }

  def remove(): UnsafeRow = {
    var row: UnsafeRow = null
    if (reading != null) {
      row = reading.remove()
    }
    if (row == null) {
      if (reading != null) {
        reading.close()
      }
      synchronized {
        reading = queues.remove()
      }
      assert(reading != null, s"queue should not be empty")
      row = reading.remove()
      assert(row != null, s"$reading should have at least one row")
    }
    row
  }

  def close(): Unit = {
    if (reading != null) {
      reading.close()
      reading = null
    }
    synchronized {
      while (!queues.isEmpty) {
        queues.remove().close()
      }
    }
  }
}


/**
 * A physical plan that evaluates a [[PythonUDF]], one partition of tuples at a time.
 *
 * Python evaluation works by sending the necessary (projected) input data via a socket to an
 * external Python process, and combine the result from the Python process with the original row.
 *
 * For each row we send to Python, we also put it in a queue. For each output row from Python,
 * we drain the queue to find the original input row. Note that if the Python process is way too
 * slow, this could lead to the queue growing unbounded and eventually run out of memory.
 */
case class BatchEvalPythonExec(udfs: Seq[PythonUDF], output: Seq[Attribute], child: SparkPlan)
  extends SparkPlan {

  def children: Seq[SparkPlan] = child :: Nil

  override def producedAttributes: AttributeSet = AttributeSet(output.drop(child.output.length))

  private def collectFunctions(udf: PythonUDF): (ChainedPythonFunctions, Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val (chained, children) = collectFunctions(u)
        (ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(_.find(_.isInstanceOf[PythonUDF]).isEmpty))
        (ChainedPythonFunctions(Seq(udf.func)), udf.children)
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())
    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)

    inputRDD.mapPartitions { iter =>
      EvaluatePython.registerPicklers()  // register pickler for Row

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = HybridRowQueue(
        TaskContext.get().taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)),
        child.output.length)

      val (pyFuncs, inputs) = udfs.map(collectFunctions).unzip

      // flatten all the arguments
      val allInputs = new ArrayBuffer[Expression]
      val dataTypes = new ArrayBuffer[DataType]
      val argOffsets = inputs.map { input =>
        input.map { e =>
          if (allInputs.exists(_.semanticEquals(e))) {
            allInputs.indexWhere(_.semanticEquals(e))
          } else {
            allInputs += e
            dataTypes += e.dataType
            allInputs.length - 1
          }
        }.toArray
      }.toArray
      val projection = newMutableProjection(allInputs, child.output)
      val schema = StructType(dataTypes.map(dt => StructField("", dt)))
      val needConversion = dataTypes.exists(EvaluatePython.needConversionInPython)

      // enable memo iff we serialize the row with schema (schema and class should be memorized)
      val pickle = new Pickler(needConversion)
      // Input iterator to Python: input rows are grouped so we send them in batches to Python.
      // For each row, add it to the queue.
      val inputIterator = iter.grouped(100).map { inputRows =>
        val toBePickled = inputRows.map { inputRow =>
          queue.add(inputRow.asInstanceOf[UnsafeRow])
          val row = projection(inputRow)
          if (needConversion) {
            EvaluatePython.toJava(row, schema)
          } else {
            // fast path for these types that does not need conversion in Python
            val fields = new Array[Any](row.numFields)
            var i = 0
            while (i < row.numFields) {
              val dt = dataTypes(i)
              fields(i) = EvaluatePython.toJava(row.get(i, dt), dt)
              i += 1
            }
            fields
          }
        }.toArray
        pickle.dumps(toBePickled)
      }

      val context = TaskContext.get()

      // Output iterator for results from Python.
      val outputIterator = new PythonRunner(pyFuncs, bufferSize, reuseWorker, true, argOffsets)
        .compute(inputIterator, context.partitionId(), context)

      val unpickle = new Unpickler
      val mutableRow = new GenericMutableRow(1)
      val joined = new JoinedRow
      val resultType = if (udfs.length == 1) {
        udfs.head.dataType
      } else {
        StructType(udfs.map(u => StructField("", u.dataType, u.nullable)))
      }
      val resultProj = UnsafeProjection.create(output, output)
      val resultIter = outputIterator.flatMap { pickedResult =>
        val unpickledBatch = unpickle.loads(pickedResult)
        unpickledBatch.asInstanceOf[java.util.ArrayList[Any]].asScala
      }.map { result =>
        val row = if (udfs.length == 1) {
          // fast path for single UDF
          mutableRow(0) = EvaluatePython.fromJava(result, resultType)
          mutableRow
        } else {
          EvaluatePython.fromJava(result, resultType).asInstanceOf[InternalRow]
        }
        resultProj(joined(queue.remove(), row))
      }
      // Cleanup all the resources
      CompletionIterator[UnsafeRow, Iterator[UnsafeRow]](resultIter, queue.close)
    }
  }
}
