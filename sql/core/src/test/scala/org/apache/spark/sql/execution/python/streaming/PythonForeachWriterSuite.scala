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

package org.apache.spark.sql.execution.python.streaming

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import org.mockito.Mockito.when
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.python.streaming.PythonForeachWriter.UnsafeRowBuffer
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.util.Utils

class PythonForeachWriterSuite extends SparkFunSuite with Eventually with MockitoSugar {

  testWithBuffer("UnsafeRowBuffer: iterator blocks when no data is available") { b =>
    b.assertIteratorBlocked()

    b.add(Seq(1))
    b.assertOutput(Seq(1))
    b.assertIteratorBlocked()

    b.add(2 to 100)
    b.assertOutput(1 to 100)
    b.assertIteratorBlocked()
  }

  testWithBuffer("UnsafeRowBuffer: iterator unblocks when all data added") { b =>
    b.assertIteratorBlocked()
    b.add(Seq(1))
    b.assertIteratorBlocked()

    b.allAdded()
    b.assertThreadTerminated()
    b.assertOutput(Seq(1))
  }

  testWithBuffer(
      "UnsafeRowBuffer: handles more data than memory",
      memBytes = 5,
      sleepPerRowReadMs = 1) { b =>

    b.assertIteratorBlocked()
    b.add(1 to 2000)
    b.assertOutput(1 to 2000)
  }

  test("SPARK-60310: UnsafeRowBuffer: close does not free a row that the reading thread uses") {
    val buffer = createBuffer(4 << 10)
    val intProj = UnsafeProjection.create(Array[DataType](IntegerType))
    buffer.add(intProj(new GenericInternalRow(Array[Any](1))))
    val rowRead = new CountDownLatch(1)
    val bufferClosed = new CountDownLatch(1)
    @volatile var value = 0
    // Like the WriterThread converting a row while the task completion listener closes the
    // buffer, e.g. after the task was killed while waiting for the WriterThread.
    val reader = new Thread() {
      override def run(): Unit = {
        val row = buffer.iterator.next()
        rowRead.countDown()
        bufferClosed.await()
        value = row.getInt(0)
      }
    }
    reader.start()
    try {
      assert(rowRead.await(60, TimeUnit.SECONDS))
      buffer.close()
    } finally {
      bufferClosed.countDown()
      reader.join()
    }
    // A row in a freed page reads 0x5a5a5a5a, the pattern of spark.memory.debugFill, which the
    // build enables for tests.
    assert(value === 1)
  }

  test("SPARK-60310: UnsafeRowBuffer: close stops the reading thread waiting for rows") {
    val buffer = createBuffer(4 << 10)
    @volatile var error: Throwable = null
    val reader = new Thread() {
      override def run(): Unit = {
        try {
          buffer.iterator.hasNext
        } catch {
          case t: Throwable => error = t
        }
      }
    }
    reader.start()
    try {
      import Thread.State._
      eventually(timeout(60.seconds)) {
        assert(reader.getState == TIMED_WAITING || reader.getState == WAITING)
      }
      // When the task is killed, `allRowsAdded()` can fail on the interrupted task thread, and
      // the task completion listener closes the buffer while the reading thread waits for rows.
      buffer.close()
      reader.join(10000)
      assert(!reader.isAlive)
      // An error, not the end of the input, so that Python does not see a complete partition.
      assert(error.isInstanceOf[IllegalStateException], error)
    } finally {
      reader.interrupt()
      reader.join()
    }
  }

  def testWithBuffer(
      name: String,
      memBytes: Long = 4 << 10,
      sleepPerRowReadMs: Int = 0
    )(f: BufferTester => Unit): Unit = {

    System.gc()
    test(name) {
      var tester: BufferTester = null
      try {
        tester = new BufferTester(memBytes, sleepPerRowReadMs)
        f(tester)
      } finally {
        if (tester != null) tester.close()
      }
    }
  }


  def createBuffer(memBytes: Long): UnsafeRowBuffer = {
    val mockEnv = mock[SparkEnv]
    val conf = new SparkConf()
    val serializerManager = new SerializerManager(new JavaSerializer(conf), conf, None)
    when(mockEnv.serializerManager).thenReturn(serializerManager)
    SparkEnv.set(mockEnv)
    val mem = new TestMemoryManager(conf)
    mem.limit(memBytes)
    val taskM = new TaskMemoryManager(mem, 0)
    new UnsafeRowBuffer(taskM, Utils.createTempDir(), 1)
  }

  class BufferTester(memBytes: Long, sleepPerRowReadMs: Int) {
    private val buffer = createBuffer(memBytes)
    private val iterator = buffer.iterator
    private val outputBuffer = new ArrayBuffer[Int]
    private val testTimeout = timeout(60.seconds)
    private val intProj = UnsafeProjection.create(Array[DataType](IntegerType))
    private val thread = new Thread() {
      override def run(): Unit = {
        try {
          while (iterator.hasNext) {
            outputBuffer.synchronized {
              outputBuffer += iterator.next().getInt(0)
            }
            Thread.sleep(sleepPerRowReadMs)
          }
        } finally {
          buffer.close()
        }
      }
    }
    thread.start()

    def add(ints: Seq[Int]): Unit = {
      ints.foreach { i => buffer.add(intProj.apply(new GenericInternalRow(Array[Any](i)))) }
    }

    def allAdded(): Unit = { buffer.allRowsAdded() }

    def assertOutput(expectedOutput: Seq[Int]): Unit = {
      eventually(testTimeout) {
        val output = outputBuffer.synchronized { outputBuffer.toArray }.toSeq
        assert(output == expectedOutput)
      }
    }

    def assertIteratorBlocked(): Unit = {
      import Thread.State._
      eventually(testTimeout) {
        assert(thread.isAlive)
        assert(thread.getState == TIMED_WAITING || thread.getState == WAITING)
      }
    }

    def assertThreadTerminated(): Unit = {
      eventually(testTimeout) { assert(!thread.isAlive) }
    }

    def close(): Unit = {
      thread.interrupt()
      thread.join()
    }
  }
}
