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

package org.apache.spark.api.python

import java.io.{DataOutputStream, FilterOutputStream}
import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ClosedByInterruptException, ServerSocketChannel, SocketChannel}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{LocalSparkContext, SparkContext, SparkEnv, SparkException, SparkFunSuite}
import org.apache.spark.util.DirectByteBufferOutputStream

class CloseShieldOutputStreamSuite extends SparkFunSuite with LocalSparkContext {

  test("SPARK-60305: forwards bulk writes and does not close the underlying stream") {
    var byteWrites = 0
    val bufferStream = new DirectByteBufferOutputStream() {
      override def write(b: Int): Unit = {
        byteWrites += 1
        super.write(b)
      }
    }
    val shielded = new CloseShieldOutputStream(bufferStream)
    shielded.write(Array[Byte](1, 2, 3), 1, 2)
    assert(byteWrites === 0)
    shielded.write(4)
    assert(byteWrites === 1)
    shielded.close()
    assert(bufferStream.size() === 3)
    val bytes = new Array[Byte](3)
    bufferStream.toByteBuffer.get(bytes)
    assert(bytes.toSeq === Seq[Byte](2, 3, 4))
    bufferStream.close()
  }

  test("SPARK-60305: an interrupted channel write does not close the shielded buffer") {
    val bufferStream = new DirectByteBufferOutputStream()
    val channel =
      Channels.newChannel(new DataOutputStream(new CloseShieldOutputStream(bufferStream)))
    // With the interrupt flag already set, the channel closes itself, and the stream it writes
    // to, on this thread before it writes.
    Thread.currentThread().interrupt()
    try {
      intercept[ClosedByInterruptException](channel.write(ByteBuffer.wrap(new Array[Byte](16))))
    } finally {
      Thread.interrupted()
    }
    assert(bufferStream.size() === 16)
    bufferStream.close()
  }

  test("SPARK-60305: interrupting the task thread inside a channel write keeps the " +
      "ReaderInputStream buffer alive") {
    sc = new SparkContext("local", "test")
    val func = SimplePythonFunction(Seq.empty[Byte], new JHashMap[String, String](),
      new JArrayList[String](), "python3", "3", new JArrayList(), null)
    val runner = new PythonRunner(Seq(ChainedPythonFunctions(Seq(func))), None)
    val inWrite = new CountDownLatch(1)
    val resume = new AtomicBoolean(false)
    @volatile var writeError: Throwable = null

    val server = ServerSocketChannel.open()
    server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    val client = SocketChannel.open(server.getLocalAddress)
    val accepted = server.accept()
    client.configureBlocking(false)
    val worker = PythonWorker(client).refresh()
    try {
      val writer = new runner.Writer(SparkEnv.get, worker, Iterator.empty, 0, null) {
        // An interrupt closes the stream the writer writes to; with the shield, this only
        // flushes.
        override def open(dataOut: DataOutputStream): Unit = dataOut.close()
        override protected def writeCommand(dataOut: DataOutputStream): Unit = {}
        // Writes the way the Arrow inputs do: through `Channels.newChannel(dataOut)`, which the
        // JVM closes, together with `dataOut`, when the writing thread is interrupted. The
        // write blocks until the test has interrupted the thread, then forwards to `dataOut`.
        override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
          val channel = Channels.newChannel(new FilterOutputStream(dataOut) {
            override def write(b: Array[Byte], off: Int, len: Int): Unit = {
              inWrite.countDown()
              while (!resume.get()) Thread.onSpinWait()
              try {
                out.write(b, off, len)
              } catch {
                case t: Throwable =>
                  writeError = t
                  throw t
              }
            }
          })
          channel.write(ByteBuffer.wrap(new Array[Byte](16)))
          false
        }
      }
      val input = new runner.ReaderInputStream(worker, writer, None, false, 0, false)
      @volatile var error: Throwable = null
      val taskThread = new Thread(() => {
        try {
          input.read(new Array[Byte](1))
        } catch {
          case t: Throwable => error = t
        }
      })
      taskThread.start()
      assert(inWrite.await(1, TimeUnit.MINUTES), s"task thread failed before the write: $error")
      // Like Task.kill(interruptThread = true). This closes the channel and `dataOut` on this
      // thread while the task thread is still inside the write.
      taskThread.interrupt()
      resume.set(true)
      taskThread.join(TimeUnit.MINUTES.toMillis(1))
      assert(!taskThread.isAlive)
      // The write into the buffer still succeeds: the buffer was not freed under the writer.
      assert(writeError === null)
      assert(error.isInstanceOf[ClosedByInterruptException])
      // The failed write frees the buffer on the task thread instead of leaving it to GC, and
      // a later read fails instead of reusing the freed buffer. Closing the peer first makes a
      // regression return -1 rather than wait for output.
      accepted.close()
      val e = intercept[SparkException](input.read(new Array[Byte](1)))
      assert(e.getMessage.contains("closed DirectByteBufferOutputStream"))
    } finally {
      // Never leave the writer spinning, even if the test failed before releasing it.
      resume.set(true)
      worker.stop()
      accepted.close()
      server.close()
    }
  }
}
