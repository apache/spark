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

package org.apache.spark.sql.execution.streaming

import java.io.{IOException, OutputStreamWriter}
import java.net.ServerSocket
import java.util.concurrent.LinkedBlockingQueue

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class TextSocketStreamSuite extends StreamTest with SharedSQLContext with BeforeAndAfterEach {
  import testImplicits._

  override def afterEach() {
    sqlContext.streams.active.foreach(_.stop())
    if (serverThread != null) {
      serverThread.interrupt()
      serverThread.join()
      serverThread = null
    }
    if (source != null) {
      source.stop()
      source = null
    }
  }

  private var serverThread: ServerThread = null
  private var source: Source = null

  test("basic usage") {
    serverThread = new ServerThread()
    serverThread.start()

    val provider = new TextSocketSourceProvider
    val parameters = Map("host" -> "localhost", "port" -> serverThread.port.toString)
    val schema = provider.sourceSchema(sqlContext, None, "", parameters)._2
    assert(schema === StructType(StructField("value", StringType) :: Nil))

    source = provider.createSource(sqlContext, "", None, "", parameters)

    failAfter(streamingTimeout) {
      serverThread.enqueue("hello")
      while (source.getOffset.isEmpty) {
        Thread.sleep(10)
      }
      val offset1 = source.getOffset.get
      val batch1 = source.getBatch(None, offset1)
      assert(batch1.as[String].collect().toSeq === Seq("hello"))

      serverThread.enqueue("world")
      while (source.getOffset.get === offset1) {
        Thread.sleep(10)
      }
      val offset2 = source.getOffset.get
      val batch2 = source.getBatch(Some(offset1), offset2)
      assert(batch2.as[String].collect().toSeq === Seq("world"))

      val both = source.getBatch(None, offset2)
      assert(both.as[String].collect().sorted.toSeq === Seq("hello", "world"))

      // Try stopping the source to make sure this does not block forever.
      source.stop()
      source = null
    }
  }

  test("params not given") {
    val provider = new TextSocketSourceProvider
    intercept[AnalysisException] {
      provider.sourceSchema(sqlContext, None, "", Map())
    }
    intercept[AnalysisException] {
      provider.sourceSchema(sqlContext, None, "", Map("host" -> "localhost"))
    }
    intercept[AnalysisException] {
      provider.sourceSchema(sqlContext, None, "", Map("port" -> "1234"))
    }
  }

  test("no server up") {
    val provider = new TextSocketSourceProvider
    val parameters = Map("host" -> "localhost", "port" -> "0")
    intercept[IOException] {
      source = provider.createSource(sqlContext, "", None, "", parameters)
    }
  }

  private class ServerThread extends Thread with Logging {
    private val serverSocket = new ServerSocket(0)
    private val messageQueue = new LinkedBlockingQueue[String]()

    val port = serverSocket.getLocalPort

    override def run(): Unit = {
      try {
        val clientSocket = serverSocket.accept()
        clientSocket.setTcpNoDelay(true)
        val out = new OutputStreamWriter(clientSocket.getOutputStream)
        while (true) {
          val line = messageQueue.take()
          out.write(line + "\n")
          out.flush()
        }
      } catch {
        case e: InterruptedException =>
      } finally {
        serverSocket.close()
      }
    }

    def enqueue(line: String): Unit = {
      messageQueue.put(line)
    }
  }
}
