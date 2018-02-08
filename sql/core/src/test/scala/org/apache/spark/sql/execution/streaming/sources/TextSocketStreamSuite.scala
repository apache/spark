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

package org.apache.spark.sql.execution.streaming.sources

import java.io.{IOException, OutputStreamWriter}
import java.net.ServerSocket
import java.sql.Timestamp
import java.util.Optional
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

class TextSocketStreamSuite extends StreamTest with SharedSQLContext with BeforeAndAfterEach {

  override def afterEach() {
    sqlContext.streams.active.foreach(_.stop())
    if (serverThread != null) {
      serverThread.interrupt()
      serverThread.join()
      serverThread = null
    }
    if (batchReader != null) {
      batchReader.stop()
      batchReader = null
    }
  }

  private var serverThread: ServerThread = null
  private var batchReader: MicroBatchReader = null

  test("V2 basic usage") {
    serverThread = new ServerThread()
    serverThread.start()

    val provider = new TextSocketSourceProvider
    val options = new DataSourceOptions(
      Map("host" -> "localhost", "port" -> serverThread.port.toString).asJava)
    batchReader = provider.createMicroBatchReader(Optional.empty(), "", options)

    val schema = batchReader.readSchema()
    assert(schema === StructType(StructField("value", StringType) :: Nil))

    failAfter(streamingTimeout) {
      serverThread.enqueue("hello")
      batchReader.setOffsetRange(Optional.empty(), Optional.empty())
      while (batchReader.getEndOffset.asInstanceOf[LongOffset].offset == -1L) {
        batchReader.setOffsetRange(Optional.empty(), Optional.empty())
        Thread.sleep(10)
      }
      withSQLConf("spark.sql.streaming.unsupportedOperationCheck" -> "false") {
        val offset1 = batchReader.getEndOffset
        val batch1 = new ListBuffer[Row]
        batchReader.createDataReaderFactories().asScala.map(_.createDataReader()).foreach { r =>
          while (r.next()) {
            batch1.append(r.get())
          }
        }
        assert(batch1.map(_.getAs[String](0)) === Seq("hello"))

        serverThread.enqueue("world")
        while (batchReader.getEndOffset === offset1) {
          batchReader.setOffsetRange(Optional.of(offset1), Optional.empty())
          Thread.sleep(10)
        }
        val offset2 = batchReader.getEndOffset
        val batch2 = new ListBuffer[Row]
        batchReader.createDataReaderFactories().asScala.map(_.createDataReader()).foreach { r =>
          while (r.next()) {
            batch2.append(r.get())
          }
        }
        assert(batch2.map(_.getAs[String](0)) === Seq("world"))

        batchReader.setOffsetRange(Optional.empty(), Optional.of(offset2))
        val both = new ListBuffer[Row]
        batchReader.createDataReaderFactories().asScala.map(_.createDataReader()).foreach { r =>
          while (r.next()) {
            both.append(r.get())
          }
        }
        assert(both.map(_.getAs[String](0)) === Seq("hello", "world"))
      }

      // Try stopping the source to make sure this does not block forever.
      batchReader.stop()
      batchReader = null
    }
  }

  test("timestamped usage") {
    serverThread = new ServerThread()
    serverThread.start()

    val provider = new TextSocketSourceProvider
    val options = new DataSourceOptions(Map("host" -> "localhost",
      "port" -> serverThread.port.toString, "includeTimestamp" -> "true").asJava)
    batchReader = provider.createMicroBatchReader(Optional.empty(), "", options)

    val schema = batchReader.readSchema()
    assert(schema === StructType(StructField("value", StringType) ::
      StructField("timestamp", TimestampType) :: Nil))

    failAfter(streamingTimeout) {
      serverThread.enqueue("hello")
      batchReader.setOffsetRange(Optional.empty(), Optional.empty())
      while (batchReader.getEndOffset.asInstanceOf[LongOffset].offset == -1L) {
        batchReader.setOffsetRange(Optional.empty(), Optional.empty())
        Thread.sleep(10)
      }
      withSQLConf("spark.sql.streaming.unsupportedOperationCheck" -> "false") {
        val offset1 = batchReader.getEndOffset
        val batch1 = new ListBuffer[Row]
        batchReader.createDataReaderFactories().asScala.map(_.createDataReader()).foreach { r =>
          while (r.next()) {
            batch1.append(r.get())
          }
        }
        assert(batch1.map(_.getAs[String](0)) === Seq("hello"))
        val batch1Stamp = batch1.map(_.getAs[Timestamp](1)).head

        serverThread.enqueue("world")
        while (batchReader.getEndOffset === offset1) {
          batchReader.setOffsetRange(Optional.of(offset1), Optional.empty())
          Thread.sleep(10)
        }
        val offset2 = batchReader.getEndOffset
        val batch2 = new ListBuffer[Row]
        batchReader.createDataReaderFactories().asScala.map(_.createDataReader()).foreach { r =>
          while (r.next()) {
            batch2.append(r.get())
          }
        }
        assert(batch2.map(_.getAs[String](0)) === Seq("world"))
        val batch2Stamp = batch2.map(_.getAs[Timestamp](1)).head
        assert(!batch2Stamp.before(batch1Stamp))
      }

      // Try stopping the source to make sure this does not block forever.
      batchReader.stop()
      batchReader = null
    }
  }

  test("params not given") {
    val provider = new TextSocketSourceProvider
    intercept[AnalysisException] {
      provider.createMicroBatchReader(Optional.empty(), "",
        new DataSourceOptions(Map.empty[String, String].asJava))
    }
    intercept[AnalysisException] {
      provider.createMicroBatchReader(Optional.empty(), "",
        new DataSourceOptions(Map("host" -> "localhost").asJava))
    }
    intercept[AnalysisException] {
      provider.createMicroBatchReader(Optional.empty(), "",
        new DataSourceOptions(Map("port" -> "1234").asJava))
    }
  }

  test("non-boolean includeTimestamp") {
    val provider = new TextSocketSourceProvider
    val params = Map("host" -> "localhost", "port" -> "1234", "includeTimestamp" -> "fasle")
    intercept[AnalysisException] {
      val a = new DataSourceOptions(params.asJava)
      provider.createMicroBatchReader(Optional.empty(), "", a)
    }
  }

  test("user-specified schema given") {
    val provider = new TextSocketSourceProvider
    val userSpecifiedSchema = StructType(
      StructField("name", StringType) ::
      StructField("area", StringType) :: Nil)
    val params = Map("host" -> "localhost", "port" -> "1234")
    val exception = intercept[AnalysisException] {
      provider.createMicroBatchReader(
        Optional.of(userSpecifiedSchema), "", new DataSourceOptions(params.asJava))
    }
    assert(exception.getMessage.contains(
      "socket source does not support a user-specified schema"))
  }

  test("no server up") {
    val provider = new TextSocketSourceProvider
    val parameters = Map("host" -> "localhost", "port" -> "0")
    intercept[IOException] {
      batchReader = provider.createMicroBatchReader(
        Optional.empty(), "", new DataSourceOptions(parameters.asJava))
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
