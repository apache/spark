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

import java.net.{InetSocketAddress, SocketException}
import java.nio.ByteBuffer
import java.nio.channels.ServerSocketChannel
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit._

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.read.streaming.{Offset, SparkDataStream}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamTest}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TextSocketStreamSuite extends StreamTest with SharedSparkSession {

  override def afterEach(): Unit = {
    sqlContext.streams.active.foreach(_.stop())
    if (serverThread != null) {
      serverThread.interrupt()
      serverThread.join()
      serverThread = null
    }
  }

  private var serverThread: ServerThread = null

  case class AddSocketData(data: String*) extends AddData {
    override def addData(query: Option[StreamExecution]): (SparkDataStream, Offset) = {
      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active socket source")

      val sources = query.get.logicalPlan.collect {
        case r: StreamingDataSourceV2Relation
            if r.stream.isInstanceOf[TextSocketMicroBatchStream] =>
          r.stream.asInstanceOf[TextSocketMicroBatchStream]
      }
      if (sources.isEmpty) {
        throw new Exception(
          "Could not find socket source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the socket source in the StreamExecution logical plan as there" +
            "are multiple socket sources:\n\t" + sources.mkString("\n\t"))
      }
      val socketSource = sources.head

      assert(serverThread != null && serverThread.port != 0)
      val currOffset = socketSource.getCurrentOffset()
      data.foreach(serverThread.enqueue)

      val newOffset = LongOffset(currOffset.offset + data.size)
      (socketSource, newOffset)
    }

    override def toString: String = s"AddSocketData(data = $data)"
  }

  test("backward compatibility with old path") {
    val ds = DataSource.lookupDataSource(
      "org.apache.spark.sql.execution.streaming.TextSocketSourceProvider",
      spark.sqlContext.conf).newInstance()
    assert(ds.isInstanceOf[TextSocketSourceProvider], "Could not find socket source")
  }

  test("basic usage") {
    serverThread = new ServerThread()
    serverThread.start()

    withSQLConf(SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED.key -> "false") {
      val ref = spark
      import ref.implicits._

      val socket = spark
        .readStream
        .format("socket")
        .options(Map("host" -> "localhost", "port" -> serverThread.port.toString))
        .load()
        .as[String]

      assert(socket.schema === StructType(StructField("value", StringType) :: Nil))

      testStream(socket)(
        StartStream(),
        AddSocketData("hello"),
        CheckAnswer("hello"),
        AddSocketData("world"),
        CheckLastBatch("world"),
        CheckAnswer("hello", "world"),
        StopStream
      )
    }
  }

  test("timestamped usage") {
    serverThread = new ServerThread()
    serverThread.start()

    withSQLConf(SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED.key -> "false") {
      val socket = spark
        .readStream
        .format("socket")
        .options(Map(
          "host" -> "localhost",
          "port" -> serverThread.port.toString,
          "includeTimestamp" -> "true"))
        .load()

      assert(socket.schema === StructType(StructField("value", StringType) ::
        StructField("timestamp", TimestampType) :: Nil))

      var batch1Stamp: Timestamp = null
      var batch2Stamp: Timestamp = null

      val curr = System.currentTimeMillis()
      testStream(socket)(
        StartStream(),
        AddSocketData("hello"),
        CheckAnswerRowsByFunc(
          rows => {
            assert(rows.size === 1)
            assert(rows.head.getAs[String](0) === "hello")
            batch1Stamp = rows.head.getAs[Timestamp](1)
            Thread.sleep(10)
          },
          true),
        AddSocketData("world"),
        CheckAnswerRowsByFunc(
          rows => {
            assert(rows.size === 1)
            assert(rows.head.getAs[String](0) === "world")
            batch2Stamp = rows.head.getAs[Timestamp](1)
          },
          true),
        StopStream
      )

      // Timestamp for rate stream is round to second which leads to milliseconds lost, that will
      // make batch1stamp smaller than current timestamp if both of them are in the same second.
      // Comparing by second to make sure the correct behavior.
      assert(batch1Stamp.getTime >= SECONDS.toMillis(MILLISECONDS.toSeconds(curr)))
      assert(!batch2Stamp.before(batch1Stamp))
    }
  }

  test("params not given") {
    val provider = new TextSocketSourceProvider
    intercept[AnalysisException] {
      provider.getTable(CaseInsensitiveStringMap.empty())
    }
    intercept[AnalysisException] {
      provider.getTable(new CaseInsensitiveStringMap(Map("host" -> "localhost").asJava))
    }
    intercept[AnalysisException] {
      provider.getTable(new CaseInsensitiveStringMap(Map("port" -> "1234").asJava))
    }
  }

  test("non-boolean includeTimestamp") {
    val provider = new TextSocketSourceProvider
    val params = Map("host" -> "localhost", "port" -> "1234", "includeTimestamp" -> "fasle")
    intercept[AnalysisException] {
      provider.getTable(new CaseInsensitiveStringMap(params.asJava))
    }
  }

  test("user-specified schema given") {
    val userSpecifiedSchema = StructType(
      StructField("name", StringType) ::
      StructField("area", StringType) :: Nil)
    val params = Map("host" -> "localhost", "port" -> "1234")
    val exception = intercept[UnsupportedOperationException] {
      spark.readStream.schema(userSpecifiedSchema).format("socket").options(params).load()
    }
    assert(exception.getMessage.contains(
      "TextSocketSourceProvider source does not support user-specified schema"))
  }

  test("input row metrics") {
    serverThread = new ServerThread()
    serverThread.start()

    withSQLConf(SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED.key -> "false") {
      val ref = spark
      import ref.implicits._

      val socket = spark
        .readStream
        .format("socket")
        .options(Map("host" -> "localhost", "port" -> serverThread.port.toString))
        .load()
        .as[String]

      assert(socket.schema === StructType(StructField("value", StringType) :: Nil))

      testStream(socket)(
        StartStream(),
        AddSocketData("hello"),
        CheckAnswer("hello"),
        AssertOnQuery { q =>
          val numRowMetric =
            q.lastExecution.executedPlan.collectLeaves().head.metrics.get("numOutputRows")
          numRowMetric.nonEmpty && numRowMetric.get.value == 1
        },
        StopStream
      )
    }
  }

  test("verify ServerThread only accepts the first connection") {
    serverThread = new ServerThread()
    serverThread.start()

    withSQLConf(SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED.key -> "false") {
      val ref = spark
      import ref.implicits._

      val socket = spark
        .readStream
        .format("socket")
        .options(Map("host" -> "localhost", "port" -> serverThread.port.toString))
        .load()
        .as[String]

      assert(socket.schema === StructType(StructField("value", StringType) :: Nil))

      testStream(socket)(
        StartStream(),
        AddSocketData("hello"),
        CheckAnswer("hello"),
        AddSocketData("world"),
        CheckLastBatch("world"),
        CheckAnswer("hello", "world"),
        StopStream
      )

      // we are trying to connect to the server once again which should fail
      try {
        val socket2 = spark
          .readStream
          .format("socket")
          .options(Map("host" -> "localhost", "port" -> serverThread.port.toString))
          .load()
          .as[String]

        testStream(socket2)(
          StartStream(),
          AddSocketData("hello"),
          CheckAnswer("hello"),
          AddSocketData("world"),
          CheckLastBatch("world"),
          CheckAnswer("hello", "world"),
          StopStream
        )

        fail("StreamingQueryException is expected!")
      } catch {
        case e: StreamingQueryException if e.cause.isInstanceOf[SocketException] => // pass
      }
    }
  }

  test("continuous data") {
    serverThread = new ServerThread()
    serverThread.start()

    val stream = new TextSocketContinuousStream(
      host = "localhost",
      port = serverThread.port,
      numPartitions = 2,
      options = CaseInsensitiveStringMap.empty())
    val partitions = stream.planInputPartitions(stream.initialOffset())
    assert(partitions.length == 2)

    val numRecords = 10
    val data = scala.collection.mutable.ListBuffer[Int]()
    val offsets = scala.collection.mutable.ListBuffer[Int]()
    val readerFactory = stream.createContinuousReaderFactory()
    import org.scalatest.time.SpanSugar._
    failAfter(5.seconds) {
      // inject rows, read and check the data and offsets
      for (i <- 0 until numRecords) {
        serverThread.enqueue(i.toString)
      }
      partitions.foreach {
        case t: TextSocketContinuousInputPartition =>
          val r = readerFactory.createReader(t).asInstanceOf[TextSocketContinuousPartitionReader]
          for (i <- 0 until numRecords / 2) {
            r.next()
            offsets.append(r.getOffset().asInstanceOf[ContinuousRecordPartitionOffset].offset)
            data.append(r.get().getString(0).toInt)
            // commit the offsets in the middle and validate if processing continues
            if (i == 2) {
              commitOffset(t.partitionId, i + 1)
            }
          }
          assert(offsets.toSeq == Range.inclusive(1, 5))
          assert(data.toSeq == Range(t.partitionId, 10, 2))
          offsets.clear()
          data.clear()
        case _ => throw new IllegalStateException("Unexpected task type")
      }
      assert(stream.startOffset.offsets == List(3, 3))
      stream.commit(TextSocketOffset(List(5, 5)))
      assert(stream.startOffset.offsets == List(5, 5))
    }

    def commitOffset(partition: Int, offset: Int): Unit = {
      val offsetsToCommit = stream.startOffset.offsets.updated(partition, offset)
      stream.commit(TextSocketOffset(offsetsToCommit))
      assert(stream.startOffset.offsets == offsetsToCommit)
    }
  }

  test("continuous data - invalid commit") {
    serverThread = new ServerThread()
    serverThread.start()

    val stream = new TextSocketContinuousStream(
      host = "localhost",
      port = serverThread.port,
      numPartitions = 2,
      options = CaseInsensitiveStringMap.empty())

    stream.startOffset = TextSocketOffset(List(5, 5))
    assertThrows[IllegalStateException] {
      stream.commit(TextSocketOffset(List(6, 6)))
    }
  }

  test("continuous data with timestamp") {
    serverThread = new ServerThread()
    serverThread.start()

    val stream = new TextSocketContinuousStream(
      host = "localhost",
      port = serverThread.port,
      numPartitions = 2,
      options = new CaseInsensitiveStringMap(Map("includeTimestamp" -> "true").asJava))
    val partitions = stream.planInputPartitions(stream.initialOffset())
    assert(partitions.size == 2)

    val numRecords = 4
    // inject rows, read and check the data and offsets
    for (i <- 0 until numRecords) {
      serverThread.enqueue(i.toString)
    }
    val readerFactory = stream.createContinuousReaderFactory()
    partitions.foreach {
      case t: TextSocketContinuousInputPartition =>
        val r = readerFactory.createReader(t).asInstanceOf[TextSocketContinuousPartitionReader]
        for (_ <- 0 until numRecords / 2) {
          r.next()
          assert(r.get().numFields === 2)
          // just try to read columns one by one - it would throw error if the row is corrupted
          r.get().getString(0)
          r.get().getLong(1)
        }
      case _ => throw new IllegalStateException("Unexpected task type")
    }
  }

  /**
   * This class tries to mimic the behavior of netcat, so that we can ensure
   * TextSocketStream supports netcat, which only accepts the first connection
   * and exits the process when the first connection is closed.
   *
   * Please refer SPARK-24466 for more details.
   */
  private class ServerThread extends Thread with Logging {
    private val serverSocketChannel = ServerSocketChannel.open()
    serverSocketChannel.bind(new InetSocketAddress(0))
    private val messageQueue = new LinkedBlockingQueue[String]()

    val port = serverSocketChannel.socket().getLocalPort

    override def run(): Unit = {
      try {
        val clientSocketChannel = serverSocketChannel.accept()

        // Close server socket channel immediately to mimic the behavior that
        // only first connection will be made and deny any further connections
        // Note that the first client socket channel will be available
        serverSocketChannel.close()

        clientSocketChannel.configureBlocking(false)
        clientSocketChannel.socket().setTcpNoDelay(true)

        while (true) {
          val line = messageQueue.take() + "\n"
          clientSocketChannel.write(ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8)))
        }
      } catch {
        case e: InterruptedException =>
      } finally {
        // no harm to call close() again...
        serverSocketChannel.close()
      }
    }

    def enqueue(line: String): Unit = {
      messageQueue.put(line)
    }
  }
}
