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

import java.util
import java.util.concurrent.CountDownLatch

import scala.collection.JavaConverters._
import scala.concurrent.TimeoutException

import org.apache.zookeeper.KeeperException.UnimplementedException
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.CONTINUOUS_READ
import org.apache.spark.sql.connector.read.{streaming, InputPartition, Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{ContinuousPartitionReaderFactory, ContinuousStream, PartitionOffset}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ContinuousExecutionSuite extends StreamTest {

  test("SPARK-30143: stop waits until timeout if blocked") {
    val latch = new CountDownLatch(1)
    BlockOnStopSourceProvider.setLatch(latch)
    val sq = spark.readStream.format(BlockOnStopSourceProvider.getClass.getName)
      .load()
      .writeStream
      .format("console")
      .trigger(ContinuousTrigger("1 second"))
      .start()

    failAfter(60.seconds) {
      val startTime = System.nanoTime()
      withSQLConf(SQLConf.STREAMING_STOP_TIMEOUT.key -> "2000") {
        intercept[TimeoutException] {
          sq.stop()
        }
      }
      val duration = (System.nanoTime() - startTime) / 1e6
      assert(duration >= 2000 * 1e6,
        s"Should have waiter more than 2000 millis, but waited $duration millis")

      latch.countDown()
      withSQLConf(SQLConf.STREAMING_STOP_TIMEOUT.key -> "0") {
        sq.stop()
      }
    }
  }
}

class BlockOnStopSourceTable(latch: CountDownLatch) extends Table with SupportsRead {
  override def schema(): StructType = BlockOnStopSourceProvider.schema

  override def name(): String = "blockingSource"

  override def capabilities(): util.Set[TableCapability] = Set(CONTINUOUS_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ScanBuilder {
      override def build(): Scan = new Scan {
        override def readSchema(): StructType = schema()

        override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
          new BlockOnStopContinuousStream(latch)
        }
      }
    }
  }
}

class BlockOnStopContinuousStream(latch: CountDownLatch) extends ContinuousStream {

  // Blocks until latch countdowns
  override def stop(): Unit = latch.await()

  // Boiler-plate
  override def planInputPartitions(start: streaming.Offset): Array[InputPartition] = Array.empty
  override def mergeOffsets(offsets: Array[PartitionOffset]): streaming.Offset = LongOffset(0L)
  override def deserializeOffset(json: String): streaming.Offset = LongOffset(0L)
  override def initialOffset(): Offset = LongOffset(0)
  override def commit(end: streaming.Offset): Unit = {}
  override def createContinuousReaderFactory(): ContinuousPartitionReaderFactory = {
    throw new UnimplementedException
  }
}
