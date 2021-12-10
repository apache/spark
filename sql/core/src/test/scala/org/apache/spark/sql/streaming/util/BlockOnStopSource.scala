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

package org.apache.spark.sql.streaming.util

import java.util
import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.KeeperException.UnimplementedException

import org.apache.spark.sql.{DataFrame, Row, SparkSession, SQLContext}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.CONTINUOUS_READ
import org.apache.spark.sql.connector.read.{streaming, InputPartition, Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{ContinuousPartitionReaderFactory, ContinuousStream, PartitionOffset}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** The V1 and V2 provider of a streaming source, which blocks indefinitely on the call of stop() */
object BlockOnStopSourceProvider {
  private var _latch: CountDownLatch = _
  val schema: StructType = new StructType().add("id", LongType)

  /** Set the latch that we will use to block the streaming query thread. */
  def enableBlocking(): Unit = {
    if (_latch == null || _latch.getCount == 0) {
      _latch = new CountDownLatch(1)
    }
  }

  def disableBlocking(): Unit = {
    if (_latch != null) {
      _latch.countDown()
      _latch = null
    }
  }
}

class BlockOnStopSourceProvider extends StreamSourceProvider with SimpleTableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new BlockOnStopSourceTable(BlockOnStopSourceProvider._latch)
  }

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    "blockingSource" -> BlockOnStopSourceProvider.schema
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    new BlockOnStopSource(sqlContext.sparkSession, BlockOnStopSourceProvider._latch)
  }
}

/** A V1 Streaming Source which blocks on stop(). It does not produce any data. */
class BlockOnStopSource(spark: SparkSession, latch: CountDownLatch) extends Source {
  // Blocks until latch countdowns
  override def stop(): Unit = latch.await()

  // Boiler-plate
  override val schema: StructType = BlockOnStopSourceProvider.schema
  override def getOffset: Option[Offset] = Some(LongOffset(0))
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }
}

/** A V2 Table, which can create a blocking streaming source for ContinuousExecution. */
class BlockOnStopSourceTable(latch: CountDownLatch) extends Table with SupportsRead {
  override def schema(): StructType = BlockOnStopSourceProvider.schema

  override def name(): String = "blockingSource"

  override def capabilities(): util.Set[TableCapability] = util.EnumSet.of(CONTINUOUS_READ)

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

/**
 * A V2 Streaming Source which blocks on stop(). It does not produce any data. We use this for
 * testing stopping in ContinuousExecution.
 */
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
