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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.api.java.StorageLevels

import org.apache.spark.sql.{Dataset, DataFrame, SQLContext, Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.{RowEncoder, encoderFor}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Union, LeafNode}
import org.apache.spark.storage.{StreamBlockId, BlockId}

object MemoryStream {
  protected val currentBlockId = new AtomicInteger(0)

  def apply[A : Encoder]: MemoryStream[A] =
    new MemoryStream[A](encoderFor[A].schema.toAttributes)
}

case class MemoryStream[A : Encoder](output: Seq[Attribute]) extends LeafNode with Source {
  protected var blocks = new ArrayBuffer[BlockId]
  protected var currentWatermark: Watermark = new Watermark(-1)
  protected val encoder = encoderFor[A]

  protected def blockManager = SparkEnv.get.blockManager

  def watermark: Watermark = currentWatermark

  def toDS()(implicit sqlContext: SQLContext): Dataset[A] = {
    new Dataset(sqlContext, this)
  }

  def toDF()(implicit sqlContext: SQLContext): DataFrame = {
    new DataFrame(sqlContext, this)
  }

  def addData(data: TraversableOnce[A]): Watermark = {
    val blockId = StreamBlockId(0, MemoryStream.currentBlockId.incrementAndGet())
    blockManager.putIterator(
      blockId,
      data.toIterator.map(encoder.toRow).map(_.copy()),
      StorageLevels.MEMORY_ONLY_SER)

    synchronized {
      currentWatermark = currentWatermark + 1
      blocks.append(blockId)
      currentWatermark
    }
  }

  def getSlice(sqlContext: SQLContext, start: Watermark, end: Watermark): RDD[InternalRow] = {
    val newBlocks = blocks.slice(start.offset.toInt + 1, end.offset.toInt + 1).toArray
    logDebug(s"Running [$start, $end] on blocks ${newBlocks.mkString(", ")}")
    new BlockRDD[InternalRow](sqlContext.sparkContext, newBlocks)
  }

  override def toString: String = s"MemoryStream[${output.mkString(",")}]"
}

class MemorySink(schema: StructType) extends Sink with Logging {
  private var batches = new ArrayBuffer[(StreamProgress, Seq[Row])]()
  private val output = schema.toAttributes

  def currentWatermarks: StreamProgress = batches.lastOption.map(_._1).getOrElse(new StreamProgress)
  def currentWatermark(source: Source): Option[Watermark] = currentWatermarks.get(source)

  def allData: Seq[Row] = batches.flatMap(_._2)

  val externalRowConverter = RowEncoder(schema)
  def addBatch(watermarks: Map[Source, Watermark], rdd: RDD[InternalRow]): Unit = {
    batches.append((currentWatermarks.copy(), rdd.collect().map(externalRowConverter.fromRow)))
    watermarks.foreach(currentWatermarks.update)
  }

  def dropBatches(num: Int): Unit = {
    batches.remove(batches.size - num, num)
  }

  override def toString: String = batches.map(b => s"${b._1}: ${b._2.mkString(" ")}").mkString("\n")
}