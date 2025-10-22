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

import java.io.DataInputStream
import java.nio.channels.Channels
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import net.razorvine.pickle.Unpickler

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.streaming.runtime.LongOffset


class PythonProfileMicroBatchStream
  extends MicroBatchStream with Logging {

  @GuardedBy("this")
  private var readThread: Thread = null

  @GuardedBy("this")
  private val batches = new ListBuffer[java.util.List[java.util.Map[String, String]]]

  @GuardedBy("this")
  private var currentOffset: LongOffset = LongOffset(-1L)

  @GuardedBy("this")
  private var lastOffsetCommitted: LongOffset = LongOffset(-1L)

  private val initialized: AtomicBoolean = new AtomicBoolean(false)

  private def initialize(): Unit = synchronized {
    readThread = new Thread(s"PythonProfileMicroBatchStream") {
      setDaemon(true)

      override def run(): Unit = {
        val unpickler = new Unpickler
        val extraChannel = SparkEnv.get.pythonWorkers.values
          .head.getAllDaemonWorkers.map(_._1.extraChannel).head
        extraChannel.foreach { s =>
          val inputStream = new DataInputStream(Channels.newInputStream(s))
          while (true) {
            val len = inputStream.readInt()
            val buf = new Array[Byte](len)
            var totalRead = 0
            while (totalRead < len) {
              val readNow = inputStream.read(buf, totalRead, len - totalRead)
              assert(readNow != -1)
              totalRead += readNow
            }
            currentOffset += 1
            batches.append(
              unpickler.loads(buf).asInstanceOf[java.util.List[java.util.Map[String, String]]])
          }
        }
      }
    }
    readThread.start()
  }

  override def initialOffset(): Offset = LongOffset(-1L)

  override def latestOffset(): Offset = currentOffset

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOrdinal = start.asInstanceOf[LongOffset].offset.toInt + 1
    val endOrdinal = end.asInstanceOf[LongOffset].offset.toInt + 1

    val rawList = synchronized {
      if (initialized.compareAndSet(false, true)) {
        initialize()
      }

      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      batches.slice(sliceStart, sliceEnd)
    }

    Array(PythonProfileInputPartition(rawList))
  }

  override def createReaderFactory(): PartitionReaderFactory =
    (partition: InputPartition) => {
      val stats = partition.asInstanceOf[PythonProfileInputPartition].stats
      new PartitionReader[InternalRow] {
        private var currentIdx = -1

        override def next(): Boolean = {
          currentIdx += 1
          currentIdx < stats.size
        }

        override def get(): InternalRow = {
          InternalRow.fromSeq(
            CatalystTypeConverters.convertToCatalyst(
              stats(currentIdx).asScala.toSeq.map(_.asScala)) :: Nil)
        }

        override def close(): Unit = {}
      }
    }

  override def commit(end: Offset): Unit = synchronized {
    val newOffset = end.asInstanceOf[LongOffset]

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      throw new IllegalStateException(
        s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    batches.dropInPlace(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  override def toString: String = s"PythonProfile"

  override def stop(): Unit = { }
}

case class PythonProfileInputPartition(
    stats: ListBuffer[java.util.List[java.util.Map[String, String]]]) extends InputPartition
