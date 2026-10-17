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

package org.apache.spark.status

import java.io.ByteArrayOutputStream

import scala.collection.mutable

import org.apache.spark.status.api.v1.AccumulableInfo
import org.apache.spark.status.protobuf.CompactTaskDetails
import org.apache.spark.util.collection.CompactLongArray
import org.apache.spark.util.kvstore.{KVIndex, KVStoreRecordCodec}

/**
 * Keeps completed tasks in stage-attempt column blocks. Small pending batches use signed
 * varints instead of allocating a full column buffer for every short stage. Active task
 * snapshots remain directly accessible, avoiding packing on every executor heartbeat.
 *
 * Published records are immutable snapshots. Sealing changes only their representation;
 * removing or replacing a record never changes a snapshot held by an existing iterator.
 */
private[spark] final class CompactTaskDataCodec(
    blockSize: Int = 128,
    maxOpenBlocks: Int = 32) extends KVStoreRecordCodec[TaskDataWrapper] {
  import CompactTaskDataCodec._
  import TaskIndexNames._

  require(blockSize > 0)
  require(maxOpenBlocks > 0)

  private val pending = new mutable.LinkedHashMap[Long, Builder]()

  override def encode(value: TaskDataWrapper): KVStoreRecordCodec.Record[TaskDataWrapper] =
      synchronized {
    val key = (value.stageId.toLong << 32) | (value.stageAttemptId.toLong & 0xffffffffL)
    if (value.status == "RUNNING" || value.status == "GET RESULT") {
      new TaskRecord(new ActiveRow(key, value), 0, null)
    } else {
      val builder = pending.getOrElseUpdate(key, {
        if (pending.size >= maxOpenBlocks) {
          seal(pending.head._2)
        }
        new Builder(key)
      })
      val record = new TaskRecord(new EncodedRow(key, value), builder.used, builder)
      builder.records(builder.used) = record
      builder.used += 1
      builder.live += 1
      if (builder.used == blockSize) {
        seal(builder)
      }
      record
    }
  }

  override def compact(): Unit = synchronized {
    pending.values.toList.foreach(seal)
  }

  private class Builder(val key: Long) {
    val records = new Array[TaskRecord](blockSize)
    var used = 0
    var live = 0
  }

  private def seal(builder: Builder): Unit = {
    pending.remove(builder.key)
    if (builder.live > 0) {
      // Column headers outweigh packing savings for very small or mostly deleted batches.
      val block = if (builder.live >= 16 && builder.live * 2 >= builder.used) {
        new ColumnBlock(builder.key, builder.records, builder.used)
      } else {
        null
      }
      builder.records.take(builder.used).foreach { record =>
        if (record != null) {
          if (block != null) record.data = block
          record.owner = null
        }
      }
    }
  }

  private class TaskRecord(
      @volatile var data: TaskBlock,
      val row: Int,
      var owner: Builder) extends KVStoreRecordCodec.Record[TaskDataWrapper] {

    override def removed(): Unit = CompactTaskDataCodec.this.synchronized {
      if (owner != null) {
        owner.records(row) = null
        owner.live -= 1
        if (owner.live == 0) {
          pending.remove(owner.key)
        }
        owner = null
      }
    }

    override def decode(): TaskDataWrapper = {
      val block = data
      def number(column: Int): Long = block.number(row, column)
      val (updates, error) = block.decodedDetails(row)
      new TaskDataWrapper(
        taskId = number(0),
        index = number(1).toInt,
        attempt = number(2).toInt,
        partitionId = number(3).toInt,
        launchTime = number(4),
        resultFetchStart = number(5),
        duration = number(6),
        executorDeserializeTime = number(9),
        executorDeserializeCpuTime = number(10),
        executorRunTime = number(11),
        executorCpuTime = number(12),
        resultSize = number(13),
        jvmGcTime = number(14),
        resultSerializationTime = number(15),
        memoryBytesSpilled = number(16),
        diskBytesSpilled = number(17),
        peakExecutionMemory = number(18),
        inputBytesRead = number(19),
        inputRecordsRead = number(20),
        outputBytesWritten = number(21),
        outputRecordsWritten = number(22),
        shuffleRemoteBlocksFetched = number(23),
        shuffleLocalBlocksFetched = number(24),
        shuffleFetchWaitTime = number(25),
        shuffleRemoteBytesRead = number(26),
        shuffleRemoteBytesReadToDisk = number(27),
        shuffleLocalBytesRead = number(28),
        shuffleRecordsRead = number(29),
        shuffleCorruptMergedBlockChunks = number(30),
        shuffleMergedFetchFallbackCount = number(31),
        shuffleMergedRemoteBlocksFetched = number(32),
        shuffleMergedLocalBlocksFetched = number(33),
        shuffleMergedRemoteChunksFetched = number(34),
        shuffleMergedLocalChunksFetched = number(35),
        shuffleMergedRemoteBytesRead = number(36),
        shuffleMergedLocalBytesRead = number(37),
        shuffleRemoteReqsDuration = number(38),
        shuffleMergedRemoteReqDuration = number(39),
        shuffleBytesWritten = number(40),
        shuffleWriteTime = number(41),
        shuffleRecordsWritten = number(42),
        executorId = block.string(row, 0),
        host = block.string(row, 1),
        status = block.string(row, 2),
        taskLocality = block.string(row, 3),
        speculative = number(7) != 0L,
        hasMetrics = number(8) != 0L,
        accumulatorUpdates = updates,
        errorMessage = error,
        stageId = (block.stageKey >> 32).toInt,
        stageAttemptId = block.stageKey.toInt)
    }

    override def indexValue(indexName: String): Object = {
      val block = data
      def number(column: Int): Long = block.number(row, column)
      def actual(column: Int): Long = {
        val value = number(column)
        if (block.string(row, 2) == "SUCCESS") value else math.abs(value + 1)
      }
      val result: Any = indexName match {
        case KVIndex.NATURAL_INDEX_NAME => number(0)
        case STAGE => Array((block.stageKey >> 32).toInt, block.stageKey.toInt)
        case TASK_INDEX => number(1).toInt
        case ATTEMPT => number(2).toInt
        case TASK_PARTITION_ID => number(3).toInt
        case LAUNCH_TIME => number(4)
        case DURATION => number(6)
        case DESER_TIME => number(9)
        case DESER_CPU_TIME => number(10)
        case EXEC_RUN_TIME => number(11)
        case EXEC_CPU_TIME => number(12)
        case RESULT_SIZE => number(13)
        case GC_TIME => number(14)
        case SER_TIME => number(15)
        case MEM_SPILL => number(16)
        case DISK_SPILL => number(17)
        case PEAK_MEM => number(18)
        case INPUT_SIZE => number(19)
        case INPUT_RECORDS => number(20)
        case OUTPUT_SIZE => number(21)
        case OUTPUT_RECORDS => number(22)
        case SHUFFLE_REMOTE_BLOCKS => number(23)
        case SHUFFLE_LOCAL_BLOCKS => number(24)
        case SHUFFLE_READ_FETCH_WAIT_TIME => number(25)
        case SHUFFLE_REMOTE_READS => number(26)
        case SHUFFLE_REMOTE_READS_TO_DISK => number(27)
        case SHUFFLE_READ_RECORDS => number(29)
        case SHUFFLE_PUSH_CORRUPT_MERGED_BLOCK_CHUNKS => number(30)
        case SHUFFLE_PUSH_MERGED_FETCH_FALLBACK_COUNT => number(31)
        case SHUFFLE_PUSH_MERGED_REMOTE_BLOCKS => number(32)
        case SHUFFLE_PUSH_MERGED_LOCAL_BLOCKS => number(33)
        case SHUFFLE_PUSH_MERGED_REMOTE_CHUNKS => number(34)
        case SHUFFLE_PUSH_MERGED_LOCAL_CHUNKS => number(35)
        case SHUFFLE_PUSH_MERGED_REMOTE_READS => number(36)
        case SHUFFLE_PUSH_MERGED_LOCAL_READS => number(37)
        case SHUFFLE_REMOTE_REQS_DURATION => number(38)
        case SHUFFLE_PUSH_MERGED_REMOTE_REQS_DURATION => number(39)
        case SHUFFLE_WRITE_SIZE => number(40)
        case SHUFFLE_WRITE_TIME => number(41)
        case SHUFFLE_WRITE_RECORDS => number(42)
        case EXECUTOR => block.string(row, 0)
        case HOST => block.string(row, 1)
        case STATUS => block.string(row, 2)
        case LOCALITY => block.string(row, 3)
        case ERROR => block.error(row).getOrElse("")
        case ACCUMULATORS =>
          block.accumulators(row).headOption.map(a => s"${a.name}:${a.value}").getOrElse("")
        case COMPLETION_TIME => number(4) + number(6)
        case SCHEDULER_DELAY =>
          if (number(8) == 0L) -1L else {
            AppStatusUtils.schedulerDelay(number(4),
              number(5), number(6),
              actual(9), actual(15),
              actual(11))
          }
        case GETTING_RESULT_TIME =>
          if (number(8) == 0L) -1L else {
            AppStatusUtils.gettingResultTime(number(4),
              number(5), number(6))
          }
        case SHUFFLE_TOTAL_READS =>
          if (number(8) == 0L) -1L
          else number(28) + number(26)
        case SHUFFLE_TOTAL_BLOCKS =>
          if (number(8) == 0L) -1L
          else number(24) + number(23)
        case _ => throw new IllegalArgumentException(s"Unknown task index: $indexName")
      }
      result.asInstanceOf[AnyRef]
    }
  }

  private class ColumnBlock(
      override val stageKey: Long,
      records: Array[TaskRecord],
      size: Int) extends TaskBlock {
    private val columns = Array.fill(NUM_COLUMNS)(new CompactLongArray(size))
    private val stringColumns = Array.fill(4)(new CompactLongArray(size))
    private var taskDetails: Array[CompactTaskDetails] = _

    private val strings = {
      val dictionaries = Array.fill(4)(new mutable.HashMap[String, Int]())
      val stringValues = Array.fill(4)(new mutable.ArrayBuffer[String]())
      var index = 0
      while (index < size) {
        val record = records(index)
        if (record != null) {
          val block = record.data
          val numbers = block.numbers(record.row)
          var column = 0
          while (column < NUM_COLUMNS) {
            columns(column)(index) = numbers(column)
            column += 1
          }
          column = 0
          while (column < 4) {
            val value = block.string(record.row, column)
            val code = dictionaries(column).getOrElseUpdate(value, {
              stringValues(column) += value
              stringValues(column).size - 1
            })
            stringColumns(column)(index) = code
            column += 1
          }
          val detail = block.details(record.row)
          if (detail != null) {
            if (taskDetails == null) taskDetails = new Array[CompactTaskDetails](size)
            taskDetails(index) = detail
          }
        }
        index += 1
      }
      columns.foreach(_.compact())
      stringColumns.foreach(_.compact())
      stringValues.map(_.toArray)
    }

    override def number(row: Int, column: Int): Long = columns(column)(row)
    override def string(row: Int, column: Int): String =
      strings(column)(stringColumns(column)(row).toInt)
    override def details(row: Int): CompactTaskDetails =
      if (taskDetails == null) null else taskDetails(row)
  }
}

private[spark] object CompactTaskDataCodec {
  private val NUM_COLUMNS = 43

  private trait TaskBlock {
    def stageKey: Long
    def number(row: Int, column: Int): Long
    def string(row: Int, column: Int): String
    def details(row: Int): CompactTaskDetails
    def decodedDetails(row: Int): (collection.Seq[AccumulableInfo], Option[String]) = {
      val payload = details(row)
      if (payload == null) (Nil, None) else payload.decode()
    }
    def accumulators(row: Int): collection.Seq[AccumulableInfo] = {
      val payload = details(row)
      if (payload == null) Nil else payload.accumulators()
    }
    def error(row: Int): Option[String] = {
      val payload = details(row)
      if (payload == null) None else payload.error()
    }
    def numbers(row: Int): Array[Long] = Array.tabulate(NUM_COLUMNS)(number(row, _))
  }

  private class ActiveRow(
      override val stageKey: Long,
      value: TaskDataWrapper) extends TaskBlock {
    override def number(row: Int, column: Int): Long = numericValue(value, column)
    override def string(row: Int, column: Int): String = stringValue(value, column)
    override def details(row: Int): CompactTaskDetails =
      CompactTaskDetails(value.accumulatorUpdates, value.errorMessage)
    override def decodedDetails(row: Int): (collection.Seq[AccumulableInfo], Option[String]) =
      (value.accumulatorUpdates, value.errorMessage)
    override def accumulators(row: Int): collection.Seq[AccumulableInfo] =
      value.accumulatorUpdates
    override def error(row: Int): Option[String] = value.errorMessage
  }

  private class EncodedRow(
      override val stageKey: Long,
      value: TaskDataWrapper) extends TaskBlock {
    private val bytes = {
      val out = new ByteArrayOutputStream(NUM_COLUMNS * 2)
      var column = 0
      while (column < NUM_COLUMNS) {
        val input = numericValue(value, column)
        var encoded = (input << 1) ^ (input >> 63)
        while ((encoded & ~0x7fL) != 0L) {
          out.write((encoded.toInt & 0x7f) | 0x80)
          encoded >>>= 7
        }
        out.write(encoded.toInt)
        column += 1
      }
      out.toByteArray
    }
    private val strings = Array.tabulate(4)(stringValue(value, _))
    private val taskDetails = CompactTaskDetails(value.accumulatorUpdates, value.errorMessage)

    override def number(row: Int, column: Int): Long = {
      var offset = 0
      var current = 0
      while (current < column) {
        while ((bytes(offset) & 0x80) != 0) offset += 1
        offset += 1
        current += 1
      }
      decodeNumber(offset)._1
    }

    override def numbers(row: Int): Array[Long] = {
      val result = new Array[Long](NUM_COLUMNS)
      var offset = 0
      var column = 0
      while (column < NUM_COLUMNS) {
        val (value, next) = decodeNumber(offset)
        result(column) = value
        offset = next
        column += 1
      }
      result
    }

    private def decodeNumber(start: Int): (Long, Int) = {
      var offset = start
      var encoded = 0L
      var shift = 0
      var next = 0
      do {
        next = bytes(offset) & 0xff
        encoded |= (next & 0x7f).toLong << shift
        shift += 7
        offset += 1
      } while ((next & 0x80) != 0)
      ((encoded >>> 1) ^ -(encoded & 1), offset)
    }

    override def string(row: Int, column: Int): String = strings(column)
    override def details(row: Int): CompactTaskDetails = taskDetails
  }

  private def stringValue(value: TaskDataWrapper, column: Int): String = column match {
    case 0 => value.executorId
    case 1 => value.host
    case 2 => value.status
    case 3 => value.taskLocality
  }

  private def numericValue(value: TaskDataWrapper, column: Int): Long = column match {
      case 0 => value.taskId
      case 1 => value.index
      case 2 => value.attempt
      case 3 => value.partitionId
      case 4 => value.launchTime
      case 5 => value.resultFetchStart
      case 6 => value.duration
      case 7 => if (value.speculative) 1L else 0L
      case 8 => if (value.hasMetrics) 1L else 0L
      case 9 => value.executorDeserializeTime
      case 10 => value.executorDeserializeCpuTime
      case 11 => value.executorRunTime
      case 12 => value.executorCpuTime
      case 13 => value.resultSize
      case 14 => value.jvmGcTime
      case 15 => value.resultSerializationTime
      case 16 => value.memoryBytesSpilled
      case 17 => value.diskBytesSpilled
      case 18 => value.peakExecutionMemory
      case 19 => value.inputBytesRead
      case 20 => value.inputRecordsRead
      case 21 => value.outputBytesWritten
      case 22 => value.outputRecordsWritten
      case 23 => value.shuffleRemoteBlocksFetched
      case 24 => value.shuffleLocalBlocksFetched
      case 25 => value.shuffleFetchWaitTime
      case 26 => value.shuffleRemoteBytesRead
      case 27 => value.shuffleRemoteBytesReadToDisk
      case 28 => value.shuffleLocalBytesRead
      case 29 => value.shuffleRecordsRead
      case 30 => value.shuffleCorruptMergedBlockChunks
      case 31 => value.shuffleMergedFetchFallbackCount
      case 32 => value.shuffleMergedRemoteBlocksFetched
      case 33 => value.shuffleMergedLocalBlocksFetched
      case 34 => value.shuffleMergedRemoteChunksFetched
      case 35 => value.shuffleMergedLocalChunksFetched
      case 36 => value.shuffleMergedRemoteBytesRead
      case 37 => value.shuffleMergedLocalBytesRead
      case 38 => value.shuffleRemoteReqsDuration
      case 39 => value.shuffleMergedRemoteReqDuration
      case 40 => value.shuffleBytesWritten
      case 41 => value.shuffleWriteTime
      case 42 => value.shuffleRecordsWritten
  }
}
