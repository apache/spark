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

package org.apache.spark.sql.execution

import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream,
  InputStream}

import scala.collection.mutable

import org.apache.spark.{InterruptibleIterator, SparkContext, SparkEnv, SparkException,
  TaskContext}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.config
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{CompletionIterator, Utils}

/**
 * Options of an [[OrderedGatherIterator]].
 *
 * @param numMaps the number of map outputs of the shuffle.
 * @param limit the number of rows to return, or -1 to return all rows.
 * @param initialBatchSize with a limit, the number of map outputs in the first batch.
 * @param scaleUpFactor with a limit, a batch reads at most this many times the number of map
 *                      outputs read before it.
 * @param maxBatchSize the maximum number of map outputs in a batch. Without a limit, every batch
 *                     has this many map outputs. A batch never has more than the map outputs left.
 */
private[execution] case class OrderedGatherOptions(
    numMaps: Int,
    limit: Int,
    initialBatchSize: Int,
    scaleUpFactor: Int,
    maxBatchSize: Int) {
  require(numMaps >= 0, s"numMaps must not be negative: $numMaps")
  require(limit >= -1, s"limit must be at least -1: $limit")
  require(initialBatchSize > 0, s"initialBatchSize must be positive: $initialBatchSize")
  require(scaleUpFactor >= 2, s"scaleUpFactor must be at least 2: $scaleUpFactor")
  require(maxBatchSize > 0, s"maxBatchSize must be positive: $maxBatchSize")
}

private[execution] object OrderedGatherOptions {

  /** Creates the options from `conf`. Called on the driver. */
  def apply(conf: SQLConf, numMaps: Int, limit: Int): OrderedGatherOptions = {
    OrderedGatherOptions(
      numMaps = numMaps,
      limit = limit,
      initialBatchSize = conf.limitInitialNumPartitions,
      scaleUpFactor = math.max(conf.limitScaleUpFactor, 2),
      maxBatchSize = conf.getConf(SQLConf.COLLECT_LIMIT_ORDERED_GATHER_MAX_BATCH_PARTITIONS))
  }
}

/** The SQL metrics of an [[OrderedGatherIterator]]. */
private[execution] case class OrderedGatherMetrics(numBatches: SQLMetric, spoolSize: SQLMetric) {
  def toMap: Map[String, SQLMetric] =
    Map("orderedGatherNumBatches" -> numBatches, "orderedGatherSpoolSize" -> spoolSize)
}

private[execution] object OrderedGatherMetrics {
  def apply(sc: SparkContext): OrderedGatherMetrics = OrderedGatherMetrics(
    numBatches = SQLMetrics.createMetric(sc, "number of ordered gather batches"),
    spoolSize = SQLMetrics.createSizeMetric(sc, "ordered gather spool size"))
}

/**
 * The map outputs in a map index range, read for an [[OrderedGatherIterator]].
 *
 * @param mapIndexes the indexes of the map outputs in the range that may have data to read, in
 *                   increasing order. The other map outputs in the range are empty.
 * @param outputs returns each of the `mapIndexes` map outputs exactly once, in any order, with its
 *                map index and rows, if read to the end. The rows of a map output must be read
 *                before the next map output is requested. A listed map output that is not returned
 *                fails the read with an internal error.
 */
private[execution] class MapOutputBatch(
    val mapIndexes: Array[Int],
    val outputs: Iterator[(Int, Iterator[InternalRow])])

/**
 * Returns the map outputs of a single-partition shuffle in map index order, the same way driver
 * collection returns task results in partition index order.
 *
 * Like `SparkPlan.executeTake`, the map outputs are read in batches that grow until `limit` rows
 * have been returned. With a `limit` of -1, all map outputs are read. Within a batch, a map output
 * that arrives when it is the next one to return is returned while it is read. A map output that
 * arrives before its turn is spooled to a local temporary file instead, and returned from there
 * once the map outputs before it have been returned. Only the rows that can still be returned
 * before the limit are spooled, so a map output may be spooled in part, or not at all.
 *
 * With a limit, a batch only starts once all rows of the previous batches have been returned, so
 * that its size can follow the number of rows still needed. Without a limit, the next batch starts
 * as soon as all map outputs of the current batch have arrived, so that fetching it overlaps with
 * returning the spools of the current batch.
 *
 * @param metrics the SQL metrics to update.
 * @param serializer serializes the rows of the spooled map outputs.
 * @param readMapOutputs reads the map outputs in the map index range `[start, end)`.
 */
private[execution] class OrderedGatherIterator(
    context: TaskContext,
    options: OrderedGatherOptions,
    metrics: OrderedGatherMetrics,
    serializer: Serializer,
    readMapOutputs: (Int, Int) => MapOutputBatch)
    extends Iterator[InternalRow] {
  import options.{initialBatchSize, limit, maxBatchSize, numMaps, scaleUpFactor}

  private val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager
  private val serializerManager = SparkEnv.get.serializerManager
  private val serializerInstance = serializer.newInstance()
  private val fileBufferSize = SparkEnv.get.conf.get(config.SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024

  // The first map index of the next batch.
  private var nextBatchStart = 0
  // The number of rows returned so far.
  private var numRowsReturned = 0L
  // Whether no more rows are returned, because they all have been, the limit has been reached, or
  // the task has completed.
  private var closed = false

  // The map index range [batchStart, batchEnd) of the current batch.
  private var batchStart = 0
  private var batchEnd = 0
  // The map outputs of the current batch, in arrival order.
  private var batchOutputs: Iterator[(Int, Iterator[InternalRow])] = Iterator.empty
  // The map indexes of the current batch whose map outputs may have data and have not arrived yet.
  private val pendingMapIndexes = new mutable.BitSet
  // The map outputs of the current batch that arrived before their turn, by map index. A spool is
  // removed once its rows have all been returned.
  private val spools = new mutable.HashMap[Int, Spool]
  // The map index of the next map output to return.
  private var nextMapIndex = 0
  // The rows of the map output being returned, read from the shuffle or from a spool.
  private var rows: Iterator[InternalRow] = Iterator.empty
  // Without a limit, the next batch once it has been started early, with its map index range.
  private var prefetchedBatch: Option[(Int, Int, MapOutputBatch)] = None

  context.addTaskCompletionListener[Unit](_ => close())

  override def hasNext: Boolean = {
    if (closed) {
      return false
    }
    if (limit >= 0 && numRowsReturned >= limit) {
      close()
      return false
    }
    while (!rows.hasNext) {
      if (!advance()) {
        close()
        return false
      }
    }
    true
  }

  override def next(): InternalRow = {
    if (!hasNext) {
      throw SparkCoreErrors.endOfStreamError()
    }
    numRowsReturned += 1
    rows.next()
  }

  /**
   * Takes one step towards the next rows to return: starts the next batch, moves on to the next map
   * output, or takes a map output that arrived before its turn, spooling the rows of it that can
   * still be returned. Returns false if all map outputs have been returned. Fails if a listed map
   * output is not returned.
   */
  private def advance(): Boolean = {
    skipEmptyMapOutputs()
    if (nextMapIndex == batchEnd) {
      finishBatch()
      startNextBatch()
    } else if (spools.contains(nextMapIndex)) {
      prefetchNextBatch()
      returnSpooledMapOutput()
      true
    } else if (batchOutputs.hasNext) {
      takeArrivedMapOutput()
      true
    } else {
      // The next map output was listed but not returned. It may have rows, which would otherwise
      // be dropped without an error.
      throw SparkException.internalError(
        s"Map output $nextMapIndex was not returned when reading map outputs " +
          s"[$batchStart, $batchEnd)", category = "SHUFFLE")
    }
  }

  /** Moves past the map outputs that are known to have no rows left to return. */
  private def skipEmptyMapOutputs(): Unit = {
    while (nextMapIndex < batchEnd && !pendingMapIndexes.contains(nextMapIndex) &&
        !spools.contains(nextMapIndex)) {
      nextMapIndex += 1
    }
  }

  /**
   * Called once all map outputs of the batch have been returned. Reaches the end of the batch's map
   * outputs, which releases the resources used to fetch them.
   */
  private def finishBatch(): Unit = {
    if (batchOutputs.hasNext) {
      throw SparkException.internalError(
        s"Unexpected map output ${batchOutputs.next()._1} after reading map outputs " +
          s"[$batchStart, $batchEnd)", category = "SHUFFLE")
    }
  }

  /** Returns the next map output from its spool. */
  private def returnSpooledMapOutput(): Unit = {
    rows = spools(nextMapIndex).read()
    nextMapIndex += 1
  }

  /**
   * Takes the next map output that has arrived. It is returned while it is read if it is the next
   * one to return, and spooled otherwise.
   */
  private def takeArrivedMapOutput(): Unit = {
    val (mapIndex, mapRows) = batchOutputs.next()
    if (!pendingMapIndexes.remove(mapIndex)) {
      throw SparkException.internalError(
        s"Unexpected map output $mapIndex when reading map outputs [$batchStart, $batchEnd)",
        category = "SHUFFLE")
    }
    if (mapIndex == nextMapIndex) {
      rows = mapRows
      nextMapIndex += 1
    } else if (mapRows.hasNext) {
      // The rows of this map output come after those of all lower ones, so at most the rows left
      // before the limit, minus those of the spooled lower ones, can be returned. Lower ones that
      // have not arrived are not counted, so this may spool more rows than needed, never fewer.
      // The spools are all unread here, as a map output is only taken once `rows` is used up.
      val maxRows = if (limit < 0) {
        Long.MaxValue
      } else {
        limit - numRowsReturned -
          spools.iterator.collect { case (i, spool) if i < mapIndex => spool.numRows }.sum
      }
      if (maxRows > 0) {
        val spool = new Spool(mapIndex)
        // Register the spool before writing it, so that it is deleted if the task fails.
        spools(mapIndex) = spool
        spool.write(mapRows, maxRows)
      } else {
        // None of its rows can be returned, but they must be read before the next map output.
        mapRows.foreach(_ => ())
      }
    }
  }

  /**
   * Without a limit, starts the next batch once all map outputs of the current batch have arrived,
   * that is, when only spools of the current batch are left to return. Its map outputs are then
   * fetched while the spools are returned. Called when `rows` is used up, so that the current
   * batch's map outputs have all been read.
   */
  private def prefetchNextBatch(): Unit = {
    if (limit < 0 && pendingMapIndexes.isEmpty && prefetchedBatch.isEmpty &&
        nextBatchStart < numMaps) {
      // Releases the resources used to fetch the current batch.
      finishBatch()
      prefetchedBatch = Some(readNextBatch())
    }
  }

  /** Starts reading the next batch of map outputs. Returns false if there are none left. */
  private def startNextBatch(): Boolean = {
    val next = prefetchedBatch.orElse {
      if (nextBatchStart < numMaps) Some(readNextBatch()) else None
    }
    prefetchedBatch = None
    next match {
      case Some((start, end, batch)) =>
        pendingMapIndexes ++= batch.mapIndexes
        batchOutputs = batch.outputs
        batchStart = start
        batchEnd = end
        nextMapIndex = start
        true
      case None => false
    }
  }

  /** Reads the next batch of map outputs, and returns its map index range and map outputs. */
  private def readNextBatch(): (Int, Int, MapOutputBatch) = {
    val start = nextBatchStart
    val end = start + nextBatchSize()
    val batch = readMapOutputs(start, end)
    metrics.numBatches += 1
    batch.mapIndexes.foreach { mapIndex =>
      if (mapIndex < start || mapIndex >= end) {
        throw SparkException.internalError(
          s"Map output $mapIndex to read is not in the map outputs [$start, $end)",
          category = "SHUFFLE")
      }
    }
    nextBatchStart = end
    (start, end, batch)
  }

  // Follows how `SparkPlan.executeTake` grows the number of partitions to scan, capped at
  // `maxBatchSize` to bound the number of map outputs fetched at a time. With a limit, a batch only
  // starts once all rows of the previous batches have been returned.
  private def nextBatchSize(): Int = {
    val size = if (limit < 0) {
      maxBatchSize.toLong
    } else if (nextBatchStart == 0) {
      initialBatchSize.toLong
    } else if (numRowsReturned == 0) {
      nextBatchStart.toLong * scaleUpFactor
    } else {
      val left = limit - numRowsReturned
      math.min(math.ceil(1.5 * left * nextBatchStart / numRowsReturned).toLong,
        nextBatchStart.toLong * scaleUpFactor)
    }
    math.max(math.min(size, math.min(maxBatchSize, numMaps - nextBatchStart).toLong), 1L).toInt
  }

  /** Stops returning rows and deletes the spools. */
  private def close(): Unit = {
    closed = true
    rows = Iterator.empty
    batchOutputs = Iterator.empty
    prefetchedBatch = None
    pendingMapIndexes.clear()
    spools.values.foreach(_.delete())
    spools.clear()
  }

  /** A map output that arrived before its turn, spooled to a local temporary file. */
  private class Spool(mapIndex: Int) {
    private val (blockId, file) = diskBlockManager.createTempLocalBlock()
    private var in: InputStream = _
    private var rowsWritten = 0L

    /** The number of rows spooled. */
    def numRows: Long = rowsWritten

    /**
     * Writes at most `maxRows` of `mapRows`. The other rows are never returned, but are still read,
     * because they must be read before the next map output.
     */
    def write(mapRows: Iterator[InternalRow], maxRows: Long): Unit = {
      val out = Utils.tryInitializeResource(
          new BufferedOutputStream(new FileOutputStream(file), fileBufferSize)) { fileOut =>
        serializerInstance.serializeStream(serializerManager.wrapStream(blockId, fileOut))
      }
      Utils.tryWithSafeFinally {
        while (rowsWritten < maxRows && mapRows.hasNext) {
          out.writeValue(mapRows.next())
          rowsWritten += 1
        }
      } {
        out.close()
      }
      mapRows.foreach(_ => ())
      val spoolSize = file.length()
      context.taskMetrics().incDiskBytesSpilled(spoolSize)
      metrics.spoolSize += spoolSize
    }

    /** Reads the spooled rows. The spool is deleted once they have all been read. */
    def read(): Iterator[InternalRow] = {
      in = Utils.tryInitializeResource(
          new BufferedInputStream(new FileInputStream(file), fileBufferSize)) { fileIn =>
        serializerManager.wrapStream(blockId, fileIn)
      }
      val spooledRows = serializerInstance.deserializeStream(in).asKeyValueIterator
        .map(_._2.asInstanceOf[InternalRow])
      new InterruptibleIterator(context,
        CompletionIterator[InternalRow, Iterator[InternalRow]](spooledRows, {
          spools.remove(mapIndex)
          delete()
        }))
    }

    def delete(): Unit = {
      if (in != null) {
        in.close()
        in = null
      }
      file.delete()
    }
  }
}
