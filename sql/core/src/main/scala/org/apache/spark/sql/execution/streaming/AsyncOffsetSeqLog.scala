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

import java.io.OutputStream
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{Clock, SystemClock}

/**
 * Used to write entries to the offset log asynchronously
 */
class AsyncOffsetSeqLog(
    sparkSession: SparkSession,
    path: String,
    executorService: ThreadPoolExecutor,
    offsetCommitIntervalMs: Long,
    clock: Clock = new SystemClock())
  extends OffsetSeqLog(sparkSession, path) {

  // the cache needs to be enabled because we may not be persisting every entry to durable storage
  // entries not persisted to durable storage will just be stored in memory for faster lookups
  assert(metadataCacheEnabled == true)

  // A map of the current pending offset writes. Key -> batch Id, Value -> CompletableFuture
  // Used to determine if a commit log entry for this batch also needs to be persisted to storage
  private val pendingOffsetWrites = new ConcurrentHashMap[Long, CompletableFuture[Long]]()

  // Keeps track the last time a commit was issued. Used for issuing commits to storage at
  // the configured intervals
  private val lastCommitIssuedTimestampMs: AtomicLong = new AtomicLong(-1)

  // A queue of batches written to storage.  Used to keep track when to purge old batches
  val writtenToDurableStorage =
    new ConcurrentLinkedDeque[Long](listBatchesOnDisk.toList.asJavaCollection)

  /**
   * Get a async offset write by batch id.  To check if a corresponding commit log entry
   * needs to be written to durable storage as well
   * @param batchId
   * @return a option to indicate whether a async offset write was issued for the batch with id
   */
  def getAsyncOffsetWrite(batchId: Long): Option[CompletableFuture[Long]] = {
    Option(pendingOffsetWrites.get(batchId))
  }

  /**
   * Remove the async offset write when we don't need to keep track of it anymore
   * @param batchId
   */
  def removeAsyncOffsetWrite(batchId: Long): Unit = {
    pendingOffsetWrites.remove(batchId)
  }

  /**
   * Writes a new batch to the offset log asynchronously
   * @param batchId id of batch to write
   * @param metadata metadata of batch to write
   * @return a CompeletableFuture that contains the batch id.  The future is completed when
   *         the async write of the batch is completed.  Future may also be completed exceptionally
   *         to indicate some write error.
   */
  def addAsync(batchId: Long, metadata: OffsetSeq): CompletableFuture[(Long, Boolean)] = {
    require(metadata != null, "'null' metadata cannot written to a metadata log")

    def issueAsyncWrite(batchId: Long): CompletableFuture[Long] = {
      lastCommitIssuedTimestampMs.set(clock.getTimeMillis())
      val future: CompletableFuture[Long] = addNewBatchByStreamAsync(batchId) { output =>
        serialize(metadata, output)
      }.thenApply((ret: Boolean) => {
        if (ret) {
          batchId
        } else {
          throw new IllegalStateException(
            s"Concurrent update to the log. Multiple streaming jobs detected for $batchId"
          )
        }
      })
      pendingOffsetWrites.put(batchId, future)
      future
    }

    val lastIssuedTs = lastCommitIssuedTimestampMs.get()
    val future: CompletableFuture[(Long, Boolean)] = {
      if (offsetCommitIntervalMs > 0) {
        if ((lastIssuedTs == -1) // haven't started any commits yet
          || (lastIssuedTs + offsetCommitIntervalMs) <= clock.getTimeMillis()) {
          issueAsyncWrite(batchId).thenApply((batchId: Long) => {
            (batchId, true)
          })
        } else {
          // just return completed future because we are not persisting this offset
          CompletableFuture.completedFuture((batchId, false))
        }
      } else {
        // offset commit interval is not enabled
        issueAsyncWrite(batchId).thenApply((batchId: Long) => {
          (batchId, true)
        })
      }
    }

    batchCache.put(batchId, metadata)
    future
  }

  /**
   * Adds new batch asynchronously
   * @param batchId id of batch to write
   * @param fn serialization function
   * @return CompletableFuture that contains a boolean do
   *         indicate whether the write was successfuly or not.
   *         Future can also be completed exceptionally to indicate write errors.
   */
  private def addNewBatchByStreamAsync(batchId: Long)(
      fn: OutputStream => Unit): CompletableFuture[Boolean] = {
    val future = new CompletableFuture[Boolean]()
    val batchMetadataFile = batchIdToPath(batchId)

    if (batchCache.containsKey(batchId)) {
      future.complete(false)
      future
    } else {
      executorService.submit(new Runnable {
        override def run(): Unit = {
          try {
            if (fileManager.exists(batchMetadataFile)) {
              future.complete(false)
            } else {
              val start = System.currentTimeMillis()
              write(
                batchMetadataFile,
                fn
              )
              logDebug(
                s"Offset commit for batch ${batchId} took" +
                s" ${System.currentTimeMillis() - start} ms to be persisted to durable storage"
              )
              writtenToDurableStorage.add(batchId)
              future.complete(true)
            }
          } catch {
            case e: Throwable =>
              logError(s"Encountered error while writing batch ${batchId} to offset log", e)
              future.completeExceptionally(e)
          }
        }
      })
      future
    }
  }

  /**
   * Purge entries in the offset log up to thresholdBatchId.
   * @param thresholdBatchId
   */
  override def purge(thresholdBatchId: Long): Unit = {
    super.purge(thresholdBatchId)
  }

  // used in tests
  def pendingAsyncOffsetWrite(): Int = {
    pendingOffsetWrites.size()
  }
}
