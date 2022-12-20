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
import java.util.concurrent.{CompletableFuture, ConcurrentLinkedDeque, ThreadPoolExecutor}

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession

/**
 * Implementation of CommitLog to perform asynchronous writes to storage
 */
class AsyncCommitLog(sparkSession: SparkSession, path: String, executorService: ThreadPoolExecutor)
  extends CommitLog(sparkSession, path) {

  // the cache needs to be enabled because we may not be persisting every entry to durable storage
  // entries not persisted to durable storage will just be stored in memory for faster lookups
  assert(metadataCacheEnabled == true)

  // A queue of batches written to storage.  Used to keep track when to purge old batches
  val writtenToDurableStorage =
    new ConcurrentLinkedDeque[Long](listBatchesOnDisk.toList.asJavaCollection)

  /**
   * Writes a new batch to the commit log asynchronously
   * @param batchId id of batch to write
   * @param metadata metadata of batch to write
   * @return a CompeletableFuture that contains the batch id.  The future is completed when
   *         the async write of the batch is completed.  Future may also be completed exceptionally
   *         to indicate some write error.
   */
  def addAsync(batchId: Long, metadata: CommitMetadata): CompletableFuture[Long] = {
    require(metadata != null, "'null' metadata cannot be written to a metadata log")
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

    batchCache.put(batchId, metadata)
    future
  }

  /**
   * Adds batch to commit log only in memory and not persisted to durable storage. This method is
   * used when we don't want to persist the commit log entry for every micro batch
   * to durable storage
   * @param batchId id of batch to write
   * @param metadata metadata of batch to write
   * @return true if operation is successful otherwise false.
   */
  def addInMemory(batchId: Long, metadata: CommitMetadata): Boolean = {
    if (batchCache.containsKey(batchId)) {
      false
    } else {
      batchCache.put(batchId, metadata)
      true
    }
  }

  /**
   * Purge entries in the commit log up to thresholdBatchId.
   * @param thresholdBatchId
   */
  override def purge(thresholdBatchId: Long): Unit = {
    super.purge(thresholdBatchId)
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
                s"Completion commit for batch ${batchId} took" +
                  s" ${System.currentTimeMillis() - start} ms to be persisted to durable storage"
              )
              writtenToDurableStorage.add(batchId)
              future.complete(true)
            }
          } catch {
            case e: Throwable =>
              logError(s"Encountered error while writing batch ${batchId} to commit log", e)
              future.completeExceptionally(e)
          }
        }
      })
      future
    }
  }
}
