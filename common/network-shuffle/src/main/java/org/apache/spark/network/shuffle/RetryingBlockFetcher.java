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

package org.apache.spark.network.shuffle;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Wraps another BlockFetcher with the ability to automatically retry fetches which fail due to
 * IOExceptions, which we hope are due to transient network conditions.
 *
 * This fetcher provides stronger guarantees regarding the parent BlockFetchingListener. In
 * particular, the listener will be invoked exactly once per blockId, with a success or failure.
 */
public class RetryingBlockFetcher {

  /**
   * Used to initiate the first fetch for all blocks, and subsequently for retrying the fetch on any
   * remaining blocks.
   */
  public interface BlockFetchStarter {
    /**
     * Creates a new BlockFetcher to fetch the given block ids which may do some synchronous
     * bootstrapping followed by fully asynchronous block fetching.
     * The BlockFetcher must eventually invoke the Listener on every input blockId, or else this
     * method must throw an exception.
     *
     * This method should always attempt to get a new TransportClient from the
     * {@link org.apache.spark.network.client.TransportClientFactory} in order to fix connection
     * issues.
     */
    void createAndStart(String[] blockIds, BlockFetchingListener listener) throws IOException;
  }

  /** Shared executor service used for waiting and retrying. */
  private static final ExecutorService executorService = Executors.newCachedThreadPool(
    NettyUtils.createThreadFactory("Block Fetch Retry"));

  private final Logger logger = LoggerFactory.getLogger(RetryingBlockFetcher.class);

  /** Used to initiate new Block Fetches on our remaining blocks. */
  private final BlockFetchStarter fetchStarter;

  /** Parent listener which we delegate all successful or permanently failed block fetches to. */
  private final BlockFetchingListener listener;

  /** Max number of times we are allowed to retry. */
  private final int maxRetries;

  /** Milliseconds to wait before each retry. */
  private final int retryWaitTime;

  // NOTE:
  // All of our non-final fields are synchronized under 'this' and should only be accessed/mutated
  // while inside a synchronized block.
  /** Number of times we've attempted to retry so far. */
  private int retryCount = 0;

  /**
   * Set of all block ids which have not been fetched successfully or with a non-IO Exception.
   * A retry involves requesting every outstanding block. Note that since this is a LinkedHashSet,
   * input ordering is preserved, so we always request blocks in the same order the user provided.
   */
  private final LinkedHashSet<String> outstandingBlocksIds;

  /**
   * The BlockFetchingListener that is active with our current BlockFetcher.
   * When we start a retry, we immediately replace this with a new Listener, which causes all any
   * old Listeners to ignore all further responses.
   */
  private RetryingBlockFetchListener currentListener;

  public RetryingBlockFetcher(
      TransportConf conf,
      BlockFetchStarter fetchStarter,
      String[] blockIds,
      BlockFetchingListener listener) {
    this.fetchStarter = fetchStarter;
    this.listener = listener;
    this.maxRetries = conf.maxIORetries();
    this.retryWaitTime = conf.ioRetryWaitTimeMs();
    this.outstandingBlocksIds = Sets.newLinkedHashSet();
    Collections.addAll(outstandingBlocksIds, blockIds);
    this.currentListener = new RetryingBlockFetchListener();
  }

  /**
   * Initiates the fetch of all blocks provided in the constructor, with possible retries in the
   * event of transient IOExceptions.
   */
  public void start() {
    fetchAllOutstanding();
  }

  /**
   * Fires off a request to fetch all blocks that have not been fetched successfully or permanently
   * failed (i.e., by a non-IOException).
   */
  private void fetchAllOutstanding() {
    // Start by retrieving our shared state within a synchronized block.
    String[] blockIdsToFetch;
    int numRetries;
    RetryingBlockFetchListener myListener;
    synchronized (this) {
      blockIdsToFetch = outstandingBlocksIds.toArray(new String[outstandingBlocksIds.size()]);
      numRetries = retryCount;
      myListener = currentListener;
    }

    // Now initiate the fetch on all outstanding blocks, possibly initiating a retry if that fails.
    try {
      fetchStarter.createAndStart(blockIdsToFetch, myListener);
    } catch (Exception e) {
      logger.error(String.format("Exception while beginning fetch of %s outstanding blocks %s",
        blockIdsToFetch.length, numRetries > 0 ? "(after " + numRetries + " retries)" : ""), e);

      if (shouldRetry(e)) {
        initiateRetry();
      } else {
        for (String bid : blockIdsToFetch) {
          listener.onBlockFetchFailure(bid, e);
        }
      }
    }
  }

  /**
   * Lightweight method which initiates a retry in a different thread. The retry will involve
   * calling fetchAllOutstanding() after a configured wait time.
   */
  private synchronized void initiateRetry() {
    retryCount += 1;
    currentListener = new RetryingBlockFetchListener();

    logger.info("Retrying fetch ({}/{}) for {} outstanding blocks after {} ms",
      retryCount, maxRetries, outstandingBlocksIds.size(), retryWaitTime);

    executorService.submit(new Runnable() {
      @Override
      public void run() {
        Uninterruptibles.sleepUninterruptibly(retryWaitTime, TimeUnit.MILLISECONDS);
        fetchAllOutstanding();
      }
    });
  }

  /**
   * Returns true if we should retry due a block fetch failure. We will retry if and only if
   * the exception was an IOException and we haven't retried 'maxRetries' times already.
   */
  private synchronized boolean shouldRetry(Throwable e) {
    boolean isIOException = e instanceof IOException
      || (e.getCause() != null && e.getCause() instanceof IOException);
    boolean hasRemainingRetries = retryCount < maxRetries;
    return isIOException && hasRemainingRetries;
  }

  /**
   * Our RetryListener intercepts block fetch responses and forwards them to our parent listener.
   * Note that in the event of a retry, we will immediately replace the 'currentListener' field,
   * indicating that any responses from non-current Listeners should be ignored.
   */
  private class RetryingBlockFetchListener implements BlockFetchingListener {
    @Override
    public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
      // We will only forward this success message to our parent listener if this block request is
      // outstanding and we are still the active listener.
      boolean shouldForwardSuccess = false;
      synchronized (RetryingBlockFetcher.this) {
        if (this == currentListener && outstandingBlocksIds.contains(blockId)) {
          outstandingBlocksIds.remove(blockId);
          shouldForwardSuccess = true;
        }
      }

      // Now actually invoke the parent listener, outside of the synchronized block.
      if (shouldForwardSuccess) {
        listener.onBlockFetchSuccess(blockId, data);
      }
    }

    @Override
    public void onBlockFetchFailure(String blockId, Throwable exception) {
      // We will only forward this failure to our parent listener if this block request is
      // outstanding, we are still the active listener, AND we cannot retry the fetch.
      boolean shouldForwardFailure = false;
      synchronized (RetryingBlockFetcher.this) {
        if (this == currentListener && outstandingBlocksIds.contains(blockId)) {
          if (shouldRetry(exception)) {
            initiateRetry();
          } else {
            logger.error(String.format("Failed to fetch block %s, and will not retry (%s retries)",
              blockId, retryCount), exception);
            outstandingBlocksIds.remove(blockId);
            shouldForwardFailure = true;
          }
        }
      }

      // Now actually invoke the parent listener, outside of the synchronized block.
      if (shouldForwardFailure) {
        listener.onBlockFetchFailure(blockId, exception);
      }
    }
  }
}
