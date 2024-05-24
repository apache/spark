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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.sasl.SaslTimeoutException;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Wraps another BlockFetcher or BlockPusher with the ability to automatically retry block
 * transfers which fail due to IOExceptions, which we hope are due to transient network conditions.
 *
 * This transferor provides stronger guarantees regarding the parent BlockTransferListener. In
 * particular, the listener will be invoked exactly once per blockId, with a success or failure.
 */
public class RetryingBlockTransferor {

  /**
   * Used to initiate the first transfer for all blocks, and subsequently for retrying the
   * transfer on any remaining blocks.
   */
  public interface BlockTransferStarter {
    /**
     * Creates a new BlockFetcher or BlockPusher to transfer the given block ids which may do
     * some synchronous bootstrapping followed by fully asynchronous block transferring.
     * The BlockFetcher or BlockPusher must eventually invoke the Listener on every input blockId,
     * or else this method must throw an exception.
     *
     * This method should always attempt to get a new TransportClient from the
     * {@link org.apache.spark.network.client.TransportClientFactory} in order to fix connection
     * issues.
     */
    void createAndStart(String[] blockIds, BlockTransferListener listener)
         throws IOException, InterruptedException;
  }

  /** Shared executor service used for waiting and retrying. */
  private static final ExecutorService executorService = Executors.newCachedThreadPool(
    NettyUtils.createThreadFactory("Block Transfer Retry"));

  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(RetryingBlockTransferor.class);

  /** Used to initiate new Block transfer on our remaining blocks. */
  private final BlockTransferStarter transferStarter;

  /** Parent listener which we delegate all successful or permanently failed block transfers to. */
  private final BlockTransferListener listener;

  /** Max number of times we are allowed to retry. */
  private final int maxRetries;

  /** Milliseconds to wait before each retry. */
  private final int retryWaitTime;

  // NOTE:
  // All of our non-final fields are synchronized under 'this' and should only be accessed/mutated
  // while inside a synchronized block.
  /** Number of times we've attempted to retry so far. */
  private int retryCount = 0;

  // Number of times SASL timeout has been retried without success.
  // If we see maxRetries consecutive failures, the request is failed.
  // On the other hand, if sasl succeeds and we are able to send other requests subsequently,
  // we reduce the SASL failures from retryCount (since SASL failures were part of
  // connection bootstrap - which ended up being successful).
  // spark.network.auth.rpcTimeout is much lower than spark.network.timeout and others -
  // and so sasl is more susceptible to failures when remote service
  // (like external shuffle service) is under load: but once it succeeds, we do not want to
  // include it as part of request retries.
  private int saslRetryCount = 0;

  /**
   * Set of all block ids which have not been transferred successfully or with a non-IO Exception.
   * A retry involves requesting every outstanding block. Note that since this is a LinkedHashSet,
   * input ordering is preserved, so we always request blocks in the same order the user provided.
   */
  private final LinkedHashSet<String> outstandingBlocksIds;

  /**
   * The BlockTransferListener that is active with our current BlockFetcher.
   * When we start a retry, we immediately replace this with a new Listener, which causes all any
   * old Listeners to ignore all further responses.
   */
  private RetryingBlockTransferListener currentListener;

  /** Whether sasl retries are enabled. */
  private final boolean enableSaslRetries;

  private final ErrorHandler errorHandler;

  public RetryingBlockTransferor(
      TransportConf conf,
      BlockTransferStarter transferStarter,
      String[] blockIds,
      BlockTransferListener listener,
      ErrorHandler errorHandler) {
    this.transferStarter = transferStarter;
    this.listener = listener;
    this.maxRetries = conf.maxIORetries();
    this.retryWaitTime = conf.ioRetryWaitTimeMs();
    this.outstandingBlocksIds = Sets.newLinkedHashSet();
    Collections.addAll(outstandingBlocksIds, blockIds);
    this.currentListener = new RetryingBlockTransferListener();
    this.errorHandler = errorHandler;
    this.enableSaslRetries = conf.enableSaslRetries();
    this.saslRetryCount = 0;
  }

  public RetryingBlockTransferor(
      TransportConf conf,
      BlockTransferStarter transferStarter,
      String[] blockIds,
      BlockFetchingListener listener) {
    this(conf, transferStarter, blockIds, listener, ErrorHandler.NOOP_ERROR_HANDLER);
  }

  @VisibleForTesting
  synchronized void setCurrentListener(RetryingBlockTransferListener listener) {
    this.currentListener = listener;
  }

  /**
   * Initiates the transfer of all blocks provided in the constructor, with possible retries
   * in the event of transient IOExceptions.
   */
  public void start() {
    transferAllOutstanding();
  }

  /**
   * Fires off a request to transfer all blocks that have not been transferred successfully or
   * permanently failed (i.e., by a non-IOException).
   */
  private void transferAllOutstanding() {
    // Start by retrieving our shared state within a synchronized block.
    String[] blockIdsToTransfer;
    int numRetries;
    RetryingBlockTransferListener myListener;
    synchronized (this) {
      blockIdsToTransfer = outstandingBlocksIds.toArray(new String[outstandingBlocksIds.size()]);
      numRetries = retryCount;
      myListener = currentListener;
    }

    // Now initiate the transfer on all outstanding blocks, possibly initiating a retry if that
    // fails.
    try {
      transferStarter.createAndStart(blockIdsToTransfer, myListener);
    } catch (Exception e) {
      if (numRetries > 0) {
        logger.error("Exception while beginning {} of {} outstanding blocks (after {} retries)", e,
          MDC.of(LogKeys.TRANSFER_TYPE$.MODULE$, listener.getTransferType()),
          MDC.of(LogKeys.NUM_BLOCKS$.MODULE$, blockIdsToTransfer.length),
          MDC.of(LogKeys.NUM_RETRY$.MODULE$, numRetries));
      } else {
        logger.error("Exception while beginning {} of {} outstanding blocks", e,
          MDC.of(LogKeys.TRANSFER_TYPE$.MODULE$, listener.getTransferType()),
          MDC.of(LogKeys.NUM_BLOCKS$.MODULE$, blockIdsToTransfer.length));
      }
      if (shouldRetry(e) && initiateRetry(e)) {
        // successfully initiated a retry
        return;
      }

      // retry is not possible, so fail remaining blocks
      for (String bid : blockIdsToTransfer) {
        listener.onBlockTransferFailure(bid, e);
      }
    }
  }

  /**
   * Lightweight method which initiates a retry in a different thread. The retry will involve
   * calling transferAllOutstanding() after a configured wait time.
   * Returns true if the retry was successfully initiated, false otherwise.
   */
  @VisibleForTesting
  synchronized boolean initiateRetry(Throwable e) {
    if (enableSaslRetries && e instanceof SaslTimeoutException) {
      saslRetryCount += 1;
    }
    retryCount += 1;
    currentListener = new RetryingBlockTransferListener();

    logger.info("Retrying {} ({}/{}) for {} outstanding blocks after {} ms",
      MDC.of(LogKeys.TRANSFER_TYPE$.MODULE$, listener.getTransferType()),
      MDC.of(LogKeys.NUM_RETRY$.MODULE$, retryCount),
      MDC.of(LogKeys.MAX_ATTEMPTS$.MODULE$, maxRetries),
      MDC.of(LogKeys.NUM_BLOCKS$.MODULE$, outstandingBlocksIds.size()),
      MDC.of(LogKeys.RETRY_WAIT_TIME$.MODULE$, retryWaitTime));

    try {
      executorService.execute(() -> {
        Uninterruptibles.sleepUninterruptibly(retryWaitTime, TimeUnit.MILLISECONDS);
        transferAllOutstanding();
      });
    } catch (Throwable t) {
      logger.error("Exception while trying to initiate retry", t);
      return false;
    }

    return true;
  }

  /**
   * Returns true if we should retry due a block transfer failure. We will retry if and only if
   * the exception was an IOException or SaslTimeoutException and we haven't retried
   * 'maxRetries' times already.
   */
  private synchronized boolean shouldRetry(Throwable e) {
    boolean isIOException = e instanceof IOException
      || e.getCause() instanceof IOException;
    boolean isSaslTimeout = enableSaslRetries && e instanceof SaslTimeoutException;
    // If this is a non SASL request failure, reduce earlier SASL failures from retryCount
    // since some subsequent SASL attempt was successful
    if (!isSaslTimeout && saslRetryCount > 0) {
      Preconditions.checkState(retryCount >= saslRetryCount,
        "retryCount must be greater than or equal to saslRetryCount");
      retryCount -= saslRetryCount;
      saslRetryCount = 0;
    }
    boolean hasRemainingRetries = retryCount < maxRetries;
    boolean shouldRetry =  (isSaslTimeout || isIOException) &&
        hasRemainingRetries && errorHandler.shouldRetryError(e);
    return shouldRetry;
  }

  @VisibleForTesting
  public int getRetryCount() {
    return retryCount;
  }

  /**
   * Our RetryListener intercepts block transfer responses and forwards them to our parent
   * listener. Note that in the event of a retry, we will immediately replace the 'currentListener'
   * field, indicating that any responses from non-current Listeners should be ignored.
   */
  @VisibleForTesting
  class RetryingBlockTransferListener implements
      BlockFetchingListener, BlockPushingListener {
    private void handleBlockTransferSuccess(String blockId, ManagedBuffer data) {
      // We will only forward this success message to our parent listener if this block request is
      // outstanding and we are still the active listener.
      boolean shouldForwardSuccess = false;
      synchronized (RetryingBlockTransferor.this) {
        if (this == currentListener && outstandingBlocksIds.contains(blockId)) {
          outstandingBlocksIds.remove(blockId);
          shouldForwardSuccess = true;
          // If there were SASL failures earlier, remove them from retryCount, as there was
          // a SASL success (and some other request post bootstrap was also successful).
          if (saslRetryCount > 0) {
            Preconditions.checkState(retryCount >= saslRetryCount,
              "retryCount must be greater than or equal to saslRetryCount");
            retryCount -= saslRetryCount;
            saslRetryCount = 0;
          }
        }
      }

      // Now actually invoke the parent listener, outside of the synchronized block.
      if (shouldForwardSuccess) {
        listener.onBlockTransferSuccess(blockId, data);
      }
    }

    private void handleBlockTransferFailure(String blockId, Throwable exception) {
      // We will only forward this failure to our parent listener if this block request is
      // outstanding, we are still the active listener, AND we cannot retry the transfer.
      boolean shouldForwardFailure = false;
      synchronized (RetryingBlockTransferor.this) {
        if (this == currentListener && outstandingBlocksIds.contains(blockId)) {
          if (shouldRetry(exception)) {
            if (!initiateRetry(exception)) {
              // failed to initiate a retry, so fail this block
              outstandingBlocksIds.remove(blockId);
              shouldForwardFailure = true;
            }
          } else {
            if (errorHandler.shouldLogError(exception)) {
              logger.error("Failed to {} block {}, and will not retry ({} retries)", exception,
                MDC.of(LogKeys.TRANSFER_TYPE$.MODULE$, listener.getTransferType()),
                MDC.of(LogKeys.BLOCK_ID$.MODULE$, blockId),
                MDC.of(LogKeys.NUM_RETRY$.MODULE$,retryCount));
            } else {
              logger.debug(
                String.format("Failed to %s block %s, and will not retry (%s retries)",
                  listener.getTransferType(), blockId, retryCount), exception);
            }
            outstandingBlocksIds.remove(blockId);
            shouldForwardFailure = true;
          }
        }
      }

      // Now actually invoke the parent listener, outside of the synchronized block.
      if (shouldForwardFailure) {
        listener.onBlockTransferFailure(blockId, exception);
      }
    }

    @Override
    public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
      handleBlockTransferSuccess(blockId, data);
    }

    @Override
    public void onBlockFetchFailure(String blockId, Throwable exception) {
      handleBlockTransferFailure(blockId, exception);
    }

    @Override
    public void onBlockPushSuccess(String blockId, ManagedBuffer data) {
      handleBlockTransferSuccess(blockId, data);
    }

    @Override
    public void onBlockPushFailure(String blockId, Throwable exception) {
      handleBlockTransferFailure(blockId, exception);
    }

    // RetryingBlockTransferListener's onBlockTransferSuccess and onBlockTransferFailure
    // shouldn't be invoked. We only invoke these 2 methods on the parent listener.
    @Override
    public void onBlockTransferSuccess(String blockId, ManagedBuffer data) {
      throw new RuntimeException(
        "Invocation on RetryingBlockTransferListener.onBlockTransferSuccess is unexpected.");
    }

    @Override
    public void onBlockTransferFailure(String blockId, Throwable exception) {
      throw new RuntimeException(
        "Invocation on RetryingBlockTransferListener.onBlockTransferFailure is unexpected.");
    }

    @Override
    public String getTransferType() {
      throw new RuntimeException(
        "Invocation on RetryingBlockTransferListener.getTransferType is unexpected.");
    }
  }
}
