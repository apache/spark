/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.clients;

import org.apache.spark.remoteshuffle.common.AppShufflePartitionId;
import org.apache.spark.remoteshuffle.common.DownloadServerVerboseInfo;
import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.exceptions.RssNetworkException;
import org.apache.spark.remoteshuffle.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class RetriableSocketReadClient implements SingleServerReadClient {
  private static final Logger logger = LoggerFactory.getLogger(RetriableSocketReadClient.class);

  private final ClientRetryOptions retryOptions;
  private final Supplier<ServerIdAwareSocketReadClient> retryClientCreator;

  private ServerIdAwareSocketReadClient delegate;

  public RetriableSocketReadClient(ServerDetail serverDetail,
                                   int timeoutMillis,
                                   ClientRetryOptions retryOptions,
                                   String user,
                                   AppShufflePartitionId appShufflePartitionId,
                                   ReadClientDataOptions dataOptions) {
    this.retryOptions = retryOptions;

    delegate = new ServerIdAwareSocketReadClient(serverDetail,
        timeoutMillis,
        user,
        appShufflePartitionId,
        dataOptions.getFetchTaskAttemptIds(),
        dataOptions.getDataAvailablePollInterval(),
        dataOptions.getDataAvailableWaitTime());

    this.retryClientCreator = () -> {
      return new ServerIdAwareSocketReadClient(serverDetail,
          timeoutMillis,
          user,
          appShufflePartitionId,
          dataOptions.getFetchTaskAttemptIds(),
          dataOptions.getDataAvailablePollInterval(),
          dataOptions.getDataAvailableWaitTime());
    };
  }

  @Override
  public DownloadServerVerboseInfo connect() {
    long startTime = System.currentTimeMillis();
    RssNetworkException lastException;
    do {
      try {
        return delegate.connect();
      } catch (RssNetworkException ex) {
        lastException = ex;
        logger.warn(String.format("Failed to connect to server: %s", delegate), ex);
        closeDelegate();
        long retryRemainingMillis =
            startTime + retryOptions.getRetryMaxMillis() - System.currentTimeMillis();
        if (retryRemainingMillis <= 0) {
          break;
        } else {
          delegate = retryClientCreator.get();
          long waitMillis = Math.min(retryOptions.getRetryIntervalMillis(), retryRemainingMillis);
          logger.info(String.format(
              "Waiting %s milliseconds (total retry milliseconds: %s, remaining milliseconds: %s) and retry to connect to server: %s",
              waitMillis, retryOptions.getRetryMaxMillis(), retryRemainingMillis, delegate));
          ThreadUtils.sleep(waitMillis);
        }
        continue;
      }
    } while (System.currentTimeMillis() <= startTime + retryOptions.getRetryMaxMillis());
    throw lastException;
  }

  @Override
  public TaskDataBlock readDataBlock() {
    return delegate.readDataBlock();
  }

  @Override
  public long getShuffleReadBytes() {
    return delegate.getShuffleReadBytes();
  }

  @Override
  public void close() {
    closeDelegate();
  }

  @Override
  public String toString() {
    return "RetriableSocketReadClient{" +
        "retryOptions=" + retryOptions +
        ", delegate=" + delegate +
        '}';
  }

  private void closeDelegate() {
    try {
      delegate.close();
    } catch (Throwable ex) {
      logger.warn("Failed to close underlying client: " + delegate, ex);
    }
  }
}
