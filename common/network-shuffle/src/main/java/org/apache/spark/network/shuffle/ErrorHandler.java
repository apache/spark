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

import java.net.ConnectException;

/**
 * Plugs into {@link RetryingBlockFetcher} to further control when an exception should be retried
 * and logged.
 * Note: {@link RetryingBlockFetcher} will delegate the exception to this handler only when
 * - remaining retries < max retries
 * - exception is an IOException
 */

public interface ErrorHandler {
  boolean shouldRetryError(Throwable t);

  default boolean shouldLogError(Throwable t) {
    return true;
  }

  /**
   * A no-op error handler instance.
   */
  ErrorHandler NOOP_ERROR_HANDLER = t -> true;

  /**
   * The error handler for pushing shuffle blocks to remote shuffle services.
   */
  class BlockPushErrorHandler implements ErrorHandler {

    @Override
    public boolean shouldRetryError(Throwable t) {
      // If it is a connection time out or a connection closed exception, no need to retry.
      if (t.getCause() != null && t.getCause() instanceof ConnectException) {
        return false;
      }
      // If the block is too late, there is no need to retry it
      return (t.getMessage() == null || !t.getMessage()
          .contains(BlockPushException.TOO_LATE_MESSAGE_SUFFIX)) && (t.getCause() == null
          || t.getCause().getMessage() == null || !t.getCause().getMessage()
          .contains(BlockPushException.TOO_LATE_MESSAGE_SUFFIX));
    }

    @Override
    public boolean shouldLogError(Throwable t) {
      return (t.getMessage() == null || (
          !t.getMessage().contains(BlockPushException.COULD_NOT_FIND_OPPORTUNITY_MSG_PREFIX)
              && !t.getMessage().contains(BlockPushException.TOO_LATE_MESSAGE_SUFFIX))) && (
          t.getCause() == null || t.getCause().getMessage() == null || (!t.getCause()
              .getMessage()
              .contains(BlockPushException.COULD_NOT_FIND_OPPORTUNITY_MSG_PREFIX) && !t.getCause()
              .getMessage()
              .contains(BlockPushException.TOO_LATE_MESSAGE_SUFFIX)));
    }
  }
}
