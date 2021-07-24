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

import java.io.FileNotFoundException;
import java.net.ConnectException;

import com.google.common.base.Throwables;

import org.apache.spark.annotation.Evolving;

/**
 * Plugs into {@link RetryingBlockFetcher} to further control when an exception should be retried
 * and logged.
 * Note: {@link RetryingBlockFetcher} will delegate the exception to this handler only when
 * - remaining retries < max retries
 * - exception is an IOException
 *
 * @since 3.1.0
 */
@Evolving
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
   *
   * @since 3.1.0
   */
  class BlockPushErrorHandler implements ErrorHandler {
    /**
     * String constant used for generating exception messages indicating a block to be merged
     * arrives too late on the server side, and also for later checking such exceptions on the
     * client side. When we get a block push failure because of the block arrives too late, we
     * will not retry pushing the block nor log the exception on the client side.
     */
    public static final String TOO_LATE_MESSAGE_SUFFIX =
      "received after merged shuffle is finalized";

    /**
     * String constant used for generating exception messages indicating the server couldn't
     * append a block after all available attempts due to collision with other blocks belonging
     * to the same shuffle partition, and also for later checking such exceptions on the client
     * side. When we get a block push failure because of the block couldn't be written due to
     * this reason, we will not log the exception on the client side.
     */
    public static final String BLOCK_APPEND_COLLISION_DETECTED_MSG_PREFIX =
      "Couldn't find an opportunity to write block";

    /**
     * String constant used for generating exception messages indicating the server encountered
     * IOExceptions multiple times, greater than the configured threshold, while trying to merged
     * shuffle blocks of the same shuffle partition. When the client receives this this response,
     * it will stop pushing any more blocks for the same shuffle partition.
     */
    public static final String IOEXCEPTIONS_EXCEEDED_THRESHOLD_PREFIX =
      "IOExceptions exceeded the threshold";

    @Override
    public boolean shouldRetryError(Throwable t) {
      // If it is a connection time-out or a connection closed exception, no need to retry.
      // If it is a FileNotFoundException originating from the client while pushing the shuffle
      // blocks to the server, even then there is no need to retry. We will still log this exception
      // once which helps with debugging.
      if (t.getCause() != null && (t.getCause() instanceof ConnectException ||
          t.getCause() instanceof FileNotFoundException)) {
        return false;
      }
      // If the block is too late, there is no need to retry it
      return !Throwables.getStackTraceAsString(t).contains(TOO_LATE_MESSAGE_SUFFIX);
    }

    @Override
    public boolean shouldLogError(Throwable t) {
      String errorStackTrace = Throwables.getStackTraceAsString(t);
      return !errorStackTrace.contains(BLOCK_APPEND_COLLISION_DETECTED_MSG_PREFIX) &&
        !errorStackTrace.contains(TOO_LATE_MESSAGE_SUFFIX);
    }
  }
}
