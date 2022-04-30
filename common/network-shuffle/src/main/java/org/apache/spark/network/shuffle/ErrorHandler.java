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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.network.server.BlockPushNonFatalFailure;

/**
 * Plugs into {@link RetryingBlockTransferor} to further control when an exception should be retried
 * and logged.
 * Note: {@link RetryingBlockTransferor} will delegate the exception to this handler only when
 * - remaining retries < max retries
 * - exception is an IOException
 *
 * @since 3.1.0
 */
@Evolving
public abstract class ErrorHandler {

  public abstract boolean shouldRetryError(Throwable t);

  public abstract boolean shouldLogError(Throwable t);

  /**
   * String constant used for generating exception messages indicating the server encountered
   * IOExceptions multiple times, greater than the configured threshold, while trying to merged
   * shuffle blocks of the same shuffle partition. When the client receives this this response,
   * it will stop pushing any more blocks for the same shuffle partition.
   */
  public static final String IOEXCEPTIONS_EXCEEDED_THRESHOLD_PREFIX =
      "IOExceptions exceeded the threshold";

  /**
   * String constant used for generating exception messages indicating the server rejecting a
   * shuffle finalize request since shuffle blocks of a higher shuffleMergeId for a shuffle is
   * already being pushed. This typically happens in the case of indeterminate stage retries
   * where if a stage attempt fails then the entirety of the shuffle output needs to be rolled
   * back. For more details refer SPARK-23243, SPARK-25341 and SPARK-32923.
   */
  public static final String STALE_SHUFFLE_FINALIZE_SUFFIX =
    "stale shuffle finalize request as shuffle blocks of a higher shuffleMergeId for the"
        + " shuffle is already being pushed";

  public static final String STALE_SHUFFLE_BLOCK_FETCH =
    "stale shuffle block fetch request as shuffle blocks of a higher shuffleMergeId for the"
        + " shuffle is available";

  /**
   * A no-op error handler instance.
   */
  public static final ErrorHandler NOOP_ERROR_HANDLER = new ErrorHandler() {
    @Override
    public boolean shouldRetryError(Throwable t) {
      return true;
    }

    @Override
    public boolean shouldLogError(Throwable t) {
      return true;
    }
  };

  private static volatile ErrorHandler instance = null;

  /**
   * The error handler for pushing shuffle blocks to remote shuffle services.
   *
   * @since 3.1.0
   */
  public static ErrorHandler blockPushErrorHandler() {
    if(instance != null) {
      synchronized (ErrorHandler.class) {
        if(instance == null) {
          instance = new ErrorHandler() {

            @Override
            public boolean shouldRetryError(Throwable t) {
              if(t instanceof BlockPushNonFatalFailure) {
                return false;
              }
              // If it is a connection time-out or a connection closed exception, no need to retry.
              // If it is a FileNotFoundException originating from the client while pushing the
              // shuffle blocks to the server, even then there is no need to retry. We will still
              // log this exception once which helps with debugging.
              if (t.getCause() != null && (t.getCause() instanceof ConnectException ||
                  t.getCause() instanceof FileNotFoundException)) {
                return false;
              }
              return true;
            }

            @Override
            public boolean shouldLogError(Throwable t) {
              return !(t instanceof BlockPushNonFatalFailure);
            }
          };
        }
      }
    }
    return instance;
  }
}
