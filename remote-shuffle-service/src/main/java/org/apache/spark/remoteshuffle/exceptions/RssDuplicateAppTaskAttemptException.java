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

package org.apache.spark.remoteshuffle.exceptions;

/***
 * This exception is thrown when a same Spark application task attempt connects to shuffle server multiple times.
 */
public class RssDuplicateAppTaskAttemptException extends RssException {
  public RssDuplicateAppTaskAttemptException() {
  }

  public RssDuplicateAppTaskAttemptException(String message) {
    super(message);
  }

  public RssDuplicateAppTaskAttemptException(String message, Throwable cause) {
    super(message, cause);
  }

  public RssDuplicateAppTaskAttemptException(Throwable cause) {
    super(cause);
  }

  public RssDuplicateAppTaskAttemptException(String message, Throwable cause,
                                             boolean enableSuppression,
                                             boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
