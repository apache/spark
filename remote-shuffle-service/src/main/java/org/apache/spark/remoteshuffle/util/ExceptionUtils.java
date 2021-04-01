/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
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

package org.apache.spark.remoteshuffle.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionUtils {
  private static final Logger logger = LoggerFactory.getLogger(ExceptionUtils.class);

  public static String getSimpleMessage(Throwable ex) {
    if (ex == null) {
      return "";
    }
    return String.format("%s (%s)", ex.getClass().getSimpleName(), ex.getMessage());
  }

  public static boolean isTimeoutException(Throwable ex) {
    if (ex == null) {
      return false;
    }
    String msg = ex.getMessage();
    if (msg == null) {
      return false;
    }
    msg = msg.toLowerCase();
    return msg.contains("timed out") ||
        msg.contains("time out") ||
        msg.contains("timedout") ||
        msg.contains("timeout");
  }

  // Throw out an exception without needing to add throws declaration on method signature
  // see https://stackoverflow.com/questions/4519557/is-there-a-way-to-throw-an-exception-without-adding-the-throws-declaration/4519576
  public static void throwException(Throwable exception) {
    ExceptionUtils.<RuntimeException>throwException1(exception);
  }

  public static void closeWithoutException(AutoCloseable closeable) {
    if (closeable == null) {
      return;
    }

    try {
      closeable.close();
    } catch (Throwable ex) {
      logger.warn("Failed to close " + closeable, ex);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void throwException1(Throwable exception) throws T {
    throw (T) exception;
  }
}
