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

package org.apache.spark.launcher;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Redirects lines read from a given input stream to a j.u.l.Logger (at INFO level).
 */
class OutputRedirector {

  private final BufferedReader reader;
  private final Logger sink;
  private final Thread thread;
  private final ChildProcAppHandle callback;

  private volatile boolean active;
  private volatile Throwable error;

  OutputRedirector(InputStream in, String loggerName, ThreadFactory tf) {
    this(in, loggerName, tf, null);
  }

  OutputRedirector(
      InputStream in,
      String loggerName,
      ThreadFactory tf,
      ChildProcAppHandle callback) {
    this.active = true;
    this.reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    this.thread = tf.newThread(this::redirect);
    this.sink = Logger.getLogger(loggerName);
    this.callback = callback;
    thread.start();
  }

  private void redirect() {
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        if (active) {
          sink.info(line.replaceFirst("\\s*$", ""));
          if ((containsIgnoreCase(line, "Error") || containsIgnoreCase(line, "Exception")) &&
              !line.contains("at ")) {
            error = new RuntimeException(line);
          }
        }
      }
    } catch (IOException e) {
      sink.log(Level.FINE, "Error reading child process output.", e);
    } finally {
      if (callback != null) {
        callback.monitorChild();
      }
    }
  }

  /**
   * This method just stops the output of the process from showing up in the local logs.
   * The child's output will still be read (and, thus, the redirect thread will still be
   * alive) to avoid the child process hanging because of lack of output buffer.
   */
  void stop() {
    active = false;
  }

  boolean isAlive() {
    return thread.isAlive();
  }

  Throwable getError() {
    return error;
  }

  /**
   * Copied from Apache Commons Lang {@code StringUtils#containsIgnoreCase(String, String)}
   */
  private static boolean containsIgnoreCase(String str, String searchStr) {
    if (str == null || searchStr == null) {
      return false;
    }
    int len = searchStr.length();
    int max = str.length() - len;
    for (int i = 0; i <= max; i++) {
      if (str.regionMatches(true, i, searchStr, 0, len)) {
        return true;
      }
    }
    return false;
  }
}
