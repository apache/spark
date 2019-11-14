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

import java.io.InputStream;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handle implementation for monitoring apps started as a child process.
 */
class ChildProcAppHandle extends AbstractAppHandle {

  private static final Logger LOG = Logger.getLogger(ChildProcAppHandle.class.getName());

  private volatile Process childProc;
  private volatile OutputRedirector redirector;

  ChildProcAppHandle(LauncherServer server) {
    super(server);
  }

  @Override
  public synchronized void disconnect() {
    try {
      super.disconnect();
    } finally {
      if (redirector != null) {
        redirector.stop();
      }
    }
  }

  /**
   * Parses the logs of {@code spark-submit} and returns the last exception thrown.
   * <p>
   * Since {@link SparkLauncher} runs {@code spark-submit} in a sub-process, it's difficult to
   * accurately retrieve the full {@link Throwable} from the {@code spark-submit} process.
   * This method parses the logs of the sub-process and provides a best-effort attempt at
   * returning the last exception thrown by the {@code spark-submit} process. Only the exception
   * message is parsed, the associated stacktrace is meaningless.
   *
   * @return an {@link Optional} containing a {@link RuntimeException} with the parsed
   * exception, otherwise returns a {@link Optional#EMPTY}
   */
  @Override
  public Optional<Throwable> getError() {
    return redirector != null ? Optional.ofNullable(redirector.getError()) : Optional.empty();
  }

  @Override
  public synchronized void kill() {
    if (!isDisposed()) {
      setState(State.KILLED);
      disconnect();
      if (childProc != null) {
        if (childProc.isAlive()) {
          childProc.destroyForcibly();
        }
        childProc = null;
      }
    }
  }

  void setChildProc(Process childProc, String loggerName, InputStream logStream) {
    this.childProc = childProc;
    if (logStream != null) {
      this.redirector = new OutputRedirector(logStream, loggerName,
        SparkLauncher.REDIRECTOR_FACTORY, this);
    } else {
      // If there is no log redirection, spawn a thread that will wait for the child process
      // to finish.
      SparkLauncher.REDIRECTOR_FACTORY.newThread(this::monitorChild).start();
    }
  }

  /**
   * Wait for the child process to exit and update the handle's state if necessary, according to
   * the exit code.
   */
  void monitorChild() {
    Process proc = childProc;
    if (proc == null) {
      // Process may have already been disposed of, e.g. by calling kill().
      return;
    }

    while (proc.isAlive()) {
      try {
        proc.waitFor();
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Exception waiting for child process to exit.", e);
      }
    }

    synchronized (this) {
      if (isDisposed()) {
        return;
      }

      int ec;
      try {
        ec = proc.exitValue();
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Exception getting child process exit code, assuming failure.", e);
        ec = 1;
      }

      if (ec != 0) {
        State currState = getState();
        // Override state with failure if the current state is not final, or is success.
        if (!currState.isFinal() || currState == State.FINISHED) {
          setState(State.FAILED, true);
        }
      }

      dispose();
    }
  }

}
