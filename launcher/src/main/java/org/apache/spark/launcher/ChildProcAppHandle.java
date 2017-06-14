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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handle implementation for monitoring apps started as a child process.
 */
class ChildProcAppHandle extends AbstractSparkAppHandle {

  private static final Logger LOG = Logger.getLogger(ChildProcAppHandle.class.getName());

  protected Process childProc;

  ChildProcAppHandle(String secret, LauncherServer server) {
    super(server, secret);
  }

  @Override
  public synchronized void kill() {
    if (!disposed) {
      disconnect();
    }
    if (childProc != null) {
      try {
        childProc.exitValue();
      } catch (IllegalThreadStateException e) {
        childProc.destroyForcibly();
      } finally {
        childProc = null;
      }
    }
  }

  void setChildProc(Process childProc, String loggerName) {
    this.childProc = childProc;
    this.redirector = new OutputRedirector(childProc.getInputStream(), loggerName,
      SparkLauncher.REDIRECTOR_FACTORY);
  }

  void waitFor() throws InterruptedException {
    this.childProc.waitFor();
  }
}
