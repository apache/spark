/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

/** A thread that has called {@link Thread#setDaemon(boolean) } with true.*/
public class Daemon extends Thread {

  {
    setDaemon(true);                              // always a daemon
  }

  Runnable runnable = null;
  /** Construct a daemon thread. */
  public Daemon() {
    super();
  }

  /** Construct a daemon thread. */
  public Daemon(Runnable runnable) {
    super(runnable);
    this.runnable = runnable;
    this.setName(((Object)runnable).toString());
  }

  /** Construct a daemon thread to be part of a specified thread group. */
  public Daemon(ThreadGroup group, Runnable runnable) {
    super(group, runnable);
    this.runnable = runnable;
    this.setName(((Object)runnable).toString());
  }

  public Runnable getRunnable() {
    return runnable;
  }
}
