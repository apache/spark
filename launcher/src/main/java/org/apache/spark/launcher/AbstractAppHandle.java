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

abstract class AbstractAppHandle implements SparkAppHandle {

  private static final Logger LOG = Logger.getLogger(ChildProcAppHandle.class.getName());

  private final LauncherServer server;

  private LauncherConnection connection;
  private List<Listener> listeners;
  private State state;
  private String appId;
  private volatile boolean disposed;

  protected AbstractAppHandle(LauncherServer server) {
    this.server = server;
    this.state = State.UNKNOWN;
  }

  @Override
  public synchronized void addListener(Listener l) {
    if (listeners == null) {
      listeners = new ArrayList<>();
    }
    listeners.add(l);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public String getAppId() {
    return appId;
  }

  @Override
  public void stop() {
    CommandBuilderUtils.checkState(connection != null, "Application is still not connected.");
    try {
      connection.send(new LauncherProtocol.Stop());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public synchronized void disconnect() {
    if (!isDisposed()) {
      if (connection != null) {
        try {
          connection.close();
        } catch (IOException ioe) {
          // no-op.
        }
      }
      dispose();
    }
  }

  void setConnection(LauncherConnection connection) {
    this.connection = connection;
  }

  LauncherConnection getConnection() {
    return connection;
  }

  boolean isDisposed() {
    return disposed;
  }

  /**
   * Mark the handle as disposed, and set it as LOST in case the current state is not final.
   */
  synchronized void dispose() {
    if (!isDisposed()) {
      // Unregister first to make sure that the connection with the app has been really
      // terminated.
      server.unregister(this);
      if (!getState().isFinal()) {
        setState(State.LOST);
      }
      this.disposed = true;
    }
  }

  void setState(State s) {
    setState(s, false);
  }

  synchronized void setState(State s, boolean force) {
    if (force || !state.isFinal()) {
      state = s;
      fireEvent(false);
    } else {
      LOG.log(Level.WARNING, "Backend requested transition from final state {0} to {1}.",
        new Object[] { state, s });
    }
  }

  synchronized void setAppId(String appId) {
    this.appId = appId;
    fireEvent(true);
  }

  private void fireEvent(boolean isInfoChanged) {
    if (listeners != null) {
      for (Listener l : listeners) {
        if (isInfoChanged) {
          l.infoChanged(this);
        } else {
          l.stateChanged(this);
        }
      }
    }
  }

}
