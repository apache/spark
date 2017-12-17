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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handle implementation for monitoring apps started as a child process.
 */
class ChildProcAppHandle implements SparkAppHandle {

  private static final Logger LOG = Logger.getLogger(ChildProcAppHandle.class.getName());

  private final String secret;
  private final LauncherServer server;

  private volatile Process childProc;
  private boolean disposed;
  private LauncherConnection connection;
  private List<Listener> listeners;
  private State state;
  private String appId;
  private OutputRedirector redirector;

  ChildProcAppHandle(String secret, LauncherServer server) {
    this.secret = secret;
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
    if (!disposed) {
      disposed = true;
      if (connection != null) {
        try {
          connection.close();
        } catch (IOException ioe) {
          // no-op.
        }
      }
      server.unregister(this);
      if (redirector != null) {
        redirector.stop();
      }
    }
  }

  @Override
  public synchronized void kill() {
    disconnect();
    if (childProc != null) {
      if (childProc.isAlive()) {
        childProc.destroyForcibly();
      }
      childProc = null;
    }
    setState(State.KILLED);
  }

  String getSecret() {
    return secret;
  }

  void setChildProc(Process childProc, String loggerName, InputStream logStream) {
    this.childProc = childProc;
    if (logStream != null) {
      this.redirector = new OutputRedirector(logStream, loggerName,
        SparkLauncher.REDIRECTOR_FACTORY, this);
    } else {
      // If there is no log redirection, spawn a thread that will wait for the child process
      // to finish.
      Thread waiter = SparkLauncher.REDIRECTOR_FACTORY.newThread(this::monitorChild);
      waiter.setDaemon(true);
      waiter.start();
    }
  }

  void setConnection(LauncherConnection connection) {
    this.connection = connection;
  }

  LauncherServer getServer() {
    return server;
  }

  LauncherConnection getConnection() {
    return connection;
  }

  synchronized void setState(State s) {
    if (!state.isFinal()) {
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

  /**
   * Wait for the child process to exit and update the handle's state if necessary, accoding to
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
      if (disposed) {
        return;
      }

      disconnect();

      int ec;
      try {
        ec = proc.exitValue();
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Exception getting child process exit code, assuming failure.", e);
        ec = 1;
      }

      State newState = null;
      if (ec != 0) {
        // Override state with failure if the current state is not final, or is success.
        if (!state.isFinal() || state == State.FINISHED) {
          newState = State.FAILED;
        }
      } else if (!state.isFinal()) {
        newState = State.LOST;
      }

      if (newState != null) {
        state = newState;
        fireEvent(false);
      }
    }
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
