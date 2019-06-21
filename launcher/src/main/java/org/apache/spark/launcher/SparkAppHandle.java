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

import java.util.Optional;

/**
 * A handle to a running Spark application.
 * <p>
 * Provides runtime information about the underlying Spark application, and actions to control it.
 *
 * @since 1.6.0
 */
public interface SparkAppHandle {

  /**
   * Represents the application's state. A state can be "final", in which case it will not change
   * after it's reached, and means the application is not running anymore.
   *
   * @since 1.6.0
   */
  enum State {
    /** The application has not reported back yet. */
    UNKNOWN(false),
    /** The application has connected to the handle. */
    CONNECTED(false),
    /** The application has been submitted to the cluster. */
    SUBMITTED(false),
    /** The application is running. */
    RUNNING(false),
    /** The application finished with a successful status. */
    FINISHED(true),
    /** The application finished with a failed status. */
    FAILED(true),
    /** The application was killed. */
    KILLED(true),
    /** The Spark Submit JVM exited with a unknown status. */
    LOST(true);

    private final boolean isFinal;

    State(boolean isFinal) {
      this.isFinal = isFinal;
    }

    /**
     * Whether this state is a final state, meaning the application is not running anymore
     * once it's reached.
     */
    public boolean isFinal() {
      return isFinal;
    }
  }

  /**
   * Adds a listener to be notified of changes to the handle's information. Listeners will be called
   * from the thread processing updates from the application, so they should avoid blocking or
   * long-running operations.
   *
   * @param l Listener to add.
   */
  void addListener(Listener l);

  /** Returns the current application state. */
  State getState();

  /** Returns the application ID, or <code>null</code> if not yet known. */
  String getAppId();

  /**
   * Asks the application to stop. This is best-effort, since the application may fail to receive
   * or act on the command. Callers should watch for a state transition that indicates the
   * application has really stopped.
   */
  void stop();

  /**
   * Tries to kill the underlying application. Implies {@link #disconnect()}. This will not send
   * a {@link #stop()} message to the application, so it's recommended that users first try to
   * stop the application cleanly and only resort to this method if that fails.
   */
  void kill();

  /**
   * Disconnects the handle from the application, without stopping it. After this method is called,
   * the handle will not be able to communicate with the application anymore.
   */
  void disconnect();

  /**
   * If the application failed due to an error, return the underlying error. If the app
   * succeeded, this method returns an empty {@link Optional}.
   */
  Optional<Throwable> getError();

  /**
   * Listener for updates to a handle's state. The callbacks do not receive information about
   * what exactly has changed, just that an update has occurred.
   *
   * @since 1.6.0
   */
  public interface Listener {

    /**
     * Callback for changes in the handle's state.
     *
     * @param handle The updated handle.
     * @see SparkAppHandle#getState()
     */
    void stateChanged(SparkAppHandle handle);

    /**
     * Callback for changes in any information that is not the handle's state.
     *
     * @param handle The updated handle.
     */
    void infoChanged(SparkAppHandle handle);

  }

}
