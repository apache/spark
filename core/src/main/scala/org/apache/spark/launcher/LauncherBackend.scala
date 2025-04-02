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

package org.apache.spark.launcher

import java.net.{InetAddress, Socket}

import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.launcher.LauncherProtocol._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * A class that can be used to talk to a launcher server. Users should extend this class to
 * provide implementation for the abstract methods.
 *
 * See `LauncherServer` for an explanation of how launcher communication works.
 */
private[spark] abstract class LauncherBackend {

  private var clientThread: Thread = _
  private var connection: BackendConnection = _
  private var lastState: SparkAppHandle.State = _
  @volatile private var _isConnected = false

  protected def conf: SparkConf

  def connect(): Unit = {
    val port = conf.getOption(LauncherProtocol.CONF_LAUNCHER_PORT)
      .orElse(sys.env.get(LauncherProtocol.ENV_LAUNCHER_PORT))
      .map(_.toInt)
    val secret = conf.getOption(LauncherProtocol.CONF_LAUNCHER_SECRET)
      .orElse(sys.env.get(LauncherProtocol.ENV_LAUNCHER_SECRET))
    if (port.isDefined && secret.isDefined) {
      val s = new Socket(InetAddress.getLoopbackAddress(), port.get)
      connection = new BackendConnection(s)
      connection.send(new Hello(secret.get, SPARK_VERSION))
      clientThread = LauncherBackend.threadFactory.newThread(connection)
      clientThread.start()
      _isConnected = true
    }
  }

  def close(): Unit = {
    if (connection != null) {
      try {
        connection.close()
      } finally {
        if (clientThread != null) {
          clientThread.join()
        }
      }
    }
  }

  def setAppId(appId: String): Unit = {
    if (connection != null && isConnected()) {
      connection.send(new SetAppId(appId))
    }
  }

  def setState(state: SparkAppHandle.State): Unit = {
    if (connection != null && isConnected() && lastState != state) {
      connection.send(new SetState(state))
      lastState = state
    }
  }

  /** Return whether the launcher handle is still connected to this backend. */
  def isConnected(): Boolean = _isConnected

  /**
   * Implementations should provide this method, which should try to stop the application
   * as gracefully as possible.
   */
  protected def onStopRequest(): Unit

  /**
   * Callback for when the launcher handle disconnects from this backend.
   */
  protected def onDisconnected() : Unit = { }

  private def fireStopRequest(): Unit = {
    val thread = LauncherBackend.threadFactory.newThread(
      () => Utils.tryLogNonFatalError { onStopRequest() })
    thread.start()
  }

  private class BackendConnection(s: Socket) extends LauncherConnection(s) {

    override protected def handle(m: Message): Unit = m match {
      case _: Stop =>
        fireStopRequest()

      case _ =>
        throw new IllegalArgumentException(s"Unexpected message type: ${m.getClass().getName()}")
    }

    override def close(): Unit = {
      try {
        _isConnected = false
        super.close()
      } finally {
        onDisconnected()
      }
    }

  }

}

private object LauncherBackend {

  val threadFactory = ThreadUtils.namedThreadFactory("LauncherBackend")

}
