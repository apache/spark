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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.launcher.LauncherProtocol._
import org.apache.spark.util.ThreadUtils

/**
 * A trait that can be mixed-in to provide support for talking to a launcher server.
 *
 * See `LauncherBackend` for an explanation of how launcher communication works.
 */
private[spark] trait LauncherBackend {

  private var clientThread: Thread = _
  private var connection: BackendConnection = _

  def connectToLauncher(): Unit = {
    val port = sys.env.get(LauncherProtocol.ENV_LAUNCHER_PORT).map(_.toInt)
    val secret = sys.env.get(LauncherProtocol.ENV_LAUNCHER_SECRET)
    if (port != None && secret != None) {
      val s = new Socket(InetAddress.getLoopbackAddress(), port.get)
      connection = new BackendConnection(s)
      connection.send(new Hello(secret.get, SPARK_VERSION))
      clientThread = LauncherBackend.threadFactory.newThread(connection)
      clientThread.start()
    }
  }

  def closeLauncherConnection(): Unit = {
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

  def updateLauncherAppId(appId: String): Unit = {
    if (connection != null) {
      connection.send(new SetAppId(appId))
    }
  }

  def updateLauncherState(state: SparkAppHandle.State): Unit = {
    if (connection != null) {
      connection.send(new SetState(state))
    }
  }

  /**
   * Implementations should provide this method, which should try to stop the application
   * as gracefully as possible.
   */
  protected def launcherRequestedStop(): Unit

  private class BackendConnection(s: Socket) extends LauncherConnection(s) {

    override protected def handle(m: Message): Unit = m match {
      case _: Stop =>
        launcherRequestedStop()

      case _ =>
        throw new IllegalArgumentException(s"Unexpected message type: ${m.getClass().getName()}")
    }

  }

}

private object LauncherBackend {

  val threadFactory = ThreadUtils.namedThreadFactory("LauncherBackend")

}
