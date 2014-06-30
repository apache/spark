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

package org.apache.spark.network

import java.net.InetSocketAddress

import org.apache.spark.{Logging, SparkException}
import org.eclipse.jetty.server.Server

private[spark] object PortManager extends Logging
{

  /**
   * Start service on given port, or attempt to fall back to the n+1 port for a certain number of
   * retries
   *
   * @param startPort
   * @param maxRetries Maximum number of retries to attempt.  A value of e.g. 3 will cause 4
   *                   total attempts, on ports n, n+1, n+2, and n+3
   * @param startService Function to start service on a given port.  Expected to throw a java.net
   *                     .BindException if the port is already in use
   * @tparam T
   * @throws SparkException When unable to start service in the given number of attempts
   * @return
   */
  def startWithIncrements[T](startPort: Int, maxRetries: Int, startService: Int => (T, Int)):
      (T, Int) = {
    for( offset <- 0 to maxRetries) {
      val tryPort = startPort + offset
      try {
        return startService(tryPort)
      } catch {
        case e: java.net.BindException => {
          if (!e.getMessage.contains("Address already in use") ||
            offset == (maxRetries-1)) {
            throw e
          }
          logInfo("Could not bind on port: " + tryPort + ". Attempting port " + (tryPort + 1))
        }
        case e: Exception => throw e
      }
    }
    throw new SparkException(s"Couldn't start service on port $startPort")
  }
}
