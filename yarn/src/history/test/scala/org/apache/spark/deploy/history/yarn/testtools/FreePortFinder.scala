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

package org.apache.spark.deploy.history.yarn.testtools

import java.net.{InetAddress, ServerSocket}

import scala.collection.mutable.ListBuffer

/**
 * Trait to find free ports on localhost
 */
trait FreePortFinder {

  /**
   * Find a free port by listening on port 0
   * @return
   */
  def findPort(): Int = {
    tryToListen(0)._2
  }

  /**
   * Simple function to see if a port is free; if it is return the address and the port allocated.
   *
   * This function can be passed to `Util.startServiceOnPort`
   * @param port port to try. If 0, the OS chooses the port
   * @return an (address, port) tuple
   */
  def tryToListen(port: Int): (InetAddress, Int) = {
    val socket = new ServerSocket(port)
    val address = socket.getInetAddress
    val localPort = socket.getLocalPort
    socket.close()
    (address, localPort)
  }

  /**
   * Return the value of the local host address -defaults to 127.0.0.1
   * @return the address to use for local/loopback addresses.
   */
  def localIPv4Address(): String = {
    "127.0.0.1"
  }

  /**
   * Get a local address as an address:port string and an integer port value
   * @return a free port to bind to
   */
  def findIPv4AddressAsPortPair(): (String, Int) = {
    val port = findPort()
    (localhostAndPort(port), port)
  }

  /**
   * Given a port, return a localhost:port pair
   * @param port port
   * @return the name for the localhost for test runs.
   */
  def localhostAndPort(port: Int): String = {
    localIPv4Address() + ":" + port
  }

  /**
   * Find the specified number of unique ports. This is done in parallel, so the
   * ports are guaranteed to be different. The ports are all closed afterwards,
   * so other network services started may grab those same ports.
   *
   * @param count number of ports to find.
   * @return a list of ports to use.
   */
  def findUniquePorts(count: Integer): Seq[Integer] = {
    val sockets = new ListBuffer[ServerSocket]()
    val ports = new ListBuffer[Integer]()
    for (i <- 1 to count) {
      val socket = new ServerSocket(0)
      sockets += socket
      ports += socket.getLocalPort
    }
    sockets.foreach(_.close())
    // Uniqueness: foreach port, there only exists one port in the list which has that value
    require(ports.map( p => ports.count(_ == p) == 1).count(_ == true) == count,
      s"Duplicate port allocation in " + ports.mkString(","))

    ports
  }
}
