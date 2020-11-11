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

package org.apache.hadoop.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

/**
 * Copied from
 * hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/net/ServerSocketUtil.java
 * for Hadoop-3.x testing
 */
public class ServerSocketUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ServerSocketUtil.class);
  private static Random rand = new Random();

  /**
   * Port scan & allocate is how most other apps find ports
   *
   * @param port given port
   * @param retries number of retries
   * @return
   * @throws IOException
   */
  public static int getPort(int port, int retries) throws IOException {
    int tryPort = port;
    int tries = 0;
    while (true) {
      if (tries > 0 || tryPort == 0) {
        tryPort = port + rand.nextInt(65535 - port);
      }
      if (tryPort == 0) {
        continue;
      }
      try (ServerSocket s = new ServerSocket(tryPort)) {
        LOG.info("Using port " + tryPort);
        return tryPort;
      } catch (IOException e) {
        tries++;
        if (tries >= retries) {
          LOG.info("Port is already in use; giving up");
          throw e;
        } else {
          LOG.info("Port is already in use; trying again");
        }
      }
    }
  }

  /**
   * Check whether port is available or not.
   *
   * @param port given port
   * @return
   */
  private static boolean isPortAvailable(int port) {
    try (ServerSocket s = new ServerSocket(port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Wait till the port available.
   *
   * @param port given port
   * @param retries number of retries for given port
   * @return
   * @throws InterruptedException
   * @throws IOException
   */
  public static int waitForPort(int port, int retries)
          throws InterruptedException, IOException {
    int tries = 0;
    while (true) {
      if (isPortAvailable(port)) {
        return port;
      } else {
        tries++;
        if (tries >= retries) {
          throw new IOException(
                  "Port is already in use; giving up after " + tries + " times.");
        }
        Thread.sleep(1000);
      }
    }
  }

  /**
   * Find the specified number of unique ports available.
   * The ports are all closed afterwards,
   * so other network services started may grab those same ports.
   *
   * @param numPorts number of required port numbers
   * @return array of available port numbers
   * @throws IOException
   */
  public static int[] getPorts(int numPorts) throws IOException {
    ServerSocket[] sockets = new ServerSocket[numPorts];
    int[] ports = new int[numPorts];
    for (int i = 0; i < numPorts; i++) {
      ServerSocket sock = new ServerSocket(0);
      sockets[i] = sock;
      ports[i] = sock.getLocalPort();
    }
    for (ServerSocket sock : sockets) {
      sock.close();
    }
    return ports;
  }
}
