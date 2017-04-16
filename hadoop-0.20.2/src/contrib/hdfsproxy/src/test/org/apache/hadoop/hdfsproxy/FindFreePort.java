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

package org.apache.hadoop.hdfsproxy;

import java.net.ServerSocket;
import java.io.IOException;
import java.net.BindException;
import java.util.Random;

public class FindFreePort {
  private static final int MIN_AVAILABLE_PORT = 10000;
  private static final int MAX_AVAILABLE_PORT = 65535;
  private static Random random = new Random();
  /**
   * 
   * @param num <= 0, find a single free port
   * @return free port next to port (>port)
   * @throws IOException
   */
  public static int findFreePort(int port) throws IOException {
    ServerSocket server;
    if (port < 0) {
      server =  new ServerSocket(0);      
    } else {
      int freePort = port+1;
      while (true) {
        try {
          server =  new ServerSocket(freePort);
          break;
        } catch (IOException e) {
          if (e instanceof BindException) {
            if (freePort >= MAX_AVAILABLE_PORT || 
                freePort < MIN_AVAILABLE_PORT) {
              throw e;
            }
          } else {
            throw e;
          }
          freePort += 1;
        }
      }
    }
    int fport = server.getLocalPort();
    server.close();
    return fport;    
  }
 /**
  * 
  * @return
  * @throws IOException
  */
  public static int findFreePortRandom() throws IOException {
    return findFreePort(MIN_AVAILABLE_PORT + random.nextInt(MAX_AVAILABLE_PORT - MIN_AVAILABLE_PORT + 1));
  }
   

  public static void main(String[] args) throws Exception {
    if(args.length < 1) {       
      System.err.println("Usage: FindFreePort < -random / <#port> >");        
      System.exit(0);      
    }
    int j = 0;
    String cmd = args[j++];
    if ("-random".equals(cmd)) {
      System.out.println(findFreePortRandom());
    } else {
      System.out.println(findFreePort(Integer.parseInt(cmd)));
    }   
  }
        
}
