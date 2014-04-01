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

package org.apache.hadoop.streaming;

import java.io.*;
import java.net.InetAddress;
import java.util.*;

/**
 * This is a class used to get the current environment
 * on the host machines running the map/reduce. This class
 * assumes that setting the environment in streaming is 
 * allowed on windows/ix/linuz/freebsd/sunos/solaris/hp-ux
 */
public class Environment extends Properties {

  public Environment() throws IOException {
    // Extend this code to fit all operating
    // environments that you expect to run in
    // http://lopica.sourceforge.net/os.html
    String command = null;
    String OS = System.getProperty("os.name");
    String lowerOs = OS.toLowerCase();
    if (OS.indexOf("Windows") > -1) {
      command = "cmd /C set";
    } else if (lowerOs.indexOf("ix") > -1 || lowerOs.indexOf("linux") > -1
               || lowerOs.indexOf("freebsd") > -1 || lowerOs.indexOf("sunos") > -1
               || lowerOs.indexOf("solaris") > -1 || lowerOs.indexOf("hp-ux") > -1) {
      command = "env";
    } else if (lowerOs.startsWith("mac os x") || lowerOs.startsWith("darwin")) {
      command = "env";
    } else {
      // Add others here
    }

    if (command == null) {
      throw new RuntimeException("Operating system " + OS + " not supported by this class");
    }

    // Read the environment variables

    Process pid = Runtime.getRuntime().exec(command);
    BufferedReader in = new BufferedReader(new InputStreamReader(pid.getInputStream()));
    while (true) {
      String line = in.readLine();
      if (line == null) break;
      int p = line.indexOf("=");
      if (p != -1) {
        String name = line.substring(0, p);
        String value = line.substring(p + 1);
        setProperty(name, value);
      }
    }
    in.close();
    try {
      pid.waitFor();
    } catch (InterruptedException e) {
      throw new IOException(e.getMessage());
    }
  }

  // to be used with Runtime.exec(String[] cmdarray, String[] envp) 
  String[] toArray() {
    String[] arr = new String[super.size()];
    Enumeration it = super.keys();
    int i = -1;
    while (it.hasMoreElements()) {
      String key = (String) it.nextElement();
      String val = (String) get(key);
      i++;
      arr[i] = key + "=" + val;
    }
    return arr;
  }

  public Map<String, String> toMap() {
    Map<String, String> map = new HashMap<String, String>();
    Enumeration<Object> it = super.keys();
    while (it.hasMoreElements()) {
      String key = (String) it.nextElement();
      String val = (String) get(key);
      map.put(key, val);
    }
    return map;
  }
  
  public String getHost() {
    String host = getProperty("HOST");
    if (host == null) {
      // HOST isn't always in the environment
      try {
        host = InetAddress.getLocalHost().getHostName();
      } catch (IOException io) {
        io.printStackTrace();
      }
    }
    return host;
  }

}
