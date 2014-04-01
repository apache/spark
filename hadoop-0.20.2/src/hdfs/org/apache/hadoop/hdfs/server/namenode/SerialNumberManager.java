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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.*;

/** Manage name-to-serial-number maps for users and groups. */
class SerialNumberManager {
  /** This is the only instance of {@link SerialNumberManager}.*/
  static final SerialNumberManager INSTANCE = new SerialNumberManager();

  private SerialNumberMap<String> usermap = new SerialNumberMap<String>();
  private SerialNumberMap<String> groupmap = new SerialNumberMap<String>();

  private SerialNumberManager() {}

  int getUserSerialNumber(String u) {return usermap.get(u);}
  int getGroupSerialNumber(String g) {return groupmap.get(g);}
  String getUser(int n) {return usermap.get(n);}
  String getGroup(int n) {return groupmap.get(n);}

  {
    getUserSerialNumber(null);
    getGroupSerialNumber(null);
  }

  private static class SerialNumberMap<T> {
    private int max = 0;
    private int nextSerialNumber() {return max++;}

    private Map<T, Integer> t2i = new HashMap<T, Integer>();
    private Map<Integer, T> i2t = new HashMap<Integer, T>();

    synchronized int get(T t) {
      Integer sn = t2i.get(t);
      if (sn == null) {
        sn = nextSerialNumber();
        t2i.put(t, sn);
        i2t.put(sn, t);
      }
      return sn;
    }

    synchronized T get(int i) {
      if (!i2t.containsKey(i)) {
        throw new IllegalStateException("!i2t.containsKey(" + i
            + "), this=" + this);
      }
      return i2t.get(i);
    }

    /** {@inheritDoc} */
    public String toString() {
      return "max=" + max + ",\n  t2i=" + t2i + ",\n  i2t=" + i2t;
    }
  }
}
