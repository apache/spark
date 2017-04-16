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
package org.apache.hadoop.net;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
 * Implements the {@link DNSToSwitchMapping} via static mappings. Used
 * in testcases that simulate racks.
 *
 */
public class StaticMapping extends Configured implements DNSToSwitchMapping {
  public void setconf(Configuration conf) {
    String[] mappings = conf.getStrings("hadoop.configured.node.mapping");
    if (mappings != null) {
      for (int i = 0; i < mappings.length; i++) {
        String str = mappings[i];
        String host = str.substring(0, str.indexOf('='));
        String rack = str.substring(str.indexOf('=') + 1);
        addNodeToRack(host, rack);
      }
    }
  }
  /* Only one instance per JVM */
  private static Map<String, String> nameToRackMap = new HashMap<String, String>();
  
  static synchronized public void addNodeToRack(String name, String rackId) {
    nameToRackMap.put(name, rackId);
  }
  public List<String> resolve(List<String> names) {
    List<String> m = new ArrayList<String>();
    synchronized (nameToRackMap) {
      for (String name : names) {
        String rackId;
        if ((rackId = nameToRackMap.get(name)) != null) {
          m.add(rackId);
        } else {
          m.add(NetworkTopology.DEFAULT_RACK);
        }
      }
      return m;
    }
  }
}
