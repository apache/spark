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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A cached implementation of DNSToSwitchMapping that takes an
 * raw DNSToSwitchMapping and stores the resolved network location in 
 * a cache. The following calls to a resolved network location
 * will get its location from the cache. 
 *
 */
public class CachedDNSToSwitchMapping implements DNSToSwitchMapping {
  private Map<String, String> cache = new ConcurrentHashMap<String, String>();
  protected DNSToSwitchMapping rawMapping;
  
  public CachedDNSToSwitchMapping(DNSToSwitchMapping rawMapping) {
    this.rawMapping = rawMapping;
  }
  
  public List<String> resolve(List<String> names) {
    // normalize all input names to be in the form of IP addresses
    names = NetUtils.normalizeHostNames(names);
    
    List <String> result = new ArrayList<String>(names.size());
    if (names.isEmpty()) {
      return result;
    }


    // find out all names without cached resolved location
    List<String> unCachedHosts = new ArrayList<String>(names.size());
    for (String name : names) {
      if (cache.get(name) == null) {
        unCachedHosts.add(name);
      } 
    }
    
    // Resolve those names
    List<String> rNames = rawMapping.resolve(unCachedHosts);
    
    // Cache the result
    if (rNames != null) {
      for (int i=0; i<unCachedHosts.size(); i++) {
        cache.put(unCachedHosts.get(i), rNames.get(i));
      }
    }
    
    // Construct the result
    for (String name : names) {
      //now everything is in the cache
      String networkLocation = cache.get(name);
      if (networkLocation != null) {
        result.add(networkLocation);
      } else { //resolve all or nothing
        return null;
      }
    }
    return result;
  }
}
