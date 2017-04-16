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
import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.conf.*;

/**
 * This class implements the {@link DNSToSwitchMapping} interface using a 
 * script configured via topology.script.file.name .
 */
public final class ScriptBasedMapping extends CachedDNSToSwitchMapping 
implements Configurable
{
  public ScriptBasedMapping() {
    super(new RawScriptBasedMapping());
  }
  
  // script must accept at least this many args
  static final int MIN_ALLOWABLE_ARGS = 1;
  
  static final int DEFAULT_ARG_COUNT = 100;
  
  static final String SCRIPT_FILENAME_KEY = "topology.script.file.name";
  static final String SCRIPT_ARG_COUNT_KEY = "topology.script.number.args";
  
  public ScriptBasedMapping(Configuration conf) {
    this();
    setConf(conf);
  }
  
  public Configuration getConf() {
    return ((RawScriptBasedMapping)rawMapping).getConf();
  }
  
  public void setConf(Configuration conf) {
    ((RawScriptBasedMapping)rawMapping).setConf(conf);
  }
  
  private static final class RawScriptBasedMapping
  implements DNSToSwitchMapping {
  private String scriptName;
  private Configuration conf;
  private int maxArgs; //max hostnames per call of the script
  private static Log LOG = 
    LogFactory.getLog(ScriptBasedMapping.class);
  public void setConf (Configuration conf) {
    this.scriptName = conf.get(SCRIPT_FILENAME_KEY);
    this.maxArgs = conf.getInt(SCRIPT_ARG_COUNT_KEY, DEFAULT_ARG_COUNT);
    this.conf = conf;
  }
  public Configuration getConf () {
    return conf;
  }
  
  public RawScriptBasedMapping() {}
  
  public List<String> resolve(List<String> names) {
    List <String> m = new ArrayList<String>(names.size());
    
    if (names.isEmpty()) {
      return m;
    }

    if (scriptName == null) {
      for (int i = 0; i < names.size(); i++) {
        m.add(NetworkTopology.DEFAULT_RACK);
      }
      return m;
    }
    
    String output = runResolveCommand(names);
    if (output != null) {
      StringTokenizer allSwitchInfo = new StringTokenizer(output);
      while (allSwitchInfo.hasMoreTokens()) {
        String switchInfo = allSwitchInfo.nextToken();
        m.add(switchInfo);
      }
      
      if (m.size() != names.size()) {
        // invalid number of entries returned by the script
        LOG.warn("Script " + scriptName + " returned "
            + Integer.toString(m.size()) + " values when "
            + Integer.toString(names.size()) + " were expected.");
        return null;
      }
    } else {
      // an error occurred. return null to signify this.
      // (exn was already logged in runResolveCommand)
      return null;
    }
    
    return m;
  }
  
  private String runResolveCommand(List<String> args) {
    int loopCount = 0;
    if (args.size() == 0) {
      return null;
    }
    StringBuffer allOutput = new StringBuffer();
    int numProcessed = 0;
    if (maxArgs < MIN_ALLOWABLE_ARGS) {
      LOG.warn("Invalid value " + Integer.toString(maxArgs)
          + " for " + SCRIPT_ARG_COUNT_KEY + "; must be >= "
          + Integer.toString(MIN_ALLOWABLE_ARGS));
      return null;
    }
    
    while (numProcessed != args.size()) {
      int start = maxArgs * loopCount;
      List <String> cmdList = new ArrayList<String>();
      cmdList.add(scriptName);
      for (numProcessed = start; numProcessed < (start + maxArgs) && 
           numProcessed < args.size(); numProcessed++) {
        cmdList.add(args.get(numProcessed)); 
      }
      File dir = null;
      String userDir;
      if ((userDir = System.getProperty("user.dir")) != null) {
        dir = new File(userDir);
      }
      ShellCommandExecutor s = new ShellCommandExecutor(
                                   cmdList.toArray(new String[0]), dir);
      try {
        s.execute();
        allOutput.append(s.getOutput() + " ");
      } catch (Exception e) {
        LOG.warn(StringUtils.stringifyException(e));
        return null;
      }
      loopCount++; 
    }
    return allOutput.toString();
  }
  }
}
