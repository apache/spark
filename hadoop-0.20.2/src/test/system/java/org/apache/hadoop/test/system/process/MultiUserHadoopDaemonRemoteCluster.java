/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.hadoop.test.system.process;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.test.system.process.HadoopDaemonRemoteCluster.HadoopDaemonInfo;

public abstract class MultiUserHadoopDaemonRemoteCluster
    extends HadoopDaemonRemoteCluster {

  public MultiUserHadoopDaemonRemoteCluster(List<HadoopDaemonInfo> daemonInfos) {
    super(daemonInfos);
  }

  @Override
  protected RemoteProcess getProcessManager(
      HadoopDaemonInfo info, String hostName) {
    return new MultiUserScriptDaemon(info.cmd, hostName, info.role);
  }

  @Override
  public boolean isMultiUserSupported() throws IOException {
    return true;
  }

  class MultiUserScriptDaemon extends ScriptDaemon {

    private static final String MULTI_USER_BINARY_PATH_KEY =
        "test.system.hdrc.multi-user.binary.path";
    private static final String MULTI_USER_MANAGING_USER =
        "test.system.hdrc.multi-user.managinguser.";
    private String binaryPath;
    /**
     * Manging user for a particular daemon is gotten by
     * MULTI_USER_MANAGING_USER + daemonname
     */
    private String mangingUser;

    public MultiUserScriptDaemon(
        String daemonName, String hostName, Enum<?> role) {
      super(daemonName, hostName, role);
      initialize(daemonName);
    }

    private void initialize(String daemonName) {
      binaryPath = conf.get(MULTI_USER_BINARY_PATH_KEY);
      if (binaryPath == null || binaryPath.trim().isEmpty()) {
        throw new IllegalArgumentException(
            "Binary path for multi-user path is not present. Please set "
                + MULTI_USER_BINARY_PATH_KEY + " correctly");
      }
      File binaryFile = new File(binaryPath);
      if (!binaryFile.exists() || !binaryFile.canExecute()) {
        throw new IllegalArgumentException(
            "Binary file path is not configured correctly. Please set "
                + MULTI_USER_BINARY_PATH_KEY
                + " to properly configured binary file.");
      }
      mangingUser = conf.get(MULTI_USER_MANAGING_USER + daemonName);
      if (mangingUser == null || mangingUser.trim().isEmpty()) {
        throw new IllegalArgumentException(
            "Manging user for daemon not present please set : "
                + MULTI_USER_MANAGING_USER + daemonName + " to correct value.");
      }
    }

    @Override
    protected String[] getCommand(String command,String confDir) {
      ArrayList<String> commandList = new ArrayList<String>();
      commandList.add(binaryPath);
      commandList.add(mangingUser);
      commandList.add(hostName);
      commandList.add("--config "
          + confDir + " " + command + " " + daemonName);
      return (String[]) commandList.toArray(new String[commandList.size()]);
    }
  }
}
