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

package org.apache.hadoop.common;

import com.jcraft.jsch.*;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import java.io.InputStream;

/**
 * Remote Execution of commands or any other utility on a remote machine.
 */

public class RemoteExecution {

  static final Log LOG = LogFactory.
      getLog(RemoteExecution.class);

  static String rsaOrDsaFileName = "id_dsa";

  public RemoteExecution() throws Exception {
  }

  /**
   * Execute command at remoteNode. 
   * @param JotTracker Client HostName, username provided,
   * command to be executed remotely.
   * @return void
   */
  public static void executeCommand(String jtClientHostName, String user, 
      String  command) throws Exception {

    JSch jsch = new JSch();

    Session session = jsch.getSession(user, jtClientHostName, 22);
    jsch.setKnownHosts("/homes/" + user + "/.ssh/known_hosts");
    jsch.addIdentity("/homes/" + user + "/.ssh/" + rsaOrDsaFileName);
    java.util.Properties config = new java.util.Properties();
    config.put("StrictHostKeyChecking", "no");
    session.setConfig(config);

    session.connect(30000);   // making a connection with timeout.

    Channel channel=session.openChannel("exec");
    ((ChannelExec)channel).setCommand(command);
    channel.setInputStream(null);
   
    ((ChannelExec)channel).setErrStream(System.err);
   
    InputStream in = channel.getInputStream();
    channel.connect();
    byte[] tmp = new byte[1024];
    while(true) {
      while(in.available()>0){
        int i=in.read(tmp, 0, 1024);
        if(i<0)break;
           System.out.print(new String(tmp, 0, i));
           LOG.info(new String(tmp, 0, i));
      }
      if(channel.isClosed()){
        System.out.println("exit-status: " + channel.getExitStatus());
        break;
      }
      try{Thread.sleep(1000);}catch(Exception ee){ee.printStackTrace();}
    }
    channel.disconnect();
    session.disconnect();
  }

  /**
   * Execute command at remoteNode.
   * @param JotTracker Client HostName, username provided,
   * comamnd to be executed remotely, rsaOrDsaFileName to be found under .ssh.
   * @return void
   */
  public static void executeCommand(String jtClientHostName, String user,
      String  command, String rsaDsaFileName) throws Exception {
      rsaOrDsaFileName = rsaDsaFileName;
      executeCommand(jtClientHostName, user, command);
  }
}
