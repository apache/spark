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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.test.system.NNProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.system.DaemonProtocol;

public privileged aspect NameNodeAspect {
  declare parents : NameNode implements NNProtocol;

  // Namename doesn't store a copy of its configuration
  // because it can be changed through the life cycle of the object
  // So, the an exposed reference needs to be added and updated after
  // new NameNode(Configuration conf) is complete
  Configuration NameNode.configRef = null;

  // Method simply assign a reference to the NameNode configuration object
  void NameNode.setRef (Configuration conf) {
    if (configRef == null)
      configRef = conf;
  }

  public Configuration NameNode.getDaemonConf() {
    return configRef;
  }

  pointcut nnConstructorPointcut(Configuration conf) :
    call(NameNode.new(Configuration)) && args(conf);

  after(Configuration conf) returning (NameNode namenode):
    nnConstructorPointcut(conf) {
    try {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      namenode.setUser(ugi.getShortUserName());
    } catch (IOException e) {
      namenode.LOG.warn("Unable to get the user information for the " +
          "Jobtracker");
    }
    namenode.setRef(conf);
    namenode.setReady(true);
  }

  pointcut getVersionAspect(String protocol, long clientVersion) :
    execution(public long NameNode.getProtocolVersion(String ,
      long) throws IOException) && args(protocol, clientVersion);

  long around(String protocol, long clientVersion) :
    getVersionAspect(protocol, clientVersion) {
    if(protocol.equals(DaemonProtocol.class.getName())) {
      return DaemonProtocol.versionID;
    } else if(protocol.equals(NNProtocol.class.getName())) {
      return NNProtocol.versionID;
    } else {
      return proceed(protocol, clientVersion);
    }
  }
}