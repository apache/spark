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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.IOException;
import java.util.AbstractList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.test.system.DNProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.system.DaemonProtocol;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;

public privileged aspect DataNodeAspect {
  declare parents : DataNode implements DNProtocol;

  public Configuration DataNode.getDaemonConf() {
    return super.getConf();
  }

  pointcut dnConstructorPointcut(Configuration conf, AbstractList<File> dirs,
      SecureResources resources) :
    call(DataNode.new(Configuration, AbstractList<File>, SecureResources))
    && args(conf, dirs, resources);

  after(Configuration conf, AbstractList<File> dirs, SecureResources resources)
    returning (DataNode datanode):
    dnConstructorPointcut(conf, dirs, resources) {
    try {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      datanode.setUser(ugi.getShortUserName());
    } catch (IOException e) {
      datanode.LOG.warn("Unable to get the user information for the " +
          "DataNode");
    }
    datanode.setReady(true);
  }

  pointcut getVersionAspect(String protocol, long clientVersion) :
    execution(public long DataNode.getProtocolVersion(String ,
      long) throws IOException) && args(protocol, clientVersion);

  long around(String protocol, long clientVersion) :
    getVersionAspect(protocol, clientVersion) {
    if(protocol.equals(DaemonProtocol.class.getName())) {
      return DaemonProtocol.versionID;
    } else if(protocol.equals(DNProtocol.class.getName())) {
      return DNProtocol.versionID;
    } else {
      return proceed(protocol, clientVersion);
    }
  }
}