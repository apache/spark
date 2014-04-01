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
package org.apache.hadoop.hdfs;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.test.system.DaemonProtocol;
import org.apache.hadoop.hdfs.test.system.DNProtocol;
import org.apache.hadoop.hdfs.test.system.NNProtocol;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;

/**
 * This aspect adds two HDFS Herriot specific protocols tp the list of 'authorized'
 * Herriot protocols.
 * Protocol descriptors i.e. 'security.nn.protocol.acl' have to be added to
 * <code>hadoop-policy.xml</code> if present
 */
public privileged aspect HDFSPolicyProviderAspect {
  private static final Log LOG = LogFactory
      .getLog(HDFSPolicyProviderAspect.class);

  ArrayList<Service> herriotHDFSServices = null;

  pointcut updateHDFSServices() :
    execution (public Service[] HDFSPolicyProvider.getServices());

  Service[] around() : updateHDFSServices () {
    herriotHDFSServices = new ArrayList<Service>();
    for (Service s : HDFSPolicyProvider.hdfsServices) {
      LOG.debug("Copying configured protocol to "
          + s.getProtocol().getCanonicalName());
      herriotHDFSServices.add(s);
    }
    herriotHDFSServices.add(new Service("security.daemon.protocol.acl",
        DaemonProtocol.class));
    herriotHDFSServices.add(new Service("security.nn.protocol.acl",
        NNProtocol.class));
    herriotHDFSServices.add(new Service("security.dn.protocol.acl",
        DNProtocol.class));
    final Service[] retArray = herriotHDFSServices
        .toArray(new Service[herriotHDFSServices.size()]);
    LOG.debug("Number of configured protocols to return: " + retArray.length);
    return retArray;
  }
}
