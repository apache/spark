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
package org.apache.hadoop.mapred;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.test.system.TTProtocol;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.test.system.DaemonProtocol;

/**
 * This aspect adds two MR specific Herriot protocols tp the list of
 * 'authorized' Herriot protocols. Protocol descriptors i.e.
 * 'security.tt.protocol.acl' have to be added to <code>hadoop-policy.xml</code>
 * if present
 */
public privileged aspect MapReducePolicyProviderAspect {
  private static final Log LOG = LogFactory
      .getLog(MapReducePolicyProviderAspect.class);
  ArrayList<Service> herriotMRServices = null;

  pointcut updateMRServices() :
    execution (public Service[] MapReducePolicyProvider.getServices());

  Service[] around() : updateMRServices () {
    herriotMRServices = new ArrayList<Service>();
    for (Service s : MapReducePolicyProvider.mapReduceServices) {
      LOG.debug("Copying configured protocol to "
          + s.getProtocol().getCanonicalName());
      herriotMRServices.add(s);
    }
    herriotMRServices.add(new Service("security.daemon.protocol.acl",
        DaemonProtocol.class));
    herriotMRServices.add(new Service("security.tt.protocol.acl",
        TTProtocol.class));
    final Service[] retArray = herriotMRServices
        .toArray(new Service[herriotMRServices.size()]);
    LOG.debug("Number of configured protocols to return: " + retArray.length);
    return retArray;
  }
}
