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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.tools.MRAdmin;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;

import junit.framework.TestCase;

/**
 * Test case to check if {@link AdminOperationsProtocol#refreshNodes()} and 
 * {@link AdminOperationsProtocol#refreshQueues()} works with service-level
 * authorization enabled i.e 'hadoop.security.authorization' set to true.
 */
public class TestAdminOperationsProtocolWithServiceAuthorization 
extends TestCase {
  public void testServiceLevelAuthorization() throws Exception {
    MiniMRCluster mr = null;
    try {
      // Turn on service-level authorization
      final JobConf conf = new JobConf();
      conf.setClass(PolicyProvider.POLICY_PROVIDER_CONFIG, 
                    MapReducePolicyProvider.class, PolicyProvider.class);
      conf.setBoolean(ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, 
                      true);
      
      // Start the mini mr cluster
      mr = new MiniMRCluster(1, "file:///", 1, null, null, conf);

      // Invoke MRAdmin commands
      MRAdmin mrAdmin = new MRAdmin(mr.createJobConf());
      assertEquals(0, mrAdmin.run(new String[] { "-refreshQueues" }));
      assertEquals(0, mrAdmin.run(new String[] { "-refreshNodes" }));
    } finally {
      if (mr != null) { 
        mr.shutdown();
      }
    }
  }
}
