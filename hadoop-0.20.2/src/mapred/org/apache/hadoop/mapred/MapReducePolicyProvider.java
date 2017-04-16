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

import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.tools.GetUserMappingsProtocol;

/**
 * {@link PolicyProvider} for Map-Reduce protocols.
 */
public class MapReducePolicyProvider extends PolicyProvider {
  private static final Service[] mapReduceServices = 
    new Service[] {
      new Service("security.inter.tracker.protocol.acl", 
                  InterTrackerProtocol.class),
      new Service("security.job.submission.protocol.acl",
                  JobSubmissionProtocol.class),
      new Service("security.task.umbilical.protocol.acl", 
                  TaskUmbilicalProtocol.class),
      new Service("security.refresh.policy.protocol.acl", 
                  RefreshAuthorizationPolicyProtocol.class),
      new Service("security.refresh.usertogroups.mappings.protocol.acl", 
                  RefreshUserMappingsProtocol.class),
      new Service("security.admin.operations.protocol.acl", 
                  AdminOperationsProtocol.class),
      new Service("security.get.user.mappings.protocol.acl",
                  GetUserMappingsProtocol.class),
  };
  
  @Override
  public Service[] getServices() {
    return mapReduceServices;
  }

}
