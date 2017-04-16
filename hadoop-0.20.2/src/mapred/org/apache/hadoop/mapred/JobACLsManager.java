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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

class JobACLsManager {

	  JobConf conf;
	  
	  public JobACLsManager(JobConf conf) {
        this.conf = conf;
      }

	  boolean areACLsEnabled() {
	    return conf.getBoolean(JobConf.MR_ACLS_ENABLED, false);
	  }

	  /**
	   * Construct the jobACLs from the configuration so that they can be kept in
	   * the memory. If authorization is disabled on the JT, nothing is constructed
	   * and an empty map is returned.
	   * 
	   * @return JobACL to AccessControlList map.
	   */
	  Map<JobACL, AccessControlList> constructJobACLs(JobConf conf) {
	    
	    Map<JobACL, AccessControlList> acls =
	      new HashMap<JobACL, AccessControlList>();

	    // Don't construct anything if authorization is disabled.
	    if (!areACLsEnabled()) {
	      return acls;
	    }

	    for (JobACL aclName : JobACL.values()) {
	      String aclConfigName = aclName.getAclName();
	      String aclConfigured = conf.get(aclConfigName);
	      if (aclConfigured == null) {
	        // If ACLs are not configured at all, we grant no access to anyone. So
	        // jobOwner and cluster administrators _only_ can do 'stuff'
	        aclConfigured = "";
	      }
	      acls.put(aclName, new AccessControlList(aclConfigured));
	    }
	    return acls;
	  }

	  /**
	   * If authorization is enabled, checks whether the user (in the callerUGI)
	   * is authorized to perform the operation specified by 'jobOperation' on
	   * the job by checking if the user is jobOwner or part of job ACL for the
	   * specific job operation.
	   * <ul>
	   * <li>The owner of the job can do any operation on the job</li>
	   * <li>For all other users/groups job-acls are checked</li>
	   * </ul>
	   * @param callerUGI
	   * @param jobOperation
	   * @param jobOwner
	   * @param jobACL
	   * @throws AccessControlException
	   */
	  boolean checkAccess(UserGroupInformation callerUGI,
	      JobACL jobOperation, String jobOwner, AccessControlList jobACL)
	      throws AccessControlException {

	    String user = callerUGI.getShortUserName();
	    if (!areACLsEnabled()) {
	      return true;
	    }

	    // Allow Job-owner for any operation on the job
	    if (user.equals(jobOwner) 
	        || jobACL.isUserAllowed(callerUGI)) {
	      return true;
	    }

	    return false;
	  }
	}
