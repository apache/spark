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

package org.apache.hadoop.mapreduce.security.token.delegation;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestDelegationToken {
  private MiniMRCluster cluster;
  private UserGroupInformation user1;
  private UserGroupInformation user2;
  
  @Before
  public void setup() throws Exception {
    user1 = UserGroupInformation.createUserForTesting("alice", 
                                                      new String[]{"users"});
    user2 = UserGroupInformation.createUserForTesting("bob", 
                                                      new String[]{"users"});
    cluster = new MiniMRCluster(0,0,1,"file:///",1);
  }
  
  @Test
  public void testDelegationToken() throws Exception {
    
    JobClient client;
    client = user1.doAs(new PrivilegedExceptionAction<JobClient>(){

      @Override
      public JobClient run() throws Exception {
        return new JobClient(cluster.createJobConf());
      }});
    JobClient bobClient;
    bobClient = user2.doAs(new PrivilegedExceptionAction<JobClient>(){

      @Override
      public JobClient run() throws Exception {
        return new JobClient(cluster.createJobConf());
      }});
    
    Token<DelegationTokenIdentifier> token = 
      client.getDelegationToken(new Text(user1.getUserName()));
    
    DataInputBuffer inBuf = new DataInputBuffer();
    byte[] bytes = token.getIdentifier();
    inBuf.reset(bytes, bytes.length);
    DelegationTokenIdentifier ident = new DelegationTokenIdentifier();
    ident.readFields(inBuf);
    
    assertEquals("alice", ident.getUser().getUserName());
    long createTime = ident.getIssueDate();
    long maxTime = ident.getMaxDate();
    long currentTime = System.currentTimeMillis();
    System.out.println("create time: " + createTime);
    System.out.println("current time: " + currentTime);
    System.out.println("max time: " + maxTime);
    assertTrue("createTime < current", createTime < currentTime);
    assertTrue("current < maxTime", currentTime < maxTime);
    client.renewDelegationToken(token);
    client.renewDelegationToken(token);
    try {
      bobClient.renewDelegationToken(token);
      Assert.fail("bob renew");
    } catch (AccessControlException ace) {
      // PASS
    }
    try {
      bobClient.cancelDelegationToken(token);
      Assert.fail("bob renew");
    } catch (AccessControlException ace) {
      // PASS
    }
    client.cancelDelegationToken(token);
    try {
      client.cancelDelegationToken(token);
      Assert.fail("second alice cancel");
    } catch (InvalidToken it) {
      // PASS
    }
  }
}

