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

package org.apache.hadoop.mapreduce.security.token;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * unit test - 
 * tests addition/deletion/cancelation of renewals of delegation tokens
 *
 */
public class TestDelegationTokenRenewal {
  private static final Log LOG = 
      LogFactory.getLog(TestDelegationTokenRenewal.class);

  private static Configuration conf;
 
  @BeforeClass
  public static void setUp() throws Exception {
    conf = new Configuration();
    
    // create a fake FileSystem (MyFS) and assosiate it
    // with "hdfs" schema.
    InetAddress iaddr = InetAddress.getByName("localhost");
    String localhostDnsName = iaddr.getCanonicalHostName();
    URI uri = new URI(DelegationTokenRenewal.SCHEME+"://" + localhostDnsName + ":0");
    System.out.println("scheme is : " + uri.getScheme());
    conf.setClass("fs." + uri.getScheme() + ".impl", MyFS.class, DistributedFileSystem.class);
    FileSystem.setDefaultUri(conf, uri);
    System.out.println("filesystem uri = " + FileSystem.getDefaultUri(conf).toString());

  }
  
  private static class MyDelegationTokenSecretManager extends DelegationTokenSecretManager {

    public MyDelegationTokenSecretManager(long delegationKeyUpdateInterval,
        long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
        long delegationTokenRemoverScanInterval, FSNamesystem namesystem) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval,
          namesystem);
    }
    
    @Override //DelegationTokenSecretManager
    public void logUpdateMasterKey(DelegationKey key) throws IOException {
      return;
    }
  }
  
  /**
   * add some extra functionality for testing
   * 1. toString();
   * 2. cancel() and isCanceled()
   */
  private static class MyToken extends Token<DelegationTokenIdentifier> {
    public String status = "GOOD";
    public static final String CANCELED = "CANCELED";

    public MyToken(DelegationTokenIdentifier dtId1,
        MyDelegationTokenSecretManager sm) {
      super(dtId1, sm);
      status = "GOOD";
    }
    
    public boolean isCanceled() {return status.equals(CANCELED);}
    public void cancelToken() {this.status=CANCELED;}
    public String toString() {
      StringBuilder sb = new StringBuilder(1024);
      
      sb.append("id=");
      String id = StringUtils.byteToHexString(this.getIdentifier());
      int idLen = id.length();
      sb.append(id.substring(idLen-6));
      sb.append(";k=");
      sb.append(this.getKind());
      sb.append(";s=");
      sb.append(this.getService());
      return sb.toString();
    }
  }

  /**
   * fake FileSystem 
   * overwrites three methods
   * 1. getDelegationToken() - generates a token
   * 2. renewDelegataionToken - counts number of calls, and remembers 
   * most recently renewed token.
   * 3. cancelToken -cancels token (subsequent renew will cause IllegalToken 
   * exception
   */
  static class MyFS extends DistributedFileSystem {
    volatile int counter=0;
    MyToken token;
    MyToken tokenToRenewIn2Sec;
    
    public MyFS() {}
    public void close() {}
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {}
    
    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> t)
    throws InvalidToken, IOException {
      MyToken token = (MyToken)t;
      if(token.isCanceled()) {
        throw new InvalidToken("token has been canceled");
      }
      counter ++;
      this.token = (MyToken)token;
      System.out.println("Called MYDFS.renewdelegationtoken " + token + 
          ";this dfs=" + this.hashCode() + ";c=" + counter);
      if(tokenToRenewIn2Sec == token) { 
        // this token first renewal in 2 seconds
        System.out.println("RENEW in 2 seconds");
        tokenToRenewIn2Sec=null;
        return 2*1000 + System.currentTimeMillis();
      } else {
        return 86400*1000 + System.currentTimeMillis();
      }
    }
    @Override 
    public MyToken getDelegationToken(Text renewer)
    throws IOException {
      System.out.println("Called MYDFS.getdelegationtoken");
      return createTokens(renewer);
    }
    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> t)
    throws IOException {
      MyToken token = (MyToken)t;
      token.cancelToken();
    }

    public void setTokenToRenewIn2Sec(MyToken t) {tokenToRenewIn2Sec=t;}
    public int getCounter() {return counter; }
    public MyToken getToken() {return token;}
  }
  
  /**
   * Auxiliary - create token
   * @param renewer
   * @return
   * @throws IOException
   */
  static MyToken createTokens(Text renewer) 
    throws IOException {
    Text user1= new Text("user1");
    
    MyDelegationTokenSecretManager sm = new MyDelegationTokenSecretManager(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT,
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT,
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT,
        3600000, null);
    sm.startThreads();
    
    DelegationTokenIdentifier dtId1 = 
      new DelegationTokenIdentifier(user1, renewer, user1);
    
    MyToken token1 = new MyToken(dtId1, sm);
    
   
    token1.setService(new Text("localhost:0"));
    return token1;
  }
  
  
  /**
   * Basic idea of the test:
   * 1. create tokens.
   * 2. Mark one of them to be renewed in 2 seconds (istead of
   * 24 hourse)
   * 3. register them for renewal
   * 4. sleep for 3 seconds
   * 5. count number of renewals (should 3 initial ones + one extra)
   * 6. register another token for 2 seconds 
   * 7. cancel it immediately
   * 8. Sleep and check that the 2 seconds renew didn't happen 
   * (totally 5 reneals)
   * 9. check cancelation
   * @throws IOException
   * @throws URISyntaxException
   */
  @Test
  public void testDTRenewal () throws IOException, URISyntaxException {
    MyFS dfs = (MyFS)FileSystem.get(conf);
    System.out.println("dfs="+(Object)dfs.hashCode() + ";conf="+conf.hashCode());
    // Test 1. - add three tokens - make sure exactly one get's renewed
    
    // get the delegation tokens
    MyToken token1, token2, token3;
    token1 = dfs.getDelegationToken(new Text("user1"));
    token2 = dfs.getDelegationToken(new Text("user2"));
    token3 = dfs.getDelegationToken(new Text("user3"));

    //to cause this one to be set for renew in 2 secs
    dfs.setTokenToRenewIn2Sec(token1); 
    System.out.println("token="+token1+" should be renewed for 2 secs");
    
    // three distinct Namenodes
    String nn1 = DelegationTokenRenewal.SCHEME + "://host1:0";
    String nn2 = DelegationTokenRenewal.SCHEME + "://host2:0";
    String nn3 = DelegationTokenRenewal.SCHEME + "://host3:0";
    
    Credentials ts = new Credentials();
    
    // register the token for renewal
    ts.addToken(new Text(nn1), token1);
    ts.addToken(new Text(nn2), token2);
    ts.addToken(new Text(nn3), token3);
    
    // register the tokens for renewal
    DelegationTokenRenewal.registerDelegationTokensForRenewal(
        new JobID("job1", 1), ts, conf);
    // first 3 initial renewals + 1 real
    int numberOfExpectedRenewals = 3+1; 
    
    int attempts = 10;
    while(attempts-- > 0) {
      try {
        Thread.sleep(3*1000); // sleep 3 seconds, so it has time to renew
      } catch (InterruptedException e) {}
      
      // since we cannot guarantee timely execution - let's give few chances
      if(dfs.getCounter()==numberOfExpectedRenewals)
        break;
    }
    
    System.out.println("dfs=" + dfs.hashCode() + 
        ";Counter = " + dfs.getCounter() + ";t="+  dfs.getToken());
    assertEquals("renew wasn't called as many times as expected(4):",
        numberOfExpectedRenewals, dfs.getCounter());
    assertEquals("most recently renewed token mismatch", dfs.getToken(), 
        token1);
    
    // Test 2. 
    // add another token ( that expires in 2 secs). Then remove it, before
    // time is up.
    // Wait for 3 secs , and make sure no renews were called
    ts = new Credentials();
    MyToken token4 = dfs.getDelegationToken(new Text("user4"));
    
    //to cause this one to be set for renew in 2 secs
    dfs.setTokenToRenewIn2Sec(token4); 
    System.out.println("token="+token4+" should be renewed for 2 secs");
    
    String nn4 = DelegationTokenRenewal.SCHEME + "://host4:0";
    ts.addToken(new Text(nn4), token4);
    

    JobID jid2 = new JobID("job2",1);
    DelegationTokenRenewal.registerDelegationTokensForRenewal(jid2, ts, conf);
    DelegationTokenRenewal.removeDelegationTokenRenewalForJob(jid2);
    numberOfExpectedRenewals = dfs.getCounter(); // number of renewals so far
    try {
      Thread.sleep(6*1000); // sleep 6 seconds, so it has time to renew
    } catch (InterruptedException e) {}
    System.out.println("Counter = " + dfs.getCounter() + ";t="+dfs.getToken());
    
    // counter and the token should stil be the old ones
    assertEquals("renew wasn't called as many times as expected",
        numberOfExpectedRenewals, dfs.getCounter());
    
    // also renewing of the cancelled token should fail
    boolean exception=false;
    try {
      dfs.renewDelegationToken(token4);
    } catch (InvalidToken ite) {
      //expected
      exception = true;
    }
    assertTrue("Renew of canceled token didn't fail", exception);
  }
}
