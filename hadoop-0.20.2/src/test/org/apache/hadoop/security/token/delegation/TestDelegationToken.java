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

package org.apache.hadoop.security.token.delegation;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestDelegationToken {
  private static final Log LOG = LogFactory.getLog(TestDelegationToken.class);
  private static final Text KIND = new Text("MY KIND");

  public static class TestDelegationTokenIdentifier 
  extends AbstractDelegationTokenIdentifier
  implements Writable {

    public TestDelegationTokenIdentifier() {
    }

    public TestDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
      super(owner, renewer, realUser);
    }

    @Override
    public Text getKind() {
      return KIND;
    }
    
    public void write(DataOutput out) throws IOException {
      super.write(out); 
    }
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
    }
  }
  
  public static class TestDelegationTokenSecretManager 
  extends AbstractDelegationTokenSecretManager<TestDelegationTokenIdentifier> {

    public TestDelegationTokenSecretManager(long delegationKeyUpdateInterval,
                         long delegationTokenMaxLifetime,
                         long delegationTokenRenewInterval,
                         long delegationTokenRemoverScanInterval) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
            delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    }

    @Override
    public TestDelegationTokenIdentifier createIdentifier() {
      return new TestDelegationTokenIdentifier();
    }
    
    @Override
    protected byte[] createPassword(TestDelegationTokenIdentifier t) {
      return super.createPassword(t);
    }
    
    public byte[] createPassword(TestDelegationTokenIdentifier t, DelegationKey key) {
      return SecretManager.createPassword(t.getBytes(), key.getKey());
    }
    
    public Map<TestDelegationTokenIdentifier, DelegationTokenInformation> getAllTokens() {
      return currentTokens;
    }
    
    public DelegationKey getKey(TestDelegationTokenIdentifier id) {
      return allKeys.get(id.getMasterKeyId());
    }
  }
  
  public static class TokenSelector extends 
  AbstractDelegationTokenSelector<TestDelegationTokenIdentifier>{

    protected TokenSelector() {
      super(KIND);
    }    
  }
  
  @Test
  public void testSerialization() throws Exception {
    TestDelegationTokenIdentifier origToken = new 
                        TestDelegationTokenIdentifier(new Text("alice"), 
                                          new Text("bob"), 
                                          new Text("colin"));
    TestDelegationTokenIdentifier newToken = new TestDelegationTokenIdentifier();
    origToken.setIssueDate(123);
    origToken.setMasterKeyId(321);
    origToken.setMaxDate(314);
    origToken.setSequenceNumber(12345);
    
    // clone origToken into newToken
    DataInputBuffer inBuf = new DataInputBuffer();
    DataOutputBuffer outBuf = new DataOutputBuffer();
    origToken.write(outBuf);
    inBuf.reset(outBuf.getData(), 0, outBuf.getLength());
    newToken.readFields(inBuf);
    
    // now test the fields
    assertEquals("alice", newToken.getUser().getUserName());
    assertEquals(new Text("bob"), newToken.getRenewer());
    assertEquals("colin", newToken.getUser().getRealUser().getUserName());
    assertEquals(123, newToken.getIssueDate());
    assertEquals(321, newToken.getMasterKeyId());
    assertEquals(314, newToken.getMaxDate());
    assertEquals(12345, newToken.getSequenceNumber());
    assertEquals(origToken, newToken);
  }
  
  private Token<TestDelegationTokenIdentifier> generateDelegationToken(
      TestDelegationTokenSecretManager dtSecretManager,
      String owner, String renewer) {
    TestDelegationTokenIdentifier dtId = 
      new TestDelegationTokenIdentifier(new Text(
        owner), new Text(renewer), null);
    return new Token<TestDelegationTokenIdentifier>(dtId, dtSecretManager);
  }
  
  private void shouldThrow(PrivilegedExceptionAction<Object> action,
                           Class<? extends Throwable> except) {
    try {
      action.run();
      Assert.fail("action did not throw " + except);
    } catch (Throwable th) {
      LOG.info("Caught an exception: " + StringUtils.stringifyException(th));
      assertEquals("action threw wrong exception", except, th.getClass());
    }
  }

  @Test
  public void testDelegationTokenSecretManager() throws Exception {
    final TestDelegationTokenSecretManager dtSecretManager = 
      new TestDelegationTokenSecretManager(24*60*60*1000,
          3*1000,1*1000,3600000);
    try {
      dtSecretManager.startThreads();
      final Token<TestDelegationTokenIdentifier> token = 
        generateDelegationToken(
          dtSecretManager, "SomeUser", "JobTracker");
      // Fake renewer should not be able to renew
      shouldThrow(new PrivilegedExceptionAction<Object>() {
        public Object run() throws Exception {
          dtSecretManager.renewToken(token, "FakeRenewer");
          return null;
        }
      }, AccessControlException.class);
      long time = dtSecretManager.renewToken(token, "JobTracker");
      assertTrue("renew time is in future", time > System.currentTimeMillis());
      TestDelegationTokenIdentifier identifier = 
        new TestDelegationTokenIdentifier();
      byte[] tokenId = token.getIdentifier();
      identifier.readFields(new DataInputStream(
          new ByteArrayInputStream(tokenId)));
      Assert.assertTrue(null != dtSecretManager.retrievePassword(identifier));
      LOG.info("Sleep to expire the token");
      Thread.sleep(2000);
      //Token should be expired
      try {
        dtSecretManager.retrievePassword(identifier);
        //Should not come here
        Assert.fail("Token should have expired");
      } catch (InvalidToken e) {
        //Success
      }
      dtSecretManager.renewToken(token, "JobTracker");
      LOG.info("Sleep beyond the max lifetime");
      Thread.sleep(2000);
      
      shouldThrow(new PrivilegedExceptionAction<Object>() {
        public Object run() throws Exception {
          dtSecretManager.renewToken(token, "JobTracker");
          return null;
        }
      }, InvalidToken.class);
    } finally {
      dtSecretManager.stopThreads();
    }
  }

  @Test 
  public void testCancelDelegationToken() throws Exception {
    final TestDelegationTokenSecretManager dtSecretManager = 
      new TestDelegationTokenSecretManager(24*60*60*1000,
        10*1000,1*1000,3600000);
    try {
      dtSecretManager.startThreads();
      final Token<TestDelegationTokenIdentifier> token = 
        generateDelegationToken(dtSecretManager, "SomeUser", "JobTracker");
      //Fake renewer should not be able to renew
      shouldThrow(new PrivilegedExceptionAction<Object>() {
        public Object run() throws Exception {
          dtSecretManager.renewToken(token, "FakeCanceller");
          return null;
        }
      }, AccessControlException.class);
      dtSecretManager.cancelToken(token, "JobTracker");
      shouldThrow(new PrivilegedExceptionAction<Object>() {
        public Object run() throws Exception {
          dtSecretManager.renewToken(token, "JobTracker");
          return null;
        }
      }, InvalidToken.class);
    } finally {
      dtSecretManager.stopThreads();
    }
  }

  @Test
  public void testRollMasterKey() throws Exception {
    TestDelegationTokenSecretManager dtSecretManager = 
      new TestDelegationTokenSecretManager(24*60*60*1000,
        10*1000,1*1000,3600000);
    try {
      dtSecretManager.startThreads();
      //generate a token and store the password
      Token<TestDelegationTokenIdentifier> token = generateDelegationToken(
          dtSecretManager, "SomeUser", "JobTracker");
      byte[] oldPasswd = token.getPassword();
      //store the length of the keys list
      int prevNumKeys = dtSecretManager.getAllKeys().length;
      
      dtSecretManager.rollMasterKey();
      
      //after rolling, the length of the keys list must increase
      int currNumKeys = dtSecretManager.getAllKeys().length;
      Assert.assertEquals((currNumKeys - prevNumKeys) >= 1, true);
      
      //after rolling, the token that was generated earlier must
      //still be valid (retrievePassword will fail if the token
      //is not valid)
      ByteArrayInputStream bi = 
        new ByteArrayInputStream(token.getIdentifier());
      TestDelegationTokenIdentifier identifier = 
        dtSecretManager.createIdentifier();
      identifier.readFields(new DataInputStream(bi));
      byte[] newPasswd = 
        dtSecretManager.retrievePassword(identifier);
      //compare the passwords
      Assert.assertEquals(oldPasswd, newPasswd);
    } finally {
      dtSecretManager.stopThreads();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDelegationTokenSelector() throws Exception {
    TestDelegationTokenSecretManager dtSecretManager = 
      new TestDelegationTokenSecretManager(24*60*60*1000,
        10*1000,1*1000,3600000);
    try {
      dtSecretManager.startThreads();
      AbstractDelegationTokenSelector ds = 
      new AbstractDelegationTokenSelector<TestDelegationTokenIdentifier>(KIND);
      
      //Creates a collection of tokens
      Token<TestDelegationTokenIdentifier> token1 = generateDelegationToken(
          dtSecretManager, "SomeUser1", "JobTracker");
      token1.setService(new Text("MY-SERVICE1"));
      
      Token<TestDelegationTokenIdentifier> token2 = generateDelegationToken(
          dtSecretManager, "SomeUser2", "JobTracker");
      token2.setService(new Text("MY-SERVICE2"));
      
      List<Token<TestDelegationTokenIdentifier>> tokens =
        new ArrayList<Token<TestDelegationTokenIdentifier>>();
      tokens.add(token1);
      tokens.add(token2);
      
      //try to select a token with a given service name (created earlier)
      Token<TestDelegationTokenIdentifier> t = 
        ds.selectToken(new Text("MY-SERVICE1"), tokens);
      Assert.assertEquals(t, token1);
    } finally {
      dtSecretManager.stopThreads();
    }
  }
  
  @Test
  public void testParallelDelegationTokenCreation() throws Exception {
    final TestDelegationTokenSecretManager dtSecretManager = 
        new TestDelegationTokenSecretManager(2000, 24 * 60 * 60 * 1000, 
            7 * 24 * 60 * 60 * 1000, 2000);
    try {
      dtSecretManager.startThreads();
      int numThreads = 100;
      final int numTokensPerThread = 100;
      class tokenIssuerThread implements Runnable {

        public void run() {
          for(int i =0;i <numTokensPerThread; i++) {
            generateDelegationToken(dtSecretManager, "auser", "arenewer");
            try {
              Thread.sleep(250); 
            } catch (Exception e) {
            }
          }
        }
      }
      Thread[] issuers = new Thread[numThreads];
      for (int i =0; i <numThreads; i++) {
        issuers[i] = new Daemon(new tokenIssuerThread());
        issuers[i].start();
      }
      for (int i =0; i <numThreads; i++) {
        issuers[i].join();
      }
      Map<TestDelegationTokenIdentifier, DelegationTokenInformation> tokenCache = dtSecretManager
          .getAllTokens();
      Assert.assertEquals(numTokensPerThread*numThreads, tokenCache.size());
      Iterator<TestDelegationTokenIdentifier> iter = tokenCache.keySet().iterator();
      while (iter.hasNext()) {
        TestDelegationTokenIdentifier id = iter.next();
        DelegationTokenInformation info = tokenCache.get(id);
        Assert.assertTrue(info != null);
        DelegationKey key = dtSecretManager.getKey(id);
        Assert.assertTrue(key != null);
        byte[] storedPassword = dtSecretManager.retrievePassword(id);
        byte[] password = dtSecretManager.createPassword(id, key);
        Assert.assertTrue(Arrays.equals(password, storedPassword));
      }
    } finally {
      dtSecretManager.stopThreads();
    }
  }
  
  @Test 
  public void testDelegationTokenNullRenewer() throws Exception {
    TestDelegationTokenSecretManager dtSecretManager = 
      new TestDelegationTokenSecretManager(24*60*60*1000,
        10*1000,1*1000,3600000);
    dtSecretManager.startThreads();
    TestDelegationTokenIdentifier dtId = new TestDelegationTokenIdentifier(new Text(
        "theuser"), null, null);
    Token<TestDelegationTokenIdentifier> token = new Token<TestDelegationTokenIdentifier>(
        dtId, dtSecretManager);
    Assert.assertTrue(token != null);
    try {
      dtSecretManager.renewToken(token, "");
      Assert.fail("Renewal must not succeed");
    } catch (IOException e) {
      //PASS
    }
  }

}
