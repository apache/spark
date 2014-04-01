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
package org.apache.hadoop.tools;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Before;
import org.junit.Test;

public class TestDelegationTokenFetcher {
  private DistributedFileSystem dfs;
  private DataOutputStream out;
  private UserGroupInformation ugi;
  private Configuration conf;

  @Before 
  public void init() {
    dfs = mock(DistributedFileSystem.class);
    out = mock(DataOutputStream.class);
    ugi = mock(UserGroupInformation.class);
    conf = new Configuration();
  }
  
  /**
   * Verify that when the DelegationTokenFetcher runs, it talks to the Namenode,
   * pulls out the correct user's token and successfully serializes it to disk.
   */
  @Test
  public void expectedTokenIsRetrievedFromDFS() throws Exception {
    final String LONG_NAME = "TheDoctor@TARDIS";
    final String SHORT_NAME = "TheDoctor";
    final String SERVICE_VALUE = "localhost:2005";
    URI uri = new URI("hdfs://" + SERVICE_VALUE);
    FileSystem.setDefaultUri(conf, uri);
    
    // Mock out the user's long and short names.
    when(ugi.getUserName()).thenReturn(LONG_NAME);
    when(ugi.getShortUserName()).thenReturn(SHORT_NAME);
    
    // Create a token for the fetcher to fetch, wire NN to return it when asked
    // for this particular user.
    Token<DelegationTokenIdentifier> t = new Token<DelegationTokenIdentifier>();
    when(dfs.getDelegationToken(eq(new Text(LONG_NAME)))).thenReturn(t);
 
    // Now, actually let the TokenFetcher go fetch the token.
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    out = new DataOutputStream(baos);
    new DelegationTokenFetcher(dfs, out, ugi, conf).go();
    
    // now read the data back in and verify correct values
    Credentials ts = new Credentials();
    DataInputStream dis = 
      new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    
    ts.readTokenStorageStream(dis);
    Token<? extends TokenIdentifier> newToken = ts.getToken(new Text(SHORT_NAME));
    
    assertEquals("Should only be one token in storage", ts.numberOfTokens(), 1);
    assertEquals("Service value should have survived", 
        "127.0.0.1:2005", newToken.getService().toString());
  }

  private void checkWithNullParam(String s) {
    try {
      new DelegationTokenFetcher(dfs, out, ugi, conf);
    } catch (IllegalArgumentException iae) {
      assertEquals("Expected exception message not received", 
          s + " cannot be null.", iae.getMessage());
      return; // received expected exception. We're good.
    }
    fail("null parameter should have failed.");
  }
  
  @Test
  public void dfsCannotBeNull() {
    dfs = null;
    String s = "dfs";
    checkWithNullParam(s);
  }

  @Test
  public void dosCannotBeNull() {
    out = null;
    String s = "out";
    checkWithNullParam(s);
  }
  
  @Test
  public void ugiCannotBeNull() {
    ugi = null;
    String s = "ugi";
    checkWithNullParam(s);
  }
}
