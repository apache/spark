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


package org.apache.hadoop.security;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Collection;

import static org.mockito.Mockito.mock;

import javax.crypto.KeyGenerator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestJobCredentials {
  private static final String DEFAULT_HMAC_ALGORITHM = "HmacSHA1";
  private static final File tmpDir =
    new File(System.getProperty("test.build.data", "/tmp"), "mapred");  
    
  @Before
  public void setUp() {
    tmpDir.mkdir();
  }
  
  @SuppressWarnings("unchecked")
  @Test 
  public <T extends TokenIdentifier> void testReadWriteStorage() 
  throws IOException, NoSuchAlgorithmException{
    // create tokenStorage Object
    Credentials ts = new Credentials();
    
    Token<T> token1 = new Token();
    Token<T> token2 = new Token();
    Text service1 = new Text("service1");
    Text service2 = new Text("service2");
    Collection<Text> services = new ArrayList<Text>();
    
    services.add(service1);
    services.add(service2);
    
    token1.setService(service1);
    token2.setService(service2);
    ts.addToken(new Text("sometoken1"), token1);
    ts.addToken(new Text("sometoken2"), token2);
    
    // create keys and put it in
    final KeyGenerator kg = KeyGenerator.getInstance(DEFAULT_HMAC_ALGORITHM);
    String alias = "alias";
    Map<Text, byte[]> m = new HashMap<Text, byte[]>(10);
    for(int i=0; i<10; i++) {
      Key key = kg.generateKey();
      m.put(new Text(alias+i), key.getEncoded());
      ts.addSecretKey(new Text(alias+i), key.getEncoded());
    }
   
    // create file to store
    File tmpFileName = new File(tmpDir, "tokenStorageTest");
    DataOutputStream dos = 
      new DataOutputStream(new FileOutputStream(tmpFileName));
    ts.write(dos);
    dos.close();
    
    // open and read it back
    DataInputStream dis = 
      new DataInputStream(new FileInputStream(tmpFileName));    
    ts = new Credentials();
    ts.readFields(dis);
    dis.close();
    
    // get the tokens and compare the services
    Collection<Token<? extends TokenIdentifier>> list = ts.getAllTokens();
    assertEquals("getAllTokens should return collection of size 2", 
        list.size(), 2);
    boolean foundFirst = false;
    boolean foundSecond = false;
    for (Token<? extends TokenIdentifier> token : list) {
      if (token.getService().equals(service1)) {
        foundFirst = true;
      }
      if (token.getService().equals(service2)) {
        foundSecond = true;
      }
    }
    assertTrue("Tokens for services service1 and service2 must be present", 
        foundFirst && foundSecond);
    // compare secret keys
    int mapLen = m.size();
    assertEquals("wrong number of keys in the Storage", 
        mapLen, ts.numberOfSecretKeys());
    for(Text a : m.keySet()) {
      byte [] kTS = ts.getSecretKey(a);
      byte [] kLocal = m.get(a);
      assertTrue("keys don't match for " + a, 
          WritableComparator.compareBytes(kTS, 0, kTS.length, kLocal,
              0, kLocal.length)==0);
    }
  }
 }
