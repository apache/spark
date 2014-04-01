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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * This tests verifies that new DFSClients handle exceptions that would be 
 * thrown by older NameNodes (pre cdh3u3, pre apache 0.21). 
 * 
 * This is a CDH3 specific backwards compatibility test.
 */
public class TestDfsClientCreateParentCompatibility {
  public static final Log LOG = LogFactory
    .getLog(TestDfsClientCreateParentCompatibility.class);

  @Test
  public void testCreateWithoutDirsCompatibility() throws IOException {
    Configuration conf = new Configuration();
    NameNode nn = mock(NameNode.class);

    final String err = 
      "java.io.IOException: " +
      "java.lang.NoSuchMethodException: org.apache.hadoop.hdfs." +
      "protocol.ClientProtocol.create(java.lang.String, " +
      "org.apache.hadoop.fs.permission.FsPermission, " +
      "java.lang.String, boolean, boolean, short, long)";
    final AtomicInteger newCount = new AtomicInteger();
    Answer<Void> newCallCounter = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        LOG.info("New Call "+ Arrays.toString(invocation.getArguments()));
        newCount.incrementAndGet();
        throw new RemoteException(IOException.class.getName(), err);
      }
    };

    final AtomicInteger oldCount = new AtomicInteger();
    Answer<Void> oldCallCounter = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        LOG.info("Old Call "+ Arrays.toString(invocation.getArguments()));
        oldCount.incrementAndGet();
        return null;
      }
    };
    
    
    // new api client call
    doAnswer(newCallCounter).when(nn)
      .create((String)anyObject(), (FsPermission)anyObject(),
          (String)anyObject(), anyBoolean(), eq(false), anyShort(), anyLong());
    // old api client call
    doAnswer(oldCallCounter).when(nn)
      .create((String)anyObject(), (FsPermission)anyObject(),
        (String)anyObject(), anyBoolean(), anyShort(), anyLong());

    DFSClient client = new DFSClient(null, nn, conf, null);
    
    boolean createParent = false;
    client.create("foo", null, false, createParent, (short) 1, 512, null, 512);
    client.create("bar", null, false, createParent, (short) 1, 512, null, 512);
    client.create("baz", null, false, createParent, (short) 1, 512, null, 512);
    
    // no exception was thrown, three calls to the old verison.
    assertEquals(3, oldCount.get());
    assertEquals(1, newCount.get());   
  }
  
  @Test(expected=IOException.class)
  public void testCreateWithException() throws IOException {
    Configuration conf = new Configuration();
    NameNode nn = mock(NameNode.class);

    // new api client call
    Exception e = new RemoteException(IOException.class.getName(),
        "Other remote exception"); 
    
    doThrow(e).when(nn)
      .create((String)anyObject(), (FsPermission)anyObject(),
          (String)anyObject(), anyBoolean(), eq(false), anyShort(), anyLong());

    DFSClient client = new DFSClient(null, nn, conf, null);

    boolean createParent = false;
    client.create("foo", null, false, createParent, (short) 1, 512, null, 512);
    fail("Expected an IOException");
  }
  
  /**
   * Small testing program that attemps to call createNonRecursive.
   */
  public static void main(String argv[]) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path p = new Path(argv[0]);
    FSDataOutputStream out = fs.createNonRecursive(p, true, 512, (short) 1,
        512, null);
    out.close();
    fs.close();
  }

}
