/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.server;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.netty.channel.Channel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.spark.network.TestManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;

public class OneForOneStreamManagerSuite {

  List<ManagedBuffer> managedBuffersToRelease = new ArrayList<>();

  @After
  public void tearDown() {
    managedBuffersToRelease.forEach(managedBuffer -> managedBuffer.release());
    managedBuffersToRelease.clear();
  }

  private ManagedBuffer getChunk(OneForOneStreamManager manager, long streamId, int chunkIndex) {
    ManagedBuffer chunk = manager.getChunk(streamId, chunkIndex);
    if (chunk != null) {
      managedBuffersToRelease.add(chunk);
    }
    return chunk;
  }

  @Test
  public void testMissingChunk() {
    OneForOneStreamManager manager = new OneForOneStreamManager();
    List<ManagedBuffer> buffers = new ArrayList<>();
    TestManagedBuffer buffer1 = Mockito.spy(new TestManagedBuffer(10));
    TestManagedBuffer buffer2 = Mockito.spy(new TestManagedBuffer(20));
    TestManagedBuffer buffer3 = Mockito.spy(new TestManagedBuffer(20));

    buffers.add(buffer1);
    // the nulls here are to simulate a file which goes missing before being read,
    // just as a defensive measure
    buffers.add(null);
    buffers.add(buffer2);
    buffers.add(null);
    buffers.add(buffer3);

    Channel dummyChannel = Mockito.mock(Channel.class, Mockito.RETURNS_SMART_NULLS);
    long streamId = manager.registerStream("appId", buffers.iterator(), dummyChannel);
    Assert.assertEquals(1, manager.numStreamStates());
    Assert.assertNotNull(getChunk(manager, streamId, 0));
    Assert.assertNull(getChunk(manager, streamId, 1));
    Assert.assertNotNull(getChunk(manager, streamId, 2));
    manager.connectionTerminated(dummyChannel);

    // loaded buffers are not released yet as in production a ManagedBuffer returned by getChunk()
    // would only be released by Netty after it is written to the network
    Mockito.verify(buffer1, Mockito.never()).release();
    Mockito.verify(buffer2, Mockito.never()).release();
    Mockito.verify(buffer3, Mockito.times(1)).release();
  }

  @Test
  public void managedBuffersAreFreedWhenConnectionIsClosed() {
    OneForOneStreamManager manager = new OneForOneStreamManager();
    List<ManagedBuffer> buffers = new ArrayList<>();
    TestManagedBuffer buffer1 = Mockito.spy(new TestManagedBuffer(10));
    TestManagedBuffer buffer2 = Mockito.spy(new TestManagedBuffer(20));
    buffers.add(buffer1);
    buffers.add(buffer2);

    Channel dummyChannel = Mockito.mock(Channel.class, Mockito.RETURNS_SMART_NULLS);
    manager.registerStream("appId", buffers.iterator(), dummyChannel);
    Assert.assertEquals(1, manager.numStreamStates());
    manager.connectionTerminated(dummyChannel);

    Mockito.verify(buffer1, Mockito.times(1)).release();
    Mockito.verify(buffer2, Mockito.times(1)).release();
    Assert.assertEquals(0, manager.numStreamStates());
  }

  @Test
  public void streamStatesAreFreedWhenConnectionIsClosedEvenIfBufferIteratorThrowsException() {
    OneForOneStreamManager manager = new OneForOneStreamManager();

    Iterator<ManagedBuffer> buffers = Mockito.mock(Iterator.class);
    Mockito.when(buffers.hasNext()).thenReturn(true);
    Mockito.when(buffers.next()).thenThrow(RuntimeException.class);

    ManagedBuffer mockManagedBuffer = Mockito.mock(ManagedBuffer.class);

    Iterator<ManagedBuffer> buffers2 = Mockito.mock(Iterator.class);
    Mockito.when(buffers2.hasNext()).thenReturn(true).thenReturn(true);
    Mockito.when(buffers2.next()).thenReturn(mockManagedBuffer).thenThrow(RuntimeException.class);

    Channel dummyChannel = Mockito.mock(Channel.class, Mockito.RETURNS_SMART_NULLS);
    manager.registerStream("appId", buffers, dummyChannel);
    manager.registerStream("appId", buffers2, dummyChannel);

    Assert.assertEquals(2, manager.numStreamStates());

    try {
      manager.connectionTerminated(dummyChannel);
      Assert.fail("connectionTerminated should throw exception when fails to release all buffers");

    } catch (RuntimeException e) {

      Mockito.verify(buffers, Mockito.times(1)).hasNext();
      Mockito.verify(buffers, Mockito.times(1)).next();

      Mockito.verify(buffers2, Mockito.times(2)).hasNext();
      Mockito.verify(buffers2, Mockito.times(2)).next();

      Mockito.verify(mockManagedBuffer, Mockito.times(1)).release();

      Assert.assertEquals(0, manager.numStreamStates());
    }
  }
}
