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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;

import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;

/**
 * Tests that RPC handlers behind an authentication bootstrap fail closed: no chunk or stream
 * request may be served on a channel that has not completed authentication, regardless of how
 * lax the delegate StreamManager's own authorization checks are (the default
 * StreamManager.checkAuthorization is a no-op).
 */
public class AbstractAuthRpcHandlerSuite {

  private static class TestAuthRpcHandler extends AbstractAuthRpcHandler {
    TestAuthRpcHandler(RpcHandler delegate) {
      super(delegate);
    }

    @Override
    protected boolean doAuthChallenge(
        TransportClient client,
        ByteBuffer message,
        RpcResponseCallback callback) {
      return true;
    }
  }

  @Test
  public void testStreamManagerFailsClosedBeforeAuth() {
    RpcHandler delegate = mock(RpcHandler.class);
    StreamManager delegateManager = mock(StreamManager.class);
    when(delegate.getStreamManager()).thenReturn(delegateManager);
    AbstractAuthRpcHandler handler = new TestAuthRpcHandler(delegate);

    StreamManager sm = handler.getStreamManager();
    TransportClient client = mock(TransportClient.class);

    assertThrows(SecurityException.class, () -> sm.openStream("/jars/app.jar"));
    assertThrows(SecurityException.class, () -> sm.getChunk(0L, 0));
    assertThrows(SecurityException.class, () -> sm.checkAuthorization(client, 0L));
    assertThrows(SecurityException.class, () -> sm.checkAuthorization(client, "/jars/app.jar"));
    verify(delegateManager, never()).openStream(anyString());
    verify(delegateManager, never()).getChunk(anyLong(), anyInt());

    // Lifecycle callbacks still reach the delegate so per-channel state can be cleaned up
    // even for channels that never authenticated.
    Channel channel = mock(Channel.class);
    sm.connectionTerminated(channel);
    verify(delegateManager).connectionTerminated(channel);
  }

  @Test
  public void testStreamManagerDelegatesAfterAuth() {
    RpcHandler delegate = mock(RpcHandler.class);
    StreamManager delegateManager = mock(StreamManager.class);
    when(delegate.getStreamManager()).thenReturn(delegateManager);
    AbstractAuthRpcHandler handler = new TestAuthRpcHandler(delegate);
    StreamManager sm = handler.getStreamManager();

    // Complete the (test) auth handshake; the wrapper obtained pre-auth must observe it.
    handler.receive(mock(TransportClient.class), ByteBuffer.allocate(0),
      mock(RpcResponseCallback.class));
    assertTrue(handler.isAuthenticated());

    sm.openStream("/jars/app.jar");
    verify(delegateManager).openStream("/jars/app.jar");
    sm.getChunk(1L, 2);
    verify(delegateManager).getChunk(1L, 2);
    TransportClient client = mock(TransportClient.class);
    sm.checkAuthorization(client, 1L);
    verify(delegateManager).checkAuthorization(client, 1L);
  }
}
