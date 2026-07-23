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

package org.apache.spark.network.client;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportClientSuite {

  @Test
  public void isActiveFalseWhenEventLoopIsShuttingDown() {
    // SPARK-58292: a client whose netty event loop has terminated must not be treated as active,
    // even though the TCP channel still reports open/active (nothing can close it -- closing runs
    // on the now-dead loop).
    Channel channel = mock(Channel.class);
    EventLoop eventLoop = mock(EventLoop.class);
    when(channel.eventLoop()).thenReturn(eventLoop);
    when(channel.isOpen()).thenReturn(true);
    when(channel.isActive()).thenReturn(true);
    TransportClient client = new TransportClient(channel, new TransportResponseHandler(channel));

    // Live event loop -> active.
    when(eventLoop.isShuttingDown()).thenReturn(false);
    assertTrue(client.isActive());

    // Terminated event loop -> NOT active, so it will be evicted from the pool and not reused.
    when(eventLoop.isShuttingDown()).thenReturn(true);
    assertFalse(client.isActive());
  }
}
