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

package org.apache.spark.network.protocol;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/**
 * A helper class that makes sure that all incoming messages provided in a list are sent to
 * the next stage of the pipeline in a thread-safe manner. This ensures that messages which
 * are broken down into multiple buffers for efficiency, such as ChunkFetchSuccess, are
 * processed by the pipeline stages properly, by being passed down as a list of buffers until
 * the very last handler (this one).
 */
public final class MessageMuxer extends ChannelOutboundHandlerAdapter {

  private final Object lock = new Object();

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
    throws Exception {
    @SuppressWarnings("unchecked")
    List<Object> in = (List<Object>) msg;

    synchronized (lock) {
      if (in.size() == 1) {
        ctx.write(in.get(0), promise);
      } else {
        for (int i = 0; i < in.size() - 1; i++) {
          ctx.write(in.get(i), ctx.voidPromise());
        }
        ctx.write(in.get(in.size() - 1), promise);
      }
    }
  }

}

