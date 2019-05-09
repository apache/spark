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

package org.apache.spark.network;

import java.util.ArrayList;
import java.util.List;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

class ExtendedChannelPromise extends DefaultChannelPromise {

  private List<GenericFutureListener<Future<Void>>> listeners = new ArrayList<>();
  private boolean success;

  ExtendedChannelPromise(Channel channel) {
    super(channel);
    success = false;
  }

  @Override
  public ChannelPromise addListener(
      GenericFutureListener<? extends Future<? super Void>> listener) {
    @SuppressWarnings("unchecked")
    GenericFutureListener<Future<Void>> gfListener =
        (GenericFutureListener<Future<Void>>) listener;
    listeners.add(gfListener);
    return super.addListener(listener);
  }

  @Override
  public boolean isSuccess() {
    return success;
  }

  @Override
  public ChannelPromise await() throws InterruptedException {
    return this;
  }

  public void finish(boolean success) {
    this.success = success;
    listeners.forEach(listener -> {
      try {
        listener.operationComplete(this);
      } catch (Exception e) {
        // do nothing
      }
    });
  }
}
