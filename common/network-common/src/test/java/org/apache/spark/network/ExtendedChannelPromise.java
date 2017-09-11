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
  public ChannelPromise sync() throws InterruptedException {
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