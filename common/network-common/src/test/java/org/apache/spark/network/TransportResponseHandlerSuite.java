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

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalChannel;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.MergedBlockMetaResponseCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.MergedBlockMetaSuccess;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.StreamFailure;
import org.apache.spark.network.protocol.StreamResponse;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.TransportFrameDecoder;

public class TransportResponseHandlerSuite {
  @Test
  public void handleSuccessfulFetch() throws Exception {
    StreamChunkId streamChunkId = new StreamChunkId(1, 0);

    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handler.addFetchRequest(streamChunkId, callback);
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new ChunkFetchSuccess(streamChunkId, new TestManagedBuffer(123)));
    verify(callback, times(1)).onSuccess(eq(0), any());
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void handleFailedFetch() throws Exception {
    StreamChunkId streamChunkId = new StreamChunkId(1, 0);
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handler.addFetchRequest(streamChunkId, callback);
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new ChunkFetchFailure(streamChunkId, "some error msg"));
    verify(callback, times(1)).onFailure(eq(0), any());
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void clearAllOutstandingRequests() throws Exception {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handler.addFetchRequest(new StreamChunkId(1, 0), callback);
    handler.addFetchRequest(new StreamChunkId(1, 1), callback);
    handler.addFetchRequest(new StreamChunkId(1, 2), callback);
    assertEquals(3, handler.numOutstandingRequests());

    handler.handle(new ChunkFetchSuccess(new StreamChunkId(1, 0), new TestManagedBuffer(12)));
    handler.exceptionCaught(new Exception("duh duh duhhhh"));

    // should fail both b2 and b3
    verify(callback, times(1)).onSuccess(eq(0), any());
    verify(callback, times(1)).onFailure(eq(1), any());
    verify(callback, times(1)).onFailure(eq(2), any());
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void handleSuccessfulRPC() throws Exception {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    handler.addRpcRequest(12345, callback);
    assertEquals(1, handler.numOutstandingRequests());

    // This response should be ignored.
    handler.handle(new RpcResponse(54321, new NioManagedBuffer(ByteBuffer.allocate(7))));
    assertEquals(1, handler.numOutstandingRequests());

    ByteBuffer resp = ByteBuffer.allocate(10);
    handler.handle(new RpcResponse(12345, new NioManagedBuffer(resp)));
    verify(callback, times(1)).onSuccess(eq(ByteBuffer.allocate(10)));
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void handleFailedRPC() throws Exception {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    handler.addRpcRequest(12345, callback);
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new RpcFailure(54321, "uh-oh!")); // should be ignored
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new RpcFailure(12345, "oh no"));
    verify(callback, times(1)).onFailure(any());
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void testActiveStreams() throws Exception {
    Channel c = new LocalChannel();
    c.pipeline().addLast(TransportFrameDecoder.HANDLER_NAME, new TransportFrameDecoder());
    TransportResponseHandler handler = new TransportResponseHandler(c);

    StreamResponse response = new StreamResponse("stream", 1234L, null);
    StreamCallback cb = mock(StreamCallback.class);
    handler.addStreamCallback("stream", cb);
    assertEquals(1, handler.numOutstandingRequests());
    handler.handle(response);
    assertEquals(1, handler.numOutstandingRequests());
    handler.deactivateStream();
    assertEquals(0, handler.numOutstandingRequests());

    StreamFailure failure = new StreamFailure("stream", "uh-oh");
    handler.addStreamCallback("stream", cb);
    assertEquals(1, handler.numOutstandingRequests());
    handler.handle(failure);
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void failOutstandingStreamCallbackOnClose() throws Exception {
    Channel c = new LocalChannel();
    c.pipeline().addLast(TransportFrameDecoder.HANDLER_NAME, new TransportFrameDecoder());
    TransportResponseHandler handler = new TransportResponseHandler(c);

    StreamCallback cb = mock(StreamCallback.class);
    handler.addStreamCallback("stream-1", cb);
    handler.channelInactive();

    verify(cb).onFailure(eq("stream-1"), isA(IOException.class));
  }

  @Test
  public void failOutstandingStreamCallbackOnException() throws Exception {
    Channel c = new LocalChannel();
    c.pipeline().addLast(TransportFrameDecoder.HANDLER_NAME, new TransportFrameDecoder());
    TransportResponseHandler handler = new TransportResponseHandler(c);

    StreamCallback cb = mock(StreamCallback.class);
    handler.addStreamCallback("stream-1", cb);
    handler.exceptionCaught(new IOException("Oops!"));

    verify(cb).onFailure(eq("stream-1"), isA(IOException.class));
  }

  @Test
  public void failStreamCallbackWhenInstallingInterceptorFails() throws Exception {
    // With no TransportFrameDecoder in the pipeline, the decoder lookup in the StreamResponse
    // branch returns null and installing the interceptor throws. The handler must fail the
    // callback (so the caller does not hang) and close the channel rather than reuse a
    // connection it can no longer decode.
    Channel c = mock(Channel.class);
    ChannelPipeline pipeline = mock(ChannelPipeline.class);
    when(c.pipeline()).thenReturn(pipeline);
    when(pipeline.get(TransportFrameDecoder.HANDLER_NAME)).thenReturn(null);
    TransportResponseHandler handler = new TransportResponseHandler(c);

    StreamCallback cb = mock(StreamCallback.class);
    handler.addStreamCallback("stream", cb);
    assertEquals(1, handler.numOutstandingRequests());

    // byteCount > 0 so the handler takes the interceptor-installation path.
    handler.handle(new StreamResponse("stream", 1234L, null));

    verify(cb, times(1)).onFailure(eq("stream"), isA(NullPointerException.class));
    verify(c, times(1)).close();
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void streamResponseWithMismatchedStreamIdThrows() throws Exception {
    // The FIFO streamCallbacks queue matches responses to callbacks by poll() order, which is
    // correct only if a response's streamId equals the head-of-queue callback's registered streamId
    // (the server answers StreamRequests in send order; SPARK-11265). This asserts that invariant:
    // a StreamResponse whose streamId does not match means the queue is desynced, and delivering it
    // would feed the wrong block's bytes to the callback. The check must throw instead
    // (SPARK-59142); throwing propagates to Netty's exceptionCaught, which closes the desynced
    // connection and fails its remaining outstanding requests.
    // A mismatch is simulated by registering "stream-A" and handling a response for "stream-B".
    Channel c = new LocalChannel();
    c.pipeline().addLast(TransportFrameDecoder.HANDLER_NAME, new TransportFrameDecoder());
    TransportResponseHandler handler = new TransportResponseHandler(c);

    StreamCallback cb = mock(StreamCallback.class);
    handler.addStreamCallback("stream-A", cb);

    // A response for a different streamId arrives at the head of the FIFO queue.
    StreamResponse mismatched = new StreamResponse("stream-B", 1234L, null);
    IllegalStateException e = assertThrows(IllegalStateException.class,
      () -> handler.handle(mismatched));
    assertTrue(e.getMessage().contains("desynced"),
      "expected a desync error, got: " + e.getMessage());
    // The mismatched response must NOT have been delivered to the wrong callback as success...
    verify(cb, never()).onComplete(any());
    // ...and the polled callback is failed (with its OWN streamId) so its caller does not hang.
    verify(cb, times(1)).onFailure(eq("stream-A"), isA(IOException.class));
  }

  @Test
  public void streamFailureWithMismatchedStreamIdThrows() throws Exception {
    Channel c = new LocalChannel();
    c.pipeline().addLast(TransportFrameDecoder.HANDLER_NAME, new TransportFrameDecoder());
    TransportResponseHandler handler = new TransportResponseHandler(c);

    StreamCallback cb = mock(StreamCallback.class);
    handler.addStreamCallback("stream-A", cb);

    StreamFailure mismatched = new StreamFailure("stream-B", "uh-oh");
    IllegalStateException e = assertThrows(IllegalStateException.class,
      () -> handler.handle(mismatched));
    assertTrue(e.getMessage().contains("desynced"),
      "expected a desync error, got: " + e.getMessage());
    // The failure must NOT be routed under the wrong (response) streamId; the polled callback is
    // failed under its OWN streamId instead, so its caller does not hang.
    verify(cb, never()).onFailure(eq("stream-B"), any());
    verify(cb, times(1)).onFailure(eq("stream-A"), isA(IOException.class));
  }

  @Test
  public void desyncTearsDownConnectionAndFailsAllOutstandingRequests() throws Exception {
    // Upstream impact of the streamId assert, driven through the production channel-handler path.
    // A mismatched StreamResponse is written through a real TransportChannelHandler on an
    // EmbeddedChannel: handle() throws, Netty routes the throwable to
    // TransportChannelHandler.exceptionCaught, which fails EVERY remaining outstanding request on
    // the channel and closes it. This shows the meaning for callers: the whole (poisoned)
    // connection is torn down and all its in-flight requests -- the mismatched stream AND any
    // innocent concurrent chunk fetch sharing the channel -- are failed retriably (the thrown
    // exception carries an IOException cause, which RetryingBlockTransferor retries), and the
    // channel is closed so the requests retry in order on a fresh connection. None receive data.
    TransportConf conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
    TransportContext context = new TransportContext(conf, new NoOpRpcHandler());
    try {
      // No TransportFrameDecoder in the pipeline: the desync is detected in handle() before any
      // interceptor/frame-decoder interaction, and the decoder would reject a non-ByteBuf inbound.
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
      TransportClient client = new TransportClient(channel, responseHandler);
      TransportRequestHandler requestHandler = new TransportRequestHandler(
        channel, client, new NoOpRpcHandler(), Long.MAX_VALUE, null);
      TransportChannelHandler channelHandler = new TransportChannelHandler(
        client, responseHandler, requestHandler, conf.connectionTimeoutMs(), false, false, context);
      channel.pipeline().addLast("channelHandler", channelHandler);

      // An innocent chunk fetch is in flight on the same connection...
      StreamChunkId chunkId = new StreamChunkId(1, 0);
      ChunkReceivedCallback chunkCb = mock(ChunkReceivedCallback.class);
      responseHandler.addFetchRequest(chunkId, chunkCb);
      // ...and a stream fetch for "stream-A".
      StreamCallback streamCb = mock(StreamCallback.class);
      responseHandler.addStreamCallback("stream-A", streamCb);
      assertEquals(2, responseHandler.numOutstandingRequests());
      assertTrue(channel.isOpen());

      // A StreamResponse for the wrong streamId arrives through the real inbound pipeline.
      channel.writeInbound(new StreamResponse("stream-B", 1234L, null));

      // The desynced (polled) stream callback was failed inline with an IOException (not delivered
      // to) so its caller does not hang.
      ArgumentCaptor<Throwable> streamErr = ArgumentCaptor.forClass(Throwable.class);
      verify(streamCb, times(1)).onFailure(eq("stream-A"), streamErr.capture());
      verify(streamCb, never()).onComplete(any());
      assertTrue(streamErr.getValue() instanceof IOException,
        "polled stream callback should fail with an IOException, got: " + streamErr.getValue());

      // exceptionCaught failed the innocent concurrent chunk fetch too. Its failure carries an
      // IOException cause, so RetryingBlockTransferor treats it as retryable, not permanent.
      ArgumentCaptor<Throwable> chunkErr = ArgumentCaptor.forClass(Throwable.class);
      verify(chunkCb, times(1)).onFailure(eq(0), chunkErr.capture());
      assertTrue(hasIOExceptionCause(chunkErr.getValue()),
        "collateral fetch failure should carry an IOException cause, got: " + chunkErr.getValue());

      // The poisoned connection was torn down and nothing is left outstanding.
      assertFalse(channel.isOpen());
      assertEquals(0, responseHandler.numOutstandingRequests());
    } finally {
      context.close();
    }
  }

  private static boolean hasIOExceptionCause(Throwable t) {
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      if (cur instanceof IOException) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void streamResponseWithMatchingStreamIdIsDelivered() throws Exception {
    // Regression guard: the streamId check must not disturb the normal in-order case. A response
    // whose streamId matches the head-of-queue callback is handled exactly as before.
    Channel c = new LocalChannel();
    c.pipeline().addLast(TransportFrameDecoder.HANDLER_NAME, new TransportFrameDecoder());
    TransportResponseHandler handler = new TransportResponseHandler(c);

    StreamCallback cb = mock(StreamCallback.class);
    handler.addStreamCallback("stream", cb);
    assertEquals(1, handler.numOutstandingRequests());

    // byteCount == 0 -> the handler calls onComplete inline (no interceptor install needed).
    handler.handle(new StreamResponse("stream", 0L, null));
    verify(cb, times(1)).onComplete(eq("stream"));
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void handleSuccessfulMergedBlockMeta() throws Exception {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    MergedBlockMetaResponseCallback callback = mock(MergedBlockMetaResponseCallback.class);
    handler.addRpcRequest(13, callback);
    assertEquals(1, handler.numOutstandingRequests());

    // This response should be ignored.
    handler.handle(new MergedBlockMetaSuccess(22, 2,
      new NioManagedBuffer(ByteBuffer.allocate(7))));
    assertEquals(1, handler.numOutstandingRequests());

    ByteBuffer resp = ByteBuffer.allocate(10);
    handler.handle(new MergedBlockMetaSuccess(13, 2, new NioManagedBuffer(resp)));
    ArgumentCaptor<NioManagedBuffer> bufferCaptor = ArgumentCaptor.forClass(NioManagedBuffer.class);
    verify(callback, times(1)).onSuccess(eq(2), bufferCaptor.capture());
    assertEquals(resp, bufferCaptor.getValue().nioByteBuffer());
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void handleFailedMergedBlockMeta() throws Exception {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    MergedBlockMetaResponseCallback callback = mock(MergedBlockMetaResponseCallback.class);
    handler.addRpcRequest(51, callback);
    assertEquals(1, handler.numOutstandingRequests());

    // This response should be ignored.
    handler.handle(new RpcFailure(6, "failed"));
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new RpcFailure(51, "failed"));
    verify(callback, times(1)).onFailure(any());
    assertEquals(0, handler.numOutstandingRequests());
  }
}
