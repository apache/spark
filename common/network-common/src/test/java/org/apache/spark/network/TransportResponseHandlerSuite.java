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
import io.netty.channel.local.LocalChannel;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.MergedBlockMetaResponseCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.MergedBlockMetaSuccess;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.StreamFailure;
import org.apache.spark.network.protocol.StreamResponse;
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

  // A success callback that throws has already been removed from the outstanding requests, so a
  // later connection close cannot fail it. The handler must fail it itself, release the body, and
  // rethrow so the connection is still torn down.

  @Test
  public void failFetchWhenSuccessCallbackThrows() throws Exception {
    for (Throwable error : new Throwable[] {
        new RuntimeException("boom"), new OutOfMemoryError("Direct buffer memory")}) {
      StreamChunkId streamChunkId = new StreamChunkId(1, 3);
      TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
      ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
      doThrow(error).when(callback).onSuccess(anyInt(), any());
      handler.addFetchRequest(streamChunkId, callback);
      ManagedBuffer body = spy(new NioManagedBuffer(ByteBuffer.allocate(8)));

      Throwable thrown = assertThrows(Throwable.class,
        () -> handler.handle(new ChunkFetchSuccess(streamChunkId, body)));
      assertSame(error, thrown);
      verify(callback, times(1)).onFailure(eq(3), same(error));
      verify(body, times(1)).release();
      assertEquals(0, handler.numOutstandingRequests());
    }
  }

  /**
   * What onFailure throws in the tests below: an Exception, an Error, or the success-callback
   * error itself. Either way the success-callback error must be the one rethrown.
   */
  private static Throwable[] failureCallbackErrors(Throwable successCallbackError) {
    return new Throwable[] {
      new RuntimeException("onFailure"), new OutOfMemoryError("onFailure"), successCallbackError};
  }

  private static void assertSuppressedFailureCallbackError(
      Throwable successCallbackError, Throwable failureCallbackError) {
    Throwable[] expected = failureCallbackError == successCallbackError ?
      new Throwable[0] : new Throwable[] {failureCallbackError};
    assertArrayEquals(expected, successCallbackError.getSuppressed());
  }

  @Test
  public void rethrowSuccessCallbackErrorWhenFailureCallbackAlsoThrows() throws Exception {
    for (int i = 0; i < 3; i++) {
      StreamChunkId streamChunkId = new StreamChunkId(1, 0);
      TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
      ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
      RuntimeException error = new RuntimeException("onSuccess");
      Throwable failureError = failureCallbackErrors(error)[i];
      doThrow(error).when(callback).onSuccess(anyInt(), any());
      doThrow(failureError).when(callback).onFailure(anyInt(), any());
      handler.addFetchRequest(streamChunkId, callback);
      ManagedBuffer body = spy(new NioManagedBuffer(ByteBuffer.allocate(8)));

      assertSame(error, assertThrows(RuntimeException.class,
        () -> handler.handle(new ChunkFetchSuccess(streamChunkId, body))));
      assertSuppressedFailureCallbackError(error, failureError);
      verify(body, times(1)).release();
    }
  }

  @Test
  public void failRpcWhenSuccessCallbackThrows() throws Exception {
    for (Throwable error : new Throwable[] {
        new RuntimeException("boom"), new OutOfMemoryError("Java heap space")}) {
      TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
      RpcResponseCallback callback = mock(RpcResponseCallback.class);
      doThrow(error).when(callback).onSuccess(any());
      handler.addRpcRequest(12345, callback);
      ManagedBuffer body = spy(new NioManagedBuffer(ByteBuffer.allocate(8)));

      assertSame(error, assertThrows(Throwable.class,
        () -> handler.handle(new RpcResponse(12345, body))));
      verify(callback, times(1)).onFailure(same(error));
      verify(body, times(1)).release();
      assertEquals(0, handler.numOutstandingRequests());
    }
  }

  @Test
  public void rethrowRpcSuccessCallbackErrorWhenFailureCallbackAlsoThrows() throws Exception {
    for (int i = 0; i < 3; i++) {
      TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
      RpcResponseCallback callback = mock(RpcResponseCallback.class);
      RuntimeException error = new RuntimeException("onSuccess");
      Throwable failureError = failureCallbackErrors(error)[i];
      doThrow(error).when(callback).onSuccess(any());
      doThrow(failureError).when(callback).onFailure(any());
      handler.addRpcRequest(12345, callback);
      ManagedBuffer body = spy(new NioManagedBuffer(ByteBuffer.allocate(8)));

      assertSame(error, assertThrows(RuntimeException.class,
        () -> handler.handle(new RpcResponse(12345, body))));
      assertSuppressedFailureCallbackError(error, failureError);
      verify(body, times(1)).release();
    }
  }

  @Test
  public void failMergedBlockMetaWhenSuccessCallbackThrows() throws Exception {
    for (Throwable error : new Throwable[] {
        new RuntimeException("boom"), new OutOfMemoryError("Java heap space")}) {
      TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
      MergedBlockMetaResponseCallback callback = mock(MergedBlockMetaResponseCallback.class);
      doThrow(error).when(callback).onSuccess(anyInt(), any());
      handler.addRpcRequest(13, callback);
      ManagedBuffer body = spy(new NioManagedBuffer(ByteBuffer.allocate(8)));

      assertSame(error, assertThrows(Throwable.class,
        () -> handler.handle(new MergedBlockMetaSuccess(13, 2, body))));
      verify(callback, times(1)).onFailure(same(error));
      verify(body, times(1)).release();
      assertEquals(0, handler.numOutstandingRequests());
    }
  }

  @Test
  public void rethrowMergedBlockMetaSuccessCallbackErrorWhenFailureCallbackAlsoThrows()
      throws Exception {
    for (int i = 0; i < 3; i++) {
      TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
      MergedBlockMetaResponseCallback callback = mock(MergedBlockMetaResponseCallback.class);
      RuntimeException error = new RuntimeException("onSuccess");
      Throwable failureError = failureCallbackErrors(error)[i];
      doThrow(error).when(callback).onSuccess(anyInt(), any());
      doThrow(failureError).when(callback).onFailure(any());
      handler.addRpcRequest(13, callback);
      ManagedBuffer body = spy(new NioManagedBuffer(ByteBuffer.allocate(8)));

      assertSame(error, assertThrows(RuntimeException.class,
        () -> handler.handle(new MergedBlockMetaSuccess(13, 2, body))));
      assertSuppressedFailureCallbackError(error, failureError);
      verify(body, times(1)).release();
    }
  }

  @Test
  public void failEmptyStreamWhenCompletionCallbackThrows() throws Exception {
    for (Throwable error : new Throwable[] {
        new IOException("boom"), new OutOfMemoryError("Java heap space")}) {
      TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
      StreamCallback callback = mock(StreamCallback.class);
      doThrow(error).when(callback).onComplete("stream");
      handler.addStreamCallback("stream", callback);

      assertSame(error, assertThrows(Throwable.class,
        () -> handler.handle(new StreamResponse("stream", 0L, null))));
      verify(callback, times(1)).onFailure(eq("stream"), same(error));
      assertEquals(0, handler.numOutstandingRequests());
    }
  }
}
