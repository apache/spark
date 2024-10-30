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

import java.nio.ByteBuffer;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.network.client.MergedBlockMetaResponseCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.MergedBlockMetaRequest;

/**
 * Handler for sendRPC() messages sent by {@link org.apache.spark.network.client.TransportClient}s.
 */
public abstract class RpcHandler {

  private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayRpcCallback();
  private static final MergedBlockMetaReqHandler NOOP_MERGED_BLOCK_META_REQ_HANDLER =
    new NoopMergedBlockMetaReqHandler();

  /**
   * Receive a single RPC message. Any exception thrown while in this method will be sent back to
   * the client in string form as a standard RPC failure.
   *
   * Neither this method nor #receiveStream will be called in parallel for a single
   * TransportClient (i.e., channel).
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message The serialized bytes of the RPC.
   * @param callback Callback which should be invoked exactly once upon success or failure of the
   *                 RPC.
   */
  public abstract void receive(
      TransportClient client,
      ByteBuffer message,
      RpcResponseCallback callback);

  /**
   * Receive a single RPC message which includes data that is to be received as a stream. Any
   * exception thrown while in this method will be sent back to the client in string form as a
   * standard RPC failure.
   *
   * Neither this method nor #receive will be called in parallel for a single TransportClient
   * (i.e., channel).
   *
   * An error while reading data from the stream
   * ({@link org.apache.spark.network.client.StreamCallback#onData(String, ByteBuffer)})
   * will fail the entire channel.  A failure in "post-processing" the stream in
   * {@link org.apache.spark.network.client.StreamCallback#onComplete(String)} will result in an
   * rpcFailure, but the channel will remain active.
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param messageHeader The serialized bytes of the header portion of the RPC.  This is in meant
   *                      to be relatively small, and will be buffered entirely in memory, to
   *                      facilitate how the streaming portion should be received.
   * @param callback Callback which should be invoked exactly once upon success or failure of the
   *                 RPC.
   * @return a StreamCallback for handling the accompanying streaming data
   */
  public StreamCallbackWithID receiveStream(
      TransportClient client,
      ByteBuffer messageHeader,
      RpcResponseCallback callback) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the StreamManager which contains the state about which streams are currently being
   * fetched by a TransportClient.
   */
  public abstract StreamManager getStreamManager();

  /**
   * Receives an RPC message that does not expect a reply. The default implementation will
   * call "{@link #receive(TransportClient, ByteBuffer, RpcResponseCallback)}" and log a warning if
   * any of the callback methods are called.
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message The serialized bytes of the RPC.
   */
  public void receive(TransportClient client, ByteBuffer message) {
    receive(client, message, ONE_WAY_CALLBACK);
  }

  public MergedBlockMetaReqHandler getMergedBlockMetaReqHandler() {
    return NOOP_MERGED_BLOCK_META_REQ_HANDLER;
  }

  /**
   * Invoked when the channel associated with the given client is active.
   */
  public void channelActive(TransportClient client) { }

  /**
   * Invoked when the channel associated with the given client is inactive.
   * No further requests will come from this client.
   */
  public void channelInactive(TransportClient client) { }

  public void exceptionCaught(Throwable cause, TransportClient client) { }

  private static class OneWayRpcCallback implements RpcResponseCallback {

    private static final SparkLogger logger = SparkLoggerFactory.getLogger(OneWayRpcCallback.class);

    @Override
    public void onSuccess(ByteBuffer response) {
      logger.warn("Response provided for one-way RPC.");
    }

    @Override
    public void onFailure(Throwable e) {
      logger.error("Error response provided for one-way RPC.", e);
    }

  }

  /**
   * Handler for {@link MergedBlockMetaRequest}.
   *
   * @since 3.2.0
   */
  public interface MergedBlockMetaReqHandler {

    /**
     * Receive a {@link MergedBlockMetaRequest}.
     *
     * @param client A channel client which enables the handler to make requests back to the sender
     *     of this RPC.
     * @param mergedBlockMetaRequest Request for merged block meta.
     * @param callback Callback which should be invoked exactly once upon success or failure.
     */
    void receiveMergeBlockMetaReq(
        TransportClient client,
        MergedBlockMetaRequest mergedBlockMetaRequest,
        MergedBlockMetaResponseCallback callback);
  }

  /**
   * A Noop implementation of {@link MergedBlockMetaReqHandler}. This Noop implementation is used
   * by all the RPC handlers which don't eventually delegate the {@link MergedBlockMetaRequest} to
   * ExternalBlockHandler in the network-shuffle module.
   *
   * @since 3.2.0
   */
  private static class NoopMergedBlockMetaReqHandler implements MergedBlockMetaReqHandler {

    @Override
    public void receiveMergeBlockMetaReq(TransportClient client,
      MergedBlockMetaRequest mergedBlockMetaRequest, MergedBlockMetaResponseCallback callback) {
      // do nothing
    }
  }
}
