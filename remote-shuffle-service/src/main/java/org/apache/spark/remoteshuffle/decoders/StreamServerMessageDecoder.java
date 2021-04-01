/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.decoders;

import org.apache.spark.remoteshuffle.common.DataBlockHeader;
import org.apache.spark.remoteshuffle.exceptions.RssException;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidDataException;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.metrics.NettyServerSideMetricGroupContainer;
import org.apache.spark.remoteshuffle.metrics.ServerHandlerMetrics;
import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import org.apache.spark.remoteshuffle.util.LogUtils;
import org.apache.spark.remoteshuffle.util.NettyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.spark.remoteshuffle.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/***
 * This class is per client server connection, and contains state (data) per that connection.
 * This decoder is shared in the stream server side to decode message for upload server, download server
 * and control server. It will be only one of these roles based on the header message.
 */
public class StreamServerMessageDecoder extends ByteToMessageDecoder {
  private static final Logger logger = LoggerFactory.getLogger(StreamServerMessageDecoder.class);

  private static final int INVALID_CONTROL_MESSAGE_TYPE = 0;
  private static final int INVALID_PARTITION_ID = -1;
  private static final int INVALID_SESSION_ID = -1;
  private static final int INVALID_TASK_ATTEMPT_ID = -1;

  private static NettyServerSideMetricGroupContainer<ServerHandlerMetrics> metricGroupContainer =
      new NettyServerSideMetricGroupContainer<>(ServerHandlerMetrics::new);

  private enum State {
    READ_MAGIC_BYTE_AND_VERSION,
    READ_MESSAGE_TYPE,
    READ_CONTROL_MESSAGE_LEN,
    READ_CONTROL_MESSAGE_BYTES,
    READ_TASK_ATTEMPT_ID,
    READ_DATA_MESSAGE_LEN,
    READ_DATA_MESSAGE_BYTES,
  }

  private State state = State.READ_MAGIC_BYTE_AND_VERSION;
  private int requiredBytes = 0;
  private final ByteBuf shuffleDataBuffer;
  private int controlMessageType = INVALID_CONTROL_MESSAGE_TYPE;
  private int partitionId = INVALID_PARTITION_ID;
  private long taskAttemptId = INVALID_TASK_ATTEMPT_ID;
  // store bytes for taskAttemptId to speed up serialization in DataBlockHeader.serializeToBytes
  private final byte[] taskAttemptIdBytes = new byte[Long.BYTES];

  private long startTime = System.currentTimeMillis();
  private long numIncomingBytes = 0;

  private String user = "uninitialized";

  private ServerHandlerMetrics metrics = metricGroupContainer.getMetricGroup(user);

  public StreamServerMessageDecoder(ByteBuf shuffleDataBuffer) {
    super();
    this.shuffleDataBuffer = shuffleDataBuffer;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);

    if (shuffleDataBuffer != null) {
      shuffleDataBuffer.release();
    }

    metricGroupContainer.removeMetricGroup(user);

    String connectionInfo = NettyUtils.getServerConnectionInfo(ctx);
    double dataSpeed = LogUtils
        .calculateMegaBytesPerSecond(System.currentTimeMillis() - startTime, numIncomingBytes);
    logger
        .debug("Decoder finished, total bytes: {}, speed: {} mbs, {}", numIncomingBytes, dataSpeed,
            connectionInfo);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx,
                        ByteBuf in,
                        List<Object> out) throws Exception {
    int readableBytes = in.readableBytes();
    try {
      decodeImpl(ctx, in, out);
    } finally {
      int numReadBytes = readableBytes - in.readableBytes();
      numIncomingBytes += numReadBytes;
      metrics.getNumIncomingBytes().inc(numReadBytes);
    }
  }

  private void decodeImpl(ChannelHandlerContext ctx,
                          ByteBuf in,
                          List<Object> out) {
    metrics.getNumIncomingRequests().inc(1);

    if (in.readableBytes() == 0) {
      return;
    }

    switch (state) {
      case READ_MAGIC_BYTE_AND_VERSION:
        if (in.readableBytes() < 2 * Byte.BYTES) {
          return;
        }
        byte magicByte = in.readByte();
        byte version;
        switch (magicByte) {
          case MessageConstants.UPLOAD_UPLINK_MAGIC_BYTE:
            version = in.readByte();
            if (version != MessageConstants.UPLOAD_UPLINK_VERSION_3) {
              String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
              logger.warn(
                  "Invalid notify version {} from client {}",
                  version, clientInfo);
              ctx.close();
              logger.debug("Closed connection to client {}", clientInfo);
              return;
            }
            state = State.READ_MESSAGE_TYPE;
            return;
          case MessageConstants.DOWNLOAD_UPLINK_MAGIC_BYTE:
            version = in.readByte();
            if (version != MessageConstants.DOWNLOAD_UPLINK_VERSION_3) {
              String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
              logger.warn(
                  "Invalid download version {} from client {}",
                  version, clientInfo);
              ctx.close();
              logger.debug("Closed connection to client {}", clientInfo);
              return;
            }
            state = State.READ_MESSAGE_TYPE;
            return;
          case MessageConstants.NOTIFY_UPLINK_MAGIC_BYTE:
            version = in.readByte();
            if (version != MessageConstants.NOTIFY_UPLINK_VERSION_3) {
              String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
              logger.warn(
                  "Invalid control version {} from client {}",
                  version, clientInfo);
              ctx.close();
              logger.debug("Closed connection to client {}", clientInfo);
              return;
            }
            state = State.READ_MESSAGE_TYPE;
            return;
          case MessageConstants.REGISTRY_UPLINK_MAGIC_BYTE:
            version = in.readByte();
            if (version != MessageConstants.REGISTRY_UPLINK_VERSION_3) {
              String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
              logger.warn(
                  "Invalid registry version {} from client {}",
                  version, clientInfo);
              ctx.close();
              logger.debug("Closed connection to client {}", clientInfo);
              return;
            }
            state = State.READ_MESSAGE_TYPE;
            return;
          default:
            String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
            logger.warn(
                "Invalid magic byte {} from client {}",
                magicByte, clientInfo);
            ctx.close();
            logger.debug("Closed connection to client {}", clientInfo);
            return;
        }
      case READ_MESSAGE_TYPE:
        if (in.readableBytes() < Integer.BYTES) {
          return;
        }
        int messageType = in.readInt();
        if (messageType < 0) {
          controlMessageType = messageType;
          state = State.READ_CONTROL_MESSAGE_LEN;
        } else {
          partitionId = messageType;
          state = State.READ_TASK_ATTEMPT_ID;
        }
        return;
      case READ_CONTROL_MESSAGE_LEN:
        if (in.readableBytes() < Integer.BYTES) {
          return;
        }
        requiredBytes = in.readInt();
        if (requiredBytes < 0) {
          throw new RssInvalidDataException(String.format(
              "Invalid control message length: %s, %s",
              requiredBytes, NettyUtils.getServerConnectionInfo(ctx)));
        }
        if (requiredBytes == 0) {
          Object controlMessage = getControlMessage(ctx, controlMessageType, in);
          out.add(controlMessage);
          resetData();
          state = State.READ_MESSAGE_TYPE;
        } else {
          state = State.READ_CONTROL_MESSAGE_BYTES;
        }
        return;
      case READ_CONTROL_MESSAGE_BYTES:
        if (in.readableBytes() < requiredBytes) {
          return;
        }
        Object controlMessage = getControlMessage(ctx, controlMessageType, in);
        out.add(controlMessage);
        resetData();
        state = State.READ_MESSAGE_TYPE;
        return;
      case READ_TASK_ATTEMPT_ID:
        if (in.readableBytes() < Long.BYTES) {
          return;
        }
        in.readBytes(taskAttemptIdBytes);
        taskAttemptId = ByteBufUtils.readLong(taskAttemptIdBytes, 0);
        if (taskAttemptId < 0) {
          throw new RssInvalidDataException(String.format(
              "Invalid task attempt id: %s, %s",
              taskAttemptId, NettyUtils.getServerConnectionInfo(ctx)));
        }
        state = State.READ_DATA_MESSAGE_LEN;
        return;
      case READ_DATA_MESSAGE_LEN:
        if (in.readableBytes() < Integer.BYTES) {
          return;
        }
        int dataLen = in.readInt();
        if (dataLen < 0) {
          throw new RssInvalidDataException(String.format(
              "Invalid data length: %s, %s",
              dataLen, NettyUtils.getServerConnectionInfo(ctx)));
        }
        if (dataLen == 0) {
          out.add(createShuffleDataWrapper(in, 0));
          resetData();
          requiredBytes = 0;
          state = State.READ_MESSAGE_TYPE;
        } else {
          requiredBytes = dataLen;
          state = State.READ_DATA_MESSAGE_BYTES;
          shuffleDataBuffer.clear();
        }
        return;
      case READ_DATA_MESSAGE_BYTES:
        if (in.readableBytes() < requiredBytes) {
          int count = in.readableBytes();
          shuffleDataBuffer.ensureWritable(count);
          in.readBytes(shuffleDataBuffer, count);
          requiredBytes -= count;
        } else {
          shuffleDataBuffer.ensureWritable(requiredBytes);
          in.readBytes(shuffleDataBuffer, requiredBytes);
          out.add(createShuffleDataWrapper(shuffleDataBuffer, shuffleDataBuffer.readableBytes()));
          requiredBytes = 0;
          resetData();
          state = State.READ_MESSAGE_TYPE;
        }
        return;
      default:
        throw new RssException(String.format(
            "Should not get incoming data in state %s, client %s",
            state, NettyUtils.getServerConnectionInfo(ctx)));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    M3Stats.addException(cause, this.getClass().getSimpleName());

    String connectionInfo = NettyUtils.getServerConnectionInfo(ctx);
    String msg = "Got exception " + connectionInfo;
    logger.warn(msg, cause);

    ctx.close();
  }

  private Object getControlMessage(ChannelHandlerContext ctx,
                                   int controlMessageType,
                                   ByteBuf in) {
    switch (controlMessageType) {
      case MessageConstants.MESSAGE_ConnectUploadRequest:
        ConnectUploadRequest connectUploadRequest = ConnectUploadRequest.deserialize(in);
        metricGroupContainer.removeMetricGroup(user);
        user = connectUploadRequest.getUser();
        metrics = metricGroupContainer.getMetricGroup(user);
        return connectUploadRequest;
      case MessageConstants.MESSAGE_ConnectUploadResponse:
        return ConnectUploadResponse.deserialize(in);
      case MessageConstants.MESSAGE_StartUploadMessage:
        return StartUploadMessage.deserialize(in);
      case MessageConstants.MESSAGE_FinishUploadMessage:
        return FinishUploadMessage.deserialize(in);
      case MessageConstants.MESSAGE_HeartbeatMessage:
        HeartbeatMessage heartbeatMessage = HeartbeatMessage.deserialize(in);
        return heartbeatMessage;
      case MessageConstants.MESSAGE_GetBusyStatusRequest:
        GetBusyStatusRequest getBusyStatusRequest = GetBusyStatusRequest.deserialize(in);
        return getBusyStatusRequest;
      case MessageConstants.MESSAGE_GetBusyStatusResponse:
        GetBusyStatusResponse getBusyStatusResponse = GetBusyStatusResponse.deserialize(in);
        return getBusyStatusResponse;
      case MessageConstants.MESSAGE_ConnectDownloadRequest:
        ConnectDownloadRequest connectDownloadRequest = ConnectDownloadRequest.deserialize(in);
        metricGroupContainer.removeMetricGroup(user);
        user = connectDownloadRequest.getUser();
        metrics = metricGroupContainer.getMetricGroup(user);
        return connectDownloadRequest;
      case MessageConstants.MESSAGE_ConnectDownloadResponse:
        return ConnectDownloadResponse.deserialize(in);
      case MessageConstants.MESSAGE_GetDataAvailabilityRequest:
        return GetDataAvailabilityRequest.deserialize(in);
      case MessageConstants.MESSAGE_GetDataAvailabilityResponse:
        return GetDataAvailabilityResponse.deserialize(in);
      case MessageConstants.MESSAGE_ConnectNotifyRequest:
        return ConnectNotifyRequest.deserialize(in);
      case MessageConstants.MESSAGE_ConnectNotifyResponse:
        return ConnectNotifyResponse.deserialize(in);
      case MessageConstants.MESSAGE_FinishApplicationJobRequest:
        return FinishApplicationJobRequestMessage.deserialize(in);
      case MessageConstants.MESSAGE_FinishApplicationAttemptRequest:
        return FinishApplicationAttemptRequestMessage.deserialize(in);
      case MessageConstants.MESSAGE_ConnectRegistryRequest:
        return ConnectRegistryRequest.deserialize(in);
      case MessageConstants.MESSAGE_ConnectRegistryResponse:
        return ConnectRegistryResponse.deserialize(in);
      case MessageConstants.MESSAGE_RegisterServerRequest:
        return RegisterServerRequestMessage.deserialize(in);
      case MessageConstants.MESSAGE_GetServersRequest:
        return GetServersRequestMessage.deserialize(in);
      default:
        throw new RssException(String.format(
            "Unsupported control message type %s from client %s",
            controlMessageType,
            NettyUtils.getServerConnectionInfo(ctx)));
    }
  }

  private ShuffleDataWrapper createShuffleDataWrapper(ByteBuf in, int byteCount) {
    metrics.getNumIncomingBlocks().inc(1);
    byte[] headerBytes = DataBlockHeader.serializeToBytes(taskAttemptIdBytes, byteCount);
    byte[] bytes = new byte[headerBytes.length + byteCount];
    System.arraycopy(headerBytes, 0, bytes, 0, headerBytes.length);
    in.readBytes(bytes, headerBytes.length, byteCount);
    return new ShuffleDataWrapper(partitionId, taskAttemptId, bytes);
  }

  private void resetData() {
    controlMessageType = INVALID_CONTROL_MESSAGE_TYPE;
    partitionId = INVALID_PARTITION_ID;
    taskAttemptId = INVALID_SESSION_ID;
  }
}
