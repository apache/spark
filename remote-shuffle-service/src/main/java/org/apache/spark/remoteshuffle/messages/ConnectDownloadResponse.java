/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
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

package org.apache.spark.remoteshuffle.messages;

import org.apache.spark.remoteshuffle.common.MapTaskCommitStatus;
import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

/**
 * This is reponse for ConnectDownloadRequest.
 */
public class ConnectDownloadResponse extends ServerResponseMessage {
  private String serverId;
  private String compressionCodec;

  // this could be null
  private MapTaskCommitStatus mapTaskCommitStatus;

  // if dataAvailable is true, the server sends shuffle data immediately after this message
  private boolean dataAvailable;

  public ConnectDownloadResponse(String serverId, String compressionCodec,
                                 MapTaskCommitStatus mapTaskCommitStatus, boolean dataAvailable) {
    this.serverId = serverId;
    this.compressionCodec = compressionCodec;
    this.mapTaskCommitStatus = mapTaskCommitStatus;
    this.dataAvailable = dataAvailable;
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_ConnectDownloadResponse;
  }

  @Override
  public void serialize(ByteBuf buf) {
    ByteBufUtils.writeLengthAndString(buf, serverId);
    ByteBufUtils.writeLengthAndString(buf, compressionCodec);

    if (mapTaskCommitStatus == null) {
      buf.writeBoolean(false);
    } else {
      buf.writeBoolean(true);
      mapTaskCommitStatus.serialize(buf);
    }

    buf.writeBoolean(dataAvailable);
  }

  public static ConnectDownloadResponse deserialize(ByteBuf buf) {
    String serverId = ByteBufUtils.readLengthAndString(buf);
    String compressionCodec = ByteBufUtils.readLengthAndString(buf);

    MapTaskCommitStatus mapTaskCommitStatus = null;
    boolean mapTaskCommitStatusExisting = buf.readBoolean();
    if (mapTaskCommitStatusExisting) {
      mapTaskCommitStatus = MapTaskCommitStatus.deserialize(buf);
    }

    boolean dataAvailable = buf.readBoolean();

    return new ConnectDownloadResponse(serverId, compressionCodec, mapTaskCommitStatus,
        dataAvailable);
  }

  public String getServerId() {
    return serverId;
  }

  public String getCompressionCodec() {
    return compressionCodec;
  }

  public MapTaskCommitStatus getMapTaskCommitStatus() {
    return mapTaskCommitStatus;
  }

  public boolean isDataAvailable() {
    return dataAvailable;
  }

  @Override
  public String toString() {
    String mapTaskCommitStatusStr =
        dataAvailable ? mapTaskCommitStatus.toShortString() : mapTaskCommitStatus.toString();
    return "ConnectDownloadResponse{" +
        "serverId='" + serverId + '\'' +
        ", compressionCodec='" + compressionCodec + '\'' +
        ", dataAvailable=" + dataAvailable +
        ", mapTaskCommitStatus=" + mapTaskCommitStatusStr +
        '}';
  }
}
