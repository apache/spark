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
import org.apache.spark.remoteshuffle.exceptions.RssInvalidStateException;
import io.netty.buffer.ByteBuf;

/***
 * This class is used to tell shuffle read client the status of shuffle stage on the shuffle server.
 */
public class ShuffleStageStatus extends SerializableMessage {
  public static final byte FILE_STATUS_OK = 0;
  public static final byte FILE_STATUS_SHUFFLE_STAGE_NOT_STARTED = 1;
  public static final byte FILE_STATUS_CORRUPTED = 2;

  private final byte fileStatus;
  private final MapTaskCommitStatus mapTaskCommitStatus;

  public ShuffleStageStatus(byte fileStatus, MapTaskCommitStatus mapTaskCommitStatus) {
    this.fileStatus = fileStatus;
    this.mapTaskCommitStatus = mapTaskCommitStatus;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeByte(fileStatus);

    if (mapTaskCommitStatus == null) {
      buf.writeBoolean(false);
    } else {
      buf.writeBoolean(true);
      mapTaskCommitStatus.serialize(buf);
    }
  }

  public static ShuffleStageStatus deserialize(ByteBuf buf) {
    byte fileStatus = buf.readByte();

    MapTaskCommitStatus mapTaskCommitStatus = null;
    boolean mapTaskCommitStatusExisting = buf.readBoolean();
    if (mapTaskCommitStatusExisting) {
      mapTaskCommitStatus = MapTaskCommitStatus.deserialize(buf);
    }

    return new ShuffleStageStatus(fileStatus, mapTaskCommitStatus);
  }

  public byte getFileStatus() {
    return fileStatus;
  }

  public MapTaskCommitStatus getMapTaskCommitStatus() {
    return mapTaskCommitStatus;
  }

  public byte transformToMessageResponseStatus() {
    switch (fileStatus) {
      case ShuffleStageStatus.FILE_STATUS_OK:
        return MessageConstants.RESPONSE_STATUS_OK;
      case ShuffleStageStatus.FILE_STATUS_SHUFFLE_STAGE_NOT_STARTED:
        return MessageConstants.RESPONSE_STATUS_SHUFFLE_STAGE_NOT_STARTED;
      case ShuffleStageStatus.FILE_STATUS_CORRUPTED:
        return MessageConstants.RESPONSE_STATUS_FILE_CORRUPTED;
      default:
        throw new RssInvalidStateException(String.format("Invalid file status: %s", fileStatus));
    }
  }

  @Override
  public String toString() {
    return "ShuffleStageStatus{" +
        "fileStatus=" + fileStatus +
        ", mapTaskCommitStatus=" + mapTaskCommitStatus +
        '}';
  }
}
