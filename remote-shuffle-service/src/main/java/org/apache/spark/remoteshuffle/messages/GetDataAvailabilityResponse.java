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

package org.apache.spark.remoteshuffle.messages;

import org.apache.spark.remoteshuffle.common.MapTaskCommitStatus;
import io.netty.buffer.ByteBuf;

/***
 * Response message for GetDataAvailabilityRequest
 */
public class GetDataAvailabilityResponse extends BaseMessage {

  // this could be null
  private MapTaskCommitStatus mapTaskCommitStatus;

  // if dataAvailable is true, the server sends shuffle data immediately after this message
  private boolean dataAvailable;

  public GetDataAvailabilityResponse(MapTaskCommitStatus mapTaskCommitStatus,
                                     boolean dataAvailable) {
    this.mapTaskCommitStatus = mapTaskCommitStatus;
    this.dataAvailable = dataAvailable;
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_GetDataAvailabilityResponse;
  }

  @Override
  public void serialize(ByteBuf buf) {
    if (mapTaskCommitStatus == null) {
      buf.writeBoolean(false);
    } else {
      buf.writeBoolean(true);
      mapTaskCommitStatus.serialize(buf);
    }

    buf.writeBoolean(dataAvailable);
  }

  public static GetDataAvailabilityResponse deserialize(ByteBuf buf) {
    MapTaskCommitStatus mapTaskCommitStatus = null;
    boolean mapTaskCommitStatusExisting = buf.readBoolean();
    if (mapTaskCommitStatusExisting) {
      mapTaskCommitStatus = MapTaskCommitStatus.deserialize(buf);
    }

    boolean dataAvailable = buf.readBoolean();

    return new GetDataAvailabilityResponse(mapTaskCommitStatus, dataAvailable);
  }

  public MapTaskCommitStatus getMapTaskCommitStatus() {
    return mapTaskCommitStatus;
  }

  public boolean isDataAvailable() {
    return dataAvailable;
  }

  @Override
  public String toString() {
    return "GetDataAvailabilityResponse{" +
        "mapTaskCommitStatus=" + mapTaskCommitStatus +
        "dataAvailable=" + dataAvailable +
        '}';
  }
}
