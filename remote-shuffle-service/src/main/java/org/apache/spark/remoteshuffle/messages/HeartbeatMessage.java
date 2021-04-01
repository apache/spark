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

import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class HeartbeatMessage extends BaseMessage {
  private String user;
  private String appId;
  private String appAttempt;
  private boolean keepLive;

  public HeartbeatMessage(String user, String appId, String appAttempt, boolean keepLive) {
    this.user = user;
    this.appId = appId;
    this.appAttempt = appAttempt;
    this.keepLive = keepLive;
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_HeartbeatMessage;
  }

  @Override
  public void serialize(ByteBuf buf) {
    ByteBufUtils.writeLengthAndString(buf, user);
    ByteBufUtils.writeLengthAndString(buf, appId);
    ByteBufUtils.writeLengthAndString(buf, appAttempt);
    buf.writeBoolean(keepLive);
  }

  public static HeartbeatMessage deserialize(ByteBuf buf) {
    String user = ByteBufUtils.readLengthAndString(buf);
    String appId = ByteBufUtils.readLengthAndString(buf);
    String appAttempt = ByteBufUtils.readLengthAndString(buf);
    boolean keepLive = buf.readBoolean();
    return new HeartbeatMessage(user, appId, appAttempt, keepLive);
  }

  public String getUser() {
    return user;
  }

  public String getAppId() {
    return appId;
  }

  public String getAppAttempt() {
    return appAttempt;
  }

  public boolean isKeepLive() {
    return keepLive;
  }

  @Override
  public String toString() {
    return "HeartbeatMessage{" +
        "user='" + user + '\'' +
        ", appId='" + appId + '\'' +
        ", appAttempt='" + appAttempt + '\'' +
        ", keepLive='" + keepLive + '\'' +
        '}';
  }
}
