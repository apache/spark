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

public class AppDeletionStateItem extends BaseMessage {
  private final String appId;

  public AppDeletionStateItem(String appId) {
    this.appId = appId;
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_AppDeletionStateItem;
  }

  @Override
  public void serialize(ByteBuf buf) {
    ByteBufUtils.writeLengthAndString(buf, appId);
  }

  public static AppDeletionStateItem deserialize(ByteBuf buf) {
    String appId = ByteBufUtils.readLengthAndString(buf);
    return new AppDeletionStateItem(appId);
  }

  public String getAppId() {
    return appId;
  }

  @Override
  public String toString() {
    return "AppDeletionStateItem{" +
        "appId=" + appId +
        '}';
  }
}
