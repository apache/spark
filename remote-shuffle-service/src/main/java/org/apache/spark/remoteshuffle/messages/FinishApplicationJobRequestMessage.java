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

import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class FinishApplicationJobRequestMessage extends ControlMessage {
  private String appId;
  private String appAttempt;
  private int jobId;
  private String jobStatus;
  private String exceptionName;
  private String exceptionDetail;

  public FinishApplicationJobRequestMessage(String appId, String appAttempt, int jobId,
                                            String jobStatus, String exceptionName,
                                            String exceptionDetail) {
    this.appId = appId;
    this.appAttempt = appAttempt;
    this.jobId = jobId;
    this.jobStatus = jobStatus;
    this.exceptionName = exceptionName;
    this.exceptionDetail = exceptionDetail;
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_FinishApplicationJobRequest;
  }

  @Override
  public void serialize(ByteBuf buf) {
    ByteBufUtils.writeLengthAndString(buf, appId);
    ByteBufUtils.writeLengthAndString(buf, appAttempt);
    buf.writeInt(jobId);
    ByteBufUtils.writeLengthAndString(buf, jobStatus);
    ByteBufUtils.writeLengthAndString(buf, exceptionName);
    ByteBufUtils.writeLengthAndString(buf, exceptionDetail);
  }

  public static FinishApplicationJobRequestMessage deserialize(ByteBuf buf) {
    String appId = ByteBufUtils.readLengthAndString(buf);
    String appAttempt = ByteBufUtils.readLengthAndString(buf);
    int jobId = buf.readInt();
    String jobStatus = ByteBufUtils.readLengthAndString(buf);
    String exceptionName = ByteBufUtils.readLengthAndString(buf);
    String exceptionDetail = ByteBufUtils.readLengthAndString(buf);
    return new FinishApplicationJobRequestMessage(appId, appAttempt, jobId, jobStatus,
        exceptionName, exceptionDetail);
  }

  public String getAppId() {
    return appId;
  }

  public String getAppAttempt() {
    return appAttempt;
  }

  public int getJobId() {
    return jobId;
  }

  public String getJobStatus() {
    return jobStatus;
  }

  public String getExceptionName() {
    return exceptionName;
  }

  public String getExceptionDetail() {
    return exceptionDetail;
  }

  @Override
  public String toString() {
    return "FinishApplicationJobRequestMessage{" +
        "appId='" + appId + '\'' +
        ", appAttempt='" + appAttempt + '\'' +
        ", jobId=" + jobId +
        ", jobStatus=" + jobStatus +
        ", exceptionName=" + exceptionName +
        ", exceptionDetail=" + exceptionDetail +
        '}';
  }
}
