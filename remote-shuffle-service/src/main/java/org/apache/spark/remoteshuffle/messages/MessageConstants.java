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

public class MessageConstants {
  public final static byte UPLOAD_UPLINK_MAGIC_BYTE = 'u';
  public final static byte DOWNLOAD_UPLINK_MAGIC_BYTE = 'd';
  public final static byte NOTIFY_UPLINK_MAGIC_BYTE = 'c';
  public final static byte REGISTRY_UPLINK_MAGIC_BYTE = 'r';

  public final static byte UPLOAD_UPLINK_VERSION_3 = 3;
  public final static byte DOWNLOAD_UPLINK_VERSION_3 = 3;
  public final static byte NOTIFY_UPLINK_VERSION_3 = 3;
  public final static byte REGISTRY_UPLINK_VERSION_3 = 3;

  public final static byte RESPONSE_STATUS_OK = 20;
  public final static byte RESPONSE_STATUS_SHUFFLE_STAGE_NOT_STARTED = 44;
  public final static byte RESPONSE_STATUS_FILE_CORRUPTED = 45;
  public final static byte RESPONSE_STATUS_SERVER_BUSY = 53;
  public final static byte RESPONSE_STATUS_APP_TOO_MUCH_DATA = 54;
  public final static byte RESPONSE_STATUS_STALE_TASK_ATTEMPT = 55;
  public final static byte RESPONSE_STATUS_UNSPECIFIED = 0;

  // Control messages
  public final static int MESSAGE_FinishApplicationAttemptRequest = -7;
  public final static int MESSAGE_RegisterServerRequest = -9;
  public final static int MESSAGE_GetServersRequest = -10;
  public final static int MESSAGE_FinishApplicationJobRequest = -12;

  public final static int MESSAGE_GetServersResponse = -16;
  public final static int MESSAGE_RegisterServerResponse = -19;

  public final static int MESSAGE_ConnectUploadRequest = -301;
  public final static int MESSAGE_ConnectUploadResponse = -302;
  public final static int MESSAGE_StartUploadMessage = -303;
  public final static int MESSAGE_FinishUploadMessage = -317;
  public final static int MESSAGE_HeartbeatMessage = -319;
  public final static int MESSAGE_ConnectDownloadRequest = -318;
  public final static int MESSAGE_ConnectDownloadResponse = -307;
  public final static int MESSAGE_GetDataAvailabilityRequest = -310;
  public final static int MESSAGE_GetDataAvailabilityResponse = -309;
  public final static int MESSAGE_ConnectNotifyRequest = -311;
  public final static int MESSAGE_ConnectNotifyResponse = -312;
  public final static int MESSAGE_ConnectRegistryRequest = -313;
  public final static int MESSAGE_ConnectRegistryResponse = -314;
  public final static int MESSAGE_GetBusyStatusRequest = -320;
  public final static int MESSAGE_GetBusyStatusResponse = -321;

  // State store data item
  public final static int MESSAGE_StageInfoStateItem = -401;
  public final static int MESSAGE_AppDeletionStateItem = -404;
  public final static int MESSAGE_StageCorruptionStateItem = -405;
  public final static int MESSAGE_TaskAttemptCommitStateItem = -407;

  // Other constants

  public final static int DEFAULT_SHUFFLE_DATA_MESSAGE_SIZE = 32 * 1024;
}
