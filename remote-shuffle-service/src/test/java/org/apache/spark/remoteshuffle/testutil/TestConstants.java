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

package org.apache.spark.remoteshuffle.testutil;

import org.apache.spark.remoteshuffle.clients.ShuffleWriteConfig;

public class TestConstants {
  public static final int NETWORK_TIMEOUT = 30000;

  public static final int DATA_AVAILABLE_POLL_INTERVAL = 10;
  public static final int DATA_AVAILABLE_TIMEOUT = 30000;

  public static final int COMPRESSION_BUFFER_SIZE = 64 * 1024;

  public static final ShuffleWriteConfig SHUFFLE_WRITE_CONFIG = new ShuffleWriteConfig((short) 3);

  public static final long CONNECTION_IDLE_TIMEOUT_MILLIS = 30 * 1000;
}
