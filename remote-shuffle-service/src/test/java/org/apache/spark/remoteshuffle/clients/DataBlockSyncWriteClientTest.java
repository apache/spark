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

package org.apache.spark.remoteshuffle.clients;

import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.exceptions.RssServerBusyException;
import org.apache.spark.remoteshuffle.testutil.TestConstants;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.testng.annotations.Test;

public class DataBlockSyncWriteClientTest {

  @Test
  public void writeData() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    int partitionId = 2;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
        testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
        "appAttempt1")) {
      writeClient.connect();

      writeClient.startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, 20,
          new ShuffleWriteConfig());

      ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
      buf.writeInt(1);
      buf.writeInt(2);
      buf.writeInt(3);
      writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);

      buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
      buf.writeInt(10);
      buf.writeInt(20);
      writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);

      writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void writeZeroLengthData() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    int partitionId = 2;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
        testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
        "appAttempt1")) {
      writeClient.connect();

      writeClient.startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, 20,
          new ShuffleWriteConfig());

      ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(0);
      writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);
      writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);

      writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void writeLargeData() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    int numMaps = 1;
    int partitionId = 2;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
        testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
        "appAttempt1")) {
      writeClient.connect();

      writeClient.startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, 20,
          new ShuffleWriteConfig());

      int numBytes = 8 * 1024 * 1024;
      ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(numBytes);
      for (int i = 0; i < numBytes; i++) {
        buf.writeByte(i % Byte.MAX_VALUE);
      }
      writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);

      writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
    } finally {
      testServer1.shutdown();
    }
  }

  @Test(expectedExceptions = RssServerBusyException.class)
  public void writeClientsExceedServerMaxConnections() {
    TestStreamServer testServer1 =
        TestStreamServer.createRunningServer(config -> config.setMaxConnections(1));

    int numMaps = 1;
    int partitionId = 2;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try {
      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          "appAttempt1")) {
        writeClient.connect();

        writeClient.startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, 20,
            new ShuffleWriteConfig());

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(0);
        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);
        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);

        try (DataBlockSyncWriteClient writeClient2 = new DataBlockSyncWriteClient("localhost",
            testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app2",
            "appAttempt1")) {
          writeClient2.connect();
        }
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void writeClient_MaxConnectionsEqualOne() {
    TestStreamServer testServer1 =
        TestStreamServer.createRunningServer(config -> config.setMaxConnections(1));

    int numMaps = 1;
    int partitionId = 2;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    try {
      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          "appAttempt1")) {
        writeClient.connect();

        writeClient.startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, 20,
            new ShuffleWriteConfig());

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(0);
        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);
        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);
      }
    } finally {
      testServer1.shutdown();
    }
  }
}
