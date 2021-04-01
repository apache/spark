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

package org.apache.spark.remoteshuffle.clients;

import org.apache.spark.remoteshuffle.common.AppShufflePartitionId;
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.common.DataBlock;
import org.apache.spark.remoteshuffle.common.FilePathAndLength;
import org.apache.spark.remoteshuffle.exceptions.RssNetworkException;
import org.apache.spark.remoteshuffle.exceptions.RssShuffleDataNotAvailableException;
import org.apache.spark.remoteshuffle.messages.ConnectDownloadResponse;
import org.apache.spark.remoteshuffle.messages.GetDataAvailabilityResponse;
import org.apache.spark.remoteshuffle.testutil.TestConstants;
import org.apache.spark.remoteshuffle.testutil.TestStreamServer;
import org.apache.spark.remoteshuffle.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class DataBlockSocketReadClientTest {
  private static final Logger logger =
      LoggerFactory.getLogger(DataBlockSocketReadClientTest.class);

  @Test
  public void connectReturningDataAvailable() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      int partitionId = 2;
      AppTaskAttemptId appTaskAttemptId =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appId,
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
        buf.writeInt(1);
        buf.writeInt(2);
        buf.writeInt(3);
        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);

        writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
      }

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        ConnectDownloadResponse connectDownloadResponse = readClient.connect();
        if (!connectDownloadResponse.isDataAvailable()) {
          readClient.waitDataAvailable();
        }
      }

      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        ConnectDownloadResponse connectDownloadResponse = readClient.connect();
        Assert.assertEquals(
            connectDownloadResponse.getMapTaskCommitStatus().getTaskAttemptIds().size(), 1);
        Assert.assertEquals(
            connectDownloadResponse.getMapTaskCommitStatus().getTaskAttemptIds().stream()
                .findFirst().get(), (Long) taskAttemptId);
        Assert.assertTrue(connectDownloadResponse.isDataAvailable());
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void waitDataAvailable() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      int partitionId = 2;
      AppTaskAttemptId appTaskAttemptId1 =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      long taskAttemptId2 = taskAttemptId + 1;
      AppTaskAttemptId appTaskAttemptId2 =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId2);

      // write data without finish upload, server side should not treat data as available

      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appId,
          appAttempt)) {
        writeClient.connect();
        writeClient
            .startUpload(appTaskAttemptId1.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
        buf.writeInt(1);
        buf.writeInt(2);
        buf.writeInt(3);
        writeClient.writeData(partitionId, appTaskAttemptId1.getTaskAttemptId(), buf);
      }

      // read client connects to the server

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
      DataBlockSocketReadClient readClient =
          new DataBlockSocketReadClient("localhost", testServer1.getShufflePort(),
              TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId,
              Arrays.asList(appTaskAttemptId2.getTaskAttemptId()),
              TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT);
      ConnectDownloadResponse connectDownloadResponse = readClient.connect();
      Assert.assertFalse(connectDownloadResponse.isDataAvailable());

      // write data with new task attempt id and finish upload, after that, server side should treat data as available

      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appId,
          appAttempt)) {
        writeClient.connect();
        writeClient
            .startUpload(appTaskAttemptId2.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
        buf.writeInt(1);
        buf.writeInt(2);
        buf.writeInt(3);
        writeClient.writeData(partitionId, appTaskAttemptId2.getTaskAttemptId(), buf);

        writeClient.finishUpload(appTaskAttemptId2.getTaskAttemptId());
      }

      // read client should see data available now

      GetDataAvailabilityResponse getDataAvailabilityResponse = readClient.waitDataAvailable();
      Assert.assertTrue(getDataAvailabilityResponse.isDataAvailable());

      Assert.assertEquals(
          getDataAvailabilityResponse.getMapTaskCommitStatus().getTaskAttemptIds().size(), 1);
      Assert.assertEquals(
          getDataAvailabilityResponse.getMapTaskCommitStatus().getTaskAttemptIds().stream()
              .findFirst().get(), (Long) taskAttemptId2);

      readClient.close();
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void readData() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      int partitionId = 2;
      AppTaskAttemptId appTaskAttemptId =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
        buf.writeInt(100);
        buf.writeInt(200);
        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);

        writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
      }

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();

        DataBlock dataBlock = readClient.readDataBlock();
        Assert.assertNotNull(dataBlock);
        Assert.assertEquals(dataBlock.getHeader().getTaskAttemptId(), taskAttemptId);
        Assert.assertEquals(dataBlock.getHeader().getLength(), 8);
        Assert.assertEquals(ByteBufUtils.readInt(dataBlock.getPayload(), 0), 100);
        Assert.assertEquals(ByteBufUtils.readInt(dataBlock.getPayload(), 4), 200);

        dataBlock = readClient.readDataBlock();
        Assert.assertNull(dataBlock);
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void readEmptyShuffleData() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      int partitionId = 2;
      AppTaskAttemptId appTaskAttemptId =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());
        writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
      }

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();

        DataBlock dataBlock = readClient.readDataBlock();
        Assert.assertNull(dataBlock);
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void readLargeData() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      int partitionId = 2;
      AppTaskAttemptId appTaskAttemptId =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      int numBytes = 8 * 1024 * 1024;
      byte[] bytes = new byte[numBytes];
      for (int i = 0; i < numBytes; i++) {
        bytes[i] = (byte) (i % Byte.MAX_VALUE);
      }

      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(),
            Unpooled.wrappedBuffer(bytes));
        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(),
            Unpooled.wrappedBuffer(bytes));

        writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
      }

      ByteBuf readByteBuf = Unpooled.buffer();

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();

        DataBlock dataBlock = readClient.readDataBlock();
        while (dataBlock != null) {
          Assert.assertEquals(dataBlock.getHeader().getTaskAttemptId(), taskAttemptId);

          readByteBuf.writeBytes(dataBlock.getPayload());

          dataBlock = readClient.readDataBlock();
        }

        Assert.assertEquals(readByteBuf.readableBytes(), numBytes * 2);
      }

      byte[] readBytes = new byte[numBytes];
      readByteBuf.readBytes(readBytes);
      Assert.assertEquals(readBytes, bytes);
      readByteBuf.readBytes(readBytes);
      Assert.assertEquals(readBytes, bytes);

      Assert.assertEquals(readByteBuf.readableBytes(), 0);
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void readData_readBeforeWriteData() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      int partitionId = 2;

      AppTaskAttemptId appTaskAttemptId =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      final AtomicInteger writeDataReadyToRun = new AtomicInteger(0);

      Runnable writeData = new Runnable() {
        @Override
        public void run() {
          try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
              testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
              appAttempt)) {
            writeClient.connect();

            synchronized (writeDataReadyToRun) {
              try {
                if (writeDataReadyToRun.get() == 0) {
                  writeDataReadyToRun.wait();
                }
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }

            writeClient
                .startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                    new ShuffleWriteConfig());

            ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
            buf.writeInt(300);
            buf.writeInt(400);
            writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);

            writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
          }
        }
      };

      Thread thread = new Thread(writeData);
      thread.start();

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(taskAttemptId),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        synchronized (writeDataReadyToRun) {
          writeDataReadyToRun.set(1);
          writeDataReadyToRun.notify();
        }

        readClient.connect();

        DataBlock dataBlock = readClient.readDataBlock();
        Assert.assertNotNull(dataBlock);
        Assert.assertEquals(dataBlock.getHeader().getTaskAttemptId(), taskAttemptId);
        Assert.assertEquals(dataBlock.getHeader().getLength(), 8);
        Assert.assertEquals(ByteBufUtils.readInt(dataBlock.getPayload(), 0), 300);
        Assert.assertEquals(ByteBufUtils.readInt(dataBlock.getPayload(), 4), 400);

        dataBlock = readClient.readDataBlock();
        Assert.assertNull(dataBlock);
      }

      try {
        thread.join(TestConstants.NETWORK_TIMEOUT);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void readData_ignoreStaleTaskAttemptId() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      long taskAttemptId2 = taskAttemptId + 1;
      int partitionId = 2;

      AppTaskAttemptId appTaskAttemptId1 =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);
      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId1.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
        buf.writeInt(100);
        buf.writeInt(200);
        writeClient.writeData(partitionId, appTaskAttemptId1.getTaskAttemptId(), buf);

        writeClient.finishUpload(appTaskAttemptId1.getTaskAttemptId());
      }

      AppTaskAttemptId appTaskAttemptId2 =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId2);
      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId2.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
        buf.writeInt(300);
        buf.writeInt(400);
        writeClient.writeData(partitionId, appTaskAttemptId2.getTaskAttemptId(), buf);

        writeClient.finishUpload(appTaskAttemptId2.getTaskAttemptId());
      }

      testServer1.getShuffleExecutor()
          .pollAndWaitMapAttemptCommitted(appTaskAttemptId2, TestConstants.DATA_AVAILABLE_TIMEOUT);

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId2.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();

        DataBlock dataBlock = readClient.readDataBlock();
        Assert.assertNotNull(dataBlock);
        Assert.assertEquals(dataBlock.getHeader().getTaskAttemptId(), taskAttemptId2);
        Assert.assertEquals(dataBlock.getHeader().getLength(), 8);
        Assert.assertEquals(ByteBufUtils.readInt(dataBlock.getPayload(), 0), 300);
        Assert.assertEquals(ByteBufUtils.readInt(dataBlock.getPayload(), 4), 400);

        dataBlock = readClient.readDataBlock();
        Assert.assertNull(dataBlock);
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void readData_ignoreStaleTaskAttemptId_oldTaskAttemptWriteAfterNewTaskAttempt() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      long taskAttemptId2 = taskAttemptId + 1;
      int partitionId = 2;

      AppTaskAttemptId appTaskAttemptId1 =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);
      AppTaskAttemptId appTaskAttemptId2 =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId2);

      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {

        try (DataBlockSyncWriteClient writeClient2 = new DataBlockSyncWriteClient("localhost",
            testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
            appAttempt)) {
          writeClient2.connect();

          writeClient2
              .startUpload(appTaskAttemptId2.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                  new ShuffleWriteConfig());

          ByteBuf buf2 = PooledByteBufAllocator.DEFAULT.buffer(1000);
          buf2.writeInt(300);
          buf2.writeInt(400);
          writeClient2.writeData(partitionId, appTaskAttemptId2.getTaskAttemptId(), buf2);

          writeClient2.finishUpload(appTaskAttemptId2.getTaskAttemptId());
        }

        testServer1.getShuffleExecutor().pollAndWaitMapAttemptCommitted(appTaskAttemptId2,
            TestConstants.DATA_AVAILABLE_TIMEOUT);

        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId1.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
        buf.writeInt(100);
        buf.writeInt(200);
        writeClient.writeData(partitionId, appTaskAttemptId1.getTaskAttemptId(), buf);

        writeClient.finishUpload(appTaskAttemptId1.getTaskAttemptId());
      }

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId2.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();

        DataBlock dataBlock = readClient.readDataBlock();
        Assert.assertNotNull(dataBlock);
        Assert.assertEquals(dataBlock.getHeader().getTaskAttemptId(), taskAttemptId2);
        Assert.assertEquals(dataBlock.getHeader().getLength(), 8);
        Assert.assertEquals(ByteBufUtils.readInt(dataBlock.getPayload(), 0), 300);
        Assert.assertEquals(ByteBufUtils.readInt(dataBlock.getPayload(), 4), 400);

        dataBlock = readClient.readDataBlock();
        Assert.assertNull(dataBlock);
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void readData_ignoreStaleTaskAttemptId_concurrentWriteWithMultiThreads() {
    int testRounds = 2;
    for (int testRound = 0; testRound < testRounds; testRound++) {
      TestStreamServer testServer1 = TestStreamServer.createRunningServer();
      try {
        String appId = "app" + testRound;
        String appAttempt = "attempt1";
        int shuffleId = 1;
        int numMaps = 1;
        int numPartitions = 1;
        int mapId = 2;
        long taskAttemptIdStartValue = 3;
        int partitionId = 0;

        int numTaskAttempts = 3;
        ByteBuf lastTaskAttemptWrittenData = Unpooled.buffer(1000);

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < numTaskAttempts; i++) {
          long taskAttemptId = taskAttemptIdStartValue + i;
          AppTaskAttemptId appTaskAttemptId =
              new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

          Runnable writeData = new Runnable() {
            @Override
            public void run() {
              try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
                  testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appId,
                  appAttempt)) {
                writeClient.connect();

                writeClient.startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps,
                    numPartitions, new ShuffleWriteConfig());

                Random random = new Random();

                int iterations1 = random.nextInt(1000);
                int iterations2 = random.nextInt(1000);
                logger.info(String.format("Running map task: %s, %s", iterations1, iterations2));
                for (int i = 0; i < iterations1; i++) {
                  ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
                  for (int j = 0; j < iterations2; j++) {
                    int randomValue = random.nextInt();
                    buf.writeInt(randomValue);
                    if (taskAttemptId == taskAttemptIdStartValue + numTaskAttempts - 1) {
                      synchronized (lastTaskAttemptWrittenData) {
                        lastTaskAttemptWrittenData.writeInt(randomValue);
                      }
                    }
                  }
                  writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);
                }

                writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
              }
            }
          };

          Thread thread = new Thread(writeData);
          threads.add(thread);
        }

        threads.forEach(t -> t.start());

        long largestTaskAttemptId = taskAttemptIdStartValue + numTaskAttempts - 1;
        AppShufflePartitionId appShufflePartitionId =
            new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
        try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
            testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
            appShufflePartitionId, Arrays.asList(largestTaskAttemptId),
            TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
          readClient.connect();

          DataBlock dataBlock = readClient.readDataBlock();

          int readDataLength = 0;
          ByteBuf readData = Unpooled.buffer(1000);

          while (dataBlock != null) {
            Assert.assertEquals(dataBlock.getHeader().getTaskAttemptId(), largestTaskAttemptId);

            int length = dataBlock.getHeader().getLength();
            readDataLength += length;

            readData.writeBytes(dataBlock.getPayload());

            dataBlock = readClient.readDataBlock();
          }

          Assert.assertEquals(readDataLength, lastTaskAttemptWrittenData.readableBytes());
          Assert
              .assertEquals(readData.readableBytes(), lastTaskAttemptWrittenData.readableBytes());
          Assert.assertEquals(ByteBufUtils.readBytes(readData),
              ByteBufUtils.readBytes(lastTaskAttemptWrittenData));
        }

        threads.forEach(t -> {
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
      } finally {
        testServer1.shutdown();
      }
    }
  }

  @Test(expectedExceptions = RssNetworkException.class)
  public void readEmptyFile() throws IOException {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    testServer1.getRootDir();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      int partitionId = 2;
      AppTaskAttemptId appTaskAttemptId =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
        buf.writeInt(100);
        buf.writeInt(200);
        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);

        writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
      }

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();
        DataBlock dataBlock = readClient.readDataBlock();
        Assert.assertNotNull(dataBlock);
      }

      // get shuffle file and truncate it to empty
      List<FilePathAndLength> partitionFiles = testServer1.getShuffleExecutor()
          .getPersistedBytes(appShufflePartitionId.getAppShuffleId(),
              appShufflePartitionId.getPartitionId());
      String filePath = partitionFiles.get(0).getPath();
      long length = partitionFiles.get(0).getLength();
      Assert.assertTrue(length > 0);
      FileUtils.write(new File(filePath), "", StandardCharsets.UTF_8);

      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();

        DataBlock dataBlock = readClient.readDataBlock();
        Assert.assertNotNull(dataBlock);
        Assert.assertEquals(dataBlock.getHeader().getTaskAttemptId(), taskAttemptId);
        Assert.assertEquals(dataBlock.getHeader().getLength(), 8);
        Assert.assertEquals(ByteBufUtils.readInt(dataBlock.getPayload(), 0), 100);
        Assert.assertEquals(ByteBufUtils.readInt(dataBlock.getPayload(), 4), 200);

        dataBlock = readClient.readDataBlock();
        Assert.assertNull(dataBlock);
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test(expectedExceptions = RssNetworkException.class)
  public void readIncompleteFile() throws IOException {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    testServer1.getRootDir();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      int partitionId = 2;
      AppTaskAttemptId appTaskAttemptId =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
        buf.writeInt(100);
        buf.writeInt(200);
        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(), buf);

        writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
      }

      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();
        DataBlock dataBlock = readClient.readDataBlock();
        Assert.assertNotNull(dataBlock);
      }

      // get shuffle file, and reduce its size to half
      List<FilePathAndLength> partitionFiles = testServer1.getShuffleExecutor()
          .getPersistedBytes(appShufflePartitionId.getAppShuffleId(),
              appShufflePartitionId.getPartitionId());
      String filePath = partitionFiles.get(0).getPath();
      long length = partitionFiles.get(0).getLength();
      Assert.assertTrue(length > 0);
      // set file length to half
      long halfFileLength = length / 2;
      try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw")) {
        randomAccessFile.setLength(halfFileLength);
      }

      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()),
          TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();

        DataBlock dataBlock = readClient.readDataBlock();
        Assert.assertNotNull(dataBlock);
        Assert.assertEquals(dataBlock.getHeader().getTaskAttemptId(), taskAttemptId);
        Assert.assertEquals(dataBlock.getHeader().getLength(), 8);
        Assert.assertEquals(ByteBufUtils.readInt(dataBlock.getPayload(), 0), 100);
        Assert.assertEquals(ByteBufUtils.readInt(dataBlock.getPayload(), 4), 200);

        dataBlock = readClient.readDataBlock();
        Assert.assertNull(dataBlock);
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test(expectedExceptions = RssShuffleDataNotAvailableException.class)
  public void shuffleDataNotAvailable_secondMapTaskNotFinish() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 2;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      int mapId2 = 3;
      long taskAttemptId2 = 4;
      int partitionId = 2;
      AppTaskAttemptId appTaskAttemptId =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);
      AppTaskAttemptId appTaskAttemptId2 =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId2, taskAttemptId2);

      int numBytes = 8 * 1024 * 1024;
      byte[] bytes = new byte[numBytes];
      for (int i = 0; i < numBytes; i++) {
        bytes[i] = (byte) (i % Byte.MAX_VALUE);
      }

      // first map task finishes upload
      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(),
            Unpooled.wrappedBuffer(bytes));
        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(),
            Unpooled.wrappedBuffer(bytes));

        writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
      }

      // second map task does not finish upload
      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId2.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        writeClient.writeData(partitionId, appTaskAttemptId2.getTaskAttemptId(),
            Unpooled.wrappedBuffer(bytes));
        writeClient.writeData(partitionId, appTaskAttemptId2.getTaskAttemptId(),
            Unpooled.wrappedBuffer(bytes));
      }

      int dataAvailableWaitTime = 100;
      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId, Arrays.asList(appTaskAttemptId2.getTaskAttemptId()),
          dataAvailableWaitTime / 10, dataAvailableWaitTime)) {
        readClient.connect();
        readClient.readDataBlock();
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test(expectedExceptions = RssShuffleDataNotAvailableException.class)
  public void shuffleDataNotAvailable_secondMapTaskRetryButNotFinish() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 2;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      int mapId2 = 3;
      long taskAttemptId2 = 4;
      long taskAttemptId3 = 5;
      int partitionId = 2;
      AppTaskAttemptId appTaskAttemptId =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);
      AppTaskAttemptId appTaskAttemptId2 =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId2, taskAttemptId2);
      AppTaskAttemptId appTaskAttemptId3 =
          new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId2, taskAttemptId3);

      int numBytes = 8 * 1024 * 1024;
      byte[] bytes = new byte[numBytes];
      for (int i = 0; i < numBytes; i++) {
        bytes[i] = (byte) (i % Byte.MAX_VALUE);
      }

      // first map task finishes upload
      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(),
            Unpooled.wrappedBuffer(bytes));
        writeClient.writeData(partitionId, appTaskAttemptId.getTaskAttemptId(),
            Unpooled.wrappedBuffer(bytes));

        writeClient.finishUpload(appTaskAttemptId.getTaskAttemptId());
      }

      // second map task finishes upload first time
      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId2.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        writeClient.writeData(partitionId, appTaskAttemptId2.getTaskAttemptId(),
            Unpooled.wrappedBuffer(bytes));
        writeClient.writeData(partitionId, appTaskAttemptId2.getTaskAttemptId(),
            Unpooled.wrappedBuffer(bytes));

        writeClient.finishUpload(appTaskAttemptId2.getTaskAttemptId());
      }

      // second map task retries but does not finish upload
      try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
          appAttempt)) {
        writeClient.connect();

        writeClient
            .startUpload(appTaskAttemptId3.getShuffleMapTaskAttemptId(), numMaps, numPartitions,
                new ShuffleWriteConfig());

        writeClient.writeData(partitionId, appTaskAttemptId3.getTaskAttemptId(),
            Unpooled.wrappedBuffer(bytes));
        writeClient.writeData(partitionId, appTaskAttemptId3.getTaskAttemptId(),
            Unpooled.wrappedBuffer(bytes));
      }

      int dataAvailableWaitTime = 100;
      AppShufflePartitionId appShufflePartitionId =
          new AppShufflePartitionId(appId, appAttempt, shuffleId, partitionId);
      try (DataBlockSocketReadClient readClient = new DataBlockSocketReadClient("localhost",
          testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1",
          appShufflePartitionId,
          Arrays.asList(appTaskAttemptId.getTaskAttemptId(), appTaskAttemptId3.getTaskAttemptId()),
          dataAvailableWaitTime / 10, dataAvailableWaitTime)) {
        readClient.connect();
        readClient.readDataBlock();
      }
    } finally {
      testServer1.shutdown();
    }
  }
}
