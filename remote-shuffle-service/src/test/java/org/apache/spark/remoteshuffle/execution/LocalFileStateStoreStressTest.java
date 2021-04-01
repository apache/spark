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

package org.apache.spark.remoteshuffle.execution;

import org.apache.spark.remoteshuffle.clients.ShuffleWriteConfig;
import org.apache.spark.remoteshuffle.common.AppShuffleId;
import org.apache.spark.remoteshuffle.common.AppTaskAttemptId;
import org.apache.spark.remoteshuffle.common.PartitionFilePathAndLength;
import org.apache.spark.remoteshuffle.messages.ShuffleStageStatus;
import org.apache.spark.remoteshuffle.util.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LocalFileStateStoreStressTest {

  private Path tempPath;
  private LocalFileStateStore stateStore;

  @BeforeMethod
  public void beforeMethod() throws IOException {
    tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    stateStore = new LocalFileStateStore(tempPath.toString(), 10, 100);
  }

  @AfterMethod
  public void afterMethod() {
    stateStore.close();

    FileUtils.cleanupOldFiles(tempPath.toString(), System.currentTimeMillis() + 1000);
  }

  @Test
  public void test() throws InterruptedException {
    int numThreads = 100;
    long runningMillis = 200;
    Thread[] threads = new Thread[numThreads];

    AppShuffleId appShuffleId = new AppShuffleId("app1", "1", 2);
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appShuffleId, 1, 99L);
    ShuffleWriteConfig shuffleWriteConfig = new ShuffleWriteConfig((short) 6);
    PartitionFilePathAndLength partitionFilePathAndLength =
        new PartitionFilePathAndLength(1, "file1", 123);

    ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();

    for (int i = 0; i < threads.length; i++) {
      int threadIndex = i;
      Thread thread = new Thread(() -> {
        System.out.println(String.format("Thread started: %s", threadIndex));
        Random random = new Random();
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime <= runningMillis) {
          int randomInt = random.nextInt(10);
          switch (randomInt) {
            case 0:
              stateStore.storeStageInfo(appShuffleId,
                  new StagePersistentInfo(2, 3, shuffleWriteConfig,
                      ShuffleStageStatus.FILE_STATUS_OK));
              break;
            case 1:
              stateStore.storeTaskAttemptCommit(appShuffleId,
                  Arrays.asList(2L),
                  Arrays.asList(partitionFilePathAndLength));
              break;
            case 2:
              stateStore.storeAppDeletion("app1");
              break;
            case 3:
              stateStore.storeStageCorruption(appShuffleId);
              break;
            case 4:
              stateStore.commit();
              break;
            default:
              break;
          }
        }
        System.out.println(String.format("Thread exited: %s", threadIndex));
      });
      thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          exceptions.add(e);
          e.printStackTrace();
        }
      });
      threads[i] = thread;
    }

    for (int i = 0; i < threads.length; i++) {
      threads[i].start();
    }

    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }

    Assert.assertEquals(exceptions.size(), 0);
  }

}
