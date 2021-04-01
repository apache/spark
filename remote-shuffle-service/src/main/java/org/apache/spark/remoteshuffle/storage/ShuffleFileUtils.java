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

package org.apache.spark.remoteshuffle.storage;

import org.apache.spark.remoteshuffle.common.AppShuffleId;

import java.nio.file.Paths;

/***
 * Utility methods for shuffle files.
 */
public class ShuffleFileUtils {
  public static final int MAX_SPLITS = 10000;

  public static String getShuffleFileName(int shuffleId, int partitionId) {
    return String.format("shuffle_%s_p_%s.data", shuffleId, partitionId);
  }

  public static String getShuffleFilePath(String rootDir,
                                          AppShuffleId appShuffleId,
                                          int partitionId) {
    String fileName = getShuffleFileName(
        appShuffleId.getShuffleId(), partitionId);
    String path = Paths.get(
        getAppShuffleDir(rootDir, appShuffleId.getAppId()),
        appShuffleId.getAppAttempt(),
        fileName).toString();
    return path;
  }

  public static String getAppShuffleDir(String rootDir, String appId) {
    return Paths.get(rootDir, appId).toString();
  }
}
