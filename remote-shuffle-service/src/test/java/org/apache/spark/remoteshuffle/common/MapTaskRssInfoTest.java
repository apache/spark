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

package org.apache.spark.remoteshuffle.common;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MapTaskRssInfoTest {
  @Test
  public void serializeToString() {
    int mapId = 1;
    long taskAttemptId = 2;
    int numRssServers = 3;

    MapTaskRssInfo mapTaskRssInfo = new MapTaskRssInfo(
        mapId, taskAttemptId, 3);
    String str = mapTaskRssInfo.serializeToString();

    MapTaskRssInfo deserializedMapTaskRssInfo = MapTaskRssInfo.deserializeFromString(str);
    Assert.assertEquals(deserializedMapTaskRssInfo.getMapId(), mapTaskRssInfo.getMapId());
    Assert.assertEquals(deserializedMapTaskRssInfo.getTaskAttemptId(),
        mapTaskRssInfo.getTaskAttemptId());
    Assert.assertEquals(deserializedMapTaskRssInfo.getNumRssServers(),
        mapTaskRssInfo.getNumRssServers());
  }
}
