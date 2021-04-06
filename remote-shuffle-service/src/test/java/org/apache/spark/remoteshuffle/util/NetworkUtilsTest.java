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

package org.apache.spark.remoteshuffle.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class NetworkUtilsTest {
  private final int timeout = 100;

  @Test
  public void validHostName() {
    Assert.assertTrue(NetworkUtils.isReachable("localhost", timeout));
  }

  @Test
  public void validIpAddress() {
    Assert.assertTrue(NetworkUtils.isReachable("127.0.0.1", timeout));
    Assert
        .assertTrue(NetworkUtils.isReachable("0000:0000:0000:0000:0000:0000:0000:0001", timeout));
    Assert.assertTrue(NetworkUtils.isReachable("::1", timeout));
  }

  @Test
  public void invalidHostName() {
    Assert.assertFalse(NetworkUtils.isReachable(null, timeout));
    Assert.assertFalse(NetworkUtils.isReachable("", timeout));
    Assert.assertFalse(NetworkUtils.isReachable(" ", timeout));
    Assert.assertFalse(NetworkUtils.isReachable("not_exist_host_abc_123", timeout));
  }
}
