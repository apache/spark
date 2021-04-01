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

package org.apache.spark.remoteshuffle.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MonitorUtilsTest {

  @Test
  public void hasRssException() {
    Assert.assertFalse(MonitorUtils.hasRssException(null));
    Assert.assertFalse(MonitorUtils.hasRssException(""));
    Assert.assertFalse(MonitorUtils.hasRssException(" "));

    Assert.assertFalse(MonitorUtils.hasRssException("  abc \n def \r from com.uber \n\r"));

    Assert.assertFalse(MonitorUtils.hasRssException("  abc \n def \r from com.uber.rss.xyz \n\r"));

    Assert
        .assertFalse(MonitorUtils.hasRssException("  abc \n def \r abc.RuntimeException() \n\r"));
    Assert.assertTrue(MonitorUtils.hasRssException("  abc \n def \r abc.RssException() \n\r"));
    Assert.assertTrue(MonitorUtils.hasRssException("  abc \n def \r abc.RssXyzException() \n\r"));

    Assert.assertTrue(MonitorUtils.hasRssException("  abc \n def \r abc.OutOfMemoryError() \n\r"));
    Assert.assertTrue(MonitorUtils.hasRssException("  abc \n def \r abc.KryoException() \n\r"));
  }

}
