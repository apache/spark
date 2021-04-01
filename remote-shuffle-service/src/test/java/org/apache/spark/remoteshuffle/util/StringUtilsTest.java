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

import java.util.ArrayList;
import java.util.Arrays;

public class StringUtilsTest {

  @Test
  public void toString4SortedIntList() {
    Assert.assertEquals("", StringUtils.toString4SortedNumberList(null));
    Assert.assertEquals("", StringUtils.toString4SortedNumberList(new ArrayList<>()));

    Assert.assertEquals("1", StringUtils.toString4SortedNumberList(Arrays.asList(1)));
    Assert.assertEquals("1,1", StringUtils.toString4SortedNumberList(Arrays.asList(1, 1)));
    Assert.assertEquals("1,1-2", StringUtils.toString4SortedNumberList(Arrays.asList(1, 1, 2)));
    Assert.assertEquals("1-2", StringUtils.toString4SortedNumberList(Arrays.asList(1, 2)));
    Assert.assertEquals("1-3", StringUtils.toString4SortedNumberList(Arrays.asList(1, 2, 3)));
    Assert.assertEquals("1-4", StringUtils.toString4SortedNumberList(Arrays.asList(1, 2, 3, 4)));
    Assert.assertEquals("1-4,6",
        StringUtils.toString4SortedNumberList(Arrays.asList(1, 2, 3, 4, 6)));
    Assert.assertEquals("0-1,3-4,6",
        StringUtils.toString4SortedNumberList(Arrays.asList(0, 1, 3, 4, 6)));
    Assert.assertEquals("0-1,3-4,6,6",
        StringUtils.toString4SortedNumberList(Arrays.asList(0, 1, 3, 4, 6, 6)));
    Assert.assertEquals("0-1,3-4,6,6-10",
        StringUtils.toString4SortedNumberList(Arrays.asList(0, 1, 3, 4, 6, 6, 7, 8, 9, 10)));
    Assert.assertEquals("0-1,3-4,6,6,6-10",
        StringUtils.toString4SortedNumberList(Arrays.asList(0, 1, 3, 4, 6, 6, 6, 7, 8, 9, 10)));
    Assert.assertEquals("0-1,3-4,6,6,6-10,12", StringUtils
        .toString4SortedNumberList(Arrays.asList(0, 1, 3, 4, 6, 6, 6, 7, 8, 9, 10, 12)));
    Assert.assertEquals("0-1,3-4,6,6,6-10,12,12", StringUtils
        .toString4SortedNumberList(Arrays.asList(0, 1, 3, 4, 6, 6, 6, 7, 8, 9, 10, 12, 12)));
  }
}
