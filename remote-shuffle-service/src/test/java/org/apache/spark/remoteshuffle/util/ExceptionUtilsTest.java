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

import java.net.ConnectException;

public class ExceptionUtilsTest {

  @Test
  public void getExceptionSimpleMessage() {
    Assert.assertEquals("", ExceptionUtils.getSimpleMessage(null));
    Assert.assertEquals("NullPointerException (null)",
        ExceptionUtils.getSimpleMessage(new NullPointerException()));
    Assert.assertEquals("NullPointerException ()",
        ExceptionUtils.getSimpleMessage(new NullPointerException("")));
    Assert.assertEquals("NullPointerException (null pointer)",
        ExceptionUtils.getSimpleMessage(new NullPointerException("null pointer")));
  }

  @Test
  public void isTimeoutException() {
    Assert.assertFalse(ExceptionUtils.isTimeoutException(null));
    Assert.assertFalse(ExceptionUtils.isTimeoutException(new RuntimeException((String) null)));
    Assert.assertFalse(ExceptionUtils.isTimeoutException(new RuntimeException("")));
    Assert.assertTrue(
        ExceptionUtils.isTimeoutException(new ConnectException("Connection Timed out in socket")));
    Assert.assertTrue(
        ExceptionUtils.isTimeoutException(new ConnectException("Connection timedout in socket")));
    Assert.assertTrue(
        ExceptionUtils.isTimeoutException(new ConnectException("Connection Time out in socket")));
    Assert.assertTrue(
        ExceptionUtils.isTimeoutException(new ConnectException("Connection timeout in socket")));
  }
}
