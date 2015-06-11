/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.unsafe.types;

import java.sql.Timestamp;

import junit.framework.Assert;
import org.junit.Test;

public class DateUtilsSuite {

  @Test
  public void timestamp() {
    Timestamp now = new Timestamp(System.currentTimeMillis());
    now.setNanos(100);
    long ns = DateUtils.fromJavaTimestamp(now);
    Assert.assertEquals(ns % 10000000L, 1);
    Assert.assertEquals(DateUtils.toJavaTimestamp(ns), now);

    testTimestamp(-111111111111L);
    testTimestamp(-1L);
    testTimestamp(0);
    testTimestamp(1L);
    testTimestamp(111111111111L);
  }

  private void testTimestamp(long t) {
    Timestamp ts = DateUtils.toJavaTimestamp(t);
    Assert.assertEquals(DateUtils.fromJavaTimestamp(ts), t);
    Assert.assertEquals(DateUtils.toJavaTimestamp(DateUtils.fromJavaTimestamp(ts)), ts);
  }
}
