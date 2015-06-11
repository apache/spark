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

package org.apache.spark.sql.catalyst.util

import java.sql.Timestamp

import org.apache.spark.SparkFunSuite


class DateTimeUtilsSuite extends SparkFunSuite {

  test("timestamp and 100ns") {
    val now = new Timestamp(System.currentTimeMillis())
    now.setNanos(100)
    val ns = DateTimeUtils.fromJavaTimestamp(now)
    assert(ns % 10000000L == 1)
    assert(DateTimeUtils.toJavaTimestamp(ns) == now)

    List(-111111111111L, -1L, 0, 1L, 111111111111L).foreach { t =>
      val ts = DateTimeUtils.toJavaTimestamp(t)
      assert(DateTimeUtils.fromJavaTimestamp(ts) == t)
      assert(DateTimeUtils.toJavaTimestamp(DateTimeUtils.fromJavaTimestamp(ts)) == ts)
    }
  }

  test("100ns and julian day") {
    val (d, ns) = DateTimeUtils.toJulianDay(0)
    assert(d == 2440587)
    assert(ns == DateTimeUtils.SECONDS_PER_DAY / 2 * DateTimeUtils.NANOS_PER_SECOND)
    assert(DateTimeUtils.fromJulianDay(d, ns) == 0L)

    val t = new Timestamp(61394778610000L) // (2015, 6, 11, 10, 10, 10, 100)
    val (d1, ns1) = DateTimeUtils.toJulianDay(DateTimeUtils.fromJavaTimestamp(t))
    val t2 = DateTimeUtils.toJavaTimestamp(DateTimeUtils.fromJulianDay(d1, ns1))
    assert(t.equals(t2))
  }
}
