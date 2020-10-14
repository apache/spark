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

package org.apache.spark.streaming.util

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit._

import org.apache.spark.SparkFunSuite

class RateLimitedOutputStreamSuite extends SparkFunSuite {

  private def benchmark[U](f: => U): Long = {
    val start = System.nanoTime
    f
    System.nanoTime - start
  }

  test("write") {
    val underlying = new ByteArrayOutputStream
    val data = "X" * 41000
    val stream = new RateLimitedOutputStream(underlying, desiredBytesPerSec = 10000)
    val elapsedNs = benchmark { stream.write(data.getBytes(StandardCharsets.UTF_8)) }

    val seconds = SECONDS.convert(elapsedNs, NANOSECONDS)
    assert(seconds >= 4, s"Seconds value ($seconds) is less than 4.")
    assert(seconds <= 30, s"Took more than 30 seconds ($seconds) to write data.")
    assert(underlying.toString(StandardCharsets.UTF_8.name()) === data)
  }
}
