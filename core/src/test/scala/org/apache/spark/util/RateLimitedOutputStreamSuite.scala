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

package org.apache.spark.util

import org.scalatest.FunSuite
import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit._

class RateLimitedOutputStreamSuite extends FunSuite {

  private def benchmark[U](f: => U): Long = {
    val start = System.nanoTime
    f
    System.nanoTime - start
  }

  test("write") {
    val underlying = new ByteArrayOutputStream
    val data = "X" * 41000
    val stream = new RateLimitedOutputStream(underlying, 10000)
    val elapsedNs = benchmark { stream.write(data.getBytes("UTF-8")) }
    assert(SECONDS.convert(elapsedNs, NANOSECONDS) == 4)
    assert(underlying.toString("UTF-8") == data)
  }
}
