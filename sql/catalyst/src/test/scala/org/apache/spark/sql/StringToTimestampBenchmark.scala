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

package org.apache.spark.sql

import java.util.Calendar

import org.apache.spark.sql.catalyst.util.{DateTimeTestUtils, DateTimeUtils}
import org.apache.spark.util.Benchmark

object StringToTimestampBenchmark {

  def main(args: Array[String]): Unit = {

    val len = 100000
    val benchmark = new Benchmark("string to timestamp", len)

    val tzs = Seq.fill(len)(scala.util.Random.nextInt(DateTimeTestUtils.ALL_TIMEZONES.length))
      .map(DateTimeTestUtils.ALL_TIMEZONES(_))

    benchmark.addCase("Creating calendar instance on each call") { _ =>
      tzs.foreach(Calendar.getInstance)
    }

    benchmark.addCase("Caching calendar instance") { _ =>
      tzs.foreach(DateTimeUtils.getCalendar)
    }

    /*
    Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz

    string to timestamp:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Creating calendar instance on each call         20 /   21          5.1         195.0       1.0X
    Caching calendar instance                        8 /    8         12.7          78.6       2.5X
     */
    benchmark.run()
  }
}
