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

package org.apache.spark.streaming

import scala.io.Source
import scala.language.postfixOps

import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkConf

class UISuite extends FunSuite {

  // Ignored: See SPARK-1530
  ignore("streaming tab in spark UI") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.ui.enabled", "true")
    val ssc = new StreamingContext(conf, Seconds(1))
    assert(ssc.sc.ui.isDefined, "Spark UI is not started!")
    val ui = ssc.sc.ui.get

    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      val html = Source.fromURL(ui.appUIAddress).mkString
      assert(!html.contains("random data that should not be present"))
      // test if streaming tab exist
      assert(html.toLowerCase.contains("streaming"))
      // test if other Spark tabs still exist
      assert(html.toLowerCase.contains("stages"))
    }

    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      val html = Source.fromURL(ui.appUIAddress.stripSuffix("/") + "/streaming").mkString
      assert(html.toLowerCase.contains("batch"))
      assert(html.toLowerCase.contains("network"))
    }
  }
}
