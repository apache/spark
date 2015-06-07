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

package org.apache.spark.scheduler.cluster.mesos

import org.apache.spark.{SparkConf, SparkContext}
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

class MesosSchedulerUtilsSuite extends FlatSpec with Matchers with MockitoSugar {

  // scalastyle:off structural.type
  // this is the documented way of generating fixtures in scalatest
  def fixture: Object {val sc: SparkContext; val sparkConf: SparkConf} = new {
    val sparkConf = new SparkConf
    val sc = mock[SparkContext]
    when(sc.conf).thenReturn(sparkConf)
  }
  // scalastyle:on structural.type

  "MesosSchedulerUtils" should "use at-least minimum overhead" in new MesosSchedulerUtils {
    val f = fixture
    // 384 > sc.executorMemory * 0.1 => 512 + 384 = 896
    when(f.sc.executorMemory).thenReturn(512)
    calculateTotalMemory(f.sc) shouldBe 896
  }

  it should "use overhead if it is greater than minimum value" in new MesosSchedulerUtils {
    val f = fixture
    // 384 > sc.executorMemory * 0.1 => 512 + 384 = 896
    when(f.sc.executorMemory).thenReturn(4096)
    calculateTotalMemory(f.sc) shouldBe 4505
  }

  it should "use spark.mesos.executor.memoryOverhead (if set)" in new MesosSchedulerUtils {
    val f = fixture
    // 384 > sc.executorMemory * 0.1 => 512 + 384 = 896
    when(f.sc.executorMemory).thenReturn(1024)
    f.sparkConf.set("spark.mesos.executor.memoryOverhead", "512")
    calculateTotalMemory(f.sc) shouldBe 1536
  }

  it should "parse a non-empty constraint string correctly" in new MesosSchedulerUtils {
    val expectedMap = Map(
      "tachyon" -> Set("true"),
      "zone" -> Set("us-east-1a", "us-east-1b")
    )
    parseConstraintString("tachyon:true;zone:us-east-1a,us-east-1b") should be (expectedMap)
  }

  it should "parse an empty constraint string correctly" in new MesosSchedulerUtils {
    parseConstraintString("") shouldBe Map()
  }

  it should "throw an exception when the input is malformed" in new MesosSchedulerUtils {
    an[IllegalArgumentException] should be thrownBy parseConstraintString("tachyon;zone:us-east")
  }

}
