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

import org.apache.spark.{SparkFunSuite, SparkConf, SparkContext}
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

class MesosSchedulerUtilsSuite extends SparkFunSuite with Matchers with MockitoSugar {

  // scalastyle:off structural.type
  // this is the documented way of generating fixtures in scalatest
  def fixture: Object {val sc: SparkContext; val sparkConf: SparkConf} = new {
    val sparkConf = new SparkConf
    val sc = mock[SparkContext]
    when(sc.conf).thenReturn(sparkConf)
  }
  val utils = new MesosSchedulerUtils { }
  // scalastyle:on structural.type

  test("use at-least minimum overhead") {
    val f = fixture
    // 384 > sc.executorMemory * 0.1 => 512 + 384 = 896
    when(f.sc.executorMemory).thenReturn(512)
    utils.calculateTotalMemory(f.sc) shouldBe 896
  }

  test("use overhead if it is greater than minimum value") {
    val f = fixture
    // 384 > sc.executorMemory * 0.1 => 512 + 384 = 896
    when(f.sc.executorMemory).thenReturn(4096)
    utils.calculateTotalMemory(f.sc) shouldBe 4505
  }

  test("use spark.mesos.executor.memoryOverhead (if set)") {
    val f = fixture
    val utils = new MesosSchedulerUtils { }
    // 384 > sc.executorMemory * 0.1 => 512 + 384 = 896
    when(f.sc.executorMemory).thenReturn(1024)
    f.sparkConf.set("spark.mesos.executor.memoryOverhead", "512")
    utils.calculateTotalMemory(f.sc) shouldBe 1536
  }

  test("parse a non-empty constraint string correctly") {
    val expectedMap = Map(
      "tachyon" -> Set("true"),
      "zone" -> Set("us-east-1a", "us-east-1b")
    )
    utils.parseConstraintString("tachyon:true;zone:us-east-1a,us-east-1b") should be (expectedMap)
  }

  test("parse an empty constraint string correctly") {
    val utils = new MesosSchedulerUtils { }
    utils.parseConstraintString("") shouldBe Map()
  }

  test("throw an exception when the input is malformed") {
    an[IllegalArgumentException] should be thrownBy
      utils.parseConstraintString("tachyon;zone:us-east")
  }

  test("empty values for attributes' constraints matches all values") {
    val constraintsStr = "tachyon:"
    val parsedConstraints = utils.parseConstraintString(constraintsStr)

    parsedConstraints shouldBe Map("tachyon" -> Set())

    val `offer with no tachyon` = Map("zone" -> Set("us-east-1a", "us-east-1b"))
    val `offer with tachyon:true` = Map("tachyon" -> Set("true"))
    val `offer with tachyon:false` = Map("tachyon" -> Set("false"))

    utils.matchesAttributeRequirements(parsedConstraints, `offer with no tachyon`) shouldBe false
    utils.matchesAttributeRequirements(parsedConstraints, `offer with tachyon:true`) shouldBe true
    utils.matchesAttributeRequirements(parsedConstraints, `offer with tachyon:false`) shouldBe true
  }

  test("subset match is performed constraint attributes") {
    val `constraint with superset` = Map(
      "tachyon" -> Set("true"),
      "zone" -> Set("us-east-1a", "us-east-1b", "us-east-1c"))

    val zoneConstraintStr = "tachyon:;zone:us-east-1a,us-east-1c"
    val parsedConstraints = utils.parseConstraintString(zoneConstraintStr)

    utils.matchesAttributeRequirements(parsedConstraints, `constraint with superset`) shouldBe true
  }

}
