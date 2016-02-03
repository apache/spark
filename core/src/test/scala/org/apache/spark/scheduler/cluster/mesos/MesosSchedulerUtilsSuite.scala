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

import scala.language.reflectiveCalls

import org.apache.mesos.Protos.Value
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

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
    when(f.sc.executorMemory).thenReturn(512)
    utils.calculateTotalMemory(f.sc) shouldBe 896
  }

  test("use overhead if it is greater than minimum value") {
    val f = fixture
    when(f.sc.executorMemory).thenReturn(4096)
    utils.calculateTotalMemory(f.sc) shouldBe 4505
  }

  test("use spark.mesos.executor.memoryOverhead (if set)") {
    val f = fixture
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

    val zoneSet = Value.Set.newBuilder().addItem("us-east-1a").addItem("us-east-1b").build()
    val noTachyonOffer = Map("zone" -> zoneSet)
    val tachyonTrueOffer = Map("tachyon" -> Value.Text.newBuilder().setValue("true").build())
    val tachyonFalseOffer = Map("tachyon" -> Value.Text.newBuilder().setValue("false").build())

    utils.matchesAttributeRequirements(parsedConstraints, noTachyonOffer) shouldBe false
    utils.matchesAttributeRequirements(parsedConstraints, tachyonTrueOffer) shouldBe true
    utils.matchesAttributeRequirements(parsedConstraints, tachyonFalseOffer) shouldBe true
  }

  test("subset match is performed for set attributes") {
    val supersetConstraint = Map(
      "tachyon" -> Value.Text.newBuilder().setValue("true").build(),
      "zone" -> Value.Set.newBuilder()
        .addItem("us-east-1a")
        .addItem("us-east-1b")
        .addItem("us-east-1c")
        .build())

    val zoneConstraintStr = "tachyon:;zone:us-east-1a,us-east-1c"
    val parsedConstraints = utils.parseConstraintString(zoneConstraintStr)

    utils.matchesAttributeRequirements(parsedConstraints, supersetConstraint) shouldBe true
  }

  test("less than equal match is performed on scalar attributes") {
    val offerAttribs = Map("gpus" -> Value.Scalar.newBuilder().setValue(3).build())

    val ltConstraint = utils.parseConstraintString("gpus:2")
    val eqConstraint = utils.parseConstraintString("gpus:3")
    val gtConstraint = utils.parseConstraintString("gpus:4")

    utils.matchesAttributeRequirements(ltConstraint, offerAttribs) shouldBe true
    utils.matchesAttributeRequirements(eqConstraint, offerAttribs) shouldBe true
    utils.matchesAttributeRequirements(gtConstraint, offerAttribs) shouldBe false
  }

  test("contains match is performed for range attributes") {
    val offerAttribs = Map("ports" -> Value.Range.newBuilder().setBegin(7000).setEnd(8000).build())
    val ltConstraint = utils.parseConstraintString("ports:6000")
    val eqConstraint = utils.parseConstraintString("ports:7500")
    val gtConstraint = utils.parseConstraintString("ports:8002")
    val multiConstraint = utils.parseConstraintString("ports:5000,7500,8300")

    utils.matchesAttributeRequirements(ltConstraint, offerAttribs) shouldBe false
    utils.matchesAttributeRequirements(eqConstraint, offerAttribs) shouldBe true
    utils.matchesAttributeRequirements(gtConstraint, offerAttribs) shouldBe false
    utils.matchesAttributeRequirements(multiConstraint, offerAttribs) shouldBe true
  }

  test("equality match is performed for text attributes") {
    val offerAttribs = Map("tachyon" -> Value.Text.newBuilder().setValue("true").build())

    val trueConstraint = utils.parseConstraintString("tachyon:true")
    val falseConstraint = utils.parseConstraintString("tachyon:false")

    utils.matchesAttributeRequirements(trueConstraint, offerAttribs) shouldBe true
    utils.matchesAttributeRequirements(falseConstraint, offerAttribs) shouldBe false
  }

}
