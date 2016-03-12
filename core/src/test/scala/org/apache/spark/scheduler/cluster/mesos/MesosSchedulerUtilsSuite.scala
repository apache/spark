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

import scala.collection.JavaConverters._
import scala.language.reflectiveCalls

import org.apache.mesos.Protos.{Resource, Value}
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

  def createTestPortResource(range: (Long, Long),
                              role: Option[String] = None): Resource = {

    val rangeValue = Value.Range.newBuilder()
    rangeValue.setBegin(range._1)
    rangeValue.setEnd(range._2)
    val builder = Resource.newBuilder()
      .setName("ports")
      .setType(Value.Type.RANGES)
      .setRanges(Value.Ranges.newBuilder().addRange(rangeValue))

    role.foreach { r => builder.setRole(r) }
    builder.build()
  }

  def rangesResourcesToTuple(resources: List[Resource]): List[(Long, Long)] = {
    resources.flatMap{resource => resource.getRanges.getRangeList
      .asScala.map(range => (range.getBegin, range.getEnd))}
  }

  def compareForEqualityOfPortRangeArrays(array1: Array[(Long, Long)], array2: Array[(Long, Long)]):
  Boolean = {
    array1.sortBy(identity).deep == array2.sortBy(identity).deep
  }

  def compareForEqualityOfPortArrays(array1: Array[Long], array2: Array[Long]):
  Boolean = {
    array1.sortBy(identity).deep == array2.sortBy(identity).deep
  }

  val utils = new MesosSchedulerUtils { }
  // scalastyle:on structural.type

  test("use at-least minimum overhead") {
    val f = fixture
    when(f.sc.executorMemory).thenReturn(512)
    utils.executorMemory(f.sc) shouldBe 896
  }

  test("use overhead if it is greater than minimum value") {
    val f = fixture
    when(f.sc.executorMemory).thenReturn(4096)
    utils.executorMemory(f.sc) shouldBe 4505
  }

  test("use spark.mesos.executor.memoryOverhead (if set)") {
    val f = fixture
    when(f.sc.executorMemory).thenReturn(1024)
    f.sparkConf.set("spark.mesos.executor.memoryOverhead", "512")
    utils.executorMemory(f.sc) shouldBe 1536
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

  test("Port reservation is done correctly with user specified ports only") {
    val conf = new SparkConf()
    conf.set("spark.executor.port", "3000" )
    conf.set("spark.blockManager.port", "4000")
    val portResource = createTestPortResource((3000, 5000), Some("my_role"))

    val (resourcesLeft, resourcesToBeUsed, portsToUse) = utils
      .partitionPorts(conf, List(portResource))
    portsToUse.length shouldBe 2

    compareForEqualityOfPortArrays(portsToUse.toArray, Array(3000L, 4000L)) shouldBe true

    val portsRangesLeft = rangesResourcesToTuple(resourcesLeft)
    val portRangesToBeUsed = rangesResourcesToTuple(resourcesToBeUsed)
    val expectedLeft = Array((3001L, 3999L), (4001L, 5000L))
    val expectedUSed = Array((3000L, 3000L), (4000L, 4000L))

    compareForEqualityOfPortRangeArrays(portsRangesLeft.toArray, expectedLeft) shouldBe true
    compareForEqualityOfPortRangeArrays(portRangesToBeUsed.toArray, expectedUSed) shouldBe true
  }

  test("Port reservation is done correctly with some user specified ports (spark.executor.port)") {
    val conf = new SparkConf()
    conf.set("spark.executor.port", "3100" )
    val portResource = createTestPortResource((3000, 5000), Some("my_role"))

    val (resourcesLeft, resourcesToBeUsed, portsToUse) = utils
      .partitionPorts(conf, List(portResource))
    portsToUse.length shouldBe 2
    portsToUse.contains(3100) shouldBe true
    val randomPort = portsToUse.filterNot(_ == 3100).head
    randomPort > 3000 shouldBe true
    randomPort <= 5000 shouldBe true
    randomPort != 3100 shouldBe true

    val portsRangesLeft = rangesResourcesToTuple(resourcesLeft)
    val portRangesToBeUsed = rangesResourcesToTuple(resourcesToBeUsed)

    if(randomPort > 3100) {
      if (randomPort == 3101) {
        val expectedLeft = Array((3000L, 3099L), (randomPort + 1, 5000L))
        val expectedUsed = Array((3100L, 3100L), (randomPort, randomPort))

        compareForEqualityOfPortRangeArrays(portsRangesLeft.toArray, expectedLeft) shouldBe true
        compareForEqualityOfPortRangeArrays(portRangesToBeUsed.toArray, expectedUsed) shouldBe true

      } else {
        val expectedLeft = Array((3000L, 3099L), (3101L, randomPort - 1), (randomPort + 1, 5000L))
        val expectedUsed = Array((3100L, 3100L), (randomPort, randomPort))

        compareForEqualityOfPortRangeArrays(portsRangesLeft.toArray, expectedLeft) shouldBe true
        compareForEqualityOfPortRangeArrays(portRangesToBeUsed.toArray, expectedUsed) shouldBe true
      }

    } else {
      if (randomPort == 3000) {
        val expectedLeft = Array((3001L, 3099L), (3101L, 5000L))
        val expectedUsed = Array((3100L, 3100L), (randomPort, randomPort))

        compareForEqualityOfPortRangeArrays(portsRangesLeft.toArray, expectedLeft) shouldBe true
        compareForEqualityOfPortRangeArrays(portRangesToBeUsed.toArray, expectedUsed) shouldBe true

      } else {
        val expectedLeft = Array((3000L, randomPort - 1), (randomPort + 1, 3099L), (30101L, 5000L))
        val expectedUsed = Array((3100L, 3100L), (randomPort, randomPort))

        compareForEqualityOfPortRangeArrays(portsRangesLeft.toArray, expectedLeft) shouldBe true
        compareForEqualityOfPortRangeArrays(portRangesToBeUsed.toArray, expectedUsed) shouldBe true
      }
    }
  }

  test("Port reservation is done correctly with all random ports") {
    val conf = new SparkConf()
    conf.set("spark.blockmanager.port", "3600" )
    val portResource = createTestPortResource((3000L, 5000L), Some("my_role"))

    val (resourcesLeft, resourcesToBeUsed, portsToUse) = utils
      .partitionPorts(conf, List(portResource))

    portsToUse.length shouldBe 2

    portsToUse.head != portsToUse(1) shouldBe true
    val sortedPortsToUse = portsToUse.sortBy(identity)
    val portsRangesLeft = rangesResourcesToTuple(resourcesLeft)
    val portRangesToBeUsed = rangesResourcesToTuple(resourcesToBeUsed)

    if (!sortedPortsToUse.contains(3000L) && !sortedPortsToUse.contains(5000L)) {
      val portLeftPair1 = (3000L, sortedPortsToUse.head - 1)
      val portLeftPair2 = (sortedPortsToUse.head + 1, sortedPortsToUse(1) - 1)
      val portLeftPair3 = (sortedPortsToUse(1) + 1, 5000L)
      val expectedLeft = Array(portLeftPair1, portLeftPair2, portLeftPair3)

      val portUsedPair1 = (sortedPortsToUse.head, sortedPortsToUse.head)
      val portUsedPair2 = (sortedPortsToUse(1), sortedPortsToUse(1))
      val expectedUsed = Array(portUsedPair1, portUsedPair2)

      compareForEqualityOfPortRangeArrays(portsRangesLeft.toArray, expectedLeft) shouldBe true
      compareForEqualityOfPortRangeArrays(portRangesToBeUsed.toArray, expectedUsed) shouldBe true
    }
  }

  test("Port reservation is done correctly with user specified ports only - multiple ranges") {
    val conf = new SparkConf()
    conf.set("spark.executor.port", "2100" )
    conf.set("spark.blockManager.port", "4000")
    val portResourceList = List(createTestPortResource((3000, 5000), Some("my_role")),
      createTestPortResource((2000, 2500), Some("other_role")))

    val (resourcesLeft, resourcesToBeUsed, portsToUse) = utils
      .partitionPorts(conf, portResourceList)
    portsToUse.length shouldBe 2
    val portsRangesLeft = rangesResourcesToTuple(resourcesLeft)
    val portRangesToBeUsed = rangesResourcesToTuple(resourcesToBeUsed)
    val leftExpected = Array((2000L, 2099L), (2101L, 2500L), (3000L, 3999L), (4001L, 5000L))
    val expectedUsed = Array((2100L, 2100L), (4000L, 4000L))

    compareForEqualityOfPortArrays(portsToUse.toArray, Array(2100L, 4000L)) shouldBe true
    compareForEqualityOfPortRangeArrays(portsRangesLeft.toArray, leftExpected) shouldBe true
    compareForEqualityOfPortRangeArrays(portRangesToBeUsed.toArray, expectedUsed) shouldBe true
  }

  test("Port reservation is done correctly with all random ports - multiple ranges") {
    val conf = new SparkConf()
    conf.set("spark.blockmanager.port", "3600" )
    val portResourceList = List(createTestPortResource((3000, 5000), Some("my_role")),
      createTestPortResource((2000, 2500), Some("other_role")))

    val (resourcesLeft, resourcesToBeUsed, portsToUse) = utils
      .partitionPorts(conf, portResourceList)

    portsToUse.length shouldBe 2

    portsToUse.head != portsToUse(1) shouldBe true
    val sortedPortsToUse = portsToUse.sortBy(identity)
    val portsRangesLeft = rangesResourcesToTuple(resourcesLeft)
    val portRangesToBeUsed = rangesResourcesToTuple(resourcesToBeUsed)

    if (sortedPortsToUse.head > 3000 && sortedPortsToUse(1) > 3000 &&
      !sortedPortsToUse.contains(5000)) {

      val portLeftPair1 = (2000L, 2500L)
      val portLeftPair2 = (3000L, sortedPortsToUse.head - 1)
      val portLeftPair3 = (sortedPortsToUse.head + 1, sortedPortsToUse(1) - 1)
      val portLeftPair4 = (sortedPortsToUse(1) + 1, 5000L)
      val expectedLeft = Array(portLeftPair1, portLeftPair2, portLeftPair3, portLeftPair4)

      val portUsedPair1 = (sortedPortsToUse.head, sortedPortsToUse.head)
      val portUsedPair2 = (sortedPortsToUse(1), sortedPortsToUse(1))
      val expectedUsed = Array(portUsedPair1, portUsedPair2)

      compareForEqualityOfPortRangeArrays(portRangesToBeUsed.toArray, expectedUsed) shouldBe true
      compareForEqualityOfPortRangeArrays(portsRangesLeft.toArray, expectedLeft) shouldBe true
    }

    if (sortedPortsToUse.exists(_ < 2500) && sortedPortsToUse.exists(_ > 3000) &&
      !sortedPortsToUse.exists(port => port == 2000 || port == 5000)) {

      val portLeftPair1 = (2000L, sortedPortsToUse.head -1)
      val portLeftPair2 = (sortedPortsToUse.head + 1, 2500L)
      val portLeftPair3 = (3000L, sortedPortsToUse(1) - 1)
      val portLeftPair4 = (sortedPortsToUse(1) + 1, 5000L)
      val expectedLeft = Array(portLeftPair1, portLeftPair2, portLeftPair3, portLeftPair4)

      val portUsedPair1 = (sortedPortsToUse.head, sortedPortsToUse.head)
      val portUsedPair2 = (sortedPortsToUse(1), sortedPortsToUse(1))
      val expectedUsed = Array(portUsedPair1, portUsedPair2)

      compareForEqualityOfPortRangeArrays(portRangesToBeUsed.toArray, expectedUsed) shouldBe true
      compareForEqualityOfPortRangeArrays(portsRangesLeft.toArray, expectedLeft) shouldBe true
    }
  }
}

