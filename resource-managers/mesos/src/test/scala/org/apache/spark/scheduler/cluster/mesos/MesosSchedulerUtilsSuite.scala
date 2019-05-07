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

import java.io.{File, FileNotFoundException}

import scala.collection.JavaConverters._

import com.google.common.io.Files
import org.apache.mesos.Protos.{FrameworkInfo, Resource, Value}
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.deploy.mesos.{config => mesosConfig}
import org.apache.spark.internal.config._
import org.apache.spark.util.SparkConfWithEnv

class MesosSchedulerUtilsSuite extends SparkFunSuite with Matchers with MockitoSugar {

  class SparkConfFixture {
    val sparkConf = new SparkConf
    val sc: SparkContext = mock[SparkContext]
    when(sc.conf).thenReturn(sparkConf)
  }

  def fixture: SparkConfFixture = new SparkConfFixture()

  private def createTestPortResource(range: (Long, Long), role: Option[String] = None): Resource = {
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

  private def rangesResourcesToTuple(resources: List[Resource]): List[(Long, Long)] = {
    resources.flatMap{resource => resource.getRanges.getRangeList
      .asScala.map(range => (range.getBegin, range.getEnd))}
  }

  def arePortsEqual(array1: Array[(Long, Long)], array2: Array[(Long, Long)])
    : Boolean = {
    array1.sortBy(identity).deep == array2.sortBy(identity).deep
  }

  def arePortsEqual(array1: Array[Long], array2: Array[Long])
    : Boolean = {
    array1.sortBy(identity).deep == array2.sortBy(identity).deep
  }

  def getRangesFromResources(resources: List[Resource]): List[(Long, Long)] = {
    resources.flatMap{ resource =>
      resource.getRanges.getRangeList.asScala.toList.map{
        range => (range.getBegin, range.getEnd)}}
  }

  val utils = new MesosSchedulerUtils { }

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
    f.sparkConf.set(mesosConfig.EXECUTOR_MEMORY_OVERHEAD, 512)
    utils.executorMemory(f.sc) shouldBe 1536
  }

  test("parse a non-empty constraint string correctly") {
    val expectedMap = Map(
      "os" -> Set("centos7"),
      "zone" -> Set("us-east-1a", "us-east-1b")
    )
    utils.parseConstraintString("os:centos7;zone:us-east-1a,us-east-1b") should be (expectedMap)
  }

  test("parse an empty constraint string correctly") {
    utils.parseConstraintString("") shouldBe Map()
  }

  test("throw an exception when the input is malformed") {
    an[IllegalArgumentException] should be thrownBy
      utils.parseConstraintString("os;zone:us-east")
  }

  test("empty values for attributes' constraints matches all values") {
    val constraintsStr = "os:"
    val parsedConstraints = utils.parseConstraintString(constraintsStr)

    parsedConstraints shouldBe Map("os" -> Set())

    val zoneSet = Value.Set.newBuilder().addItem("us-east-1a").addItem("us-east-1b").build()
    val noOsOffer = Map("zone" -> zoneSet)
    val centosOffer = Map("os" -> Value.Text.newBuilder().setValue("centos").build())
    val ubuntuOffer = Map("os" -> Value.Text.newBuilder().setValue("ubuntu").build())

    utils.matchesAttributeRequirements(parsedConstraints, noOsOffer) shouldBe false
    utils.matchesAttributeRequirements(parsedConstraints, centosOffer) shouldBe true
    utils.matchesAttributeRequirements(parsedConstraints, ubuntuOffer) shouldBe true
  }

  test("subset match is performed for set attributes") {
    val supersetConstraint = Map(
      "os" -> Value.Text.newBuilder().setValue("ubuntu").build(),
      "zone" -> Value.Set.newBuilder()
        .addItem("us-east-1a")
        .addItem("us-east-1b")
        .addItem("us-east-1c")
        .build())

    val zoneConstraintStr = "os:;zone:us-east-1a,us-east-1c"
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
    val offerAttribs = Map("os" -> Value.Text.newBuilder().setValue("centos7").build())

    val trueConstraint = utils.parseConstraintString("os:centos7")
    val falseConstraint = utils.parseConstraintString("os:ubuntu")

    utils.matchesAttributeRequirements(trueConstraint, offerAttribs) shouldBe true
    utils.matchesAttributeRequirements(falseConstraint, offerAttribs) shouldBe false
  }

  test("Port reservation is done correctly with user specified ports only") {
    val conf = new SparkConf()
    conf.set(BLOCK_MANAGER_PORT, 4000)
    val portResource = createTestPortResource((3000, 5000), Some("my_role"))

    val (resourcesLeft, resourcesToBeUsed) = utils
      .partitionPortResources(List(4000), List(portResource))
    resourcesToBeUsed.length shouldBe 1

    val portsToUse = getRangesFromResources(resourcesToBeUsed).map{r => r._1}.toArray

    portsToUse.length shouldBe 1
    arePortsEqual(portsToUse, Array(4000L)) shouldBe true

    val portRangesToBeUsed = rangesResourcesToTuple(resourcesToBeUsed)

    val expectedUSed = Array((4000L, 4000L))

    arePortsEqual(portRangesToBeUsed.toArray, expectedUSed) shouldBe true
  }

  test("Port reservation is done correctly with all random ports") {
    val conf = new SparkConf()
    val portResource = createTestPortResource((3000L, 5000L), Some("my_role"))

    val (resourcesLeft, resourcesToBeUsed) = utils
      .partitionPortResources(List(), List(portResource))
    val portsToUse = getRangesFromResources(resourcesToBeUsed).map{r => r._1}

    portsToUse.isEmpty shouldBe true
  }

  test("Port reservation is done correctly with user specified ports only - multiple ranges") {
    val conf = new SparkConf()
    conf.set(BLOCK_MANAGER_PORT, 4000)
    val portResourceList = List(createTestPortResource((3000, 5000), Some("my_role")),
      createTestPortResource((2000, 2500), Some("other_role")))
    val (resourcesLeft, resourcesToBeUsed) = utils
      .partitionPortResources(List(4000), portResourceList)
    val portsToUse = getRangesFromResources(resourcesToBeUsed).map{r => r._1}

    portsToUse.length shouldBe 1
    val portsRangesLeft = rangesResourcesToTuple(resourcesLeft)
    val portRangesToBeUsed = rangesResourcesToTuple(resourcesToBeUsed)

    val expectedUsed = Array((4000L, 4000L))

    arePortsEqual(portsToUse.toArray, Array(4000L)) shouldBe true
    arePortsEqual(portRangesToBeUsed.toArray, expectedUsed) shouldBe true
  }

  test("Port reservation is done correctly with all random ports - multiple ranges") {
    val conf = new SparkConf()
    val portResourceList = List(createTestPortResource((3000, 5000), Some("my_role")),
      createTestPortResource((2000, 2500), Some("other_role")))
    val (resourcesLeft, resourcesToBeUsed) = utils
      .partitionPortResources(List(), portResourceList)
    val portsToUse = getRangesFromResources(resourcesToBeUsed).map{r => r._1}
    portsToUse.isEmpty shouldBe true
  }

  test("Principal specified via spark.mesos.principal") {
    val conf = new SparkConf()
    conf.set(mesosConfig.CREDENTIAL_PRINCIPAL, "test-principal")

    val credBuilder = utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    credBuilder.hasPrincipal shouldBe true
    credBuilder.getPrincipal shouldBe "test-principal"
  }

  test("Principal specified via spark.mesos.principal.file") {
    val pFile = File.createTempFile("MesosSchedulerUtilsSuite", ".txt")
    pFile.deleteOnExit()
    Files.write("test-principal".getBytes("UTF-8"), pFile)
    val conf = new SparkConf()
    conf.set(mesosConfig.CREDENTIAL_PRINCIPAL_FILE, pFile.getAbsolutePath())

    val credBuilder = utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    credBuilder.hasPrincipal shouldBe true
    credBuilder.getPrincipal shouldBe "test-principal"
  }

  test("Principal specified via spark.mesos.principal.file that does not exist") {
    val conf = new SparkConf()
    conf.set(mesosConfig.CREDENTIAL_PRINCIPAL_FILE, "/tmp/does-not-exist")

    intercept[FileNotFoundException] {
      utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    }
  }

  test("Principal specified via SPARK_MESOS_PRINCIPAL") {
    val conf = new SparkConfWithEnv(Map("SPARK_MESOS_PRINCIPAL" -> "test-principal"))

    val credBuilder = utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    credBuilder.hasPrincipal shouldBe true
    credBuilder.getPrincipal shouldBe "test-principal"
  }

  test("Principal specified via SPARK_MESOS_PRINCIPAL_FILE") {
    val pFile = File.createTempFile("MesosSchedulerUtilsSuite", ".txt")
    pFile.deleteOnExit()
    Files.write("test-principal".getBytes("UTF-8"), pFile)
    val conf = new SparkConfWithEnv(Map("SPARK_MESOS_PRINCIPAL_FILE" -> pFile.getAbsolutePath()))

    val credBuilder = utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    credBuilder.hasPrincipal shouldBe true
    credBuilder.getPrincipal shouldBe "test-principal"
  }

  test("Principal specified via SPARK_MESOS_PRINCIPAL_FILE that does not exist") {
    val conf = new SparkConfWithEnv(Map("SPARK_MESOS_PRINCIPAL_FILE" -> "/tmp/does-not-exist"))

    intercept[FileNotFoundException] {
      utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    }
  }

  test("Secret specified via spark.mesos.secret") {
    val conf = new SparkConf()
    conf.set(mesosConfig.CREDENTIAL_PRINCIPAL, "test-principal")
    conf.set(mesosConfig.CREDENTIAL_SECRET, "my-secret")

    val credBuilder = utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    credBuilder.hasPrincipal shouldBe true
    credBuilder.getPrincipal shouldBe "test-principal"
    credBuilder.hasSecret shouldBe true
    credBuilder.getSecret shouldBe "my-secret"
  }

  test("Principal specified via spark.mesos.secret.file") {
    val sFile = File.createTempFile("MesosSchedulerUtilsSuite", ".txt")
    sFile.deleteOnExit()
    Files.write("my-secret".getBytes("UTF-8"), sFile)
    val conf = new SparkConf()
    conf.set(mesosConfig.CREDENTIAL_PRINCIPAL, "test-principal")
    conf.set(mesosConfig.CREDENTIAL_SECRET_FILE, sFile.getAbsolutePath())

    val credBuilder = utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    credBuilder.hasPrincipal shouldBe true
    credBuilder.getPrincipal shouldBe "test-principal"
    credBuilder.hasSecret shouldBe true
    credBuilder.getSecret shouldBe "my-secret"
  }

  test("Principal specified via spark.mesos.secret.file that does not exist") {
    val conf = new SparkConf()
    conf.set(mesosConfig.CREDENTIAL_PRINCIPAL, "test-principal")
    conf.set(mesosConfig.CREDENTIAL_SECRET_FILE, "/tmp/does-not-exist")

    intercept[FileNotFoundException] {
      utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    }
  }

  test("Principal specified via SPARK_MESOS_SECRET") {
    val env = Map("SPARK_MESOS_SECRET" -> "my-secret")
    val conf = new SparkConfWithEnv(env)
    conf.set(mesosConfig.CREDENTIAL_PRINCIPAL, "test-principal")

    val credBuilder = utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    credBuilder.hasPrincipal shouldBe true
    credBuilder.getPrincipal shouldBe "test-principal"
    credBuilder.hasSecret shouldBe true
    credBuilder.getSecret shouldBe "my-secret"
  }

  test("Principal specified via SPARK_MESOS_SECRET_FILE") {
    val sFile = File.createTempFile("MesosSchedulerUtilsSuite", ".txt")
    sFile.deleteOnExit()
    Files.write("my-secret".getBytes("UTF-8"), sFile)

    val sFilePath = sFile.getAbsolutePath()
    val env = Map("SPARK_MESOS_SECRET_FILE" -> sFilePath)
    val conf = new SparkConfWithEnv(env)
    conf.set(mesosConfig.CREDENTIAL_PRINCIPAL, "test-principal")

    val credBuilder = utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    credBuilder.hasPrincipal shouldBe true
    credBuilder.getPrincipal shouldBe "test-principal"
    credBuilder.hasSecret shouldBe true
    credBuilder.getSecret shouldBe "my-secret"
  }

  test("Secret specified with no principal") {
    val conf = new SparkConf()
    conf.set(mesosConfig.CREDENTIAL_SECRET, "my-secret")

    intercept[SparkException] {
      utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    }
  }

  test("Principal specification preference") {
    val conf = new SparkConfWithEnv(Map("SPARK_MESOS_PRINCIPAL" -> "other-principal"))
    conf.set(mesosConfig.CREDENTIAL_PRINCIPAL, "test-principal")

    val credBuilder = utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    credBuilder.hasPrincipal shouldBe true
    credBuilder.getPrincipal shouldBe "test-principal"
  }

  test("Secret specification preference") {
    val conf = new SparkConfWithEnv(Map("SPARK_MESOS_SECRET" -> "other-secret"))
    conf.set(mesosConfig.CREDENTIAL_PRINCIPAL, "test-principal")
    conf.set(mesosConfig.CREDENTIAL_SECRET, "my-secret")

    val credBuilder = utils.buildCredentials(conf, FrameworkInfo.newBuilder())
    credBuilder.hasPrincipal shouldBe true
    credBuilder.getPrincipal shouldBe "test-principal"
    credBuilder.hasSecret shouldBe true
    credBuilder.getSecret shouldBe "my-secret"
  }
}
