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

package org.apache.spark.deploy.yarn

import java.io.{File, FileInputStream, FileNotFoundException, FileOutputStream}
import java.net.URI
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap => MutableHashMap}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords.{GetNewApplicationResponse, SubmitApplicationRequest}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.event.{Dispatcher, Event, EventHandler}
import org.apache.hadoop.yarn.server.resourcemanager.{ClientRMService, RMAppManager, RMContext}
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager
import org.apache.hadoop.yarn.util.Records
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyShort, eq => meq}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite, TestUtils}
import org.apache.spark.deploy.yarn.ResourceRequestHelper._
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceID
import org.apache.spark.resource.ResourceUtils.AMOUNT
import org.apache.spark.util.{SparkConfWithEnv, Utils}

class ClientSuite extends SparkFunSuite with Matchers {
  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  import Client._

  var oldSystemProperties: Properties = null

  test("default Yarn application classpath") {
    getDefaultYarnApplicationClasspath should be(Fixtures.knownDefYarnAppCP)
  }

  test("default MR application classpath") {
    getDefaultMRApplicationClasspath should be(Fixtures.knownDefMRAppCP)
  }

  test("resultant classpath for an application that defines a classpath for YARN") {
    withAppConf(Fixtures.mapYARNAppConf) { conf =>
      val env = newEnv
      populateHadoopClasspath(conf, env)
      classpath(env) should be(Fixtures.knownYARNAppCP +: getDefaultMRApplicationClasspath)
    }
  }

  test("resultant classpath for an application that defines a classpath for MR") {
    withAppConf(Fixtures.mapMRAppConf) { conf =>
      val env = newEnv
      populateHadoopClasspath(conf, env)
      classpath(env) should be(getDefaultYarnApplicationClasspath :+ Fixtures.knownMRAppCP)
    }
  }

  test("resultant classpath for an application that defines both classpaths, YARN and MR") {
    withAppConf(Fixtures.mapAppConf) { conf =>
      val env = newEnv
      populateHadoopClasspath(conf, env)
      classpath(env) should be(Array(Fixtures.knownYARNAppCP, Fixtures.knownMRAppCP))
    }
  }

  private val SPARK = "local:/sparkJar"
  private val USER = "local:/userJar"
  private val ADDED = "local:/addJar1,local:/addJar2,/addJar3"

  private val PWD = "{{PWD}}"

  test("Local jar URIs") {
    val conf = new Configuration()
    val sparkConf = new SparkConf()
      .set(SPARK_JARS, Seq(SPARK))
      .set(USER_CLASS_PATH_FIRST, true)
      .set("spark.yarn.dist.jars", ADDED)
    val env = new MutableHashMap[String, String]()
    val args = new ClientArguments(Array("--jar", USER))

    populateClasspath(args, conf, sparkConf, env)

    val cp = env("CLASSPATH").split(":|;|<CPS>")
    s"$SPARK,$USER,$ADDED".split(",").foreach({ entry =>
      val uri = new URI(entry)
      if (Utils.LOCAL_SCHEME.equals(uri.getScheme())) {
        cp should contain (uri.getPath())
      } else {
        cp should not contain (uri.getPath())
      }
    })
    cp should not contain ("local")
    cp should contain(PWD)
    cp should contain (s"$PWD${Path.SEPARATOR}${LOCALIZED_CONF_DIR}")
    cp should not contain (APP_JAR)
  }

  test("Jar path propagation through SparkConf") {
    val conf = new Configuration()
    val sparkConf = new SparkConf()
      .set(SPARK_JARS, Seq(SPARK))
      .set("spark.yarn.dist.jars", ADDED)
    val client = createClient(sparkConf, args = Array("--jar", USER))
    doReturn(new Path("/")).when(client).copyFileToRemote(any(classOf[Path]),
      any(classOf[Path]), anyShort(), any(classOf[MutableHashMap[URI, Path]]), anyBoolean(), any())

    val tempDir = Utils.createTempDir()
    try {
      // Because we mocked "copyFileToRemote" above to avoid having to create fake local files,
      // we need to create a fake config archive in the temp dir to avoid having
      // prepareLocalResources throw an exception.
      new FileOutputStream(new File(tempDir, LOCALIZED_CONF_ARCHIVE)).close()

      client.prepareLocalResources(new Path(tempDir.getAbsolutePath()), Nil)
      sparkConf.get(APP_JAR) should be (Some(USER))

      // The non-local path should be propagated by name only, since it will end up in the app's
      // staging dir.
      val expected = ADDED.split(",")
        .map(p => {
          val uri = new URI(p)
          if (Utils.LOCAL_SCHEME == uri.getScheme()) {
            p
          } else {
            Option(uri.getFragment()).getOrElse(new File(p).getName())
          }
        })
        .mkString(",")

      sparkConf.get(SECONDARY_JARS) should be (Some(expected.split(",").toSeq))
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("Cluster path translation") {
    val conf = new Configuration()
    val sparkConf = new SparkConf()
      .set(SPARK_JARS, Seq("local:/localPath/spark.jar"))
      .set(GATEWAY_ROOT_PATH, "/localPath")
      .set(REPLACEMENT_ROOT_PATH, "/remotePath")

    getClusterPath(sparkConf, "/localPath") should be ("/remotePath")
    getClusterPath(sparkConf, "/localPath/1:/localPath/2") should be (
      "/remotePath/1:/remotePath/2")

    val env = new MutableHashMap[String, String]()
    populateClasspath(null, conf, sparkConf, env, extraClassPath = Some("/localPath/my1.jar"))
    val cp = classpath(env)
    cp should contain ("/remotePath/spark.jar")
    cp should contain ("/remotePath/my1.jar")
  }

  test("configuration and args propagate through createApplicationSubmissionContext") {
    // When parsing tags, duplicates and leading/trailing whitespace should be removed.
    // Spaces between non-comma strings should be preserved as single tags. Empty strings may or
    // may not be removed depending on the version of Hadoop being used.
    val sparkConf = new SparkConf()
      .set(APPLICATION_TAGS.key, ",tag1, dup,tag2 , ,multi word , dup")
      .set(MAX_APP_ATTEMPTS, 42)
      .set("spark.app.name", "foo-test-app")
      .set(QUEUE_NAME, "staging-queue")
      .set(APPLICATION_PRIORITY, 1)
    val args = new ClientArguments(Array())

    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    val getNewApplicationResponse = Records.newRecord(classOf[GetNewApplicationResponse])
    val containerLaunchContext = Records.newRecord(classOf[ContainerLaunchContext])

    val client = new Client(args, sparkConf, null)
    client.createApplicationSubmissionContext(
      new YarnClientApplication(getNewApplicationResponse, appContext),
      containerLaunchContext)

    appContext.getApplicationName should be ("foo-test-app")
    appContext.getQueue should be ("staging-queue")
    appContext.getAMContainerSpec should be (containerLaunchContext)
    appContext.getApplicationType should be ("SPARK")
    appContext.getClass.getMethods.filter(_.getName == "getApplicationTags").foreach { method =>
      val tags = method.invoke(appContext).asInstanceOf[java.util.Set[String]]
      tags should contain allOf ("tag1", "dup", "tag2", "multi word")
      tags.asScala.count(_.nonEmpty) should be (4)
    }
    appContext.getMaxAppAttempts should be (42)
    appContext.getPriority.getPriority should be (1)
  }

  test("specify a more specific type for the application") {
    // TODO (SPARK-31733) Make this test case pass with Hadoop-3.2
    assume(!isYarnResourceTypesAvailable)
    // When the type exceeds 20 characters will be truncated by yarn
    val appTypes = Map(
      1 -> ("", ""),
      2 -> (" ", " "),
      3 -> ("SPARK-SQL", "SPARK-SQL"),
      4 -> ("012345678901234567890123", "01234567890123456789"))

    for ((id, (sourceType, targetType)) <- appTypes) {
      val sparkConf = new SparkConf().set("spark.yarn.applicationType", sourceType)
      val args = new ClientArguments(Array())

      val appContext = spy(Records.newRecord(classOf[ApplicationSubmissionContext]))
      val appId = ApplicationId.newInstance(123456, id)
      appContext.setApplicationId(appId)
      val getNewApplicationResponse = Records.newRecord(classOf[GetNewApplicationResponse])
      val containerLaunchContext = Records.newRecord(classOf[ContainerLaunchContext])

      val client = new Client(args, sparkConf, null)
      val context = client.createApplicationSubmissionContext(
        new YarnClientApplication(getNewApplicationResponse, appContext),
        containerLaunchContext)

      val yarnClient = mock(classOf[YarnClient])
      when(yarnClient.submitApplication(any())).thenAnswer((invocationOnMock: InvocationOnMock) => {
        val subContext = invocationOnMock.getArguments()(0)
          .asInstanceOf[ApplicationSubmissionContext]
        val request = Records.newRecord(classOf[SubmitApplicationRequest])
        request.setApplicationSubmissionContext(subContext)

        val rmContext = mock(classOf[RMContext])
        val conf = mock(classOf[Configuration])
        val map = new ConcurrentHashMap[ApplicationId, RMApp]()
        when(rmContext.getRMApps).thenReturn(map)
        val dispatcher = mock(classOf[Dispatcher])
        when(rmContext.getDispatcher).thenReturn(dispatcher)
        when[EventHandler[_]](dispatcher.getEventHandler).thenReturn(
          new EventHandler[Event[_]] {
            override def handle(event: Event[_]): Unit = {}
          }
        )
        val writer = mock(classOf[RMApplicationHistoryWriter])
        when(rmContext.getRMApplicationHistoryWriter).thenReturn(writer)
        val publisher = mock(classOf[SystemMetricsPublisher])
        when(rmContext.getSystemMetricsPublisher).thenReturn(publisher)
        when(appContext.getUnmanagedAM).thenReturn(true)

        val rmAppManager = new RMAppManager(rmContext,
          null,
          null,
          mock(classOf[ApplicationACLsManager]),
          conf)
        val clientRMService = new ClientRMService(rmContext,
          null,
          rmAppManager,
          null,
          null,
          null)
        clientRMService.submitApplication(request)

        assert(map.get(subContext.getApplicationId).getApplicationType === targetType)
        null
      })

      yarnClient.submitApplication(context)
    }
  }

  test("spark.yarn.jars with multiple paths and globs") {
    val libs = Utils.createTempDir()
    val single = Utils.createTempDir()
    val jar1 = TestUtils.createJarWithFiles(Map(), libs)
    val jar2 = TestUtils.createJarWithFiles(Map(), libs)
    val jar3 = TestUtils.createJarWithFiles(Map(), single)
    val jar4 = TestUtils.createJarWithFiles(Map(), single)

    val jarsConf = Seq(
      s"${libs.getAbsolutePath()}/*",
      jar3.getPath(),
      s"local:${jar4.getPath()}",
      s"local:${single.getAbsolutePath()}/*")

    val sparkConf = new SparkConf().set(SPARK_JARS, jarsConf)
    val client = createClient(sparkConf)

    val tempDir = Utils.createTempDir()
    client.prepareLocalResources(new Path(tempDir.getAbsolutePath()), Nil)

    assert(sparkConf.get(SPARK_JARS) ===
      Some(Seq(s"local:${jar4.getPath()}", s"local:${single.getAbsolutePath()}/*")))

    verify(client).copyFileToRemote(any(classOf[Path]), meq(new Path(jar1.toURI())), anyShort(),
      any(classOf[MutableHashMap[URI, Path]]), anyBoolean(), any())
    verify(client).copyFileToRemote(any(classOf[Path]), meq(new Path(jar2.toURI())), anyShort(),
      any(classOf[MutableHashMap[URI, Path]]), anyBoolean(), any())
    verify(client).copyFileToRemote(any(classOf[Path]), meq(new Path(jar3.toURI())), anyShort(),
      any(classOf[MutableHashMap[URI, Path]]), anyBoolean(), any())

    val cp = classpath(client)
    cp should contain (buildPath(PWD, LOCALIZED_LIB_DIR, "*"))
    cp should not contain (jar3.getPath())
    cp should contain (jar4.getPath())
    cp should contain (buildPath(single.getAbsolutePath(), "*"))
  }

  test("distribute jars archive") {
    val temp = Utils.createTempDir()
    val archive = TestUtils.createJarWithFiles(Map(), temp)

    val sparkConf = new SparkConf().set(SPARK_ARCHIVE, archive.getPath())
    val client = createClient(sparkConf)
    client.prepareLocalResources(new Path(temp.getAbsolutePath()), Nil)

    verify(client).copyFileToRemote(any(classOf[Path]), meq(new Path(archive.toURI())), anyShort(),
      any(classOf[MutableHashMap[URI, Path]]), anyBoolean(), any())
    classpath(client) should contain (buildPath(PWD, LOCALIZED_LIB_DIR, "*"))

    sparkConf.set(SPARK_ARCHIVE, Utils.LOCAL_SCHEME + ":" + archive.getPath())
    intercept[IllegalArgumentException] {
      client.prepareLocalResources(new Path(temp.getAbsolutePath()), Nil)
    }
  }

  test("distribute archive multiple times") {
    val libs = Utils.createTempDir()
    // Create jars dir and RELEASE file to avoid IllegalStateException.
    val jarsDir = new File(libs, "jars")
    assert(jarsDir.mkdir())
    new FileOutputStream(new File(libs, "RELEASE")).close()

    val userLib1 = Utils.createTempDir()
    val testJar = TestUtils.createJarWithFiles(Map(), userLib1)

    // Case 1:  FILES_TO_DISTRIBUTE and ARCHIVES_TO_DISTRIBUTE can't have duplicate files
    val sparkConf = new SparkConfWithEnv(Map("SPARK_HOME" -> libs.getAbsolutePath))
      .set(FILES_TO_DISTRIBUTE, Seq(testJar.getPath))
      .set(ARCHIVES_TO_DISTRIBUTE, Seq(testJar.getPath))

    val client = createClient(sparkConf)
    val tempDir = Utils.createTempDir()
    intercept[IllegalArgumentException] {
      client.prepareLocalResources(new Path(tempDir.getAbsolutePath()), Nil)
    }

    // Case 2: FILES_TO_DISTRIBUTE can't have duplicate files.
    val sparkConfFiles = new SparkConfWithEnv(Map("SPARK_HOME" -> libs.getAbsolutePath))
      .set(FILES_TO_DISTRIBUTE, Seq(testJar.getPath, testJar.getPath))

    val clientFiles = createClient(sparkConfFiles)
    val tempDirForFiles = Utils.createTempDir()
    intercept[IllegalArgumentException] {
      clientFiles.prepareLocalResources(new Path(tempDirForFiles.getAbsolutePath()), Nil)
    }

    // Case 3: ARCHIVES_TO_DISTRIBUTE can't have duplicate files.
    val sparkConfArchives = new SparkConfWithEnv(Map("SPARK_HOME" -> libs.getAbsolutePath))
      .set(ARCHIVES_TO_DISTRIBUTE, Seq(testJar.getPath, testJar.getPath))

    val clientArchives = createClient(sparkConfArchives)
    val tempDirForArchives = Utils.createTempDir()
    intercept[IllegalArgumentException] {
      clientArchives.prepareLocalResources(new Path(tempDirForArchives.getAbsolutePath()), Nil)
    }

    // Case 4: FILES_TO_DISTRIBUTE can have unique file.
    val sparkConfFilesUniq = new SparkConfWithEnv(Map("SPARK_HOME" -> libs.getAbsolutePath))
      .set(FILES_TO_DISTRIBUTE, Seq(testJar.getPath))

    val clientFilesUniq = createClient(sparkConfFilesUniq)
    val tempDirForFilesUniq = Utils.createTempDir()
    clientFilesUniq.prepareLocalResources(new Path(tempDirForFilesUniq.getAbsolutePath()), Nil)

    // Case 5: ARCHIVES_TO_DISTRIBUTE can have unique file.
    val sparkConfArchivesUniq = new SparkConfWithEnv(Map("SPARK_HOME" -> libs.getAbsolutePath))
      .set(ARCHIVES_TO_DISTRIBUTE, Seq(testJar.getPath))

    val clientArchivesUniq = createClient(sparkConfArchivesUniq)
    val tempDirArchivesUniq = Utils.createTempDir()
    clientArchivesUniq.prepareLocalResources(new Path(tempDirArchivesUniq.getAbsolutePath()), Nil)

  }

  test("distribute local spark jars") {
    val temp = Utils.createTempDir()
    val jarsDir = new File(temp, "jars")
    assert(jarsDir.mkdir())
    val jar = TestUtils.createJarWithFiles(Map(), jarsDir)
    new FileOutputStream(new File(temp, "RELEASE")).close()

    val sparkConf = new SparkConfWithEnv(Map("SPARK_HOME" -> temp.getAbsolutePath()))
    val client = createClient(sparkConf)
    client.prepareLocalResources(new Path(temp.getAbsolutePath()), Nil)
    classpath(client) should contain (buildPath(PWD, LOCALIZED_LIB_DIR, "*"))
  }

  test("ignore same name jars") {
    val libs = Utils.createTempDir()
    val jarsDir = new File(libs, "jars")
    assert(jarsDir.mkdir())
    new FileOutputStream(new File(libs, "RELEASE")).close()
    val userLib1 = Utils.createTempDir()
    val userLib2 = Utils.createTempDir()

    val jar1 = TestUtils.createJarWithFiles(Map(), jarsDir)
    val jar2 = TestUtils.createJarWithFiles(Map(), userLib1)
    // Copy jar2 to jar3 with same name
    val jar3 = {
      val target = new File(userLib2, new File(jar2.toURI).getName)
      val input = new FileInputStream(jar2.getPath)
      val output = new FileOutputStream(target)
      Utils.copyStream(input, output, closeStreams = true)
      target.toURI.toURL
    }

    val sparkConf = new SparkConfWithEnv(Map("SPARK_HOME" -> libs.getAbsolutePath))
      .set(JARS_TO_DISTRIBUTE, Seq(jar2.getPath, jar3.getPath))

    val client = createClient(sparkConf)
    val tempDir = Utils.createTempDir()
    client.prepareLocalResources(new Path(tempDir.getAbsolutePath()), Nil)

    // Only jar2 will be added to SECONDARY_JARS, jar3 which has the same name with jar2 will be
    // ignored.
    sparkConf.get(SECONDARY_JARS) should be (Some(Seq(new File(jar2.toURI).getName)))
  }

  Seq(
    "client" -> YARN_AM_RESOURCE_TYPES_PREFIX,
    "cluster" -> YARN_DRIVER_RESOURCE_TYPES_PREFIX
  ).foreach { case (deployMode, prefix) =>
    test(s"custom resource request ($deployMode mode)") {
      assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
      val resources = Map("fpga" -> 2, "gpu" -> 3)
      ResourceRequestTestHelper.initializeResourceTypes(resources.keys.toSeq)

      val conf = new SparkConf().set(SUBMIT_DEPLOY_MODE, deployMode)
      resources.foreach { case (name, v) =>
        conf.set(s"${prefix}${name}.${AMOUNT}", v.toString)
      }

      val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
      val getNewApplicationResponse = Records.newRecord(classOf[GetNewApplicationResponse])
      val containerLaunchContext = Records.newRecord(classOf[ContainerLaunchContext])

      val client = new Client(new ClientArguments(Array()), conf, null)
      client.createApplicationSubmissionContext(
        new YarnClientApplication(getNewApplicationResponse, appContext),
        containerLaunchContext)

      resources.foreach { case (name, value) =>
        ResourceRequestTestHelper.getRequestedValue(appContext.getResource, name) should be (value)
      }
    }
  }

  test("custom driver resource request yarn config and spark config fails") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())

    val conf = new SparkConf().set(SUBMIT_DEPLOY_MODE, "cluster")
    val resources = Map(conf.get(YARN_GPU_DEVICE) -> "gpu", conf.get(YARN_FPGA_DEVICE) -> "fpga")
    ResourceRequestTestHelper.initializeResourceTypes(resources.keys.toSeq)
    resources.keys.foreach { yarnName =>
      conf.set(s"${YARN_DRIVER_RESOURCE_TYPES_PREFIX}${yarnName}.${AMOUNT}", "2")
    }
    resources.values.foreach { rName =>
      conf.set(new ResourceID(SPARK_DRIVER_PREFIX, rName).amountConf, "3")
    }

    val error = intercept[SparkException] {
      ResourceRequestHelper.validateResources(conf)
    }.getMessage()

    assert(error.contains("Do not use spark.yarn.driver.resource.yarn.io/fpga.amount," +
      " please use spark.driver.resource.fpga.amount"))
    assert(error.contains("Do not use spark.yarn.driver.resource.yarn.io/gpu.amount," +
      " please use spark.driver.resource.gpu.amount"))
  }

  test("custom executor resource request yarn config and spark config fails") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    val conf = new SparkConf().set(SUBMIT_DEPLOY_MODE, "cluster")
    val resources = Map(conf.get(YARN_GPU_DEVICE) -> "gpu", conf.get(YARN_FPGA_DEVICE) -> "fpga")
    ResourceRequestTestHelper.initializeResourceTypes(resources.keys.toSeq)
    resources.keys.foreach { yarnName =>
      conf.set(s"${YARN_EXECUTOR_RESOURCE_TYPES_PREFIX}${yarnName}.${AMOUNT}", "2")
    }
    resources.values.foreach { rName =>
      conf.set(new ResourceID(SPARK_EXECUTOR_PREFIX, rName).amountConf, "3")
    }

    val error = intercept[SparkException] {
      ResourceRequestHelper.validateResources(conf)
    }.getMessage()

    assert(error.contains("Do not use spark.yarn.executor.resource.yarn.io/fpga.amount," +
      " please use spark.executor.resource.fpga.amount"))
    assert(error.contains("Do not use spark.yarn.executor.resource.yarn.io/gpu.amount," +
      " please use spark.executor.resource.gpu.amount"))
  }


  test("custom resources spark config mapped to yarn config") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    val conf = new SparkConf().set(SUBMIT_DEPLOY_MODE, "cluster")
    val yarnMadeupResource = "yarn.io/madeup"
    val resources = Map(conf.get(YARN_GPU_DEVICE) -> "gpu",
      conf.get(YARN_FPGA_DEVICE) -> "fpga",
      yarnMadeupResource -> "madeup")

    ResourceRequestTestHelper.initializeResourceTypes(resources.keys.toSeq)

    resources.values.foreach { rName =>
      conf.set(new ResourceID(SPARK_DRIVER_PREFIX, rName).amountConf, "3")
    }
    // also just set yarn one that we don't convert
    conf.set(s"${YARN_DRIVER_RESOURCE_TYPES_PREFIX}${yarnMadeupResource}.${AMOUNT}", "5")
    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    val getNewApplicationResponse = Records.newRecord(classOf[GetNewApplicationResponse])
    val containerLaunchContext = Records.newRecord(classOf[ContainerLaunchContext])

    val client = new Client(new ClientArguments(Array()), conf, null)
    val newContext = client.createApplicationSubmissionContext(
      new YarnClientApplication(getNewApplicationResponse, appContext),
      containerLaunchContext)

    val yarnRInfo = ResourceRequestTestHelper.getResources(newContext.getResource)
    val allResourceInfo = yarnRInfo.map(rInfo => (rInfo.name -> rInfo.value)).toMap
    assert(allResourceInfo.get(conf.get(YARN_GPU_DEVICE)).nonEmpty)
    assert(allResourceInfo.get(conf.get(YARN_GPU_DEVICE)).get === 3)
    assert(allResourceInfo.get(conf.get(YARN_FPGA_DEVICE)).nonEmpty)
    assert(allResourceInfo.get(conf.get(YARN_FPGA_DEVICE)).get === 3)
    assert(allResourceInfo.get(yarnMadeupResource).nonEmpty)
    assert(allResourceInfo.get(yarnMadeupResource).get === 5)
  }

  test("gpu/fpga spark resources mapped to custom yarn resources") {
    assume(ResourceRequestHelper.isYarnResourceTypesAvailable())
    val conf = new SparkConf().set(SUBMIT_DEPLOY_MODE, "cluster")
    val gpuCustomName = "custom/gpu"
    val fpgaCustomName = "custom/fpga"
    conf.set(YARN_GPU_DEVICE.key, gpuCustomName)
    conf.set(YARN_FPGA_DEVICE.key, fpgaCustomName)
    val resources = Map(gpuCustomName -> "gpu",
      fpgaCustomName -> "fpga")

    ResourceRequestTestHelper.initializeResourceTypes(resources.keys.toSeq)
    resources.values.foreach { rName =>
      conf.set(new ResourceID(SPARK_DRIVER_PREFIX, rName).amountConf, "3")
    }
    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    val getNewApplicationResponse = Records.newRecord(classOf[GetNewApplicationResponse])
    val containerLaunchContext = Records.newRecord(classOf[ContainerLaunchContext])

    val client = new Client(new ClientArguments(Array()), conf, null)
    val newContext = client.createApplicationSubmissionContext(
      new YarnClientApplication(getNewApplicationResponse, appContext),
      containerLaunchContext)

    val yarnRInfo = ResourceRequestTestHelper.getResources(newContext.getResource)
    val allResourceInfo = yarnRInfo.map(rInfo => (rInfo.name -> rInfo.value)).toMap
    assert(allResourceInfo.get(gpuCustomName).nonEmpty)
    assert(allResourceInfo.get(gpuCustomName).get === 3)
    assert(allResourceInfo.get(fpgaCustomName).nonEmpty)
    assert(allResourceInfo.get(fpgaCustomName).get === 3)
  }

  test("test yarn jars path not exists") {
    withTempDir { dir =>
      val conf = new SparkConf().set(SPARK_JARS, Seq(dir.getAbsolutePath + "/test"))
      val client = new Client(new ClientArguments(Array()), conf, null)
      withTempDir { distDir =>
        intercept[FileNotFoundException] {
          client.prepareLocalResources(new Path(distDir.getAbsolutePath), Nil)
        }
      }
    }
  }

  test("SPARK-31582 Being able to not populate Hadoop classpath") {
    Seq(true, false).foreach { populateHadoopClassPath =>
      withAppConf(Fixtures.mapAppConf) { conf =>
        val sparkConf = new SparkConf()
          .set(POPULATE_HADOOP_CLASSPATH, populateHadoopClassPath)
        val env = new MutableHashMap[String, String]()
        val args = new ClientArguments(Array("--jar", USER))
        populateClasspath(args, conf, sparkConf, env)
        if (populateHadoopClassPath) {
          classpath(env) should
            (contain (Fixtures.knownYARNAppCP) and contain (Fixtures.knownMRAppCP))
        } else {
          classpath(env) should
            (not contain (Fixtures.knownYARNAppCP) and not contain (Fixtures.knownMRAppCP))
        }
      }
    }
  }

  private val matching = Seq(
    ("files URI match test1", "file:///file1", "file:///file2"),
    ("files URI match test2", "file:///c:file1", "file://c:file2"),
    ("files URI match test3", "file://host/file1", "file://host/file2"),
    ("wasb URI match test", "wasb://bucket1@user", "wasb://bucket1@user/"),
    ("hdfs URI match test", "hdfs:/path1", "hdfs:/path1")
  )

  matching.foreach { t =>
      test(t._1) {
        assert(Client.compareUri(new URI(t._2), new URI(t._3)),
          s"No match between ${t._2} and ${t._3}")
      }
  }

  private val unmatching = Seq(
    ("files URI unmatch test1", "file:///file1", "file://host/file2"),
    ("files URI unmatch test2", "file://host/file1", "file:///file2"),
    ("files URI unmatch test3", "file://host/file1", "file://host2/file2"),
    ("wasb URI unmatch test1", "wasb://bucket1@user", "wasb://bucket2@user/"),
    ("wasb URI unmatch test2", "wasb://bucket1@user", "wasb://bucket1@user2/"),
    ("s3 URI unmatch test", "s3a://user@pass:bucket1/", "s3a://user2@pass2:bucket1/"),
    ("hdfs URI unmatch test1", "hdfs://namenode1/path1", "hdfs://namenode1:8080/path2"),
    ("hdfs URI unmatch test2", "hdfs://namenode1:8020/path1", "hdfs://namenode1:8080/path2")
  )

  unmatching.foreach { t =>
      test(t._1) {
        assert(!Client.compareUri(new URI(t._2), new URI(t._3)),
          s"match between ${t._2} and ${t._3}")
      }
  }

  object Fixtures {

    val knownDefYarnAppCP: Seq[String] =
      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.toSeq

    val knownDefMRAppCP: Seq[String] =
      MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH.split(",").toSeq

    val knownYARNAppCP = "/known/yarn/path"

    val knownMRAppCP = "/known/mr/path"

    val mapMRAppConf = Map("mapreduce.application.classpath" -> knownMRAppCP)

    val mapYARNAppConf = Map(YarnConfiguration.YARN_APPLICATION_CLASSPATH -> knownYARNAppCP)

    val mapAppConf = mapYARNAppConf ++ mapMRAppConf
  }

  def withAppConf(m: Map[String, String] = Map())(testCode: (Configuration) => Any): Unit = {
    val conf = new Configuration
    m.foreach { case (k, v) => conf.set(k, v, "ClientSpec") }
    testCode(conf)
  }

  def newEnv: MutableHashMap[String, String] = MutableHashMap[String, String]()

  def classpath(env: MutableHashMap[String, String]): Array[String] =
    env(Environment.CLASSPATH.name).split(":|;|<CPS>")

  private def createClient(
      sparkConf: SparkConf,
      args: Array[String] = Array()): Client = {
    val clientArgs = new ClientArguments(args)
    spy(new Client(clientArgs, sparkConf, null))
  }

  private def classpath(client: Client): Array[String] = {
    val env = new MutableHashMap[String, String]()
    populateClasspath(null, new Configuration(), client.sparkConf, env)
    classpath(env)
  }
}
