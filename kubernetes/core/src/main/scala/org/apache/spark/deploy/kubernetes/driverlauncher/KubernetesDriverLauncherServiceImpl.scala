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
package org.apache.spark.deploy.kubernetes.driverlauncher

import java.io.File
import java.net.URI
import java.nio.file.Paths
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import javax.ws.rs.NotAuthorizedException

import com.google.common.io.Files
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.FileUtils
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import scala.concurrent.{Future, ExecutionContext}

private[spark] class KubernetesDriverLauncherServiceImpl(
    private val expectedApplicationSecret: String,
    private val server: KubernetesDriverLauncherServer
) extends KubernetesDriverLauncherService with Logging {

  private val uploadedJarsFolder = Files.createTempDir()
  private val javaExecutable = s"${System.getenv("JAVA_HOME")}/bin/java"
  private val sparkHome = System.getenv("SPARK_HOME")
  private implicit val waitForApplicationExecutor = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
      .setNameFormat("wait-for-application-%d")
      .setDaemon(true)
      .build()))
  private var applicationCompleteFuture: Future[_] = null
  private var submitComplete = false

  override def submitApplication(
      applicationSecret: String,
      applicationConfiguration: KubernetesSparkDriverConfiguration): Unit = synchronized {
    if (!expectedApplicationSecret.equals(applicationSecret)) {
      throw new NotAuthorizedException("Unauthorized to run application.")
    }
    if (submitComplete) {
      throw new IllegalStateException("Application is already started, cannot submit again.")
    }

    val usedFileNames = new mutable.HashSet[String]
    val localJarPaths = new mutable.HashMap[String, String]

    for (jar <- applicationConfiguration.localJarsContents) {
      assert(jar._1.startsWith("file:"), "Uploaded jars should only contain local files")
      var fileName = new File(URI.create(jar._1).getPath).getName
      var fileNameSuffix = 1
      while (usedFileNames.contains(fileName)) {
        fileName = s"${Files.getNameWithoutExtension(fileName)}" +
          s"-$fileNameSuffix${Files.getFileExtension(fileName)}"
        fileNameSuffix += 1
      }
      usedFileNames.add(fileName)

      val writtenJarFile = new File(uploadedJarsFolder, fileName)
      val decodedJarBytes = Base64.decodeBase64(jar._2)
      Files.write(decodedJarBytes, writtenJarFile)
      localJarPaths.put(jar._1, writtenJarFile.getAbsolutePath)
    }

    applicationConfiguration
        .driverClassPath
        .map(Utils.resolveURI)
        .foreach(uri => assert(uri.getScheme == "local" || uri.getScheme == "file"))

    val driverClassPath = getDriverClassPath(applicationConfiguration, localJarPaths)

    val command = new ArrayBuffer[String]
    command += javaExecutable

    if (applicationConfiguration.jars.nonEmpty) {
      val sparkJars = applicationConfiguration.jars.map(localJarPaths(_)).mkString(",")
      command += s"-Dspark.jars=$sparkJars"
    }

    applicationConfiguration.customExecutorSpecBase64.foreach(customExecutorSpecBase64 => {
      val customExecutorSpecBytes = Base64.decodeBase64(customExecutorSpecBase64)
      val replicationControllerSpecFile = new File(
        Files.createTempDir(), "executor-replication-controller")
      FileUtils.writeByteArrayToFile(replicationControllerSpecFile, customExecutorSpecBytes)
      command += "-Dspark.kubernetes.executor.custom.spec.file=" +
        s"${replicationControllerSpecFile.getAbsolutePath}"
    })
    applicationConfiguration.customExecutorSpecContainerName.foreach(name => {
      command += s"-Dspark.kubernetes.executor.custom.spec.container.name=$name"
    })
    command += "-cp"
    command += driverClassPath.mkString(":")
    for (prop <- applicationConfiguration.sparkConf) {
      // Have to ignore spark.jars and driver extra class path since they were loaded
      // differently above
      if (prop._1 != "spark.jars" && prop._1 != "spark.driver.extraClassPath") {
        command += s"-D${prop._1}=${prop._2}"
      }
    }

    if (applicationConfiguration.sparkConf.contains("spark.driver.memory")) {
      command += s"-Xmx${applicationConfiguration.sparkConf("spark.driver.memory")}"
    }

    command += applicationConfiguration.driverMainClass
    command ++= applicationConfiguration.driverArgs

    val pb = new ProcessBuilder(command: _*)
    Paths.get(sparkHome, "logs").toFile.mkdirs
    pb.redirectOutput(Paths.get(sparkHome, "logs", "stdout").toFile)
    pb.redirectError(Paths.get(sparkHome, "logs", "stderr").toFile)
    val process = pb.start()
    submitComplete = true
    applicationCompleteFuture = Future[Unit] {
      try {
        process.waitFor()
      } finally {
        server.stop()
      }
    }
  }

  override def ping(): String = "pong"

  private def getDriverClassPath(
      applicationConfiguration: KubernetesSparkDriverConfiguration,
      localJarPaths: mutable.HashMap[String, String]): mutable.Buffer[String] = {
    val driverClassPath = new ArrayBuffer[String]
    val jarsDir = new File(sparkHome, "jars")
    jarsDir.listFiles().foreach {
      driverClassPath += _.getAbsolutePath
    }
    applicationConfiguration.driverClassPath.foreach {
      driverClassPath += localJarPaths(_)
    }
    applicationConfiguration.jars.foreach {
      driverClassPath += localJarPaths(_)
    }
    driverClassPath
  }
}
