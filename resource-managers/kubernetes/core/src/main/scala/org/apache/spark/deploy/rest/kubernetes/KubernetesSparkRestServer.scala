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
package org.apache.spark.deploy.rest.kubernetes

import java.io.File
import java.net.URI
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.google.common.io.Files
import org.apache.commons.codec.binary.Base64
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SecurityManager, SPARK_VERSION, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.rest._
import org.apache.spark.util.{ShutdownHookManager, Utils}

private case class KubernetesSparkRestServerArguments(
  val host: Option[String] = None,
  val port: Option[Int] = None,
  val secretFile: Option[String] = None) {
  def validate(): KubernetesSparkRestServerArguments = {
    require(host.isDefined, "Hostname not set via --hostname.")
    require(port.isDefined, "Port not set via --port")
    require(secretFile.isDefined, "Secret file not set via --secret-file")
    this
  }
}

private object KubernetesSparkRestServerArguments {
  def fromArgsArray(inputArgs: Array[String]): KubernetesSparkRestServerArguments = {
    var args = inputArgs.toList
    var resolvedArguments = KubernetesSparkRestServerArguments()
    while (args.nonEmpty) {
      resolvedArguments = args match {
        case "--hostname" :: value :: tail =>
          args = tail
          resolvedArguments.copy(host = Some(value))
        case "--port" :: value :: tail =>
          args = tail
          resolvedArguments.copy(port = Some(value.toInt))
        case "--secret-file" :: value :: tail =>
          args = tail
          resolvedArguments.copy(secretFile = Some(value))
        // TODO polish usage message
        case Nil => resolvedArguments
        case unknown => throw new IllegalStateException(s"Unknown argument(s) found: $unknown")
      }
    }
    resolvedArguments.validate()
  }
}

private[spark] class KubernetesSparkRestServer(
    host: String,
    port: Int,
    conf: SparkConf,
    expectedApplicationSecret: Array[Byte])
  extends RestSubmissionServer(host, port, conf) {

  private val javaExecutable = s"${System.getenv("JAVA_HOME")}/bin/java"
  private val sparkHome = System.getenv("SPARK_HOME")
  private val securityManager = new SecurityManager(conf)
  override protected lazy val contextToServlet = Map[String, RestServlet](
    s"$baseContext/create/*" -> submitRequestServlet,
    s"$baseContext/ping/*" -> pingServlet)

  private val pingServlet = new PingServlet
  override protected val submitRequestServlet: SubmitRequestServlet
    = new KubernetesSubmitRequestServlet
  // TODO
  override protected val statusRequestServlet: StatusRequestServlet = null
  override protected val killRequestServlet: KillRequestServlet = null

  private class PingServlet extends RestServlet {
    protected override def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
      sendResponse(new PingResponse, response)
    }
  }

  private class KubernetesSubmitRequestServlet extends SubmitRequestServlet {

    // TODO validating the secret should be done as part of a header of the request.
    // Instead here we have to specify the secret in the body.
    override protected def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
      requestMessage match {
        case KubernetesCreateSubmissionRequest(
            appResource,
            mainClass,
            appArgs,
            sparkProperties,
            secret,
            uploadedDriverExtraClasspath,
            uploadedJars) =>
          val decodedSecret = Base64.decodeBase64(secret)
          if (!expectedApplicationSecret.sameElements(decodedSecret)) {
            responseServlet.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
            handleError("Unauthorized to submit application.")
          } else {
            val tempDir = Utils.createTempDir()
            val appResourcePath = resolvedAppResource(appResource, tempDir)
            val driverClasspathDirectory = new File(tempDir, "driver-extra-classpath")
            if (!driverClasspathDirectory.mkdir) {
              throw new IllegalStateException("Failed to create driver extra classpath" +
                s" dir at ${driverClasspathDirectory.getAbsolutePath}")
            }
            val jarsDirectory = new File(tempDir, "jars")
            if (!jarsDirectory.mkdir) {
              throw new IllegalStateException("Failed to create jars dir at" +
                 s"${jarsDirectory.getAbsolutePath}")
            }
            val writtenDriverExtraClasspath = writeBase64ContentsToFiles(
              uploadedDriverExtraClasspath, driverClasspathDirectory)
            val writtenJars = writeBase64ContentsToFiles(uploadedJars, jarsDirectory)
            val originalDriverExtraClasspath = sparkProperties.get("spark.driver.extraClassPath")
              .map(_.split(","))
              .getOrElse(Array.empty[String])
            val resolvedDriverExtraClasspath = writtenDriverExtraClasspath ++
              originalDriverExtraClasspath
            val originalJars = sparkProperties.get("spark.jars")
              .map(_.split(","))
              .getOrElse(Array.empty[String])
            val resolvedJars = writtenJars ++ originalJars ++ Array(appResourcePath)
            val sparkJars = new File(sparkHome, "jars").listFiles().map(_.getAbsolutePath)
            val driverClasspath = resolvedDriverExtraClasspath ++
              resolvedJars ++
              sparkJars ++
              Array(appResourcePath)
            val resolvedSparkProperties = new mutable.HashMap[String, String]
            resolvedSparkProperties ++= sparkProperties
            resolvedSparkProperties("spark.jars") = resolvedJars.mkString(",")

            val command = new ArrayBuffer[String]
            command += javaExecutable
            command += "-cp"
            command += s"${driverClasspath.mkString(":")}"
            for (prop <- resolvedSparkProperties) {
              command += s"-D${prop._1}=${prop._2}"
            }
            val driverMemory = resolvedSparkProperties.getOrElse("spark.driver.memory", "1g")
            command += s"-Xms$driverMemory"
            command += s"-Xmx$driverMemory"
            command += mainClass
            command ++= appArgs
            val pb = new ProcessBuilder(command: _*)
            Paths.get(sparkHome, "logs").toFile.mkdirs
            pb.redirectOutput(Paths.get(sparkHome, "logs", "stdout").toFile)
            pb.redirectError(Paths.get(sparkHome, "logs", "stderr").toFile)
            val process = pb.start()
            ShutdownHookManager.addShutdownHook(() => {
              logInfo("Received stop command, shutting down the running Spark application...")
              process.destroy()
            })
            val response = new CreateSubmissionResponse
            response.success = true
            response.submissionId = null
            response.message = "success"
            response.serverSparkVersion = SPARK_VERSION
            response
          }
        case unexpected =>
          responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
          handleError(s"Received message of unexpected type ${unexpected.messageType}.")
      }
    }

    def resolvedAppResource(appResource: AppResource, tempDir: File): String = {
      val appResourcePath = appResource match {
        case UploadedAppResource(resourceContentsBase64, resourceName) =>
          val resourceFile = new File(tempDir, resourceName)
          val resourceFilePath = resourceFile.getAbsolutePath
          if (resourceFile.createNewFile()) {
            val resourceContentsBytes = Base64.decodeBase64(resourceContentsBase64)
            Files.write(resourceContentsBytes, resourceFile)
            resourceFile.getAbsolutePath
          } else {
            throw new IllegalStateException(s"Failed to write main app resource file" +
              s" to $resourceFilePath")
          }
        case RemoteAppResource(resource) =>
          Utils.fetchFile(resource, tempDir, conf,
            securityManager, SparkHadoopUtil.get.newConfiguration(conf),
            System.currentTimeMillis(), useCache = false)
          val fileName = Utils.decodeFileNameInURI(URI.create(resource))
          val downloadedFile = new File(tempDir, fileName)
          val downloadedFilePath = downloadedFile.getAbsolutePath
          if (!downloadedFile.isFile) {
            throw new IllegalStateException(s"Main app resource is not a file or" +
              s" does not exist at $downloadedFilePath")
          }
          downloadedFilePath
      }
      appResourcePath
    }
  }

  private def writeBase64ContentsToFiles(
        maybeCompressedFiles: Option[TarGzippedData],
        rootDir: File): Seq[String] = {
    maybeCompressedFiles.map { compressedFiles =>
      CompressionUtils.unpackAndWriteCompressedFiles(compressedFiles, rootDir)
    }.getOrElse(Seq.empty[String])
  }
}

private[spark] object KubernetesSparkRestServer {
  private val barrier = new CountDownLatch(1)
  def main(args: Array[String]): Unit = {
    val parsedArguments = KubernetesSparkRestServerArguments.fromArgsArray(args)
    val secretFile = new File(parsedArguments.secretFile.get)
    if (!secretFile.isFile) {
      throw new IllegalArgumentException(s"Secret file specified by --secret-file" +
        " is not a file, or does not exist.")
    }
    val secretBytes = Files.toByteArray(secretFile)
    val sparkConf = new SparkConf(true)
    val server = new KubernetesSparkRestServer(
      parsedArguments.host.get,
      parsedArguments.port.get,
      sparkConf,
      secretBytes)
    server.start()
    ShutdownHookManager.addShutdownHook(() => {
      try {
        server.stop()
      } finally {
        barrier.countDown()
      }
    })
    barrier.await()
  }
}

