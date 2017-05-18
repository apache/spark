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
package org.apache.spark.deploy.rest.kubernetes.v1

import java.io.{File, FileOutputStream, StringReader}
import java.net.URI
import java.nio.file.Paths
import java.security.SecureRandom
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, ByteStreams, Files}
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang3.RandomStringUtils
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SecurityManager, SPARK_VERSION => sparkVersion, SparkConf, SparkException, SSLOptions}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.kubernetes.{CompressionUtils, KubernetesCredentials}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.submit.KubernetesFileUtils
import org.apache.spark.deploy.rest._
import org.apache.spark.internal.config.OptionalConfigEntry
import org.apache.spark.util.{ShutdownHookManager, ThreadUtils, Utils}

private case class KubernetesSparkRestServerArguments(
    host: Option[String] = None,
    port: Option[Int] = None,
    useSsl: Boolean = false,
    secretFile: Option[String] = None,
    keyStoreFile: Option[String] = None,
    keyStorePasswordFile: Option[String] = None,
    keyStoreType: Option[String] = None,
    keyPasswordFile: Option[String] = None,
    keyPemFile: Option[String] = None,
    certPemFile: Option[String] = None) {
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
        case "--use-ssl" :: value :: tail =>
          args = tail
          resolvedArguments.copy(useSsl = value.toBoolean)
        case "--keystore-file" :: value :: tail =>
          args = tail
          resolvedArguments.copy(keyStoreFile = Some(value))
        case "--keystore-password-file" :: value :: tail =>
          args = tail
          resolvedArguments.copy(keyStorePasswordFile = Some(value))
        case "--keystore-type" :: value :: tail =>
          args = tail
          resolvedArguments.copy(keyStoreType = Some(value))
        case "--keystore-key-password-file" :: value :: tail =>
          args = tail
          resolvedArguments.copy(keyPasswordFile = Some(value))
        case "--key-pem-file" :: value :: tail =>
          args = tail
          resolvedArguments.copy(keyPemFile = Some(value))
        case "--cert-pem-file" :: value :: tail =>
          args = tail
          resolvedArguments.copy(certPemFile = Some(value))
        // TODO polish usage message
        case Nil => resolvedArguments
        case unknown => throw new IllegalStateException(s"Unknown argument(s) found: $unknown")
      }
    }
    resolvedArguments.validate()
  }
}

/**
 * Runs in the driver pod and receives a request to run an application. Note that
 * unlike the submission rest server in standalone mode, this server is expected
 * to be used to run one application only, and then shut down once that application
 * is complete.
 */
private[spark] class KubernetesSparkRestServer(
    host: String,
    port: Int,
    conf: SparkConf,
    expectedApplicationSecret: Array[Byte],
    shutdownLock: CountDownLatch,
    exitCode: AtomicInteger,
    sslOptions: SSLOptions = new SSLOptions)
  extends RestSubmissionServer(host, port, conf, sslOptions) {

  private val SERVLET_LOCK = new Object
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

    private val waitForProcessCompleteExecutor = ThreadUtils
        .newDaemonSingleThreadExecutor("wait-for-spark-app-complete")
    private var startedApplication = false

    // TODO validating the secret should be done as part of a header of the request.
    // Instead here we have to specify the secret in the body.
    override protected def handleSubmit(
        requestMessageJson: String,
        requestMessage: SubmitRestProtocolMessage,
        responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
      SERVLET_LOCK.synchronized {
        if (startedApplication) {
          throw new IllegalStateException("Application has already been submitted.")
        } else {
          requestMessage match {
            case KubernetesCreateSubmissionRequest(
                appResource,
                mainClass,
                appArgs,
                sparkProperties,
                secret,
                driverPodKubernetesCredentials,
                uploadedJars,
                uploadedFiles) =>
              val decodedSecret = Base64.decodeBase64(secret)
              if (!expectedApplicationSecret.sameElements(decodedSecret)) {
                responseServlet.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
                handleError("Unauthorized to submit application.")
              } else {
                val tempDir = Utils.createTempDir()
                val resolvedAppResource = resolveAppResource(appResource, tempDir)
                val writtenJars = writeUploadedJars(uploadedJars, tempDir)
                val writtenFiles = writeUploadedFiles(uploadedFiles)
                val resolvedSparkProperties = new mutable.HashMap[String, String]
                resolvedSparkProperties ++= sparkProperties
                val originalJars = sparkProperties.get("spark.jars")
                  .map(_.split(","))
                  .getOrElse(Array.empty)

                // The driver at this point has handed us the value of spark.jars verbatim as
                // specified in spark-submit. At this point, remove all jars that were local
                // to the submitting user's disk, and replace them with the paths that were
                // written to disk above.
                val onlyContainerLocalOrRemoteJars = KubernetesFileUtils
                  .getNonSubmitterLocalFiles(originalJars)
                val resolvedJars = (writtenJars ++
                  onlyContainerLocalOrRemoteJars ++
                  Array(resolvedAppResource.sparkJarPath)).toSet
                if (resolvedJars.nonEmpty) {
                  resolvedSparkProperties("spark.jars") = resolvedJars.mkString(",")
                } else {
                  resolvedSparkProperties.remove("spark.jars")
                }

                // Determining the driver classpath is similar. It's the combination of:
                // - Jars written from uploads
                // - Jars in (spark.jars + mainAppResource) that has a "local" prefix
                // - spark.driver.extraClasspath
                // - Spark core jars from the installation
                val sparkCoreJars = new File(sparkHome, "jars").listFiles().map(_.getAbsolutePath)
                val driverExtraClasspath = sparkProperties
                  .get("spark.driver.extraClassPath")
                  .map(_.split(","))
                  .getOrElse(Array.empty[String])
                val onlyContainerLocalJars = KubernetesFileUtils
                  .getOnlyContainerLocalFiles(originalJars)
                val driverClasspath = driverExtraClasspath ++
                  Seq(resolvedAppResource.localPath) ++
                  writtenJars ++
                  onlyContainerLocalJars ++
                  sparkCoreJars

                // Resolve spark.files similarly to spark.jars.
                val originalFiles = sparkProperties.get("spark.files")
                  .map(_.split(","))
                  .getOrElse(Array.empty[String])
                val onlyContainerLocalOrRemoteFiles = KubernetesFileUtils
                  .getNonSubmitterLocalFiles(originalFiles)
                val resolvedFiles = writtenFiles ++ onlyContainerLocalOrRemoteFiles
                if (resolvedFiles.nonEmpty) {
                  resolvedSparkProperties("spark.files") = resolvedFiles.mkString(",")
                } else {
                  resolvedSparkProperties.remove("spark.files")
                }
                resolvedSparkProperties ++= writeKubernetesCredentials(
                  driverPodKubernetesCredentials, tempDir)

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
                val extraJavaOpts = resolvedSparkProperties.get("spark.driver.extraJavaOptions")
                  .map(Utils.splitCommandString)
                  .getOrElse(Seq.empty)
                command ++= extraJavaOpts
                command += mainClass
                command ++= appArgs
                val pb = new ProcessBuilder(command: _*).inheritIO()
                val process = pb.start()
                ShutdownHookManager.addShutdownHook(() => {
                  logInfo("Received stop command, shutting down the running Spark application...")
                  process.destroy()
                  shutdownLock.countDown()
                })
                waitForProcessCompleteExecutor.submit(new Runnable {
                  override def run(): Unit = {
                    // set the REST service's exit code to the exit code of the driver subprocess
                    exitCode.set(process.waitFor)
                    SERVLET_LOCK.synchronized {
                      logInfo("Spark application complete. Shutting down submission server...")
                      KubernetesSparkRestServer.this.stop
                      shutdownLock.countDown()
                    }
                  }
                })
                startedApplication = true
                val response = new CreateSubmissionResponse
                response.success = true
                response.submissionId = null
                response.message = "success"
                response.serverSparkVersion = sparkVersion
                response
              }
            case unexpected =>
              responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
              handleError(s"Received message of unexpected type ${unexpected.messageType}.")
          }
        }
      }
    }

    private def writeUploadedJars(jars: TarGzippedData, rootTempDir: File):
        Seq[String] = {
      val resolvedDirectory = new File(rootTempDir, "jars")
      if (!resolvedDirectory.mkdir()) {
        throw new IllegalStateException(s"Failed to create jars dir at " +
          resolvedDirectory.getAbsolutePath)
      }
      CompressionUtils.unpackAndWriteCompressedFiles(jars, resolvedDirectory)
    }

    private def writeUploadedFiles(files: TarGzippedData): Seq[String] = {
      val workingDir = Paths.get("").toFile.getAbsoluteFile
      CompressionUtils.unpackAndWriteCompressedFiles(files, workingDir)
    }

    private def writeKubernetesCredentials(
        kubernetesCredentials: KubernetesCredentials,
        rootTempDir: File): Map[String, String] = {
      val resolvedDirectory = new File(rootTempDir, "kubernetes-credentials")
      if (!resolvedDirectory.mkdir()) {
        throw new IllegalStateException(s"Failed to create credentials dir at "
          + resolvedDirectory.getAbsolutePath)
      }
      val oauthTokenFile = writeRawStringCredentialAndGetConf("oauth-token.txt", resolvedDirectory,
        KUBERNETES_DRIVER_MOUNTED_OAUTH_TOKEN,
        kubernetesCredentials.oauthTokenBase64.map { base64 =>
          new String(BaseEncoding.base64().decode(base64), Charsets.UTF_8)
        })
      val caCertFile = writeBase64CredentialAndGetConf("ca.crt", resolvedDirectory,
        KUBERNETES_DRIVER_MOUNTED_CA_CERT_FILE, kubernetesCredentials.caCertDataBase64)
      val clientKeyFile = writeBase64CredentialAndGetConf("key.key", resolvedDirectory,
        KUBERNETES_DRIVER_MOUNTED_CLIENT_KEY_FILE, kubernetesCredentials.clientKeyDataBase64)
      val clientCertFile = writeBase64CredentialAndGetConf("cert.crt", resolvedDirectory,
        KUBERNETES_DRIVER_MOUNTED_CLIENT_CERT_FILE, kubernetesCredentials.clientCertDataBase64)
      (oauthTokenFile ++ caCertFile ++ clientKeyFile ++ clientCertFile).toMap
    }

    private def writeRawStringCredentialAndGetConf(
        fileName: String,
        dir: File,
        conf: OptionalConfigEntry[String],
        credential: Option[String]): Option[(String, String)] = {
      credential.map { cred =>
        val credentialFile = new File(dir, fileName)
        Files.write(cred, credentialFile, Charsets.UTF_8)
        (conf.key, credentialFile.getAbsolutePath)
      }
    }

    private def writeBase64CredentialAndGetConf(
        fileName: String,
        dir: File,
        conf: OptionalConfigEntry[String],
        credential: Option[String]): Option[(String, String)] = {
      credential.map { cred =>
        val credentialFile = new File(dir, fileName)
        Files.write(BaseEncoding.base64().decode(cred), credentialFile)
        (conf.key, credentialFile.getAbsolutePath)
      }
    }

    /**
     * Retrieve the path on the driver container where the main app resource is, and what value it
     * ought to have in the spark.jars property. The two may be different because for non-local
     * dependencies, we have to fetch the resource (if it is not "local") but still want to use
     * the full URI in spark.jars.
     */
    private def resolveAppResource(appResource: AppResource, tempDir: File):
        ResolvedAppResource = {
      appResource match {
        case UploadedAppResource(resourceContentsBase64, resourceName) =>
          val resourceFile = new File(tempDir, resourceName)
          val resourceFilePath = resourceFile.getAbsolutePath
          if (resourceFile.createNewFile()) {
            Utils.tryWithResource(new StringReader(resourceContentsBase64)) { reader =>
              Utils.tryWithResource(new FileOutputStream(resourceFile)) { os =>
                Utils.tryWithResource(BaseEncoding.base64().decodingStream(reader)) {
                    decodingStream =>
                  ByteStreams.copy(decodingStream, os)
                }
              }
            }
            ResolvedAppResource(resourceFile.getAbsolutePath, resourceFile.getAbsolutePath)
          } else {
            throw new IllegalStateException(s"Failed to write main app resource file" +
              s" to $resourceFilePath")
          }
        case ContainerAppResource(resource) =>
          ResolvedAppResource(Utils.resolveURI(resource).getPath, resource)
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
          ResolvedAppResource(downloadedFilePath, resource)
      }
    }
  }

  private case class ResolvedAppResource(localPath: String, sparkJarPath: String)
}

private[spark] object KubernetesSparkRestServer {
  private val barrier = new CountDownLatch(1)
  private val SECURE_RANDOM = new SecureRandom()

  def main(args: Array[String]): Unit = {
    val parsedArguments = KubernetesSparkRestServerArguments.fromArgsArray(args)
    val secretFile = new File(parsedArguments.secretFile.get)
    require(secretFile.isFile, "Secret file specified by --secret-file is not a file, or" +
      " does not exist.")
    val sslOptions = if (parsedArguments.useSsl) {
      validateSslOptions(parsedArguments)
      val keyPassword = parsedArguments
        .keyPasswordFile
        .map(new File(_))
        .map(Files.toString(_, Charsets.UTF_8))
        // If key password isn't set but we're using PEM files, generate a password
        .orElse(parsedArguments.keyPemFile.map(_ => randomPassword()))
      val keyStorePassword = parsedArguments
        .keyStorePasswordFile
        .map(new File(_))
        .map(Files.toString(_, Charsets.UTF_8))
        // If keystore password isn't set but we're using PEM files, generate a password
        .orElse(parsedArguments.keyPemFile.map(_ => randomPassword()))
      val resolvedKeyStore = parsedArguments.keyStoreFile.map(new File(_)).orElse(
        parsedArguments.keyPemFile.map(keyPemFile => {
          parsedArguments.certPemFile.map(certPemFile => {
            PemsToKeyStoreConverter.convertPemsToTempKeyStoreFile(
              new File(keyPemFile),
              new File(certPemFile),
              "provided-key",
              keyStorePassword,
              keyPassword,
              parsedArguments.keyStoreType)
          })
        }).getOrElse(throw new SparkException("When providing PEM files to set up TLS for the" +
          " submission server, both the key and the certificate must be specified.")))
      new SSLOptions(
        enabled = true,
        keyStore = resolvedKeyStore,
        keyStoreType = parsedArguments.keyStoreType,
        keyStorePassword = keyStorePassword,
        keyPassword = keyPassword)
    } else {
      new SSLOptions
    }
    val secretBytes = Files.toByteArray(secretFile)
    val sparkConf = new SparkConf(true)
    val exitCode = new AtomicInteger(0)
    val server = new KubernetesSparkRestServer(
      parsedArguments.host.get,
      parsedArguments.port.get,
      sparkConf,
      secretBytes,
      barrier,
      exitCode,
      sslOptions)
    server.start()
    ShutdownHookManager.addShutdownHook(() => {
      try {
        server.stop()
      } finally {
        barrier.countDown()
      }
    })
    barrier.await()
    System.exit(exitCode.get())
  }

  private def validateSslOptions(parsedArguments: KubernetesSparkRestServerArguments): Unit = {
    parsedArguments.keyStoreFile.foreach { _ =>
      require(parsedArguments.keyPemFile.orElse(parsedArguments.certPemFile).isEmpty,
        "Cannot provide both key/cert PEM files and a keyStore file; select one or the other" +
          " for configuring SSL.")
    }
    parsedArguments.keyPemFile.foreach { _ =>
      require(parsedArguments.certPemFile.isDefined,
        "When providing the key PEM file, the certificate PEM file must also be provided.")
    }
    parsedArguments.certPemFile.foreach { _ =>
      require(parsedArguments.keyPemFile.isDefined,
        "When providing the certificate PEM file, the key PEM file must also be provided.")
    }
  }

  private def randomPassword(): String = {
    RandomStringUtils.random(1024, 0, Integer.MAX_VALUE, false, false, null, SECURE_RANDOM)
  }
}

