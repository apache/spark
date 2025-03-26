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

package org.apache.spark.deploy

import java.io._
import java.lang.reflect.{InvocationTargetException, UndeclaredThrowableException}
import java.net.{URI, URL}
import java.nio.file.Files
import java.security.PrivilegedExceptionAction
import java.util.ServiceLoader
import java.util.jar.JarInputStream

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Properties, Try}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark._
import org.apache.spark.api.r.RUtils
import org.apache.spark.deploy.rest._
import org.apache.spark.internal.{LogEntry, Logging, LogKeys, MDC}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.UI._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util._
import org.apache.spark.util.ArrayImplicits._

/**
 * Whether to submit, kill, or request the status of an application.
 * The latter two operations are currently supported only for standalone cluster mode.
 */
private[deploy] object SparkSubmitAction extends Enumeration {
  type SparkSubmitAction = Value
  val SUBMIT, KILL, REQUEST_STATUS, PRINT_VERSION = Value
}

/**
 * Main gateway of launching a Spark application.
 *
 * This program handles setting up the classpath with relevant Spark dependencies and provides
 * a layer over the different cluster managers and deploy modes that Spark supports.
 */
private[spark] class SparkSubmit extends Logging {

  override protected def logName: String = classOf[SparkSubmit].getName

  import DependencyUtils._
  import SparkSubmit._

  def doSubmit(args: Array[String]): Unit = {
    val appArgs = parseArguments(args)
    val sparkConf = appArgs.toSparkConf()

    // For interpreters, structured logging is disabled by default to avoid generating mixed
    // plain text and structured logs on the same console.
    if (isShell(appArgs.primaryResource) || isSqlShell(appArgs.mainClass)) {
      Logging.disableStructuredLogging()
    } else {
      // For non-shell applications, enable structured logging if it's not explicitly disabled
      // via the configuration `spark.log.structuredLogging.enabled`.
      Utils.resetStructuredLogging(sparkConf)
    }

    // We should initialize log again after `spark.log.structuredLogging.enabled` effected
    Logging.uninitialize()

    // Initialize logging if it hasn't been done yet. Keep track of whether logging needs to
    // be reset before the application starts.
    val uninitLog = initializeLogIfNecessary(true, silent = true)

    if (appArgs.verbose) {
      logInfo(appArgs.toString)
    }
    appArgs.action match {
      case SparkSubmitAction.SUBMIT => submit(appArgs, uninitLog, sparkConf)
      case SparkSubmitAction.KILL => kill(appArgs, sparkConf)
      case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs, sparkConf)
      case SparkSubmitAction.PRINT_VERSION => printVersion()
    }
  }

  protected def parseArguments(args: Array[String]): SparkSubmitArguments = {
    new SparkSubmitArguments(args.toImmutableArraySeq)
  }

  /**
   * Kill an existing submission.
   */
  private def kill(args: SparkSubmitArguments, sparkConf: SparkConf): Unit = {
    if (RestSubmissionClient.supportsRestClient(args.master)) {
      val response = new RestSubmissionClient(args.master)
        .killSubmission(args.submissionToKill)
      if (response.success) {
        logInfo(s"${args.submissionToKill} is killed successfully.")
      } else {
        logError(response.message)
      }
    } else {
      sparkConf.set("spark.master", args.master)
      SparkSubmitUtils
        .getSubmitOperations(args.master)
        .kill(args.submissionToKill, sparkConf)
    }
  }

  /**
   * Request the status of an existing submission.
   */
  private def requestStatus(args: SparkSubmitArguments, sparkConf: SparkConf): Unit = {
    if (RestSubmissionClient.supportsRestClient(args.master)) {
      new RestSubmissionClient(args.master)
        .requestSubmissionStatus(args.submissionToRequestStatusFor)
    } else {
      sparkConf.set("spark.master", args.master)
      SparkSubmitUtils
        .getSubmitOperations(args.master)
        .printSubmissionStatus(args.submissionToRequestStatusFor, sparkConf)
    }
  }

  /** Print version information to the log. */
  private def printVersion(): Unit = {
    logInfo("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
                        """.format(SPARK_VERSION))
    logInfo(log"Using Scala ${MDC(LogKeys.SCALA_VERSION, Properties.versionString)}," +
      log" ${MDC(LogKeys.JAVA_VM_NAME, Properties.javaVmName)}," +
      log" ${MDC(LogKeys.JAVA_VERSION, Properties.javaVersion)}")
    logInfo(log"Branch ${MDC(LogKeys.SPARK_BRANCH, SPARK_BRANCH)}")
    logInfo(log"Compiled by user ${MDC(LogKeys.SPARK_BUILD_USER, SPARK_BUILD_USER)} on" +
      log" ${MDC(LogKeys.SPARK_BUILD_DATE, SPARK_BUILD_DATE)}")
    logInfo(log"Revision ${MDC(LogKeys.SPARK_REVISION, SPARK_REVISION)}")
    logInfo(log"Url ${MDC(LogKeys.SPARK_REPO_URL, SPARK_REPO_URL)}")
    logInfo("Type --help for more information.")
  }

  /**
   * Submit the application using the provided parameters, ensuring to first wrap
   * in a doAs when --proxy-user is specified.
   */
  @tailrec
  private def submit(args: SparkSubmitArguments, uninitLog: Boolean, sparkConf: SparkConf): Unit = {

    def doRunMain(): Unit = {
      if (args.proxyUser != null) {
        // Here we are checking for client mode because when job is sumbitted in cluster
        // deploy mode with k8s resource manager, the spark submit in the driver container
        // is done in client mode.
        val isKubernetesClusterModeDriver = args.master.startsWith("k8s") &&
          "client".equals(args.deployMode) &&
          sparkConf.getBoolean("spark.kubernetes.submitInDriver", false)
        if (isKubernetesClusterModeDriver) {
          logInfo("Running driver with proxy user. Cluster manager: Kubernetes")
          SparkHadoopUtil.get.runAsSparkUser(() => runMain(args, uninitLog))
        } else {
          val proxyUser = UserGroupInformation.createProxyUser(args.proxyUser,
            UserGroupInformation.getCurrentUser())
          try {
            proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
              override def run(): Unit = {
                runMain(args, uninitLog)
              }
            })
          } catch {
            case e: Exception =>
              // Hadoop's AuthorizationException suppresses the exception's stack trace, which
              // makes the message printed to the output by the JVM not very helpful. Instead,
              // detect exceptions with empty stack traces here, and treat them differently.
              if (e.getStackTrace().length == 0) {
                error(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
              } else {
                throw e
              }
          } finally {
            FileSystem.closeAllForUGI(proxyUser)
          }
        }
      } else {
        runMain(args, uninitLog)
      }
    }

    // In standalone cluster mode, there are two submission gateways:
    //   (1) The traditional RPC gateway using o.a.s.deploy.Client as a wrapper
    //   (2) The new REST-based gateway introduced in Spark 1.3
    // The latter is the default behavior as of Spark 1.3, but Spark submit will fail over
    // to use the legacy gateway if the master endpoint turns out to be not a REST server.
    if (args.isStandaloneCluster && args.useRest) {
      try {
        logInfo("Running Spark using the REST application submission protocol.")
        doRunMain()
      } catch {
        // Fail over to use the legacy submission gateway
        case e: SubmitRestConnectionException =>
          logWarning(log"Master endpoint ${MDC(LogKeys.MASTER_URL, args.master)} " +
            log"was not a REST server. Falling back to legacy submission gateway instead.")
          args.useRest = false
          submit(args, false, sparkConf)
      }
    // In all other modes, just run the main class as prepared
    } else {
      doRunMain()
    }
  }

  /**
   * Prepare the environment for submitting an application.
   *
   * @param args the parsed SparkSubmitArguments used for environment preparation.
   * @param conf the Hadoop Configuration, this argument will only be set in unit test.
   * @return a 4-tuple:
   *        (1) the arguments for the child process,
   *        (2) a list of classpath entries for the child,
   *        (3) a map of system properties, and
   *        (4) the main class for the child
   *
   * Exposed for testing.
   */
  private[deploy] def prepareSubmitEnvironment(
      args: SparkSubmitArguments,
      conf: Option[HadoopConfiguration] = None)
      : (Seq[String], Seq[String], SparkConf, String) = {
    // Return values
    val childArgs = new ArrayBuffer[String]()
    val childClasspath = new ArrayBuffer[String]()
    val sparkConf = args.toSparkConf()
    var childMainClass = ""

    // Set the cluster manager
    val clusterManager: Int = args.maybeMaster match {
      case Some(v) =>
        assert(args.maybeRemote.isEmpty)
        v match {
          case "yarn" => YARN
          case m if m.startsWith("spark") => STANDALONE
          case m if m.startsWith("k8s") => KUBERNETES
          case m if m.startsWith("local") => LOCAL
          case _ =>
            error("Master must either be yarn or start with spark, k8s, or local")
            -1
        }
      case None => LOCAL // default master or remote mode.
    }

    // Set the deploy mode; default is client mode
    val deployMode: Int = args.deployMode match {
      case "client" | null => CLIENT
      case "cluster" => CLUSTER
      case _ =>
        error("Deploy mode must be either client or cluster")
        -1
    }

    if (clusterManager == YARN) {
      // Make sure YARN is included in our build if we're trying to use it
      if (!Utils.classIsLoadable(YARN_CLUSTER_SUBMIT_CLASS) && !Utils.isTesting) {
        error(
          "Could not load YARN classes. " +
          "This copy of Spark may not have been compiled with YARN support.")
      }
    }

    if (clusterManager == KUBERNETES) {
      args.maybeMaster = Option(Utils.checkAndGetK8sMasterUrl(args.master))
      // Make sure KUBERNETES is included in our build if we're trying to use it
      if (!Utils.classIsLoadable(KUBERNETES_CLUSTER_SUBMIT_CLASS) && !Utils.isTesting) {
        error(
          "Could not load KUBERNETES classes. " +
            "This copy of Spark may not have been compiled with KUBERNETES support.")
      }
    }

    // Fail fast, the following modes are not supported or applicable
    (clusterManager, deployMode) match {
      case (STANDALONE, CLUSTER) if args.isPython =>
        error("Cluster deploy mode is currently not supported for python " +
          "applications on standalone clusters.")
      case (STANDALONE, CLUSTER) if args.isR =>
        error("Cluster deploy mode is currently not supported for R " +
          "applications on standalone clusters.")
      case (LOCAL, CLUSTER) =>
        error("Cluster deploy mode is not compatible with master \"local\"")
      case (_, CLUSTER) if isShell(args.primaryResource) =>
        error("Cluster deploy mode is not applicable to Spark shells.")
      case (_, CLUSTER) if isSqlShell(args.mainClass) =>
        error("Cluster deploy mode is not applicable to Spark SQL shell.")
      case (_, CLUSTER) if isThriftServer(args.mainClass) =>
        error("Cluster deploy mode is not applicable to Spark Thrift server.")
      case (_, CLUSTER) if isConnectServer(args.mainClass) =>
        error("Cluster deploy mode is not applicable to Spark Connect server.")
      case _ =>
    }

    // Update args.deployMode if it is null. It will be passed down as a Spark property later.
    (args.deployMode, deployMode) match {
      case (null, CLIENT) => args.deployMode = "client"
      case (null, CLUSTER) => args.deployMode = "cluster"
      case _ =>
    }
    val isYarnCluster = clusterManager == YARN && deployMode == CLUSTER
    val isStandAloneCluster = clusterManager == STANDALONE && deployMode == CLUSTER
    val isKubernetesCluster = clusterManager == KUBERNETES && deployMode == CLUSTER
    val isKubernetesClient = clusterManager == KUBERNETES && deployMode == CLIENT
    val isKubernetesClusterModeDriver = isKubernetesClient &&
      sparkConf.getBoolean("spark.kubernetes.submitInDriver", false)
    val isCustomClasspathInClusterModeDisallowed =
      !sparkConf.get(ALLOW_CUSTOM_CLASSPATH_BY_PROXY_USER_IN_CLUSTER_MODE) &&
      args.proxyUser != null &&
      (isYarnCluster || isStandAloneCluster || isKubernetesCluster)

    if (!isStandAloneCluster) {
      // Resolve maven dependencies if there are any and add classpath to jars. Add them to py-files
      // too for packages that include Python code
      val resolvedMavenCoordinates = DependencyUtils.resolveMavenDependencies(
        packagesTransitive = true, args.packagesExclusions, args.packages,
        args.repositories, args.ivyRepoPath, args.ivySettingsPath)

      if (resolvedMavenCoordinates.nonEmpty) {
        if (isKubernetesCluster) {
          // We need this in K8s cluster mode so that we can upload local deps
          // via the k8s application, like in cluster mode driver
          childClasspath ++= resolvedMavenCoordinates
        } else {
          // In K8s client mode, when in the driver, add resolved jars early as we might need
          // them at the submit time for artifact downloading.
          // For example we might use the dependencies for downloading
          // files from a Hadoop Compatible fs e.g. S3. In this case the user might pass:
          // --packages com.amazonaws:aws-java-sdk:1.7.4:org.apache.hadoop:hadoop-aws:2.7.6
          if (isKubernetesClusterModeDriver) {
            val loader = getSubmitClassLoader(sparkConf)
            for (jar <- resolvedMavenCoordinates) {
              addJarToClasspath(jar, loader)
            }
          }

          args.jars = mergeFileLists(args.jars, mergeFileLists(resolvedMavenCoordinates: _*))
          if (args.isPython || isInternal(args.primaryResource)) {
            args.pyFiles = mergeFileLists(args.pyFiles,
              mergeFileLists(resolvedMavenCoordinates: _*))
          }
        }
      }

      // install any R packages that may have been passed through --jars or --packages.
      // Spark Packages may contain R source code inside the jar.
      if (args.isR && !StringUtils.isBlank(args.jars)) {
        RPackageUtils.checkAndBuildRPackage(args.jars, printStream, args.verbose)
      }
    }

    // update spark config from args
    args.toSparkConf(Option(sparkConf))
    val hadoopConf = conf.getOrElse(SparkHadoopUtil.newConfiguration(sparkConf))
    val targetDir = Utils.createTempDir()

    // Kerberos is not supported in standalone mode
    if (clusterManager != STANDALONE
        && args.principal != null
        && args.keytab != null) {
      // If client mode, make sure the keytab is just a local path.
      if (deployMode == CLIENT && Utils.isLocalUri(args.keytab)) {
        args.keytab = new URI(args.keytab).getPath()
      }

      if (!Utils.isLocalUri(args.keytab)) {
        require(new File(args.keytab).exists(), s"Keytab file: ${args.keytab} does not exist")
        UserGroupInformation.loginUserFromKeytab(args.principal, args.keytab)
      }
    }

    // Resolve glob path for different resources.
    args.jars = Option(args.jars).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.files = Option(args.files).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.pyFiles = Option(args.pyFiles).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.archives = Option(args.archives).map(resolveGlobPaths(_, hadoopConf)).orNull


    // In client mode, download remote files.
    var localPrimaryResource: String = null
    var localJars: String = null
    var localPyFiles: String = null
    if (deployMode == CLIENT) {
      localPrimaryResource = Option(args.primaryResource).map {
        downloadFile(_, targetDir, sparkConf, hadoopConf)
      }.orNull
      localJars = Option(args.jars).map {
        downloadFileList(_, targetDir, sparkConf, hadoopConf)
      }.orNull
      localPyFiles = Option(args.pyFiles).map {
        downloadFileList(_, targetDir, sparkConf, hadoopConf)
      }.orNull

      if (isKubernetesClusterModeDriver) {
        // SPARK-33748: this mimics the behaviour of Yarn cluster mode. If the driver is running
        // in cluster mode, the archives should be available in the driver's current working
        // directory too.
        // SPARK-33782 : This downloads all the files , jars , archiveFiles and pyfiles to current
        // working directory
        // SPARK-43540: add current working directory into driver classpath
        // SPARK-47475: make download to driver optional so executors may fetch resource from remote
        // url directly to avoid overwhelming driver network when resource is big and executor count
        // is high
        val workingDirectory = "."
        childClasspath += workingDirectory
        def downloadResourcesToCurrentDirectory(
            uris: String,
            isArchive: Boolean = false,
            avoidDownload: String => Boolean = _ => false): String = {
          val resolvedUris = Utils.stringToSeq(uris).map(Utils.resolveURI)
          val (avoidDownloads, toDownloads) =
            resolvedUris.partition(uri => avoidDownload(uri.getScheme))
          val localResources = downloadFileList(
            toDownloads.map(
              Utils.getUriBuilder(_).fragment(null).build().toString).mkString(","),
            targetDir, sparkConf, hadoopConf)
          (Utils.stringToSeq(localResources).map(Utils.resolveURI).zip(toDownloads).map {
            case (localResources, resolvedUri) =>
              val source = new File(localResources.getPath).getCanonicalFile
              val dest = new File(
                workingDirectory,
                if (resolvedUri.getFragment != null) resolvedUri.getFragment else source.getName)
                .getCanonicalFile
              logInfo(log"Files ${MDC(LogKeys.URI, resolvedUri)}" +
                log" from ${MDC(LogKeys.SOURCE_PATH, source)}" +
                log" to ${MDC(LogKeys.DESTINATION_PATH, dest)}")
              Utils.deleteRecursively(dest)
              if (isArchive) {
                Utils.unpack(source, dest)
              } else {
                Files.copy(source.toPath, dest.toPath)
              }
              // Keep the URIs of local files with the given fragments.
              Utils.getUriBuilder(
                localResources).fragment(resolvedUri.getFragment).build().toString
          } ++ avoidDownloads.map(_.toString)).mkString(",")
        }

        val avoidJarDownloadSchemes = sparkConf.get(KUBERNETES_JARS_AVOID_DOWNLOAD_SCHEMES)

        def avoidJarDownload(scheme: String): Boolean =
          avoidJarDownloadSchemes.contains("*") || avoidJarDownloadSchemes.contains(scheme)

        val filesLocalFiles = Option(args.files).map {
          downloadResourcesToCurrentDirectory(_)
        }.orNull
        val updatedJars = Option(args.jars).map {
          downloadResourcesToCurrentDirectory(_, avoidDownload = avoidJarDownload)
        }.orNull
        val archiveLocalFiles = Option(args.archives).map {
          downloadResourcesToCurrentDirectory(_, true)
        }.orNull
        val pyLocalFiles = Option(args.pyFiles).map {
          downloadResourcesToCurrentDirectory(_)
        }.orNull
        args.files = filesLocalFiles
        args.archives = archiveLocalFiles
        args.pyFiles = pyLocalFiles
        args.jars = updatedJars
      }
    }

    // When running in YARN, for some remote resources with scheme:
    //   1. Hadoop FileSystem doesn't support them.
    //   2. We explicitly bypass Hadoop FileSystem with "spark.yarn.dist.forceDownloadSchemes".
    // We will download them to local disk prior to add to YARN's distributed cache.
    // For yarn client mode, since we already download them with above code, so we only need to
    // figure out the local path and replace the remote one.
    if (clusterManager == YARN) {
      val forceDownloadSchemes = sparkConf.get(FORCE_DOWNLOAD_SCHEMES)

      def shouldDownload(scheme: String): Boolean = {
        forceDownloadSchemes.contains("*") || forceDownloadSchemes.contains(scheme) ||
          Try { FileSystem.getFileSystemClass(scheme, hadoopConf) }.isFailure
      }

      def downloadResource(resource: String): String = {
        val uri = Utils.resolveURI(resource)
        uri.getScheme match {
          case "local" | "file" => resource
          case e if shouldDownload(e) =>
            val file = new File(targetDir, new Path(uri).getName)
            if (file.exists()) {
              file.toURI.toString
            } else {
              downloadFile(resource, targetDir, sparkConf, hadoopConf)
            }
          case _ => uri.toString
        }
      }

      args.primaryResource = Option(args.primaryResource).map { downloadResource }.orNull
      args.files = Option(args.files).map { files =>
        Utils.stringToSeq(files).map(downloadResource).mkString(",")
      }.orNull
      args.pyFiles = Option(args.pyFiles).map { pyFiles =>
        Utils.stringToSeq(pyFiles).map(downloadResource).mkString(",")
      }.orNull
      args.jars = Option(args.jars).map { jars =>
        Utils.stringToSeq(jars).map(downloadResource).mkString(",")
      }.orNull
      args.archives = Option(args.archives).map { archives =>
        Utils.stringToSeq(archives).map(downloadResource).mkString(",")
      }.orNull
    }

    // At this point, we have attempted to download all remote resources.
    // Now we try to resolve the main class if our primary resource is a JAR.
    if (args.mainClass == null && !args.isPython && !args.isR) {
      try {
        val uri = new URI(
          Option(localPrimaryResource).getOrElse(args.primaryResource)
        )
        val fs = FileSystem.get(uri, hadoopConf)

        Utils.tryWithResource(new JarInputStream(fs.open(new Path(uri)))) { jar =>
          args.mainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
        }
      } catch {
        case e: Throwable =>
          error(
            s"Failed to get main class in JAR with error '${e.getMessage}'. " +
            " Please specify one with --class."
          )
      }

      if (args.mainClass == null) {
        // If we still can't figure out the main class at this point, blow up.
        error("No main class set in JAR; please specify one with --class.")
      }
    }

    // If we're running a python app, set the main class to our specific python runner
    if (args.isPython && deployMode == CLIENT) {
      if (args.primaryResource == PYSPARK_SHELL) {
        args.mainClass = "org.apache.spark.api.python.PythonGatewayServer"
      } else {
        // If a python file is provided, add it to the child arguments and list of files to deploy.
        // Usage: PythonAppRunner <main python file> <extra python files> [app arguments]
        args.mainClass = "org.apache.spark.deploy.PythonRunner"
        args.childArgs = ArrayBuffer(localPrimaryResource, localPyFiles) ++ args.childArgs
      }
    }

    // Non-PySpark applications can need Python dependencies.
    if (deployMode == CLIENT && clusterManager != YARN) {
      // The YARN backend handles python files differently, so don't merge the lists.
      args.files = mergeFileLists(args.files, args.pyFiles)
    }

    if (localPyFiles != null) {
      sparkConf.set(SUBMIT_PYTHON_FILES, localPyFiles.split(",").toImmutableArraySeq)
    }

    // In YARN mode for an R app, add the SparkR package archive and the R package
    // archive containing all of the built R libraries to archives so that they can
    // be distributed with the job
    if (args.isR && clusterManager == YARN) {
      val sparkRPackagePath = RUtils.localSparkRPackagePath
      if (sparkRPackagePath.isEmpty) {
        error("SPARK_HOME does not exist for R application in YARN mode.")
      }
      val sparkRPackageFile = new File(sparkRPackagePath.get, SPARKR_PACKAGE_ARCHIVE)
      if (!sparkRPackageFile.exists()) {
        error(s"$SPARKR_PACKAGE_ARCHIVE does not exist for R application in YARN mode.")
      }
      val sparkRPackageURI = Utils.resolveURI(sparkRPackageFile.getAbsolutePath).toString

      // Distribute the SparkR package.
      // Assigns a symbol link name "sparkr" to the shipped package.
      args.archives = mergeFileLists(args.archives, sparkRPackageURI + "#sparkr")

      // Distribute the R package archive containing all the built R packages.
      if (!RUtils.rPackages.isEmpty) {
        val rPackageFile =
          RPackageUtils.zipRLibraries(new File(RUtils.rPackages.get), R_PACKAGE_ARCHIVE)
        if (!rPackageFile.exists()) {
          error("Failed to zip all the built R packages.")
        }

        val rPackageURI = Utils.resolveURI(rPackageFile.getAbsolutePath).toString
        // Assigns a symbol link name "rpkg" to the shipped package.
        args.archives = mergeFileLists(args.archives, rPackageURI + "#rpkg")
      }
    }

    // TODO: Support distributing R packages with standalone cluster
    if (args.isR && clusterManager == STANDALONE && !RUtils.rPackages.isEmpty) {
      error("Distributing R packages with standalone cluster is not supported.")
    }

    // If we're running an R app, set the main class to our specific R runner
    if (args.isR && deployMode == CLIENT) {
      if (args.primaryResource == SPARKR_SHELL) {
        args.mainClass = "org.apache.spark.api.r.RBackend"
      } else {
        // If an R file is provided, add it to the child arguments and list of files to deploy.
        // Usage: RRunner <main R file> [app arguments]
        args.mainClass = "org.apache.spark.deploy.RRunner"
        args.childArgs = ArrayBuffer(localPrimaryResource) ++ args.childArgs
        args.files = mergeFileLists(args.files, args.primaryResource)
      }
    }

    if (isYarnCluster && args.isR) {
      // In yarn-cluster mode for an R app, add primary resource to files
      // that can be distributed with the job
      args.files = mergeFileLists(args.files, args.primaryResource)
    }

    // Special flag to avoid deprecation warnings at the client
    sys.props("SPARK_SUBMIT") = "true"

    // A list of rules to map each argument to system properties or command-line options in
    // each deploy mode; we iterate through these below
    val options = List[OptionAssigner](

      // All cluster managers
      OptionAssigner(
        // If remote is not set, sets the master,
        if (args.maybeRemote.isEmpty) args.master
        else args.maybeMaster.orNull,
        ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, confKey = "spark.master"),
      OptionAssigner(
        args.maybeRemote.orNull, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, confKey = "spark.remote"),
      OptionAssigner(args.deployMode, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = SUBMIT_DEPLOY_MODE.key),
      OptionAssigner(args.name, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, confKey = "spark.app.name"),
      OptionAssigner(args.ivyRepoPath, ALL_CLUSTER_MGRS, CLIENT,
        confKey = JAR_IVY_REPO_PATH.key),
      OptionAssigner(args.driverMemory, ALL_CLUSTER_MGRS, CLIENT,
        confKey = DRIVER_MEMORY.key),
      OptionAssigner(args.driverExtraClassPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = DRIVER_CLASS_PATH.key),
      OptionAssigner(args.driverExtraJavaOptions, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = DRIVER_JAVA_OPTIONS.key),
      OptionAssigner(args.driverExtraLibraryPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = DRIVER_LIBRARY_PATH.key),
      OptionAssigner(args.principal, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = PRINCIPAL.key),
      OptionAssigner(args.keytab, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = KEYTAB.key),
      OptionAssigner(args.pyFiles, ALL_CLUSTER_MGRS, CLUSTER, confKey = SUBMIT_PYTHON_FILES.key),

      // Propagate attributes for dependency resolution at the driver side
      OptionAssigner(args.packages, STANDALONE | KUBERNETES,
        CLUSTER, confKey = JAR_PACKAGES.key),
      OptionAssigner(args.repositories, STANDALONE | KUBERNETES,
        CLUSTER, confKey = JAR_REPOSITORIES.key),
      OptionAssigner(args.ivyRepoPath, STANDALONE | KUBERNETES,
        CLUSTER, confKey = JAR_IVY_REPO_PATH.key),
      OptionAssigner(args.packagesExclusions, STANDALONE | KUBERNETES,
        CLUSTER, confKey = JAR_PACKAGES_EXCLUSIONS.key),

      // Yarn only
      OptionAssigner(args.queue, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.queue"),
      OptionAssigner(args.pyFiles, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.pyFiles",
        mergeFn = Some(mergeFileLists(_, _))),
      OptionAssigner(args.jars, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.jars",
        mergeFn = Some(mergeFileLists(_, _))),
      OptionAssigner(args.files, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.files",
        mergeFn = Some(mergeFileLists(_, _))),
      OptionAssigner(args.archives, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.archives",
        mergeFn = Some(mergeFileLists(_, _))),

      // Other options
      OptionAssigner(args.numExecutors, YARN | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = EXECUTOR_INSTANCES.key),
      OptionAssigner(args.executorCores, STANDALONE | YARN | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = EXECUTOR_CORES.key),
      OptionAssigner(args.executorMemory, STANDALONE | YARN | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = EXECUTOR_MEMORY.key),
      OptionAssigner(args.totalExecutorCores, STANDALONE, ALL_DEPLOY_MODES,
        confKey = CORES_MAX.key),
      OptionAssigner(args.files, LOCAL | STANDALONE | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = FILES.key),
      OptionAssigner(args.archives, LOCAL | STANDALONE | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = ARCHIVES.key),
      OptionAssigner(args.jars, LOCAL, CLIENT, confKey = JARS.key),
      OptionAssigner(args.jars, STANDALONE | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = JARS.key),
      OptionAssigner(args.driverMemory, STANDALONE | YARN | KUBERNETES, CLUSTER,
        confKey = DRIVER_MEMORY.key),
      OptionAssigner(args.driverCores, STANDALONE | YARN | KUBERNETES, CLUSTER,
        confKey = DRIVER_CORES.key),
      OptionAssigner(args.supervise.toString, STANDALONE, CLUSTER,
        confKey = DRIVER_SUPERVISE.key),
      OptionAssigner(args.ivyRepoPath, STANDALONE, CLUSTER, confKey = JAR_IVY_REPO_PATH.key),

      // An internal option used only for spark-shell to add user jars to repl's classloader,
      // previously it uses "spark.jars" or "spark.yarn.dist.jars" which now may be pointed to
      // remote jars, so adding a new option to only specify local jars for spark-shell internally.
      OptionAssigner(localJars, ALL_CLUSTER_MGRS, CLIENT, confKey = "spark.repl.local.jars")
    )

    // In client mode, launch the application main class directly
    // In addition, add the main application jar and any added jars (if any) to the classpath
    if (deployMode == CLIENT) {
      childMainClass = args.mainClass
      if (localPrimaryResource != null && isUserJar(localPrimaryResource)) {
        childClasspath += localPrimaryResource
      }
      if (localJars != null) { childClasspath ++= localJars.split(",") }
    }
    // Add the main application jar and any added jars to classpath in case YARN client
    // requires these jars.
    // This assumes both primaryResource and user jars are local jars, or already downloaded
    // to local by configuring "spark.yarn.dist.forceDownloadSchemes", otherwise it will not be
    // added to the classpath of YARN client.
    if (isYarnCluster) {
      if (isUserJar(args.primaryResource)) {
        childClasspath += args.primaryResource
      }
      if (args.jars != null) { childClasspath ++= args.jars.split(",") }
    }

    if (deployMode == CLIENT) {
      if (args.childArgs != null) { childArgs ++= args.childArgs }
    }

    // Map all arguments to command-line options or system properties for our chosen mode
    for (opt <- options) {
      if (opt.value != null &&
          (deployMode & opt.deployMode) != 0 &&
          (clusterManager & opt.clusterManager) != 0) {
        if (opt.clOption != null) { childArgs += opt.clOption += opt.value }
        if (opt.confKey != null) {
          // Used in SparkConnectClient because Spark Connect client does not have SparkConf.
          if (opt.confKey == "spark.remote") System.setProperty("spark.remote", opt.value)
          if (opt.mergeFn.isDefined && sparkConf.contains(opt.confKey)) {
            sparkConf.set(opt.confKey, opt.mergeFn.get.apply(sparkConf.get(opt.confKey), opt.value))
          } else {
            sparkConf.set(opt.confKey, opt.value)
          }
        }
      }
    }

    // In case of shells, spark.ui.showConsoleProgress can be true by default or by user. Except,
    // when Spark Connect is in local mode, because Spark Connect support its own progress
    // reporting.
    if (isShell(args.primaryResource) && !sparkConf.contains(UI_SHOW_CONSOLE_PROGRESS)) {
      sparkConf.set(UI_SHOW_CONSOLE_PROGRESS, true)
    }

    // Add the application jar automatically so the user doesn't have to call sc.addJar
    // For isKubernetesClusterModeDriver, the jar is already added in the previous spark-submit
    // For YARN cluster mode, the jar is already distributed on each node as "app.jar"
    // For python and R files, the primary resource is already distributed as a regular file
    if (!isKubernetesClusterModeDriver && !isYarnCluster && !args.isPython && !args.isR) {
      var jars = sparkConf.get(JARS)
      if (isUserJar(args.primaryResource)) {
        jars = jars ++ Seq(args.primaryResource)
      }
      sparkConf.set(JARS, jars)
    }

    // In standalone cluster mode, use the REST client to submit the application (Spark 1.3+).
    // All Spark parameters are expected to be passed to the client through system properties.
    if (args.isStandaloneCluster) {
      if (args.useRest) {
        childMainClass = REST_CLUSTER_SUBMIT_CLASS
        childArgs += args.primaryResource += args.mainClass
      } else {
        // In legacy standalone cluster mode, use Client as a wrapper around the user class
        childMainClass = STANDALONE_CLUSTER_SUBMIT_CLASS
        if (args.supervise) { childArgs += "--supervise" }
        Option(args.driverMemory).foreach { m => childArgs += "--memory" += m }
        Option(args.driverCores).foreach { c => childArgs += "--cores" += c }
        childArgs += "launch"
        childArgs += args.master += args.primaryResource += args.mainClass
      }
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    // Let YARN know it's a pyspark app, so it distributes needed libraries.
    if (clusterManager == YARN) {
      if (args.isPython) {
        sparkConf.set("spark.yarn.isPython", "true")
      }
    }

    if (clusterManager == KUBERNETES && UserGroupInformation.isSecurityEnabled) {
      setRMPrincipal(sparkConf)
    }

    // In yarn-cluster mode, use yarn.Client as a wrapper around the user class
    if (isYarnCluster) {
      childMainClass = YARN_CLUSTER_SUBMIT_CLASS
      if (args.isPython) {
        childArgs += "--primary-py-file" += args.primaryResource
        childArgs += "--class" += "org.apache.spark.deploy.PythonRunner"
      } else if (args.isR) {
        val mainFile = new Path(args.primaryResource).getName
        childArgs += "--primary-r-file" += mainFile
        childArgs += "--class" += "org.apache.spark.deploy.RRunner"
      } else {
        if (args.primaryResource != SparkLauncher.NO_RESOURCE) {
          childArgs += "--jar" += args.primaryResource
        }
        childArgs += "--class" += args.mainClass
      }
      if (args.childArgs != null) {
        args.childArgs.foreach { arg => childArgs += "--arg" += arg }
      }
    }

    if (isKubernetesCluster) {
      childMainClass = KUBERNETES_CLUSTER_SUBMIT_CLASS
      if (args.primaryResource != SparkLauncher.NO_RESOURCE) {
        if (args.isPython) {
          childArgs ++= Array("--primary-py-file", args.primaryResource)
          childArgs ++= Array("--main-class", "org.apache.spark.deploy.PythonRunner")
        } else if (args.isR) {
          childArgs ++= Array("--primary-r-file", args.primaryResource)
          childArgs ++= Array("--main-class", "org.apache.spark.deploy.RRunner")
        }
        else {
          childArgs ++= Array("--primary-java-resource", args.primaryResource)
          childArgs ++= Array("--main-class", args.mainClass)
        }
      } else {
        childArgs ++= Array("--main-class", args.mainClass)
      }
      if (args.childArgs != null) {
        args.childArgs.foreach { arg =>
          childArgs += "--arg" += arg
        }
      }
      // Pass the proxyUser to the k8s app so it is possible to add it to the driver args
      if (args.proxyUser != null) {
        childArgs += "--proxy-user" += args.proxyUser
      }
    }

    // Load any properties specified through --conf and the default properties file
    for ((k, v) <- args.sparkProperties) {
      sparkConf.setIfMissing(k, v)
    }

    // Ignore invalid spark.driver.host in cluster modes.
    if (deployMode == CLUSTER) {
      sparkConf.remove(DRIVER_HOST_ADDRESS)
    }

    // Resolve paths in certain spark properties
    val pathConfigs = Seq(
      JARS.key,
      FILES.key,
      ARCHIVES.key,
      "spark.yarn.dist.files",
      "spark.yarn.dist.archives",
      "spark.yarn.dist.jars")
    pathConfigs.foreach { config =>
      // Replace old URIs with resolved URIs, if they exist
      sparkConf.getOption(config).foreach { oldValue =>
        sparkConf.set(config, Utils.resolveURIs(oldValue))
      }
    }

    // Resolve and format python file paths properly before adding them to the PYTHONPATH.
    // The resolving part is redundant in the case of --py-files, but necessary if the user
    // explicitly sets `spark.submit.pyFiles` in his/her default properties file.
    val pyFiles = sparkConf.get(SUBMIT_PYTHON_FILES)
    val resolvedPyFiles = Utils.resolveURIs(pyFiles.mkString(","))
    val formattedPyFiles = if (deployMode != CLUSTER) {
      PythonRunner.formatPaths(resolvedPyFiles).mkString(",")
    } else {
      // Ignoring formatting python path in yarn cluster mode, these two modes
      // support dealing with remote python files, they could distribute and add python files
      // locally.
      resolvedPyFiles
    }
    sparkConf.set(SUBMIT_PYTHON_FILES, formattedPyFiles.split(",").toImmutableArraySeq)

    if (args.verbose && isSqlShell(childMainClass)) {
      childArgs ++= Seq("--verbose")
    }

    val setSubmitTimeInClusterModeDriver =
      sparkConf.getBoolean("spark.kubernetes.setSubmitTimeInDriver", true)
    if (!sparkConf.contains("spark.app.submitTime")
      || isKubernetesClusterModeDriver && setSubmitTimeInClusterModeDriver) {
      sparkConf.set("spark.app.submitTime", System.currentTimeMillis().toString)
    }

    if (childClasspath.nonEmpty && isCustomClasspathInClusterModeDisallowed) {
      childClasspath.clear()
      logWarning(log"Ignore classpath " +
        log"${MDC(LogKeys.CLASS_PATH, childClasspath.mkString(", "))} " +
        log"with proxy user specified in Cluster mode when " +
        log"${MDC(LogKeys.CONFIG, ALLOW_CUSTOM_CLASSPATH_BY_PROXY_USER_IN_CLUSTER_MODE.key)} is " +
        log"disabled")
    }

    (childArgs.toSeq, childClasspath.toSeq, sparkConf, childMainClass)
  }

  // [SPARK-20328]. HadoopRDD calls into a Hadoop library that fetches delegation tokens with
  // renewer set to the YARN ResourceManager.  Since YARN isn't configured in Kubernetes
  // mode, we must trick it into thinking we're YARN.
  private def setRMPrincipal(sparkConf: SparkConf): Unit = {
    val shortUserName = UserGroupInformation.getCurrentUser.getShortUserName
    val key = s"spark.hadoop.${YarnConfiguration.RM_PRINCIPAL}"
    logInfo(log"Setting ${MDC(LogKeys.KEY, key)} to ${MDC(LogKeys.SHORT_USER_NAME, shortUserName)}")
    sparkConf.set(key, shortUserName)
  }

  private def getSubmitClassLoader(sparkConf: SparkConf): MutableURLClassLoader = {
    val loader =
      if (sparkConf.get(DRIVER_USER_CLASS_PATH_FIRST)) {
        new ChildFirstURLClassLoader(new Array[URL](0),
          Thread.currentThread.getContextClassLoader)
      } else {
        new MutableURLClassLoader(new Array[URL](0),
          Thread.currentThread.getContextClassLoader)
      }
    Thread.currentThread.setContextClassLoader(loader)
    loader
  }

  /**
   * Run the main method of the child class using the submit arguments.
   *
   * This runs in two steps. First, we prepare the launch environment by setting up
   * the appropriate classpath, system properties, and application arguments for
   * running the child main class based on the cluster manager and the deploy mode.
   * Second, we use this launch environment to invoke the main method of the child
   * main class.
   *
   * Note that this main class will not be the one provided by the user if we're
   * running cluster deploy mode or python applications.
   */
  private def runMain(args: SparkSubmitArguments, uninitLog: Boolean): Unit = {
    val (childArgs, childClasspath, sparkConf, childMainClass) = prepareSubmitEnvironment(args)
    // Let the main class re-initialize the logging system once it starts.
    if (uninitLog) {
      Logging.uninitialize()
    }

    if (args.verbose) {
      logInfo(log"Main class:\n${MDC(LogKeys.CLASS_NAME, childMainClass)}")
      logInfo(log"Arguments:\n${MDC(LogKeys.ARGS, childArgs.mkString("\n"))}")
      // sysProps may contain sensitive information, so redact before printing
      logInfo(log"Spark config:\n" +
      log"${MDC(LogKeys.CONFIG, Utils.redact(sparkConf.getAll.toMap).sorted.mkString("\n"))}")
      logInfo(log"Classpath elements:\n${MDC(LogKeys.CLASS_PATHS, childClasspath.mkString("\n"))}")
      logInfo("\n")
    }
    assert(!(args.deployMode == "cluster" && args.proxyUser != null && childClasspath.nonEmpty) ||
      sparkConf.get(ALLOW_CUSTOM_CLASSPATH_BY_PROXY_USER_IN_CLUSTER_MODE),
      s"Classpath of spark-submit should not change in cluster mode if proxy user is specified " +
        s"when ${ALLOW_CUSTOM_CLASSPATH_BY_PROXY_USER_IN_CLUSTER_MODE.key} is disabled")
    val loader = getSubmitClassLoader(sparkConf)
    for (jar <- childClasspath) {
      addJarToClasspath(jar, loader)
    }

    var mainClass: Class[_] = null

    try {
      mainClass = Utils.classForName(childMainClass)
    } catch {
      case e: ClassNotFoundException =>
        logError(log"Failed to load class ${MDC(LogKeys.CLASS_NAME, childMainClass)}.")
        if (childMainClass.contains("thriftserver")) {
          logInfo(log"Failed to load main class ${MDC(LogKeys.CLASS_NAME, childMainClass)}.")
          logInfo("You need to build Spark with -Phive and -Phive-thriftserver.")
        } else if (childMainClass.contains("org.apache.spark.sql.connect")) {
          logInfo(log"Failed to load main class ${MDC(LogKeys.CLASS_NAME, childMainClass)}.")
          // TODO(SPARK-42375): Should point out the user-facing page here instead.
          logInfo("You need to specify Spark Connect jars with --jars or --packages.")
        }
        throw new SparkUserAppException(CLASS_NOT_FOUND_EXIT_STATUS)
      case e: NoClassDefFoundError =>
        logError(log"Failed to load ${MDC(LogKeys.CLASS_NAME, childMainClass)}", e)
        if (e.getMessage.contains("org/apache/hadoop/hive")) {
          logInfo("Failed to load hive class.")
          logInfo("You need to build Spark with -Phive and -Phive-thriftserver.")
        }
        throw new SparkUserAppException(CLASS_NOT_FOUND_EXIT_STATUS)
    }

    val app: SparkApplication = if (classOf[SparkApplication].isAssignableFrom(mainClass)) {
      mainClass.getConstructor().newInstance().asInstanceOf[SparkApplication]
    } else {
      new JavaMainApplication(mainClass)
    }

    @tailrec
    def findCause(t: Throwable): Throwable = t match {
      case e: UndeclaredThrowableException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: InvocationTargetException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: Throwable =>
        e
    }

    try {
      app.start(childArgs.toArray, sparkConf)
    } catch {
      case t: Throwable =>
        throw findCause(t)
    } finally {
      if (args.master.startsWith("k8s") && !isShell(args.primaryResource) &&
          !isSqlShell(args.mainClass) && !isThriftServer(args.mainClass) &&
          !isConnectServer(args.mainClass)) {
        try {
          SparkContext.getActive.foreach(_.stop())
        } catch {
          case e: Throwable => logError("Failed to close SparkContext", e)
        }
      }
    }
  }

  /** Throw a SparkException with the given error message. */
  private def error(msg: String): Unit = throw new SparkException(msg)

}


/**
 * This entry point is used by the launcher library to start in-process Spark applications.
 */
private[spark] object InProcessSparkSubmit {

  def main(args: Array[String]): Unit = {
    val submit = new SparkSubmit()
    submit.doSubmit(args)
  }

}

object SparkSubmit extends CommandLineUtils with Logging {

  // Cluster managers
  private val YARN = 1
  private val STANDALONE = 2
  private val LOCAL = 8
  private val KUBERNETES = 16
  private val ALL_CLUSTER_MGRS = YARN | STANDALONE | LOCAL | KUBERNETES

  // Deploy modes
  private val CLIENT = 1
  private val CLUSTER = 2
  private val ALL_DEPLOY_MODES = CLIENT | CLUSTER

  // Special primary resource names that represent shells rather than application jars.
  private val SPARK_SHELL = "spark-shell"
  private val PYSPARK_SHELL = "pyspark-shell"
  private val SPARKR_SHELL = "sparkr-shell"
  private val CONNECT_SHELL = "connect-shell"
  private val SPARKR_PACKAGE_ARCHIVE = "sparkr.zip"
  private val R_PACKAGE_ARCHIVE = "rpkg.zip"

  private val CLASS_NOT_FOUND_EXIT_STATUS = 101

  // Following constants are visible for testing.
  private[deploy] val YARN_CLUSTER_SUBMIT_CLASS =
    "org.apache.spark.deploy.yarn.YarnClusterApplication"
  private[deploy] val REST_CLUSTER_SUBMIT_CLASS = classOf[RestSubmissionClientApp].getName()
  private[deploy] val STANDALONE_CLUSTER_SUBMIT_CLASS = classOf[ClientApp].getName()
  private[deploy] val KUBERNETES_CLUSTER_SUBMIT_CLASS =
    "org.apache.spark.deploy.k8s.submit.KubernetesClientApplication"

  override def main(args: Array[String]): Unit = {
    Option(System.getenv("SPARK_PREFER_IPV6"))
      .foreach(System.setProperty("java.net.preferIPv6Addresses", _))
    val submit = new SparkSubmit() {
      self =>

      override protected def parseArguments(args: Array[String]): SparkSubmitArguments = {
        new SparkSubmitArguments(args.toImmutableArraySeq) {
          override protected def logInfo(msg: => String): Unit = self.logInfo(msg)

          override protected def logInfo(entry: LogEntry): Unit = self.logInfo(entry)

          override protected def logWarning(msg: => String): Unit = self.logWarning(msg)

          override protected def logWarning(entry: LogEntry): Unit = self.logWarning(entry)

          override protected def logError(msg: => String): Unit = self.logError(msg)

          override protected def logError(entry: LogEntry): Unit = self.logError(entry)
        }
      }

      override protected def logInfo(msg: => String): Unit = printMessage(msg)

      override protected def logInfo(entry: LogEntry): Unit = printMessage(entry.message)

      override protected def logWarning(msg: => String): Unit = printMessage(s"Warning: $msg")

      override protected def logWarning(entry: LogEntry): Unit =
        printMessage(s"Warning: ${entry.message}")

      override protected def logError(msg: => String): Unit = printMessage(s"Error: $msg")

      override protected def logError(entry: LogEntry): Unit =
        printMessage(s"Error: ${entry.message}")

      override def doSubmit(args: Array[String]): Unit = {
        try {
          super.doSubmit(args)
        } catch {
          case e: SparkUserAppException =>
            exitFn(e.exitCode)
        }
      }

    }

    submit.doSubmit(args)
  }

  /**
   * Return whether the given primary resource represents a user jar.
   */
  private[deploy] def isUserJar(res: String): Boolean = {
    !isShell(res) && !isPython(res) && !isInternal(res) && !isR(res)
  }

  /**
   * Return whether the given primary resource represents a shell.
   */
  private[deploy] def isShell(res: String): Boolean = {
    (res == SPARK_SHELL || res == PYSPARK_SHELL || res == SPARKR_SHELL || res == CONNECT_SHELL)
  }

  /**
   * Return whether the given main class represents a sql shell.
   */
  private[deploy] def isSqlShell(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"
  }

  /**
   * Return whether the given main class represents a thrift server.
   */
  private def isThriftServer(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"
  }

  /**
   * Return whether the given main class represents a connect server.
   */
  private def isConnectServer(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.connect.service.SparkConnectServer"
  }

  /**
   * Return whether the given primary resource requires running python.
   */
  private[deploy] def isPython(res: String): Boolean = {
    res != null && res.endsWith(".py") || res == PYSPARK_SHELL
  }

  /**
   * Return whether the given primary resource requires running R.
   */
  private[deploy] def isR(res: String): Boolean = {
    res != null && (res.endsWith(".R") || res.endsWith(".r")) || res == SPARKR_SHELL
  }

  private[deploy] def isInternal(res: String): Boolean = {
    res == SparkLauncher.NO_RESOURCE
  }

}

private[spark] object SparkSubmitUtils {
  private[deploy] def getSubmitOperations(master: String): SparkSubmitOperation = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoaders =
      ServiceLoader.load(classOf[SparkSubmitOperation], loader)
        .asScala
        .filter(_.supports(master))

    serviceLoaders.size match {
      case x if x > 1 =>
        throw new SparkException(s"Multiple($x) external SparkSubmitOperations " +
          s"clients registered for master url ${master}.")
      case 1 => serviceLoaders.headOption.get
      case _ =>
        throw new IllegalArgumentException(s"No external SparkSubmitOperations " +
          s"clients found for master url: '$master'")
    }
  }

  def parseSparkConfProperty(pair: String): (String, String) = {
    pair.split("=", 2).toImmutableArraySeq match {
      case Seq(k, v) => (k, v)
      case _ => throw new SparkException(s"Spark config without '=': $pair")
    }
  }
}

/**
 * Provides an indirection layer for passing arguments as system properties or flags to
 * the user's driver program or to downstream launcher tools.
 */
private case class OptionAssigner(
    value: String,
    clusterManager: Int,
    deployMode: Int,
    clOption: String = null,
    confKey: String = null,
    mergeFn: Option[(String, String) => String] = None)

private[spark] trait SparkSubmitOperation {

  def kill(submissionId: String, conf: SparkConf): Unit

  def printSubmissionStatus(submissionId: String, conf: SparkConf): Unit

  def supports(master: String): Boolean
}
