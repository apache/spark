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

import java.io.{FileSystem => _, _}
import java.net.{InetAddress, UnknownHostException, URI}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{Locale, Properties, UUID}
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, ListBuffer, Map}
import scala.util.control.NonFatal

import com.google.common.base.Objects
import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.StringUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier
import org.apache.hadoop.yarn.util.Records

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{SparkApplication, SparkHadoopUtil}
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Python._
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle, YarnCommandBuilderUtils}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.{CallerContext, Utils}

private[spark] class Client(
    val args: ClientArguments,
    val sparkConf: SparkConf,
    val rpcEnv: RpcEnv)
  extends Logging {

  import Client._
  import YarnSparkHadoopUtil._

  private val yarnClient = YarnClient.createYarnClient
  private val hadoopConf = new YarnConfiguration(SparkHadoopUtil.newConfiguration(sparkConf))

  private val isClusterMode = sparkConf.get(SUBMIT_DEPLOY_MODE) == "cluster"

  private val isClientUnmanagedAMEnabled = sparkConf.get(YARN_UNMANAGED_AM) && !isClusterMode
  private var appMaster: ApplicationMaster = _
  private var stagingDirPath: Path = _

  // AM related configurations
  private val amMemory = if (isClusterMode) {
    sparkConf.get(DRIVER_MEMORY).toInt
  } else {
    sparkConf.get(AM_MEMORY).toInt
  }
  private val amMemoryOverhead = {
    val amMemoryOverheadEntry = if (isClusterMode) DRIVER_MEMORY_OVERHEAD else AM_MEMORY_OVERHEAD
    sparkConf.get(amMemoryOverheadEntry).getOrElse(
      math.max((MEMORY_OVERHEAD_FACTOR * amMemory).toLong, MEMORY_OVERHEAD_MIN)).toInt
  }
  private val amCores = if (isClusterMode) {
    sparkConf.get(DRIVER_CORES)
  } else {
    sparkConf.get(AM_CORES)
  }

  // Executor related configurations
  private val executorMemory = sparkConf.get(EXECUTOR_MEMORY)
  private val executorMemoryOverhead = sparkConf.get(EXECUTOR_MEMORY_OVERHEAD).getOrElse(
    math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toLong, MEMORY_OVERHEAD_MIN)).toInt

  private val isPython = sparkConf.get(IS_PYTHON_APP)
  private val pysparkWorkerMemory: Int = if (isPython) {
    sparkConf.get(PYSPARK_EXECUTOR_MEMORY).map(_.toInt).getOrElse(0)
  } else {
    0
  }

  private val distCacheMgr = new ClientDistributedCacheManager()
  private val cachedResourcesConf = new SparkConf(false)

  private val keytab = sparkConf.get(KEYTAB).orNull
  private val amKeytabFileName: Option[String] = if (keytab != null && isClusterMode) {
    val principal = sparkConf.get(PRINCIPAL).orNull
    require((principal == null) == (keytab == null),
      "Both principal and keytab must be defined, or neither.")
    logInfo(s"Kerberos credentials: principal = $principal, keytab = $keytab")
    // Generate a file name that can be used for the keytab file, that does not conflict
    // with any user file.
    Some(new File(keytab).getName() + "-" + UUID.randomUUID().toString)
  } else {
    None
  }

  require(keytab == null || !Utils.isLocalUri(keytab), "Keytab should reference a local file.")

  private val launcherBackend = new LauncherBackend() {
    override protected def conf: SparkConf = sparkConf

    override def onStopRequest(): Unit = {
      if (isClusterMode && appId != null) {
        yarnClient.killApplication(appId)
      } else {
        setState(SparkAppHandle.State.KILLED)
        stop()
      }
    }
  }
  private val fireAndForget = isClusterMode && !sparkConf.get(WAIT_FOR_APP_COMPLETION)

  private var appId: ApplicationId = null

  def reportLauncherState(state: SparkAppHandle.State): Unit = {
    launcherBackend.setState(state)
  }

  def stop(): Unit = {
    if (appMaster != null) {
      appMaster.stopUnmanaged(stagingDirPath)
    }
    launcherBackend.close()
    yarnClient.stop()
  }

  /**
   * Submit an application running our ApplicationMaster to the ResourceManager.
   *
   * The stable Yarn API provides a convenience method (YarnClient#createApplication) for
   * creating applications and setting up the application submission context. This was not
   * available in the alpha API.
   */
  def submitApplication(): ApplicationId = {
    ResourceRequestHelper.validateResources(sparkConf)

    var appId: ApplicationId = null
    try {
      launcherBackend.connect()
      yarnClient.init(hadoopConf)
      yarnClient.start()

      logInfo("Requesting a new application from cluster with %d NodeManagers"
        .format(yarnClient.getYarnClusterMetrics.getNumNodeManagers))

      // Get a new application from our RM
      val newApp = yarnClient.createApplication()
      val newAppResponse = newApp.getNewApplicationResponse()
      appId = newAppResponse.getApplicationId()

      // The app staging dir based on the STAGING_DIR configuration if configured
      // otherwise based on the users home directory.
      val appStagingBaseDir = sparkConf.get(STAGING_DIR)
        .map { new Path(_, UserGroupInformation.getCurrentUser.getShortUserName) }
        .getOrElse(FileSystem.get(hadoopConf).getHomeDirectory())
      stagingDirPath = new Path(appStagingBaseDir, getAppStagingDir(appId))

      new CallerContext("CLIENT", sparkConf.get(APP_CALLER_CONTEXT),
        Option(appId.toString)).setCurrentContext()

      // Verify whether the cluster has enough resources for our AM
      verifyClusterResources(newAppResponse)

      // Set up the appropriate contexts to launch our AM
      val containerContext = createContainerLaunchContext(newAppResponse)
      val appContext = createApplicationSubmissionContext(newApp, containerContext)

      // Finally, submit and monitor the application
      logInfo(s"Submitting application $appId to ResourceManager")
      yarnClient.submitApplication(appContext)
      launcherBackend.setAppId(appId.toString)
      reportLauncherState(SparkAppHandle.State.SUBMITTED)

      appId
    } catch {
      case e: Throwable =>
        if (stagingDirPath != null) {
          cleanupStagingDir()
        }
        throw e
    }
  }

  /**
   * Cleanup application staging directory.
   */
  private def cleanupStagingDir(): Unit = {
    if (sparkConf.get(PRESERVE_STAGING_FILES)) {
      return
    }

    def cleanupStagingDirInternal(): Unit = {
      try {
        val fs = stagingDirPath.getFileSystem(hadoopConf)
        if (fs.delete(stagingDirPath, true)) {
          logInfo(s"Deleted staging directory $stagingDirPath")
        }
      } catch {
        case ioe: IOException =>
          logWarning("Failed to cleanup staging dir " + stagingDirPath, ioe)
      }
    }

    cleanupStagingDirInternal()
  }

  /**
   * Set up the context for submitting our ApplicationMaster.
   * This uses the YarnClientApplication not available in the Yarn alpha API.
   */
  def createApplicationSubmissionContext(
      newApp: YarnClientApplication,
      containerContext: ContainerLaunchContext): ApplicationSubmissionContext = {
    val amResources =
      if (isClusterMode) {
        sparkConf.getAllWithPrefix(config.YARN_DRIVER_RESOURCE_TYPES_PREFIX).toMap
      } else {
        sparkConf.getAllWithPrefix(config.YARN_AM_RESOURCE_TYPES_PREFIX).toMap
      }
    logDebug(s"AM resources: $amResources")
    val appContext = newApp.getApplicationSubmissionContext
    appContext.setApplicationName(sparkConf.get("spark.app.name", "Spark"))
    appContext.setQueue(sparkConf.get(QUEUE_NAME))
    appContext.setAMContainerSpec(containerContext)
    appContext.setApplicationType("SPARK")

    sparkConf.get(APPLICATION_TAGS).foreach { tags =>
      appContext.setApplicationTags(new java.util.HashSet[String](tags.asJava))
    }
    sparkConf.get(MAX_APP_ATTEMPTS) match {
      case Some(v) => appContext.setMaxAppAttempts(v)
      case None => logDebug(s"${MAX_APP_ATTEMPTS.key} is not set. " +
          "Cluster's default value will be used.")
    }

    sparkConf.get(AM_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS).foreach { interval =>
      appContext.setAttemptFailuresValidityInterval(interval)
    }

    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(amMemory + amMemoryOverhead)
    capability.setVirtualCores(amCores)
    if (amResources.nonEmpty) {
      ResourceRequestHelper.setResourceRequests(amResources, capability)
    }
    logDebug(s"Created resource capability for AM request: $capability")

    sparkConf.get(AM_NODE_LABEL_EXPRESSION) match {
      case Some(expr) =>
        val amRequest = Records.newRecord(classOf[ResourceRequest])
        amRequest.setResourceName(ResourceRequest.ANY)
        amRequest.setPriority(Priority.newInstance(0))
        amRequest.setCapability(capability)
        amRequest.setNumContainers(1)
        amRequest.setNodeLabelExpression(expr)
        appContext.setAMContainerResourceRequest(amRequest)
      case None =>
        appContext.setResource(capability)
    }

    sparkConf.get(ROLLED_LOG_INCLUDE_PATTERN).foreach { includePattern =>
      try {
        val logAggregationContext = Records.newRecord(classOf[LogAggregationContext])
        logAggregationContext.setRolledLogsIncludePattern(includePattern)
        sparkConf.get(ROLLED_LOG_EXCLUDE_PATTERN).foreach { excludePattern =>
          logAggregationContext.setRolledLogsExcludePattern(excludePattern)
        }
        appContext.setLogAggregationContext(logAggregationContext)
      } catch {
        case NonFatal(e) =>
          logWarning(s"Ignoring ${ROLLED_LOG_INCLUDE_PATTERN.key} because the version of YARN " +
            "does not support it", e)
      }
    }
    appContext.setUnmanagedAM(isClientUnmanagedAMEnabled)
    appContext
  }

  /**
   * Set up security tokens for launching our ApplicationMaster container.
   *
   * In client mode, a set of credentials has been obtained by the scheduler, so they are copied
   * and sent to the AM. In cluster mode, new credentials are obtained and then sent to the AM,
   * along with whatever credentials the current user already has.
   */
  private def setupSecurityToken(amContainer: ContainerLaunchContext): Unit = {
    val currentUser = UserGroupInformation.getCurrentUser()
    val credentials = currentUser.getCredentials()

    if (isClusterMode) {
      val credentialManager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)
      credentialManager.obtainDelegationTokens(credentials)
    }

    val serializedCreds = SparkHadoopUtil.get.serialize(credentials)
    amContainer.setTokens(ByteBuffer.wrap(serializedCreds))
  }

  /** Get the application report from the ResourceManager for an application we have submitted. */
  def getApplicationReport(appId: ApplicationId): ApplicationReport =
    yarnClient.getApplicationReport(appId)

  /**
   * Return the security token used by this client to communicate with the ApplicationMaster.
   * If no security is enabled, the token returned by the report is null.
   */
  private def getClientToken(report: ApplicationReport): String =
    Option(report.getClientToAMToken).map(_.toString).getOrElse("")

  /**
   * Fail fast if we have requested more resources per container than is available in the cluster.
   */
  private def verifyClusterResources(newAppResponse: GetNewApplicationResponse): Unit = {
    val maxMem = newAppResponse.getMaximumResourceCapability().getMemory()
    logInfo("Verifying our application has not requested more than the maximum " +
      s"memory capability of the cluster ($maxMem MB per container)")
    val executorMem = executorMemory + executorMemoryOverhead + pysparkWorkerMemory
    if (executorMem > maxMem) {
      throw new IllegalArgumentException(s"Required executor memory ($executorMemory), overhead " +
        s"($executorMemoryOverhead MB), and PySpark memory ($pysparkWorkerMemory MB) is above " +
        s"the max threshold ($maxMem MB) of this cluster! Please check the values of " +
        s"'yarn.scheduler.maximum-allocation-mb' and/or 'yarn.nodemanager.resource.memory-mb'.")
    }
    val amMem = amMemory + amMemoryOverhead
    if (amMem > maxMem) {
      throw new IllegalArgumentException(s"Required AM memory ($amMemory" +
        s"+$amMemoryOverhead MB) is above the max threshold ($maxMem MB) of this cluster! " +
        "Please check the values of 'yarn.scheduler.maximum-allocation-mb' and/or " +
        "'yarn.nodemanager.resource.memory-mb'.")
    }
    logInfo("Will allocate AM container, with %d MB memory including %d MB overhead".format(
      amMem,
      amMemoryOverhead))

    // We could add checks to make sure the entire cluster has enough resources but that involves
    // getting all the node reports and computing ourselves.
  }

  /**
   * Copy the given file to a remote file system (e.g. HDFS) if needed.
   * The file is only copied if the source and destination file systems are different or the source
   * scheme is "file". This is used for preparing resources for launching the ApplicationMaster
   * container. Exposed for testing.
   */
  private[yarn] def copyFileToRemote(
      destDir: Path,
      srcPath: Path,
      replication: Short,
      symlinkCache: Map[URI, Path],
      force: Boolean = false,
      destName: Option[String] = None): Path = {
    val destFs = destDir.getFileSystem(hadoopConf)
    val srcFs = srcPath.getFileSystem(hadoopConf)
    var destPath = srcPath
    if (force || !compareFs(srcFs, destFs) || "file".equals(srcFs.getScheme)) {
      destPath = new Path(destDir, destName.getOrElse(srcPath.getName()))
      logInfo(s"Uploading resource $srcPath -> $destPath")
      FileUtil.copy(srcFs, srcPath, destFs, destPath, false, hadoopConf)
      destFs.setReplication(destPath, replication)
      destFs.setPermission(destPath, new FsPermission(APP_FILE_PERMISSION))
    } else {
      logInfo(s"Source and destination file systems are the same. Not copying $srcPath")
    }
    // Resolve any symlinks in the URI path so using a "current" symlink to point to a specific
    // version shows the specific version in the distributed cache configuration
    val qualifiedDestPath = destFs.makeQualified(destPath)
    val qualifiedDestDir = qualifiedDestPath.getParent
    val resolvedDestDir = symlinkCache.getOrElseUpdate(qualifiedDestDir.toUri(), {
      val fc = FileContext.getFileContext(qualifiedDestDir.toUri(), hadoopConf)
      fc.resolvePath(qualifiedDestDir)
    })
    new Path(resolvedDestDir, qualifiedDestPath.getName())
  }

  /**
   * Upload any resources to the distributed cache if needed. If a resource is intended to be
   * consumed locally, set up the appropriate config for downstream code to handle it properly.
   * This is used for setting up a container launch context for our ApplicationMaster.
   * Exposed for testing.
   */
  def prepareLocalResources(
      destDir: Path,
      pySparkArchives: Seq[String]): HashMap[String, LocalResource] = {
    logInfo("Preparing resources for our AM container")
    // Upload Spark and the application JAR to the remote file system if necessary,
    // and add them as local resources to the application master.
    val fs = destDir.getFileSystem(hadoopConf)

    // Used to keep track of URIs added to the distributed cache. If the same URI is added
    // multiple times, YARN will fail to launch containers for the app with an internal
    // error.
    val distributedUris = new HashSet[String]
    // Used to keep track of URIs(files) added to the distribute cache have the same name. If
    // same name but different path files are added multiple time, YARN will fail to launch
    // containers for the app with an internal error.
    val distributedNames = new HashSet[String]

    val replication = sparkConf.get(STAGING_FILE_REPLICATION).map(_.toShort)
      .getOrElse(fs.getDefaultReplication(destDir))
    val localResources = HashMap[String, LocalResource]()
    FileSystem.mkdirs(fs, destDir, new FsPermission(STAGING_DIR_PERMISSION))

    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()
    val symlinkCache: Map[URI, Path] = HashMap[URI, Path]()

    def addDistributedUri(uri: URI): Boolean = {
      val uriStr = uri.toString()
      val fileName = new File(uri.getPath).getName
      if (distributedUris.contains(uriStr)) {
        logWarning(s"Same path resource $uri added multiple times to distributed cache.")
        false
      } else if (distributedNames.contains(fileName)) {
        logWarning(s"Same name resource $uri added multiple times to distributed cache")
        false
      } else {
        distributedUris += uriStr
        distributedNames += fileName
        true
      }
    }

    /*
     * Distribute a file to the cluster.
     *
     * If the file's path is a "local:" URI, it's actually not distributed. Other files are copied
     * to HDFS (if not already there) and added to the application's distributed cache.
     *
     * @param path URI of the file to distribute.
     * @param resType Type of resource being distributed.
     * @param destName Name of the file in the distributed cache.
     * @param targetDir Subdirectory where to place the file.
     * @param appMasterOnly Whether to distribute only to the AM.
     * @return A 2-tuple. First item is whether the file is a "local:" URI. Second item is the
     *         localized path for non-local paths, or the input `path` for local paths.
     *         The localized path will be null if the URI has already been added to the cache.
     */
    def distribute(
        path: String,
        resType: LocalResourceType = LocalResourceType.FILE,
        destName: Option[String] = None,
        targetDir: Option[String] = None,
        appMasterOnly: Boolean = false): (Boolean, String) = {
      val trimmedPath = path.trim()
      val localURI = Utils.resolveURI(trimmedPath)
      if (localURI.getScheme != Utils.LOCAL_SCHEME) {
        if (addDistributedUri(localURI)) {
          val localPath = getQualifiedLocalPath(localURI, hadoopConf)
          val linkname = targetDir.map(_ + "/").getOrElse("") +
            destName.orElse(Option(localURI.getFragment())).getOrElse(localPath.getName())
          val destPath = copyFileToRemote(destDir, localPath, replication, symlinkCache)
          val destFs = FileSystem.get(destPath.toUri(), hadoopConf)
          distCacheMgr.addResource(
            destFs, hadoopConf, destPath, localResources, resType, linkname, statCache,
            appMasterOnly = appMasterOnly)
          (false, linkname)
        } else {
          (false, null)
        }
      } else {
        (true, trimmedPath)
      }
    }

    // If we passed in a keytab, make sure we copy the keytab to the staging directory on
    // HDFS, and setup the relevant environment vars, so the AM can login again.
    amKeytabFileName.foreach { kt =>
      logInfo("To enable the AM to login from keytab, credentials are being copied over to the AM" +
        " via the YARN Secure Distributed Cache.")
      val (_, localizedPath) = distribute(keytab,
        destName = Some(kt),
        appMasterOnly = true)
      require(localizedPath != null, "Keytab file already distributed.")
    }

    /**
     * Add Spark to the cache. There are two settings that control what files to add to the cache:
     * - if a Spark archive is defined, use the archive. The archive is expected to contain
     *   jar files at its root directory.
     * - if a list of jars is provided, filter the non-local ones, resolve globs, and
     *   add the found files to the cache.
     *
     * Note that the archive cannot be a "local" URI. If none of the above settings are found,
     * then upload all files found in $SPARK_HOME/jars.
     */
    val sparkArchive = sparkConf.get(SPARK_ARCHIVE)
    if (sparkArchive.isDefined) {
      val archive = sparkArchive.get
      require(!Utils.isLocalUri(archive), s"${SPARK_ARCHIVE.key} cannot be a local URI.")
      distribute(Utils.resolveURI(archive).toString,
        resType = LocalResourceType.ARCHIVE,
        destName = Some(LOCALIZED_LIB_DIR))
    } else {
      sparkConf.get(SPARK_JARS) match {
        case Some(jars) =>
          // Break the list of jars to upload, and resolve globs.
          val localJars = new ArrayBuffer[String]()
          jars.foreach { jar =>
            if (!Utils.isLocalUri(jar)) {
              val path = getQualifiedLocalPath(Utils.resolveURI(jar), hadoopConf)
              val pathFs = FileSystem.get(path.toUri(), hadoopConf)
              pathFs.globStatus(path).filter(_.isFile()).foreach { entry =>
                val uri = entry.getPath().toUri()
                statCache.update(uri, entry)
                distribute(uri.toString(), targetDir = Some(LOCALIZED_LIB_DIR))
              }
            } else {
              localJars += jar
            }
          }

          // Propagate the local URIs to the containers using the configuration.
          sparkConf.set(SPARK_JARS, localJars)

        case None =>
          // No configuration, so fall back to uploading local jar files.
          logWarning(s"Neither ${SPARK_JARS.key} nor ${SPARK_ARCHIVE.key} is set, falling back " +
            "to uploading libraries under SPARK_HOME.")
          val jarsDir = new File(YarnCommandBuilderUtils.findJarsDir(
            sparkConf.getenv("SPARK_HOME")))
          val jarsArchive = File.createTempFile(LOCALIZED_LIB_DIR, ".zip",
            new File(Utils.getLocalDir(sparkConf)))
          val jarsStream = new ZipOutputStream(new FileOutputStream(jarsArchive))

          try {
            jarsStream.setLevel(0)
            jarsDir.listFiles().foreach { f =>
              if (f.isFile && f.getName.toLowerCase(Locale.ROOT).endsWith(".jar") && f.canRead) {
                jarsStream.putNextEntry(new ZipEntry(f.getName))
                Files.copy(f, jarsStream)
                jarsStream.closeEntry()
              }
            }
          } finally {
            jarsStream.close()
          }

          distribute(jarsArchive.toURI.getPath,
            resType = LocalResourceType.ARCHIVE,
            destName = Some(LOCALIZED_LIB_DIR))
          jarsArchive.delete()
      }
    }

    /**
     * Copy user jar to the distributed cache if their scheme is not "local".
     * Otherwise, set the corresponding key in our SparkConf to handle it downstream.
     */
    Option(args.userJar).filter(_.trim.nonEmpty).foreach { jar =>
      val (isLocal, localizedPath) = distribute(jar, destName = Some(APP_JAR_NAME))
      if (isLocal) {
        require(localizedPath != null, s"Path $jar already distributed")
        // If the resource is intended for local use only, handle this downstream
        // by setting the appropriate property
        sparkConf.set(APP_JAR, localizedPath)
      }
    }

    /**
     * Do the same for any additional resources passed in through ClientArguments.
     * Each resource category is represented by a 3-tuple of:
     *   (1) comma separated list of resources in this category,
     *   (2) resource type, and
     *   (3) whether to add these resources to the classpath
     */
    val cachedSecondaryJarLinks = ListBuffer.empty[String]
    List(
      (sparkConf.get(JARS_TO_DISTRIBUTE), LocalResourceType.FILE, true),
      (sparkConf.get(FILES_TO_DISTRIBUTE), LocalResourceType.FILE, false),
      (sparkConf.get(ARCHIVES_TO_DISTRIBUTE), LocalResourceType.ARCHIVE, false)
    ).foreach { case (flist, resType, addToClasspath) =>
      flist.foreach { file =>
        val (_, localizedPath) = distribute(file, resType = resType)
        // If addToClassPath, we ignore adding jar multiple times to distributed cache.
        if (addToClasspath) {
          if (localizedPath != null) {
            cachedSecondaryJarLinks += localizedPath
          }
        } else {
          if (localizedPath == null) {
            throw new IllegalArgumentException(s"Attempt to add ($file) multiple times" +
              " to the distributed cache.")
          }
        }
      }
    }
    if (cachedSecondaryJarLinks.nonEmpty) {
      sparkConf.set(SECONDARY_JARS, cachedSecondaryJarLinks)
    }

    if (isClusterMode && args.primaryPyFile != null) {
      distribute(args.primaryPyFile, appMasterOnly = true)
    }

    pySparkArchives.foreach { f => distribute(f) }

    // The python files list needs to be treated especially. All files that are not an
    // archive need to be placed in a subdirectory that will be added to PYTHONPATH.
    sparkConf.get(PY_FILES).foreach { f =>
      val targetDir = if (f.endsWith(".py")) Some(LOCALIZED_PYTHON_DIR) else None
      distribute(f, targetDir = targetDir)
    }

    // Update the configuration with all the distributed files, minus the conf archive. The
    // conf archive will be handled by the AM differently so that we avoid having to send
    // this configuration by other means. See SPARK-14602 for one reason of why this is needed.
    distCacheMgr.updateConfiguration(cachedResourcesConf)

    // Upload the conf archive to HDFS manually, and record its location in the configuration.
    // This will allow the AM to know where the conf archive is in HDFS, so that it can be
    // distributed to the containers.
    //
    // This code forces the archive to be copied, so that unit tests pass (since in that case both
    // file systems are the same and the archive wouldn't normally be copied). In most (all?)
    // deployments, the archive would be copied anyway, since it's a temp file in the local file
    // system.
    val remoteConfArchivePath = new Path(destDir, LOCALIZED_CONF_ARCHIVE)
    val remoteFs = FileSystem.get(remoteConfArchivePath.toUri(), hadoopConf)
    cachedResourcesConf.set(CACHED_CONF_ARCHIVE, remoteConfArchivePath.toString())

    val localConfArchive = new Path(createConfArchive().toURI())
    copyFileToRemote(destDir, localConfArchive, replication, symlinkCache, force = true,
      destName = Some(LOCALIZED_CONF_ARCHIVE))

    // Manually add the config archive to the cache manager so that the AM is launched with
    // the proper files set up.
    distCacheMgr.addResource(
      remoteFs, hadoopConf, remoteConfArchivePath, localResources, LocalResourceType.ARCHIVE,
      LOCALIZED_CONF_DIR, statCache, appMasterOnly = false)

    localResources
  }

  /**
   * Create an archive with the config files for distribution.
   *
   * These will be used by AM and executors. The files are zipped and added to the job as an
   * archive, so that YARN will explode it when distributing to AM and executors. This directory
   * is then added to the classpath of AM and executor process, just to make sure that everybody
   * is using the same default config.
   *
   * This follows the order of precedence set by the startup scripts, in which HADOOP_CONF_DIR
   * shows up in the classpath before YARN_CONF_DIR.
   *
   * Currently this makes a shallow copy of the conf directory. If there are cases where a
   * Hadoop config directory contains subdirectories, this code will have to be fixed.
   *
   * The archive also contains some Spark configuration. Namely, it saves the contents of
   * SparkConf in a file to be loaded by the AM process.
   */
  private def createConfArchive(): File = {
    val hadoopConfFiles = new HashMap[String, File]()

    // SPARK_CONF_DIR shows up in the classpath before HADOOP_CONF_DIR/YARN_CONF_DIR
    sys.env.get("SPARK_CONF_DIR").foreach { localConfDir =>
      val dir = new File(localConfDir)
      if (dir.isDirectory) {
        val files = dir.listFiles(new FileFilter {
          override def accept(pathname: File): Boolean = {
            pathname.isFile && pathname.getName.endsWith(".xml")
          }
        })
        files.foreach { f => hadoopConfFiles(f.getName) = f }
      }
    }

    // SPARK-23630: during testing, Spark scripts filter out hadoop conf dirs so that user's
    // environments do not interfere with tests. This allows a special env variable during
    // tests so that custom conf dirs can be used by unit tests.
    val confDirs = Seq("HADOOP_CONF_DIR", "YARN_CONF_DIR") ++
      (if (Utils.isTesting) Seq("SPARK_TEST_HADOOP_CONF_DIR") else Nil)

    confDirs.foreach { envKey =>
      sys.env.get(envKey).foreach { path =>
        val dir = new File(path)
        if (dir.isDirectory()) {
          val files = dir.listFiles()
          if (files == null) {
            logWarning("Failed to list files under directory " + dir)
          } else {
            files.foreach { file =>
              if (file.isFile && !hadoopConfFiles.contains(file.getName())) {
                hadoopConfFiles(file.getName()) = file
              }
            }
          }
        }
      }
    }

    val confArchive = File.createTempFile(LOCALIZED_CONF_DIR, ".zip",
      new File(Utils.getLocalDir(sparkConf)))
    val confStream = new ZipOutputStream(new FileOutputStream(confArchive))

    logDebug(s"Creating an archive with the config files for distribution at $confArchive.")
    try {
      confStream.setLevel(0)

      // Upload $SPARK_CONF_DIR/log4j.properties file to the distributed cache to make sure that
      // the executors will use the latest configurations instead of the default values. This is
      // required when user changes log4j.properties directly to set the log configurations. If
      // configuration file is provided through --files then executors will be taking configurations
      // from --files instead of $SPARK_CONF_DIR/log4j.properties.

      // Also upload metrics.properties to distributed cache if exists in classpath.
      // If user specify this file using --files then executors will use the one
      // from --files instead.
      for { prop <- Seq("log4j.properties", "metrics.properties")
            url <- Option(Utils.getContextOrSparkClassLoader.getResource(prop))
            if url.getProtocol == "file" } {
        val file = new File(url.getPath())
        confStream.putNextEntry(new ZipEntry(file.getName()))
        Files.copy(file, confStream)
        confStream.closeEntry()
      }

      // Save the Hadoop config files under a separate directory in the archive. This directory
      // is appended to the classpath so that the cluster-provided configuration takes precedence.
      confStream.putNextEntry(new ZipEntry(s"$LOCALIZED_HADOOP_CONF_DIR/"))
      confStream.closeEntry()
      hadoopConfFiles.foreach { case (name, file) =>
        if (file.canRead()) {
          confStream.putNextEntry(new ZipEntry(s"$LOCALIZED_HADOOP_CONF_DIR/$name"))
          Files.copy(file, confStream)
          confStream.closeEntry()
        }
      }

      // Save the YARN configuration into a separate file that will be overlayed on top of the
      // cluster's Hadoop conf.
      confStream.putNextEntry(new ZipEntry(SparkHadoopUtil.SPARK_HADOOP_CONF_FILE))
      hadoopConf.writeXml(confStream)
      confStream.closeEntry()

      // Save Spark configuration to a file in the archive.
      val props = confToProperties(sparkConf)

      // If propagating the keytab to the AM, override the keytab name with the name of the
      // distributed file. Otherwise remove princpal/keytab from the conf, so they're not seen
      // by the AM at all.
      amKeytabFileName match {
        case Some(kt) =>
          props.setProperty(KEYTAB.key, kt)
        case None =>
          props.remove(PRINCIPAL.key)
          props.remove(KEYTAB.key)
      }

      writePropertiesToArchive(props, SPARK_CONF_FILE, confStream)

      // Write the distributed cache config to the archive.
      writePropertiesToArchive(confToProperties(cachedResourcesConf), DIST_CACHE_CONF_FILE,
        confStream)
    } finally {
      confStream.close()
    }
    confArchive
  }

  /**
   * Set up the environment for launching our ApplicationMaster container.
   */
  private def setupLaunchEnv(
      stagingDirPath: Path,
      pySparkArchives: Seq[String]): HashMap[String, String] = {
    logInfo("Setting up the launch environment for our AM container")
    val env = new HashMap[String, String]()
    populateClasspath(args, hadoopConf, sparkConf, env, sparkConf.get(DRIVER_CLASS_PATH))
    env("SPARK_YARN_STAGING_DIR") = stagingDirPath.toString
    env("SPARK_USER") = UserGroupInformation.getCurrentUser().getShortUserName()

    // Pick up any environment variables for the AM provided through spark.yarn.appMasterEnv.*
    val amEnvPrefix = "spark.yarn.appMasterEnv."
    sparkConf.getAll
      .filter { case (k, v) => k.startsWith(amEnvPrefix) }
      .map { case (k, v) => (k.substring(amEnvPrefix.length), v) }
      .foreach { case (k, v) => YarnSparkHadoopUtil.addPathToEnvironment(env, k, v) }

    // If pyFiles contains any .py files, we need to add LOCALIZED_PYTHON_DIR to the PYTHONPATH
    // of the container processes too. Add all non-.py files directly to PYTHONPATH.
    //
    // NOTE: the code currently does not handle .py files defined with a "local:" scheme.
    val pythonPath = new ListBuffer[String]()
    val (pyFiles, pyArchives) = sparkConf.get(PY_FILES).partition(_.endsWith(".py"))
    if (pyFiles.nonEmpty) {
      pythonPath += buildPath(Environment.PWD.$$(), LOCALIZED_PYTHON_DIR)
    }
    (pySparkArchives ++ pyArchives).foreach { path =>
      val uri = Utils.resolveURI(path)
      if (uri.getScheme != Utils.LOCAL_SCHEME) {
        pythonPath += buildPath(Environment.PWD.$$(), new Path(uri).getName())
      } else {
        pythonPath += uri.getPath()
      }
    }

    // Finally, update the Spark config to propagate PYTHONPATH to the AM and executors.
    if (pythonPath.nonEmpty) {
      val pythonPathList = (sys.env.get("PYTHONPATH") ++ pythonPath)
      env("PYTHONPATH") = (env.get("PYTHONPATH") ++ pythonPathList)
        .mkString(ApplicationConstants.CLASS_PATH_SEPARATOR)
      val pythonPathExecutorEnv = (sparkConf.getExecutorEnv.toMap.get("PYTHONPATH") ++
        pythonPathList).mkString(ApplicationConstants.CLASS_PATH_SEPARATOR)
      sparkConf.setExecutorEnv("PYTHONPATH", pythonPathExecutorEnv)
    }

    if (isClusterMode) {
      // propagate PYSPARK_DRIVER_PYTHON and PYSPARK_PYTHON to driver in cluster mode
      Seq("PYSPARK_DRIVER_PYTHON", "PYSPARK_PYTHON").foreach { envname =>
        if (!env.contains(envname)) {
          sys.env.get(envname).foreach(env(envname) = _)
        }
      }
      sys.env.get("PYTHONHASHSEED").foreach(env.put("PYTHONHASHSEED", _))
    }

    sys.env.get(ENV_DIST_CLASSPATH).foreach { dcp =>
      env(ENV_DIST_CLASSPATH) = dcp
    }

    env
  }

  /**
   * Set up a ContainerLaunchContext to launch our ApplicationMaster container.
   * This sets up the launch environment, java options, and the command for launching the AM.
   */
  private def createContainerLaunchContext(newAppResponse: GetNewApplicationResponse)
    : ContainerLaunchContext = {
    logInfo("Setting up container launch context for our AM")
    val appId = newAppResponse.getApplicationId
    val pySparkArchives =
      if (sparkConf.get(IS_PYTHON_APP)) {
        findPySparkArchives()
      } else {
        Nil
      }

    val launchEnv = setupLaunchEnv(stagingDirPath, pySparkArchives)
    val localResources = prepareLocalResources(stagingDirPath, pySparkArchives)

    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources.asJava)
    amContainer.setEnvironment(launchEnv.asJava)

    val javaOpts = ListBuffer[String]()

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    var prefixEnv: Option[String] = None

    // Add Xmx for AM memory
    javaOpts += "-Xmx" + amMemory + "m"

    val tmpDir = new Path(Environment.PWD.$$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR)
    javaOpts += "-Djava.io.tmpdir=" + tmpDir

    // TODO: Remove once cpuset version is pushed out.
    // The context is, default gc for server class machines ends up using all cores to do gc -
    // hence if there are multiple containers in same node, Spark GC affects all other containers'
    // performance (which can be that of other Spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce "proper" Spark behavior in
    // multi-tenant environments. Not sure how default Java GC behaves if it is limited to subset
    // of cores on a node.
    val useConcurrentAndIncrementalGC = launchEnv.get("SPARK_USE_CONC_INCR_GC").exists(_.toBoolean)
    if (useConcurrentAndIncrementalGC) {
      // In our expts, using (default) throughput collector has severe perf ramifications in
      // multi-tenant machines
      javaOpts += "-XX:+UseConcMarkSweepGC"
      javaOpts += "-XX:MaxTenuringThreshold=31"
      javaOpts += "-XX:SurvivorRatio=8"
      javaOpts += "-XX:+CMSIncrementalMode"
      javaOpts += "-XX:+CMSIncrementalPacing"
      javaOpts += "-XX:CMSIncrementalDutyCycleMin=0"
      javaOpts += "-XX:CMSIncrementalDutyCycle=10"
    }

    // Include driver-specific java options if we are launching a driver
    if (isClusterMode) {
      sparkConf.get(DRIVER_JAVA_OPTIONS).foreach { opts =>
        javaOpts ++= Utils.splitCommandString(opts)
          .map(Utils.substituteAppId(_, appId.toString))
          .map(YarnSparkHadoopUtil.escapeForShell)
      }
      val libraryPaths = Seq(sparkConf.get(DRIVER_LIBRARY_PATH),
        sys.props.get("spark.driver.libraryPath")).flatten
      if (libraryPaths.nonEmpty) {
        prefixEnv = Some(createLibraryPathPrefix(libraryPaths.mkString(File.pathSeparator),
          sparkConf))
      }
      if (sparkConf.get(AM_JAVA_OPTIONS).isDefined) {
        logWarning(s"${AM_JAVA_OPTIONS.key} will not take effect in cluster mode")
      }
    } else {
      // Validate and include yarn am specific java options in yarn-client mode.
      sparkConf.get(AM_JAVA_OPTIONS).foreach { opts =>
        if (opts.contains("-Dspark")) {
          val msg = s"${AM_JAVA_OPTIONS.key} is not allowed to set Spark options (was '$opts')."
          throw new SparkException(msg)
        }
        if (opts.contains("-Xmx")) {
          val msg = s"${AM_JAVA_OPTIONS.key} is not allowed to specify max heap memory settings " +
            s"(was '$opts'). Use spark.yarn.am.memory instead."
          throw new SparkException(msg)
        }
        javaOpts ++= Utils.splitCommandString(opts)
          .map(Utils.substituteAppId(_, appId.toString))
          .map(YarnSparkHadoopUtil.escapeForShell)
      }
      sparkConf.get(AM_LIBRARY_PATH).foreach { paths =>
        prefixEnv = Some(createLibraryPathPrefix(paths, sparkConf))
      }
    }

    // For log4j configuration to reference
    javaOpts += ("-Dspark.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)

    val userClass =
      if (isClusterMode) {
        Seq("--class", YarnSparkHadoopUtil.escapeForShell(args.userClass))
      } else {
        Nil
      }
    val userJar =
      if (args.userJar != null) {
        Seq("--jar", args.userJar)
      } else {
        Nil
      }
    val primaryPyFile =
      if (isClusterMode && args.primaryPyFile != null) {
        Seq("--primary-py-file", new Path(args.primaryPyFile).getName())
      } else {
        Nil
      }
    val primaryRFile =
      if (args.primaryRFile != null) {
        Seq("--primary-r-file", args.primaryRFile)
      } else {
        Nil
      }
    val amClass =
      if (isClusterMode) {
        Utils.classForName("org.apache.spark.deploy.yarn.ApplicationMaster").getName
      } else {
        Utils.classForName("org.apache.spark.deploy.yarn.ExecutorLauncher").getName
      }
    if (args.primaryRFile != null && args.primaryRFile.endsWith(".R")) {
      args.userArgs = ArrayBuffer(args.primaryRFile) ++ args.userArgs
    }
    val userArgs = args.userArgs.flatMap { arg =>
      Seq("--arg", YarnSparkHadoopUtil.escapeForShell(arg))
    }
    val amArgs =
      Seq(amClass) ++ userClass ++ userJar ++ primaryPyFile ++ primaryRFile ++ userArgs ++
      Seq("--properties-file",
        buildPath(Environment.PWD.$$(), LOCALIZED_CONF_DIR, SPARK_CONF_FILE)) ++
      Seq("--dist-cache-conf",
        buildPath(Environment.PWD.$$(), LOCALIZED_CONF_DIR, DIST_CACHE_CONF_FILE))

    // Command for the ApplicationMaster
    val commands = prefixEnv ++
      Seq(Environment.JAVA_HOME.$$() + "/bin/java", "-server") ++
      javaOpts ++ amArgs ++
      Seq(
        "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    // TODO: it would be nicer to just make sure there are no null commands here
    val printableCommands = commands.map(s => if (s == null) "null" else s).toList
    amContainer.setCommands(printableCommands.asJava)

    logDebug("===============================================================================")
    logDebug("YARN AM launch context:")
    logDebug(s"    user class: ${Option(args.userClass).getOrElse("N/A")}")
    logDebug("    env:")
    if (log.isDebugEnabled) {
      Utils.redact(sparkConf, launchEnv.toSeq).foreach { case (k, v) =>
        logDebug(s"        $k -> $v")
      }
    }
    logDebug("    resources:")
    localResources.foreach { case (k, v) => logDebug(s"        $k -> $v")}
    logDebug("    command:")
    logDebug(s"        ${printableCommands.mkString(" ")}")
    logDebug("===============================================================================")

    // send the acl settings into YARN to control who has access via YARN interfaces
    val securityManager = new SecurityManager(sparkConf)
    amContainer.setApplicationACLs(
      YarnSparkHadoopUtil.getApplicationAclsForYarn(securityManager).asJava)
    setupSecurityToken(amContainer)
    amContainer
  }

  /**
   * Report the state of an application until it has exited, either successfully or
   * due to some failure, then return a pair of the yarn application state (FINISHED, FAILED,
   * KILLED, or RUNNING) and the final application state (UNDEFINED, SUCCEEDED, FAILED,
   * or KILLED).
   *
   * @param appId ID of the application to monitor.
   * @param returnOnRunning Whether to also return the application state when it is RUNNING.
   * @param logApplicationReport Whether to log details of the application report every iteration.
   * @param interval How often to poll the YARN RM for application status (in ms).
   * @return A pair of the yarn application state and the final application state.
   */
  def monitorApplication(
      appId: ApplicationId,
      returnOnRunning: Boolean = false,
      logApplicationReport: Boolean = true,
      interval: Long = sparkConf.get(REPORT_INTERVAL)): YarnAppReport = {
    var lastState: YarnApplicationState = null
    while (true) {
      Thread.sleep(interval)
      val report: ApplicationReport =
        try {
          getApplicationReport(appId)
        } catch {
          case e: ApplicationNotFoundException =>
            logError(s"Application $appId not found.")
            cleanupStagingDir()
            return YarnAppReport(YarnApplicationState.KILLED, FinalApplicationStatus.KILLED, None)
          case NonFatal(e) =>
            val msg = s"Failed to contact YARN for application $appId."
            logError(msg, e)
            // Don't necessarily clean up staging dir because status is unknown
            return YarnAppReport(YarnApplicationState.FAILED, FinalApplicationStatus.FAILED,
              Some(msg))
        }
      val state = report.getYarnApplicationState

      if (logApplicationReport) {
        logInfo(s"Application report for $appId (state: $state)")

        // If DEBUG is enabled, log report details every iteration
        // Otherwise, log them every time the application changes state
        if (log.isDebugEnabled) {
          logDebug(formatReportDetails(report))
        } else if (lastState != state) {
          logInfo(formatReportDetails(report))
        }
      }

      if (lastState != state) {
        state match {
          case YarnApplicationState.RUNNING =>
            reportLauncherState(SparkAppHandle.State.RUNNING)
          case YarnApplicationState.FINISHED =>
            report.getFinalApplicationStatus match {
              case FinalApplicationStatus.FAILED =>
                reportLauncherState(SparkAppHandle.State.FAILED)
              case FinalApplicationStatus.KILLED =>
                reportLauncherState(SparkAppHandle.State.KILLED)
              case _ =>
                reportLauncherState(SparkAppHandle.State.FINISHED)
            }
          case YarnApplicationState.FAILED =>
            reportLauncherState(SparkAppHandle.State.FAILED)
          case YarnApplicationState.KILLED =>
            reportLauncherState(SparkAppHandle.State.KILLED)
          case _ =>
        }
      }

      if (state == YarnApplicationState.FINISHED ||
          state == YarnApplicationState.FAILED ||
          state == YarnApplicationState.KILLED) {
        cleanupStagingDir()
        return createAppReport(report)
      }

      if (returnOnRunning && state == YarnApplicationState.RUNNING) {
        return createAppReport(report)
      }
      if (state == YarnApplicationState.ACCEPTED && isClientUnmanagedAMEnabled &&
          appMaster == null && report.getAMRMToken != null) {
        appMaster = startApplicationMasterService(report)
      }
      lastState = state
    }

    // Never reached, but keeps compiler happy
    throw new SparkException("While loop is depleted! This should never happen...")
  }

  private def startApplicationMasterService(report: ApplicationReport): ApplicationMaster = {
    // Add AMRMToken to establish connection between RM and AM
    val token = report.getAMRMToken
    val amRMToken: org.apache.hadoop.security.token.Token[AMRMTokenIdentifier] =
      new org.apache.hadoop.security.token.Token[AMRMTokenIdentifier](
        token.getIdentifier().array(), token.getPassword().array,
        new Text(token.getKind()), new Text(token.getService()))
    val currentUGI = UserGroupInformation.getCurrentUser
    currentUGI.addToken(amRMToken)

    // Start Application Service in a separate thread and continue with application monitoring
    val appMaster = new ApplicationMaster(
      new ApplicationMasterArguments(Array.empty), sparkConf, hadoopConf)
    val amService = new Thread("Unmanaged Application Master Service") {
      override def run(): Unit = {
        appMaster.runUnmanaged(rpcEnv, report.getCurrentApplicationAttemptId,
          stagingDirPath, cachedResourcesConf)
      }
    }
    amService.setDaemon(true)
    amService.start()
    appMaster
  }

  private def formatReportDetails(report: ApplicationReport): String = {
    val details = Seq[(String, String)](
      ("client token", getClientToken(report)),
      ("diagnostics", report.getDiagnostics),
      ("ApplicationMaster host", report.getHost),
      ("ApplicationMaster RPC port", report.getRpcPort.toString),
      ("queue", report.getQueue),
      ("start time", report.getStartTime.toString),
      ("final status", report.getFinalApplicationStatus.toString),
      ("tracking URL", report.getTrackingUrl),
      ("user", report.getUser)
    )

    // Use more loggable format if value is null or empty
    details.map { case (k, v) =>
      val newValue = Option(v).filter(_.nonEmpty).getOrElse("N/A")
      s"\n\t $k: $newValue"
    }.mkString("")
  }

  /**
   * Submit an application to the ResourceManager.
   * If set spark.yarn.submit.waitAppCompletion to true, it will stay alive
   * reporting the application's status until the application has exited for any reason.
   * Otherwise, the client process will exit after submission.
   * If the application finishes with a failed, killed, or undefined status,
   * throw an appropriate SparkException.
   */
  def run(): Unit = {
    this.appId = submitApplication()
    if (!launcherBackend.isConnected() && fireAndForget) {
      val report = getApplicationReport(appId)
      val state = report.getYarnApplicationState
      logInfo(s"Application report for $appId (state: $state)")
      logInfo(formatReportDetails(report))
      if (state == YarnApplicationState.FAILED || state == YarnApplicationState.KILLED) {
        throw new SparkException(s"Application $appId finished with status: $state")
      }
    } else {
      val YarnAppReport(appState, finalState, diags) = monitorApplication(appId)
      if (appState == YarnApplicationState.FAILED || finalState == FinalApplicationStatus.FAILED) {
        diags.foreach { err =>
          logError(s"Application diagnostics message: $err")
        }
        throw new SparkException(s"Application $appId finished with failed status")
      }
      if (appState == YarnApplicationState.KILLED || finalState == FinalApplicationStatus.KILLED) {
        throw new SparkException(s"Application $appId is killed")
      }
      if (finalState == FinalApplicationStatus.UNDEFINED) {
        throw new SparkException(s"The final status of application $appId is undefined")
      }
    }
  }

  private def findPySparkArchives(): Seq[String] = {
    sys.env.get("PYSPARK_ARCHIVES_PATH")
      .map(_.split(",").toSeq)
      .getOrElse {
        val pyLibPath = Seq(sys.env("SPARK_HOME"), "python", "lib").mkString(File.separator)
        val pyArchivesFile = new File(pyLibPath, "pyspark.zip")
        require(pyArchivesFile.exists(),
          s"$pyArchivesFile not found; cannot run pyspark application in YARN mode.")
        val py4jFile = new File(pyLibPath, "py4j-0.10.8.1-src.zip")
        require(py4jFile.exists(),
          s"$py4jFile not found; cannot run pyspark application in YARN mode.")
        Seq(pyArchivesFile.getAbsolutePath(), py4jFile.getAbsolutePath())
      }
  }

}

private object Client extends Logging {

  // Alias for the user jar
  val APP_JAR_NAME: String = "__app__.jar"

  // Staging directory for any temporary jars or files
  val SPARK_STAGING: String = ".sparkStaging"


  // Staging directory is private! -> rwx--------
  val STAGING_DIR_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("700", 8).toShort)

  // App files are world-wide readable and owner writable -> rw-r--r--
  val APP_FILE_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("644", 8).toShort)

  // Distribution-defined classpath to add to processes
  val ENV_DIST_CLASSPATH = "SPARK_DIST_CLASSPATH"

  // Subdirectory where the user's Spark and Hadoop config files will be placed.
  val LOCALIZED_CONF_DIR = "__spark_conf__"

  // Subdirectory in the conf directory containing Hadoop config files.
  val LOCALIZED_HADOOP_CONF_DIR = "__hadoop_conf__"

  // File containing the conf archive in the AM. See prepareLocalResources().
  val LOCALIZED_CONF_ARCHIVE = LOCALIZED_CONF_DIR + ".zip"

  // Name of the file in the conf archive containing Spark configuration.
  val SPARK_CONF_FILE = "__spark_conf__.properties"

  // Name of the file in the conf archive containing the distributed cache info.
  val DIST_CACHE_CONF_FILE = "__spark_dist_cache__.properties"

  // Subdirectory where the user's python files (not archives) will be placed.
  val LOCALIZED_PYTHON_DIR = "__pyfiles__"

  // Subdirectory where Spark libraries will be placed.
  val LOCALIZED_LIB_DIR = "__spark_libs__"

  /**
   * Return the path to the given application's staging directory.
   */
  private def getAppStagingDir(appId: ApplicationId): String = {
    buildPath(SPARK_STAGING, appId.toString())
  }

  /**
   * Populate the classpath entry in the given environment map with any application
   * classpath specified through the Hadoop and Yarn configurations.
   */
  private[yarn] def populateHadoopClasspath(conf: Configuration, env: HashMap[String, String])
    : Unit = {
    val classPathElementsToAdd = getYarnAppClasspath(conf) ++ getMRAppClasspath(conf)
    classPathElementsToAdd.foreach { c =>
      YarnSparkHadoopUtil.addPathToEnvironment(env, Environment.CLASSPATH.name, c.trim)
    }
  }

  private def getYarnAppClasspath(conf: Configuration): Seq[String] =
    Option(conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) match {
      case Some(s) => s.toSeq
      case None => getDefaultYarnApplicationClasspath
    }

  private def getMRAppClasspath(conf: Configuration): Seq[String] =
    Option(conf.getStrings("mapreduce.application.classpath")) match {
      case Some(s) => s.toSeq
      case None => getDefaultMRApplicationClasspath
    }

  private[yarn] def getDefaultYarnApplicationClasspath: Seq[String] =
    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.toSeq

  private[yarn] def getDefaultMRApplicationClasspath: Seq[String] =
    StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH).toSeq

  /**
   * Populate the classpath entry in the given environment map.
   *
   * User jars are generally not added to the JVM's system classpath; those are handled by the AM
   * and executor backend. When the deprecated `spark.yarn.user.classpath.first` is used, user jars
   * are included in the system classpath, though. The extra class path and other uploaded files are
   * always made available through the system class path.
   *
   * @param args Client arguments (when starting the AM) or null (when starting executors).
   */
  private[yarn] def populateClasspath(
      args: ClientArguments,
      conf: Configuration,
      sparkConf: SparkConf,
      env: HashMap[String, String],
      extraClassPath: Option[String] = None): Unit = {
    extraClassPath.foreach { cp =>
      addClasspathEntry(getClusterPath(sparkConf, cp), env)
    }

    addClasspathEntry(Environment.PWD.$$(), env)

    addClasspathEntry(Environment.PWD.$$() + Path.SEPARATOR + LOCALIZED_CONF_DIR, env)

    if (sparkConf.get(USER_CLASS_PATH_FIRST)) {
      // in order to properly add the app jar when user classpath is first
      // we have to do the mainJar separate in order to send the right thing
      // into addFileToClasspath
      val mainJar =
        if (args != null) {
          getMainJarUri(Option(args.userJar))
        } else {
          getMainJarUri(sparkConf.get(APP_JAR))
        }
      mainJar.foreach(addFileToClasspath(sparkConf, conf, _, APP_JAR_NAME, env))

      val secondaryJars =
        if (args != null) {
          getSecondaryJarUris(Option(sparkConf.get(JARS_TO_DISTRIBUTE)))
        } else {
          getSecondaryJarUris(sparkConf.get(SECONDARY_JARS))
        }
      secondaryJars.foreach { x =>
        addFileToClasspath(sparkConf, conf, x, null, env)
      }
    }

    // Add the Spark jars to the classpath, depending on how they were distributed.
    addClasspathEntry(buildPath(Environment.PWD.$$(), LOCALIZED_LIB_DIR, "*"), env)
    if (sparkConf.get(SPARK_ARCHIVE).isEmpty) {
      sparkConf.get(SPARK_JARS).foreach { jars =>
        jars.filter(Utils.isLocalUri).foreach { jar =>
          val uri = new URI(jar)
          addClasspathEntry(getClusterPath(sparkConf, uri.getPath()), env)
        }
      }
    }

    populateHadoopClasspath(conf, env)
    sys.env.get(ENV_DIST_CLASSPATH).foreach { cp =>
      addClasspathEntry(getClusterPath(sparkConf, cp), env)
    }

    // Add the localized Hadoop config at the end of the classpath, in case it contains other
    // files (such as configuration files for different services) that are not part of the
    // YARN cluster's config.
    addClasspathEntry(
      buildPath(Environment.PWD.$$(), LOCALIZED_CONF_DIR, LOCALIZED_HADOOP_CONF_DIR), env)
  }

  /**
   * Returns a list of URIs representing the user classpath.
   *
   * @param conf Spark configuration.
   */
  def getUserClasspath(conf: SparkConf): Array[URI] = {
    val mainUri = getMainJarUri(conf.get(APP_JAR))
    val secondaryUris = getSecondaryJarUris(conf.get(SECONDARY_JARS))
    (mainUri ++ secondaryUris).toArray
  }

  private def getMainJarUri(mainJar: Option[String]): Option[URI] = {
    mainJar.flatMap { path =>
      val uri = Utils.resolveURI(path)
      if (uri.getScheme == Utils.LOCAL_SCHEME) Some(uri) else None
    }.orElse(Some(new URI(APP_JAR_NAME)))
  }

  private def getSecondaryJarUris(secondaryJars: Option[Seq[String]]): Seq[URI] = {
    secondaryJars.getOrElse(Nil).map(new URI(_))
  }

  /**
   * Adds the given path to the classpath, handling "local:" URIs correctly.
   *
   * If an alternate name for the file is given, and it's not a "local:" file, the alternate
   * name will be added to the classpath (relative to the job's work directory).
   *
   * If not a "local:" file and no alternate name, the linkName will be added to the classpath.
   *
   * @param conf        Spark configuration.
   * @param hadoopConf  Hadoop configuration.
   * @param uri         URI to add to classpath (optional).
   * @param fileName    Alternate name for the file (optional).
   * @param env         Map holding the environment variables.
   */
  private def addFileToClasspath(
      conf: SparkConf,
      hadoopConf: Configuration,
      uri: URI,
      fileName: String,
      env: HashMap[String, String]): Unit = {
    if (uri != null && uri.getScheme == Utils.LOCAL_SCHEME) {
      addClasspathEntry(getClusterPath(conf, uri.getPath), env)
    } else if (fileName != null) {
      addClasspathEntry(buildPath(Environment.PWD.$$(), fileName), env)
    } else if (uri != null) {
      val localPath = getQualifiedLocalPath(uri, hadoopConf)
      val linkName = Option(uri.getFragment()).getOrElse(localPath.getName())
      addClasspathEntry(buildPath(Environment.PWD.$$(), linkName), env)
    }
  }

  /**
   * Add the given path to the classpath entry of the given environment map.
   * If the classpath is already set, this appends the new path to the existing classpath.
   */
  private def addClasspathEntry(path: String, env: HashMap[String, String]): Unit =
    YarnSparkHadoopUtil.addPathToEnvironment(env, Environment.CLASSPATH.name, path)

  /**
   * Returns the path to be sent to the NM for a path that is valid on the gateway.
   *
   * This method uses two configuration values:
   *
   *  - spark.yarn.config.gatewayPath: a string that identifies a portion of the input path that may
   *    only be valid in the gateway node.
   *  - spark.yarn.config.replacementPath: a string with which to replace the gateway path. This may
   *    contain, for example, env variable references, which will be expanded by the NMs when
   *    starting containers.
   *
   * If either config is not available, the input path is returned.
   */
  def getClusterPath(conf: SparkConf, path: String): String = {
    val localPath = conf.get(GATEWAY_ROOT_PATH)
    val clusterPath = conf.get(REPLACEMENT_ROOT_PATH)
    if (localPath != null && clusterPath != null) {
      path.replace(localPath, clusterPath)
    } else {
      path
    }
  }

  /**
   * Return whether two URI represent file system are the same
   */
  private[spark] def compareUri(srcUri: URI, dstUri: URI): Boolean = {

    if (srcUri.getScheme() == null || srcUri.getScheme() != dstUri.getScheme()) {
      return false
    }

    val srcAuthority = srcUri.getAuthority()
    val dstAuthority = dstUri.getAuthority()
    if (srcAuthority != null && !srcAuthority.equalsIgnoreCase(dstAuthority)) {
      return false
    }

    var srcHost = srcUri.getHost()
    var dstHost = dstUri.getHost()

    // In HA or when using viewfs, the host part of the URI may not actually be a host, but the
    // name of the HDFS namespace. Those names won't resolve, so avoid even trying if they
    // match.
    if (srcHost != null && dstHost != null && srcHost != dstHost) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName()
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName()
      } catch {
        case e: UnknownHostException =>
          return false
      }
    }

    Objects.equal(srcHost, dstHost) && srcUri.getPort() == dstUri.getPort()

  }

  /**
   * Return whether the two file systems are the same.
   */
  protected def compareFs(srcFs: FileSystem, destFs: FileSystem): Boolean = {
    val srcUri = srcFs.getUri()
    val dstUri = destFs.getUri()

    compareUri(srcUri, dstUri)
  }

  /**
   * Given a local URI, resolve it and return a qualified local path that corresponds to the URI.
   * This is used for preparing local resources to be included in the container launch context.
   */
  private def getQualifiedLocalPath(localURI: URI, hadoopConf: Configuration): Path = {
    val qualifiedURI =
      if (localURI.getScheme == null) {
        // If not specified, assume this is in the local filesystem to keep the behavior
        // consistent with that of Hadoop
        new URI(FileSystem.getLocal(hadoopConf).makeQualified(new Path(localURI)).toString)
      } else {
        localURI
      }
    new Path(qualifiedURI)
  }

  /**
   * Whether to consider jars provided by the user to have precedence over the Spark jars when
   * loading user classes.
   */
  def isUserClassPathFirst(conf: SparkConf, isDriver: Boolean): Boolean = {
    if (isDriver) {
      conf.get(DRIVER_USER_CLASS_PATH_FIRST)
    } else {
      conf.get(EXECUTOR_USER_CLASS_PATH_FIRST)
    }
  }

  /**
   * Joins all the path components using Path.SEPARATOR.
   */
  def buildPath(components: String*): String = {
    components.mkString(Path.SEPARATOR)
  }

  def createAppReport(report: ApplicationReport): YarnAppReport = {
    val diags = report.getDiagnostics()
    val diagsOpt = if (diags != null && diags.nonEmpty) Some(diags) else None
    YarnAppReport(report.getYarnApplicationState(), report.getFinalApplicationStatus(), diagsOpt)
  }

  /**
   * Create a properly quoted and escaped library path string to be added as a prefix to the command
   * executed by YARN. This is different from normal quoting / escaping due to YARN executing the
   * command through "bash -c".
   */
  def createLibraryPathPrefix(libpath: String, conf: SparkConf): String = {
    val cmdPrefix = if (Utils.isWindows) {
      Utils.libraryPathEnvPrefix(Seq(libpath))
    } else {
      val envName = Utils.libraryPathEnvName
      // For quotes, escape both the quote and the escape character when encoding in the command
      // string.
      val quoted = libpath.replace("\"", "\\\\\\\"")
      envName + "=\\\"" + quoted + File.pathSeparator + "$" + envName + "\\\""
    }
    getClusterPath(conf, cmdPrefix)
  }

  def confToProperties(conf: SparkConf): Properties = {
    val props = new Properties()
    conf.getAll.foreach { case (k, v) =>
      props.setProperty(k, v)
    }
    props
  }

  def writePropertiesToArchive(props: Properties, name: String, out: ZipOutputStream): Unit = {
    out.putNextEntry(new ZipEntry(name))
    val writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)
    props.store(writer, "Spark configuration.")
    writer.flush()
    out.closeEntry()
  }
}

private[spark] class YarnClusterApplication extends SparkApplication {

  override def start(args: Array[String], conf: SparkConf): Unit = {
    // SparkSubmit would use yarn cache to distribute files & jars in yarn mode,
    // so remove them from sparkConf here for yarn mode.
    conf.remove(JARS)
    conf.remove(FILES)

    new Client(new ClientArguments(args), conf, null).run()
  }

}

private[spark] case class YarnAppReport(
    appState: YarnApplicationState,
    finalState: FinalApplicationStatus,
    diagnostics: Option[String])
