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

import java.io.File
import java.net.{InetAddress, UnknownHostException, URI, URISyntaxException}

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ListBuffer, Map}
import scala.util.{Try, Success, Failure}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.util.StringUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext, SparkException}

/**
 * The entry point (starting in Client#main() and Client#run()) for launching Spark on YARN. The
 * Client submits an application to the YARN ResourceManager.
 */
trait ClientBase extends Logging {
  val args: ClientArguments
  val conf: Configuration
  val sparkConf: SparkConf
  val yarnConf: YarnConfiguration
  val credentials = UserGroupInformation.getCurrentUser().getCredentials()
  private val SPARK_STAGING: String = ".sparkStaging"
  private val distCacheMgr = new ClientDistributedCacheManager()

  // Staging directory is private! -> rwx--------
  val STAGING_DIR_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("700", 8).toShort)
  // App files are world-wide readable and owner writable -> rw-r--r--
  val APP_FILE_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("644", 8).toShort)

  // Additional memory overhead - in mb.
  protected def memoryOverhead: Int = sparkConf.getInt("spark.yarn.driver.memoryOverhead",
    YarnSparkHadoopUtil.DEFAULT_MEMORY_OVERHEAD)

  // TODO(harvey): This could just go in ClientArguments.
  def validateArgs() = {
    Map(
      (args.numExecutors <= 0) -> "Error: You must specify at least 1 executor!",
      (args.amMemory <= memoryOverhead) -> ("Error: AM memory size must be" +
        "greater than: " + memoryOverhead),
      (args.executorMemory <= memoryOverhead) -> ("Error: Executor memory size" +
        "must be greater than: " + memoryOverhead.toString)
    ).foreach { case(cond, errStr) =>
      if (cond) {
        logError(errStr)
        throw new IllegalArgumentException(args.getUsageMessage())
      }
    }
  }

  def getAppStagingDir(appId: ApplicationId): String = {
    SPARK_STAGING + Path.SEPARATOR + appId.toString() + Path.SEPARATOR
  }

  def verifyClusterResources(app: GetNewApplicationResponse) = {
    val maxMem = app.getMaximumResourceCapability().getMemory()
    logInfo("Max mem capabililty of a single resource in this cluster " + maxMem)

    // If we have requested more then the clusters max for a single resource then exit.
    if (args.executorMemory > maxMem) {
      val errorMessage =
        "Required executor memory (%d MB), is above the max threshold (%d MB) of this cluster."
          .format(args.executorMemory, maxMem)

      logError(errorMessage)
      throw new IllegalArgumentException(errorMessage)
    }
    val amMem = args.amMemory + memoryOverhead
    if (amMem > maxMem) {

      val errorMessage = "Required AM memory (%d) is above the max threshold (%d) of this cluster."
        .format(amMem, maxMem)
      logError(errorMessage)
      throw new IllegalArgumentException(errorMessage)
    }

    // We could add checks to make sure the entire cluster has enough resources but that involves
    // getting all the node reports and computing ourselves.
  }

  /** See if two file systems are the same or not. */
  private def compareFs(srcFs: FileSystem, destFs: FileSystem): Boolean = {
    val srcUri = srcFs.getUri()
    val dstUri = destFs.getUri()
    if (srcUri.getScheme() == null) {
      return false
    }
    if (!srcUri.getScheme().equals(dstUri.getScheme())) {
      return false
    }
    var srcHost = srcUri.getHost()
    var dstHost = dstUri.getHost()
    if ((srcHost != null) && (dstHost != null)) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName()
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName()
      } catch {
        case e: UnknownHostException =>
          return false
      }
      if (!srcHost.equals(dstHost)) {
        return false
      }
    } else if (srcHost == null && dstHost != null) {
      return false
    } else if (srcHost != null && dstHost == null) {
      return false
    }
    if (srcUri.getPort() != dstUri.getPort()) {
      false
    } else {
      true
    }
  }

  /** Copy the file into HDFS if needed. */
  private[yarn] def copyRemoteFile(
      dstDir: Path,
      originalPath: Path,
      replication: Short,
      setPerms: Boolean = false): Path = {
    val fs = FileSystem.get(conf)
    val remoteFs = originalPath.getFileSystem(conf)
    var newPath = originalPath
    if (!compareFs(remoteFs, fs)) {
      newPath = new Path(dstDir, originalPath.getName())
      logInfo("Uploading " + originalPath + " to " + newPath)
      FileUtil.copy(remoteFs, originalPath, fs, newPath, false, conf)
      fs.setReplication(newPath, replication)
      if (setPerms) fs.setPermission(newPath, new FsPermission(APP_FILE_PERMISSION))
    }
    // Resolve any symlinks in the URI path so using a "current" symlink to point to a specific
    // version shows the specific version in the distributed cache configuration
    val qualPath = fs.makeQualified(newPath)
    val fc = FileContext.getFileContext(qualPath.toUri(), conf)
    val destPath = fc.resolvePath(qualPath)
    destPath
  }

  private def qualifyForLocal(localURI: URI): Path = {
    var qualifiedURI = localURI
    // If not specified, assume these are in the local filesystem to keep behavior like Hadoop
    if (qualifiedURI.getScheme() == null) {
      qualifiedURI = new URI(FileSystem.getLocal(conf).makeQualified(new Path(qualifiedURI)).toString)
    }
    new Path(qualifiedURI)
  }

  def prepareLocalResources(appStagingDir: String): HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    // Upload Spark and the application JAR to the remote file system if necessary. Add them as
    // local resources to the application master.
    val fs = FileSystem.get(conf)
    val dst = new Path(fs.getHomeDirectory(), appStagingDir)
    val nns = ClientBase.getNameNodesToAccess(sparkConf) + dst
    ClientBase.obtainTokensForNamenodes(nns, conf, credentials)

    val replication = sparkConf.getInt("spark.yarn.submit.file.replication", 3).toShort
    val localResources = HashMap[String, LocalResource]()
    FileSystem.mkdirs(fs, dst, new FsPermission(STAGING_DIR_PERMISSION))

    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()

    val oldLog4jConf = Option(System.getenv("SPARK_LOG4J_CONF"))
    if (oldLog4jConf.isDefined) {
      logWarning(
        "SPARK_LOG4J_CONF detected in the system environment. This variable has been " +
        "deprecated. Please refer to the \"Launching Spark on YARN\" documentation " +
        "for alternatives.")
    }

    List(
      (ClientBase.SPARK_JAR, ClientBase.sparkJar(sparkConf), ClientBase.CONF_SPARK_JAR),
      (ClientBase.APP_JAR, args.userJar, ClientBase.CONF_SPARK_USER_JAR),
      ("log4j.properties", oldLog4jConf.getOrElse(null), null)
    ).foreach { case(destName, _localPath, confKey) =>
      val localPath: String = if (_localPath != null) _localPath.trim() else ""
      if (! localPath.isEmpty()) {
        val localURI = new URI(localPath)
        if (!ClientBase.LOCAL_SCHEME.equals(localURI.getScheme())) {
          val setPermissions = if (destName.equals(ClientBase.APP_JAR)) true else false
          val destPath = copyRemoteFile(dst, qualifyForLocal(localURI), replication, setPermissions)
          val destFs = FileSystem.get(destPath.toUri(), conf)
          distCacheMgr.addResource(destFs, conf, destPath, localResources, LocalResourceType.FILE,
            destName, statCache)
        } else if (confKey != null) {
          sparkConf.set(confKey, localPath)
        }
      }
    }

    val cachedSecondaryJarLinks = ListBuffer.empty[String]
    val fileLists = List( (args.addJars, LocalResourceType.FILE, true),
      (args.files, LocalResourceType.FILE, false),
      (args.archives, LocalResourceType.ARCHIVE, false) )
    fileLists.foreach { case (flist, resType, addToClasspath) =>
      if (flist != null && !flist.isEmpty()) {
        flist.split(',').foreach { case file: String =>
          val localURI = new URI(file.trim())
          if (!ClientBase.LOCAL_SCHEME.equals(localURI.getScheme())) {
            val localPath = new Path(localURI)
            val linkname = Option(localURI.getFragment()).getOrElse(localPath.getName())
            val destPath = copyRemoteFile(dst, localPath, replication)
            distCacheMgr.addResource(fs, conf, destPath, localResources, resType,
              linkname, statCache)
            if (addToClasspath) {
              cachedSecondaryJarLinks += linkname
            }
          } else if (addToClasspath) {
            cachedSecondaryJarLinks += file.trim()
          }
        }
      }
    }
    logInfo("Prepared Local resources " + localResources)
    sparkConf.set(ClientBase.CONF_SPARK_YARN_SECONDARY_JARS, cachedSecondaryJarLinks.mkString(","))

    UserGroupInformation.getCurrentUser().addCredentials(credentials)
    localResources
  }

  /** Get all application master environment variables set on this SparkConf */
  def getAppMasterEnv: Seq[(String, String)] = {
    val prefix = "spark.yarn.appMasterEnv."
    sparkConf.getAll.filter{case (k, v) => k.startsWith(prefix)}
      .map{case (k, v) => (k.substring(prefix.length), v)}
  }


  def setupLaunchEnv(
      localResources: HashMap[String, LocalResource],
      stagingDir: String): HashMap[String, String] = {
    logInfo("Setting up the launch environment")

    val env = new HashMap[String, String]()

    val extraCp = sparkConf.getOption("spark.driver.extraClassPath")
    ClientBase.populateClasspath(args, yarnConf, sparkConf, env, extraCp)
    env("SPARK_YARN_MODE") = "true"
    env("SPARK_YARN_STAGING_DIR") = stagingDir
    env("SPARK_USER") = UserGroupInformation.getCurrentUser().getShortUserName()

    // Set the environment variables to be passed on to the executors.
    distCacheMgr.setDistFilesEnv(env)
    distCacheMgr.setDistArchivesEnv(env)

    getAppMasterEnv.foreach { case (key, value) =>
      YarnSparkHadoopUtil.addToEnvironment(env, key, value, File.pathSeparator)
    }

    // Keep this for backwards compatibility but users should move to the config
    sys.env.get("SPARK_YARN_USER_ENV").foreach { userEnvs =>
      // Allow users to specify some environment variables.
      YarnSparkHadoopUtil.setEnvFromInputString(env, userEnvs, File.pathSeparator)

      // Pass SPARK_YARN_USER_ENV itself to the AM so it can use it to set up executor environments.
      env("SPARK_YARN_USER_ENV") = userEnvs
    }
    env
  }

  def userArgsToString(clientArgs: ClientArguments): String = {
    val prefix = " --arg "
    val args = clientArgs.userArgs
    val retval = new StringBuilder()
    for (arg <- args) {
      retval.append(prefix).append(" ").append(YarnSparkHadoopUtil.escapeForShell(arg))
    }
    retval.toString
  }

  def calculateAMMemory(newApp: GetNewApplicationResponse): Int

  def setupSecurityToken(amContainer: ContainerLaunchContext)

  def createContainerLaunchContext(
        newApp: GetNewApplicationResponse,
        localResources: HashMap[String, LocalResource],
        env: HashMap[String, String]): ContainerLaunchContext = {
    logInfo("Setting up container launch context")
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources)

    val isLaunchingDriver = args.userClass != null

    // In cluster mode, if the deprecated SPARK_JAVA_OPTS is set, we need to propagate it to
    // executors. But we can't just set spark.executor.extraJavaOptions, because the driver's
    // SparkContext will not let that set spark* system properties, which is expected behavior for
    // Yarn clients. So propagate it through the environment.
    //
    // Note that to warn the user about the deprecation in cluster mode, some code from
    // SparkConf#validateSettings() is duplicated here (to avoid triggering the condition
    // described above).
    if (isLaunchingDriver) {
      sys.env.get("SPARK_JAVA_OPTS").foreach { value =>
        val warning =
          s"""
            |SPARK_JAVA_OPTS was detected (set to '$value').
            |This is deprecated in Spark 1.0+.
            |
            |Please instead use:
            | - ./spark-submit with conf/spark-defaults.conf to set defaults for an application
            | - ./spark-submit with --driver-java-options to set -X options for a driver
            | - spark.executor.extraJavaOptions to set -X options for executors
          """.stripMargin
        logWarning(warning)
        for (proc <- Seq("driver", "executor")) {
          val key = s"spark.$proc.extraJavaOptions"
          if (sparkConf.contains(key)) {
            throw new SparkException(s"Found both $key and SPARK_JAVA_OPTS. Use only the former.")
          }
        }
        env("SPARK_JAVA_OPTS") = value
      }
    }
    amContainer.setEnvironment(env)

    val amMemory = calculateAMMemory(newApp)

    val javaOpts = ListBuffer[String]()

    // Add Xmx for AM memory
    javaOpts += "-Xmx" + amMemory + "m"

    val tmpDir = new Path(Environment.PWD.$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR)
    javaOpts += "-Djava.io.tmpdir=" + tmpDir

    // TODO: Remove once cpuset version is pushed out.
    // The context is, default gc for server class machines ends up using all cores to do gc -
    // hence if there are multiple containers in same node, Spark GC affects all other containers'
    // performance (which can be that of other Spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce "proper" Spark behavior in
    // multi-tenant environments. Not sure how default Java GC behaves if it is limited to subset
    // of cores on a node.
    val useConcurrentAndIncrementalGC = env.isDefinedAt("SPARK_USE_CONC_INCR_GC") &&
      java.lang.Boolean.parseBoolean(env("SPARK_USE_CONC_INCR_GC"))
    if (useConcurrentAndIncrementalGC) {
      // In our expts, using (default) throughput collector has severe perf ramifications in
      // multi-tenant machines
      javaOpts += "-XX:+UseConcMarkSweepGC"
      javaOpts += "-XX:+CMSIncrementalMode"
      javaOpts += "-XX:+CMSIncrementalPacing"
      javaOpts += "-XX:CMSIncrementalDutyCycleMin=0"
      javaOpts += "-XX:CMSIncrementalDutyCycle=10"
    }

    // Forward the Spark configuration to the application master / executors.
    // TODO: it might be nicer to pass these as an internal environment variable rather than
    // as Java options, due to complications with string parsing of nested quotes.
    for ((k, v) <- sparkConf.getAll) {
      javaOpts += YarnSparkHadoopUtil.escapeForShell(s"-D$k=$v")
    }

    if (isLaunchingDriver) {
      sparkConf.getOption("spark.driver.extraJavaOptions")
        .orElse(sys.env.get("SPARK_JAVA_OPTS"))
        .foreach(opts => javaOpts += opts)
      sparkConf.getOption("spark.driver.libraryPath")
        .foreach(p => javaOpts += s"-Djava.library.path=$p")
    }

    val userClass =
      if (args.userClass != null) {
        Seq("--class", YarnSparkHadoopUtil.escapeForShell(args.userClass))
      } else {
        Nil
      }
    val amClass =
      if (isLaunchingDriver) {
        classOf[ApplicationMaster].getName()
      } else {
        classOf[ApplicationMaster].getName().replace("ApplicationMaster", "ExecutorLauncher")
      }
    val amArgs =
      Seq(amClass) ++ userClass ++
      (if (args.userJar != null) Seq("--jar", args.userJar) else Nil) ++
      Seq("--executor-memory", args.executorMemory.toString,
        "--executor-cores", args.executorCores.toString,
        "--num-executors ", args.numExecutors.toString,
        userArgsToString(args))

    // Command for the ApplicationMaster
    val commands = Seq(Environment.JAVA_HOME.$() + "/bin/java", "-server") ++
      javaOpts ++ amArgs ++
      Seq(
        "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    logInfo("Yarn AM launch context:")
    logInfo(s"  user class: ${args.userClass}")
    logInfo(s"  env:        $env")
    logInfo(s"  command:    ${commands.mkString(" ")}")

    // TODO: it would be nicer to just make sure there are no null commands here
    val printableCommands = commands.map(s => if (s == null) "null" else s).toList
    amContainer.setCommands(printableCommands)

    setupSecurityToken(amContainer)

    // send the acl settings into YARN to control who has access via YARN interfaces
    val securityManager = new SecurityManager(sparkConf)
    val acls = Map[ApplicationAccessType, String] (
      ApplicationAccessType.VIEW_APP -> securityManager.getViewAcls,
      ApplicationAccessType.MODIFY_APP -> securityManager.getModifyAcls)
    amContainer.setApplicationACLs(acls)
    amContainer
  }
}

object ClientBase extends Logging {
  val SPARK_JAR: String = "__spark__.jar"
  val APP_JAR: String = "__app__.jar"
  val LOCAL_SCHEME = "local"
  val CONF_SPARK_JAR = "spark.yarn.jar"
  /**
   * This is an internal config used to propagate the location of the user's jar file to the
   * driver/executors.
   */
  val CONF_SPARK_USER_JAR = "spark.yarn.user.jar"
  /**
   * This is an internal config used to propagate the list of extra jars to add to the classpath
   * of executors.
   */
  val CONF_SPARK_YARN_SECONDARY_JARS = "spark.yarn.secondary.jars"
  val ENV_SPARK_JAR = "SPARK_JAR"

  /**
   * Find the user-defined Spark jar if configured, or return the jar containing this
   * class if not.
   *
   * This method first looks in the SparkConf object for the CONF_SPARK_JAR key, and in the
   * user environment if that is not found (for backwards compatibility).
   */
  def sparkJar(conf: SparkConf) = {
    if (conf.contains(CONF_SPARK_JAR)) {
      conf.get(CONF_SPARK_JAR)
    } else if (System.getenv(ENV_SPARK_JAR) != null) {
      logWarning(
        s"$ENV_SPARK_JAR detected in the system environment. This variable has been deprecated " +
        s"in favor of the $CONF_SPARK_JAR configuration variable.")
      System.getenv(ENV_SPARK_JAR)
    } else {
      SparkContext.jarOfClass(this.getClass).head
    }
  }

  def populateHadoopClasspath(conf: Configuration, env: HashMap[String, String]) = {
    val classPathElementsToAdd = getYarnAppClasspath(conf) ++ getMRAppClasspath(conf)
    for (c <- classPathElementsToAdd.flatten) {
      YarnSparkHadoopUtil.addToEnvironment(
        env,
        Environment.CLASSPATH.name,
        c.trim,
        File.pathSeparator)
    }
    classPathElementsToAdd
  }

  private def getYarnAppClasspath(conf: Configuration): Option[Seq[String]] =
    Option(conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) match {
      case Some(s) => Some(s.toSeq)
      case None => getDefaultYarnApplicationClasspath
  }

  private def getMRAppClasspath(conf: Configuration): Option[Seq[String]] =
    Option(conf.getStrings("mapreduce.application.classpath")) match {
      case Some(s) => Some(s.toSeq)
      case None => getDefaultMRApplicationClasspath
    }

  def getDefaultYarnApplicationClasspath: Option[Seq[String]] = {
    val triedDefault = Try[Seq[String]] {
      val field = classOf[YarnConfiguration].getField("DEFAULT_YARN_APPLICATION_CLASSPATH")
      val value = field.get(null).asInstanceOf[Array[String]]
      value.toSeq
    } recoverWith {
      case e: NoSuchFieldException => Success(Seq.empty[String])
    }

    triedDefault match {
      case f: Failure[_] =>
        logError("Unable to obtain the default YARN Application classpath.", f.exception)
      case s: Success[_] =>
        logDebug(s"Using the default YARN application classpath: ${s.get.mkString(",")}")
    }

    triedDefault.toOption
  }

  /**
   * In Hadoop 0.23, the MR application classpath comes with the YARN application
   * classpath.  In Hadoop 2.0, it's an array of Strings, and in 2.2+ it's a String.
   * So we need to use reflection to retrieve it.
   */
  def getDefaultMRApplicationClasspath: Option[Seq[String]] = {
    val triedDefault = Try[Seq[String]] {
      val field = classOf[MRJobConfig].getField("DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH")
      val value = if (field.getType == classOf[String]) {
        StringUtils.getStrings(field.get(null).asInstanceOf[String]).toArray
      } else {
        field.get(null).asInstanceOf[Array[String]]
      }
      value.toSeq
    } recoverWith {
      case e: NoSuchFieldException => Success(Seq.empty[String])
    }

    triedDefault match {
      case f: Failure[_] =>
        logError("Unable to obtain the default MR Application classpath.", f.exception)
      case s: Success[_] =>
        logDebug(s"Using the default MR application classpath: ${s.get.mkString(",")}")
    }

    triedDefault.toOption
  }

  def populateClasspath(args: ClientArguments, conf: Configuration, sparkConf: SparkConf,
      env: HashMap[String, String], extraClassPath: Option[String] = None) {
    extraClassPath.foreach(addClasspathEntry(_, env))
    addClasspathEntry(Environment.PWD.$(), env)

    // Normally the users app.jar is last in case conflicts with spark jars
    if (sparkConf.get("spark.yarn.user.classpath.first", "false").toBoolean) {
      addUserClasspath(args, sparkConf, env)
      addFileToClasspath(sparkJar(sparkConf), SPARK_JAR, env)
      ClientBase.populateHadoopClasspath(conf, env)
    } else {
      addFileToClasspath(sparkJar(sparkConf), SPARK_JAR, env)
      ClientBase.populateHadoopClasspath(conf, env)
      addUserClasspath(args, sparkConf, env)
    }

    // Append all jar files under the working directory to the classpath.
    addClasspathEntry(Environment.PWD.$() + Path.SEPARATOR + "*", env);
  }

  /**
   * Adds the user jars which have local: URIs (or alternate names, such as APP_JAR) explicitly
   * to the classpath.
   */
  private def addUserClasspath(args: ClientArguments, conf: SparkConf,
      env: HashMap[String, String]) = {
    if (args != null) {
      addFileToClasspath(args.userJar, APP_JAR, env)
      if (args.addJars != null) {
        args.addJars.split(",").foreach { case file: String =>
          addFileToClasspath(file, null, env)
        }
      }
    } else {
      val userJar = conf.get(CONF_SPARK_USER_JAR, null)
      addFileToClasspath(userJar, APP_JAR, env)

      val cachedSecondaryJarLinks = conf.get(CONF_SPARK_YARN_SECONDARY_JARS, "").split(",")
      cachedSecondaryJarLinks.foreach(jar => addFileToClasspath(jar, null, env))
    }
  }

  /**
   * Adds the given path to the classpath, handling "local:" URIs correctly.
   *
   * If an alternate name for the file is given, and it's not a "local:" file, the alternate
   * name will be added to the classpath (relative to the job's work directory).
   *
   * If not a "local:" file and no alternate name, the environment is not modified.
   *
   * @param path      Path to add to classpath (optional).
   * @param fileName  Alternate name for the file (optional).
   * @param env       Map holding the environment variables.
   */
  private def addFileToClasspath(path: String, fileName: String,
      env: HashMap[String, String]) : Unit = {
    if (path != null) {
      scala.util.control.Exception.ignoring(classOf[URISyntaxException]) {
        val localPath = getLocalPath(path)
        if (localPath != null) {
          addClasspathEntry(localPath, env)
          return
        }
      }
    }
    if (fileName != null) {
      addClasspathEntry(Environment.PWD.$() + Path.SEPARATOR + fileName, env);
    }
  }

  /**
   * Returns the local path if the URI is a "local:" URI, or null otherwise.
   */
  private def getLocalPath(resource: String): String = {
    val uri = new URI(resource)
    if (LOCAL_SCHEME.equals(uri.getScheme())) {
      return uri.getPath()
    }
    null
  }

  private def addClasspathEntry(path: String, env: HashMap[String, String]) =
    YarnSparkHadoopUtil.addToEnvironment(env, Environment.CLASSPATH.name, path,
            File.pathSeparator)

  /**
   * Get the list of namenodes the user may access.
   */
  private[yarn] def getNameNodesToAccess(sparkConf: SparkConf): Set[Path] = {
    sparkConf.get("spark.yarn.access.namenodes", "").split(",").map(_.trim()).filter(!_.isEmpty)
      .map(new Path(_)).toSet
  }

  private[yarn] def getTokenRenewer(conf: Configuration): String = {
    val delegTokenRenewer = Master.getMasterPrincipal(conf)
    logDebug("delegation token renewer is: " + delegTokenRenewer)
    if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer"
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }
    delegTokenRenewer
  }

  /**
   * Obtains tokens for the namenodes passed in and adds them to the credentials.
   */
  private[yarn] def obtainTokensForNamenodes(paths: Set[Path], conf: Configuration,
    creds: Credentials) {
    if (UserGroupInformation.isSecurityEnabled()) {
      val delegTokenRenewer = getTokenRenewer(conf)

      paths.foreach {
        dst =>
          val dstFs = dst.getFileSystem(conf)
          logDebug("getting token for namenode: " + dst)
          dstFs.addDelegationTokens(delegTokenRenewer, creds)
      }
    }
  }

}
