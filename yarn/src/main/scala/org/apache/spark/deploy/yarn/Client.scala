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

import java.net.{InetAddress, UnknownHostException, URI}
import java.nio.ByteBuffer

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, FileStatus, FileSystem, Path, FileUtil}
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.YarnClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{Apps, Records}

import org.apache.spark.Logging 
import org.apache.spark.util.Utils
import org.apache.spark.deploy.SparkHadoopUtil


class Client(conf: Configuration, args: ClientArguments) extends YarnClientImpl with Logging {

  def this(args: ClientArguments) = this(new Configuration(), args)

  var rpc: YarnRPC = YarnRPC.create(conf)
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)
  val credentials = UserGroupInformation.getCurrentUser().getCredentials()
  private val SPARK_STAGING: String = ".sparkStaging"
  private val distCacheMgr = new ClientDistributedCacheManager()

  // Staging directory is private! -> rwx--------
  val STAGING_DIR_PERMISSION: FsPermission = FsPermission.createImmutable(0700:Short)

  // App files are world-wide readable and owner writable -> rw-r--r--
  val APP_FILE_PERMISSION: FsPermission = FsPermission.createImmutable(0644:Short) 

  // for client user who want to monitor app status by itself.
  def runApp() = {
    validateArgs()

    init(yarnConf)
    start()
    logClusterResourceDetails()

    val newApp = super.getNewApplication()
    val appId = newApp.getApplicationId()

    verifyClusterResources(newApp)
    val appContext = createApplicationSubmissionContext(appId)
    val appStagingDir = getAppStagingDir(appId)
    val localResources = prepareLocalResources(appStagingDir)
    val env = setupLaunchEnv(localResources, appStagingDir)
    val amContainer = createContainerLaunchContext(newApp, localResources, env)

    appContext.setQueue(args.amQueue)
    appContext.setAMContainerSpec(amContainer)
    appContext.setUser(UserGroupInformation.getCurrentUser().getShortUserName())

    submitApp(appContext)
    appId
  }

  def run() {
    val appId = runApp()
    monitorApplication(appId)
    System.exit(0)
  }

  def validateArgs() = {
    Map(
      (System.getenv("SPARK_JAR") == null) -> "Error: You must set SPARK_JAR environment variable!",
      (args.userJar == null) -> "Error: You must specify a user jar!",
      (args.userClass == null) -> "Error: You must specify a user class!",
      (args.numWorkers <= 0) -> "Error: You must specify atleast 1 worker!",
      (args.amMemory <= YarnAllocationHandler.MEMORY_OVERHEAD) -> ("Error: AM memory size must be " +
        "greater than: " + YarnAllocationHandler.MEMORY_OVERHEAD),
      (args.workerMemory <= YarnAllocationHandler.MEMORY_OVERHEAD) -> ("Error: Worker memory size " +
        "must be greater than: " + YarnAllocationHandler.MEMORY_OVERHEAD)
    ).foreach { case(cond, errStr) => 
      if (cond) {
        logError(errStr)
        args.printUsageAndExit(1)
      }
    }
  }

  def getAppStagingDir(appId: ApplicationId): String = {
    SPARK_STAGING + Path.SEPARATOR + appId.toString() + Path.SEPARATOR
  }

  def logClusterResourceDetails() {
    val clusterMetrics: YarnClusterMetrics = super.getYarnClusterMetrics
    logInfo("Got Cluster metric info from ASM, numNodeManagers = " +
      clusterMetrics.getNumNodeManagers)

    val queueInfo: QueueInfo = super.getQueueInfo(args.amQueue)
    logInfo("""Queue info ... queueName = %s, queueCurrentCapacity = %s, queueMaxCapacity = %s,
      queueApplicationCount = %s, queueChildQueueCount = %s""".format(
        queueInfo.getQueueName,
        queueInfo.getCurrentCapacity,
        queueInfo.getMaximumCapacity,
        queueInfo.getApplications.size,
        queueInfo.getChildQueues.size))
  }

  def verifyClusterResources(app: GetNewApplicationResponse) = { 
    val maxMem = app.getMaximumResourceCapability().getMemory()
    logInfo("Max mem capabililty of a single resource in this cluster " + maxMem)

    // If we have requested more then the clusters max for a single resource then exit.
    if (args.workerMemory > maxMem) {
      logError("the worker size is to large to run on this cluster " + args.workerMemory)
      System.exit(1)
    }
    val amMem = args.amMemory + YarnAllocationHandler.MEMORY_OVERHEAD
    if (amMem > maxMem) {
      logError("AM size is to large to run on this cluster "  + amMem)
      System.exit(1)
    }

    // We could add checks to make sure the entire cluster has enough resources but that involves
    // getting all the node reports and computing ourselves 
  }

  def createApplicationSubmissionContext(appId: ApplicationId): ApplicationSubmissionContext = {
    logInfo("Setting up application submission context for ASM")
    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    appContext.setApplicationId(appId)
    appContext.setApplicationName(args.appName)
    return appContext
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
    //check for ports
    if (srcUri.getPort() != dstUri.getPort()) {
      return false
    }
    return true
  }

  /** Copy the file into HDFS if needed. */
  private def copyRemoteFile(
      dstDir: Path,
      originalPath: Path,
      replication: Short,
      setPerms: Boolean = false): Path = {
    val fs = FileSystem.get(conf)
    val remoteFs = originalPath.getFileSystem(conf)
    var newPath = originalPath
    if (! compareFs(remoteFs, fs)) {
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

  def prepareLocalResources(appStagingDir: String): HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    // Upload Spark and the application JAR to the remote file system if necessary. Add them as
    // local resources to the AM.
    val fs = FileSystem.get(conf)

    val delegTokenRenewer = Master.getMasterPrincipal(conf)
    if (UserGroupInformation.isSecurityEnabled()) {
      if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
        logError("Can't get Master Kerberos principal for use as renewer")
        System.exit(1)
      }
    }
    val dst = new Path(fs.getHomeDirectory(), appStagingDir)
    val replication = System.getProperty("spark.yarn.submit.file.replication", "3").toShort

    if (UserGroupInformation.isSecurityEnabled()) {
      val dstFs = dst.getFileSystem(conf)
      dstFs.addDelegationTokens(delegTokenRenewer, credentials)
    }
    val localResources = HashMap[String, LocalResource]()
    FileSystem.mkdirs(fs, dst, new FsPermission(STAGING_DIR_PERMISSION))

    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()

    Map(Client.SPARK_JAR -> System.getenv("SPARK_JAR"), Client.APP_JAR -> args.userJar, 
      Client.LOG4J_PROP -> System.getenv("SPARK_LOG4J_CONF"))
    .foreach { case(destName, _localPath) =>
      val localPath: String = if (_localPath != null) _localPath.trim() else ""
      if (! localPath.isEmpty()) {
        var localURI = new URI(localPath)
        // if not specified assume these are in the local filesystem to keep behavior like Hadoop
        if (localURI.getScheme() == null) {
          localURI = new URI(FileSystem.getLocal(conf).makeQualified(new Path(localPath)).toString)
        }
        val setPermissions = if (destName.equals(Client.APP_JAR)) true else false
        val destPath = copyRemoteFile(dst, new Path(localURI), replication, setPermissions)
        distCacheMgr.addResource(fs, conf, destPath, localResources, LocalResourceType.FILE, 
          destName, statCache)
      }
    }

    // handle any add jars
    if ((args.addJars != null) && (!args.addJars.isEmpty())){
      args.addJars.split(',').foreach { case file: String =>
        val localURI = new URI(file.trim())
        val localPath = new Path(localURI)
        val linkname = Option(localURI.getFragment()).getOrElse(localPath.getName())
        val destPath = copyRemoteFile(dst, localPath, replication)
        distCacheMgr.addResource(fs, conf, destPath, localResources, LocalResourceType.FILE, 
          linkname, statCache, true)
      }
    }

    // handle any distributed cache files
    if ((args.files != null) && (!args.files.isEmpty())){
      args.files.split(',').foreach { case file: String =>
        val localURI = new URI(file.trim())
        val localPath = new Path(localURI)
        val linkname = Option(localURI.getFragment()).getOrElse(localPath.getName())
        val destPath = copyRemoteFile(dst, localPath, replication)
        distCacheMgr.addResource(fs, conf, destPath, localResources, LocalResourceType.FILE, 
          linkname, statCache)
      }
    }

    // handle any distributed cache archives
    if ((args.archives != null) && (!args.archives.isEmpty())) {
      args.archives.split(',').foreach { case file:String =>
        val localURI = new URI(file.trim())
        val localPath = new Path(localURI)
        val linkname = Option(localURI.getFragment()).getOrElse(localPath.getName())
        val destPath = copyRemoteFile(dst, localPath, replication)
        distCacheMgr.addResource(fs, conf, destPath, localResources, LocalResourceType.ARCHIVE, 
          linkname, statCache)
      }
    }

    UserGroupInformation.getCurrentUser().addCredentials(credentials)
    return localResources
  }

  def setupLaunchEnv(
      localResources: HashMap[String, LocalResource], 
      stagingDir: String): HashMap[String, String] = {
    logInfo("Setting up the launch environment")
    val log4jConfLocalRes = localResources.getOrElse(Client.LOG4J_PROP, null)

    val env = new HashMap[String, String]()

    Client.populateClasspath(yarnConf, log4jConfLocalRes != null, env)
    env("SPARK_YARN_MODE") = "true"
    env("SPARK_YARN_STAGING_DIR") = stagingDir

    // Set the environment variables to be passed on to the Workers.
    distCacheMgr.setDistFilesEnv(env)
    distCacheMgr.setDistArchivesEnv(env)

    // Allow users to specify some environment variables.
    Apps.setEnvFromInputString(env, System.getenv("SPARK_YARN_USER_ENV"))

    // Add each SPARK-* key to the environment.
    System.getenv().filterKeys(_.startsWith("SPARK")).foreach { case (k,v) => env(k) = v }
    env
  }

  def userArgsToString(clientArgs: ClientArguments): String = {
    val prefix = " --args "
    val args = clientArgs.userArgs
    val retval = new StringBuilder()
    for (arg <- args){
      retval.append(prefix).append(" '").append(arg).append("' ")
    }
    retval.toString
  }

  def createContainerLaunchContext(
      newApp: GetNewApplicationResponse,
      localResources: HashMap[String, LocalResource],
      env: HashMap[String, String]): ContainerLaunchContext = {
    logInfo("Setting up container launch context")
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources)
    amContainer.setEnvironment(env)

    val minResMemory: Int = newApp.getMinimumResourceCapability().getMemory()

    // TODO(harvey): This can probably be a val.
    var amMemory = ((args.amMemory / minResMemory) * minResMemory) +
      ((if ((args.amMemory % minResMemory) == 0) 0 else minResMemory) -
        YarnAllocationHandler.MEMORY_OVERHEAD)

    // Extra options for the JVM
    var JAVA_OPTS = ""

    // Add Xmx for am memory
    JAVA_OPTS += "-Xmx" + amMemory + "m "

    JAVA_OPTS += " -Djava.io.tmpdir=" + 
      new Path(Environment.PWD.$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR) + " "

    // Commenting it out for now - so that people can refer to the properties if required. Remove
    // it once cpuset version is pushed out. The context is, default gc for server class machines
    // end up using all cores to do gc - hence if there are multiple containers in same node,
    // spark gc effects all other containers performance (which can also be other spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce spark behaves 'properly' in
    // multi-tenant environments. Not sure how default java gc behaves if it is limited to subset
    // of cores on a node.
    val useConcurrentAndIncrementalGC = env.isDefinedAt("SPARK_USE_CONC_INCR_GC") &&
      java.lang.Boolean.parseBoolean(env("SPARK_USE_CONC_INCR_GC"))
    if (useConcurrentAndIncrementalGC) {
      // In our expts, using (default) throughput collector has severe perf ramnifications in
      // multi-tenant machines
      JAVA_OPTS += " -XX:+UseConcMarkSweepGC "
      JAVA_OPTS += " -XX:+CMSIncrementalMode "
      JAVA_OPTS += " -XX:+CMSIncrementalPacing "
      JAVA_OPTS += " -XX:CMSIncrementalDutyCycleMin=0 "
      JAVA_OPTS += " -XX:CMSIncrementalDutyCycle=10 "
    }

    if (env.isDefinedAt("SPARK_JAVA_OPTS")) {
      JAVA_OPTS += env("SPARK_JAVA_OPTS") + " "
    }

    // Command for the ApplicationMaster
    var javaCommand = "java"
    val javaHome = System.getenv("JAVA_HOME")
    if ((javaHome != null && !javaHome.isEmpty()) || env.isDefinedAt("JAVA_HOME")) {
      javaCommand = Environment.JAVA_HOME.$() + "/bin/java"
    }

    val commands = List[String](javaCommand + 
      " -server " +
      JAVA_OPTS +
      " " + args.amClass +
      " --class " + args.userClass + 
      " --jar " + args.userJar +
      userArgsToString(args) +
      " --worker-memory " + args.workerMemory +
      " --worker-cores " + args.workerCores +
      " --num-workers " + args.numWorkers +
      " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
    logInfo("Command for the ApplicationMaster: " + commands(0))
    amContainer.setCommands(commands)

    val capability = Records.newRecord(classOf[Resource]).asInstanceOf[Resource]
    // Memory for the ApplicationMaster.
    capability.setMemory(args.amMemory + YarnAllocationHandler.MEMORY_OVERHEAD)
    amContainer.setResource(capability)

    // Setup security tokens.
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    amContainer.setContainerTokens(ByteBuffer.wrap(dob.getData()))

    amContainer
  }

  def submitApp(appContext: ApplicationSubmissionContext) = {
    // Submit the application to the applications manager.
    logInfo("Submitting application to ASM")
    super.submitApplication(appContext)
  }

  def monitorApplication(appId: ApplicationId): Boolean = {  
    while (true) {
      Thread.sleep(1000)
      val report = super.getApplicationReport(appId)

      logInfo("Application report from ASM: \n" +
        "\t application identifier: " + appId.toString() + "\n" +
        "\t appId: " + appId.getId() + "\n" +
        "\t clientToken: " + report.getClientToken() + "\n" +
        "\t appDiagnostics: " + report.getDiagnostics() + "\n" +
        "\t appMasterHost: " + report.getHost() + "\n" +
        "\t appQueue: " + report.getQueue() + "\n" +
        "\t appMasterRpcPort: " + report.getRpcPort() + "\n" +
        "\t appStartTime: " + report.getStartTime() + "\n" +
        "\t yarnAppState: " + report.getYarnApplicationState() + "\n" +
        "\t distributedFinalState: " + report.getFinalApplicationStatus() + "\n" +
        "\t appTrackingUrl: " + report.getTrackingUrl() + "\n" +
        "\t appUser: " + report.getUser()
      )

      val state = report.getYarnApplicationState()
      val dsStatus = report.getFinalApplicationStatus()
      if (state == YarnApplicationState.FINISHED || 
        state == YarnApplicationState.FAILED ||
        state == YarnApplicationState.KILLED) {
        return true
      }
    }
    true
  }
}

object Client {
  val SPARK_JAR: String = "spark.jar"
  val APP_JAR: String = "app.jar"
  val LOG4J_PROP: String = "log4j.properties"

  def main(argStrings: Array[String]) {
    // Set an env variable indicating we are running in YARN mode.
    // Note that anything with SPARK prefix gets propagated to all (remote) processes
    System.setProperty("SPARK_YARN_MODE", "true")

    val args = new ClientArguments(argStrings)

    new Client(args).run
  }

  // Based on code from org.apache.hadoop.mapreduce.v2.util.MRApps
  def populateHadoopClasspath(conf: Configuration, env: HashMap[String, String]) {
    for (c <- conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(env, Environment.CLASSPATH.name, c.trim)
    }
  }

  def populateClasspath(conf: Configuration, addLog4j: Boolean, env: HashMap[String, String]) {
    Apps.addToEnvironment(env, Environment.CLASSPATH.name, Environment.PWD.$())
    // If log4j present, ensure ours overrides all others
    if (addLog4j) {
      Apps.addToEnvironment(env, Environment.CLASSPATH.name, Environment.PWD.$() + 
        Path.SEPARATOR + LOG4J_PROP)
    }
    // Normally the users app.jar is last in case conflicts with spark jars
    val userClasspathFirst = System.getProperty("spark.yarn.user.classpath.first", "false")
      .toBoolean
    if (userClasspathFirst) {
      Apps.addToEnvironment(env, Environment.CLASSPATH.name, Environment.PWD.$() + 
        Path.SEPARATOR + APP_JAR)
    }
    Apps.addToEnvironment(env, Environment.CLASSPATH.name, Environment.PWD.$() + 
      Path.SEPARATOR + SPARK_JAR)
    Client.populateHadoopClasspath(conf, env)

    if (!userClasspathFirst) {
      Apps.addToEnvironment(env, Environment.CLASSPATH.name, Environment.PWD.$() + 
        Path.SEPARATOR + APP_JAR)
    }
    Apps.addToEnvironment(env, Environment.CLASSPATH.name, Environment.PWD.$() + 
      Path.SEPARATOR + "*")
  }
}
