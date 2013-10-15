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

import java.net.{InetSocketAddress, URI}
import java.nio.ByteBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.client.YarnClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import org.apache.spark.Logging 
import org.apache.spark.util.Utils
import org.apache.hadoop.yarn.util.{Apps, Records, ConverterUtils}
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.spark.deploy.SparkHadoopUtil

class Client(conf: Configuration, args: ClientArguments) extends YarnClientImpl with Logging {
  
  def this(args: ClientArguments) = this(new Configuration(), args)
  
  var rpc: YarnRPC = YarnRPC.create(conf)
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)
  val credentials = UserGroupInformation.getCurrentUser().getCredentials()
  private var distFiles = None: Option[String]
  private var distFilesTimeStamps = None: Option[String]
  private var distFilesFileSizes = None: Option[String]
  private var distArchives = None: Option[String]
  private var distArchivesTimeStamps = None: Option[String]
  private var distArchivesFileSizes = None: Option[String]
  
  def run() {
    init(yarnConf)
    start()
    logClusterResourceDetails()

    val newApp = super.getNewApplication()
    val appId = newApp.getApplicationId()

    verifyClusterResources(newApp)
    val appContext = createApplicationSubmissionContext(appId)
    val localResources = prepareLocalResources(appId, ".sparkStaging")
    val env = setupLaunchEnv(localResources)
    val amContainer = createContainerLaunchContext(newApp, localResources, env)

    appContext.setQueue(args.amQueue)
    appContext.setAMContainerSpec(amContainer)
    appContext.setUser(UserGroupInformation.getCurrentUser().getShortUserName())

    submitApp(appContext)
    
    monitorApplication(appId)
    System.exit(0)
  }
  

  def logClusterResourceDetails() {
    val clusterMetrics: YarnClusterMetrics = super.getYarnClusterMetrics
    logInfo("Got Cluster metric info from ASM, numNodeManagers=" + clusterMetrics.getNumNodeManagers)

    val queueInfo: QueueInfo = super.getQueueInfo(args.amQueue)
    logInfo("Queue info .. queueName=" + queueInfo.getQueueName + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity +
      ", queueMaxCapacity=" + queueInfo.getMaximumCapacity + ", queueApplicationCount=" + queueInfo.getApplications.size +
      ", queueChildQueueCount=" + queueInfo.getChildQueues.size)
  }

  
  def verifyClusterResources(app: GetNewApplicationResponse) = { 
    val maxMem = app.getMaximumResourceCapability().getMemory()
    logInfo("Max mem capabililty of a single resource in this cluster " + maxMem)
    
    // if we have requested more then the clusters max for a single resource then exit.
    if (args.workerMemory > maxMem) {
      logError("the worker size is to large to run on this cluster " + args.workerMemory);
      System.exit(1)
    }
    val amMem = args.amMemory + YarnAllocationHandler.MEMORY_OVERHEAD
    if (amMem > maxMem) {
      logError("AM size is to large to run on this cluster "  + amMem)
      System.exit(1)
    }

    // We could add checks to make sure the entire cluster has enough resources but that involves getting
    // all the node reports and computing ourselves 
  }
  
  def createApplicationSubmissionContext(appId: ApplicationId): ApplicationSubmissionContext = {
    logInfo("Setting up application submission context for ASM")
    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    appContext.setApplicationId(appId)
    appContext.setApplicationName(args.appName)
    return appContext
  }

  /**
   * Copy the local file into HDFS and configure to be distributed with the
   * job via the distributed cache.
   * If a fragment is specified the file will be referenced as that fragment.
   */
  private def copyLocalFile(
      dstDir: Path,
      resourceType: LocalResourceType,
      originalPath: Path,
      replication: Short,
      localResources: HashMap[String,LocalResource],
      fragment: String,
      appMasterOnly: Boolean = false): Unit = {
    val fs = FileSystem.get(conf)
    val newPath = new Path(dstDir, originalPath.getName())
    logInfo("Uploading " + originalPath + " to " + newPath)
    fs.copyFromLocalFile(false, true, originalPath, newPath)
    fs.setReplication(newPath, replication);
    val destStatus = fs.getFileStatus(newPath)

    val amJarRsrc = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
    amJarRsrc.setType(resourceType)
    amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION)
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(newPath))
    amJarRsrc.setTimestamp(destStatus.getModificationTime())
    amJarRsrc.setSize(destStatus.getLen())
    var pathURI: URI = new URI(newPath.toString() + "#" + originalPath.getName());
    if ((fragment == null) || (fragment.isEmpty())){
      localResources(originalPath.getName()) = amJarRsrc
    } else {
      localResources(fragment) = amJarRsrc
      pathURI = new URI(newPath.toString() + "#" + fragment);
    }
    val distPath = pathURI.toString()
    if (appMasterOnly == true) return
    if (resourceType == LocalResourceType.FILE) {
      distFiles match {
        case Some(path) =>
          distFilesFileSizes = Some(distFilesFileSizes.get + "," + 
            destStatus.getLen().toString())
          distFilesTimeStamps = Some(distFilesTimeStamps.get + "," + 
            destStatus.getModificationTime().toString())
          distFiles = Some(path + "," + distPath)
        case _ => 
          distFilesFileSizes = Some(destStatus.getLen().toString())
          distFilesTimeStamps = Some(destStatus.getModificationTime().toString())
          distFiles = Some(distPath)
      }
    } else {
      distArchives match {
        case Some(path) =>
          distArchivesTimeStamps = Some(distArchivesTimeStamps.get + "," +
            destStatus.getModificationTime().toString())
          distArchivesFileSizes = Some(distArchivesFileSizes.get + "," + 
            destStatus.getLen().toString())
          distArchives = Some(path + "," + distPath)
        case _ => 
          distArchivesTimeStamps = Some(destStatus.getModificationTime().toString())
          distArchivesFileSizes = Some(destStatus.getLen().toString())
          distArchives = Some(distPath)
      }
    }
  }

  def prepareLocalResources(appId: ApplicationId, sparkStagingDir: String): HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    // Upload Spark and the application JAR to the remote file system
    // Add them as local resources to the AM
    val fs = FileSystem.get(conf)

    val delegTokenRenewer = Master.getMasterPrincipal(conf);
    if (UserGroupInformation.isSecurityEnabled()) {
      if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
        logError("Can't get Master Kerberos principal for use as renewer")
        System.exit(1)
      }
    }

    val pathSuffix = sparkStagingDir + "/" + appId.toString() + "/"
    val dst = new Path(fs.getHomeDirectory(), pathSuffix)
    val replication = System.getProperty("spark.yarn.submit.file.replication", "3").toShort

    if (UserGroupInformation.isSecurityEnabled()) {
      val dstFs = dst.getFileSystem(conf)
      dstFs.addDelegationTokens(delegTokenRenewer, credentials);
    }
    val localResources = HashMap[String, LocalResource]()

    Map("spark.jar" -> System.getenv("SPARK_JAR"), "app.jar" -> args.userJar, "log4j.properties" -> System.getenv("SPARK_LOG4J_CONF"))
    .foreach { case(destName, _localPath) =>
      val localPath: String = if (_localPath != null) _localPath.trim() else ""
      if (! localPath.isEmpty()) {
        val src = new Path(localPath)
        val newPath = new Path(dst, destName)
        logInfo("Uploading " + src + " to " + newPath)
        fs.copyFromLocalFile(false, true, src, newPath)
        fs.setReplication(newPath, replication);
        val destStatus = fs.getFileStatus(newPath)

        val amJarRsrc = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
        amJarRsrc.setType(LocalResourceType.FILE)
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION)
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(newPath))
        amJarRsrc.setTimestamp(destStatus.getModificationTime())
        amJarRsrc.setSize(destStatus.getLen())
        localResources(destName) = amJarRsrc
      }
    }

    // handle any add jars
    if ((args.addJars != null) && (!args.addJars.isEmpty())){
      args.addJars.split(',').foreach { case file: String =>
        val tmpURI = new URI(file)
        val tmp = new Path(tmpURI)
        copyLocalFile(dst, LocalResourceType.FILE, tmp, replication, localResources,
          tmpURI.getFragment(), true)
      }
    }

    // handle any distributed cache files
    if ((args.files != null) && (!args.files.isEmpty())){
      args.files.split(',').foreach { case file: String =>
        val tmpURI = new URI(file)
        val tmp = new Path(tmpURI)
        copyLocalFile(dst, LocalResourceType.FILE, tmp, replication, localResources,
          tmpURI.getFragment())
      }
    }

    // handle any distributed cache archives
    if ((args.archives != null) && (!args.archives.isEmpty())) {
      args.archives.split(',').foreach { case file:String =>
        val tmpURI = new URI(file)
        val tmp = new Path(tmpURI)
        copyLocalFile(dst, LocalResourceType.ARCHIVE, tmp, replication, 
          localResources, tmpURI.getFragment())
      }
    }

    UserGroupInformation.getCurrentUser().addCredentials(credentials);
    return localResources
  }
  
  def setupLaunchEnv(localResources: HashMap[String, LocalResource]): HashMap[String, String] = {
    logInfo("Setting up the launch environment")
    val log4jConfLocalRes = localResources.getOrElse("log4j.properties", null)

    val env = new HashMap[String, String]()

    Apps.addToEnvironment(env, Environment.CLASSPATH.name, Environment.PWD.$())
    Apps.addToEnvironment(env, Environment.CLASSPATH.name,
      Environment.PWD.$() + Path.SEPARATOR + "*")

    Client.populateHadoopClasspath(yarnConf, env)
    env("SPARK_YARN_MODE") = "true"
    env("SPARK_YARN_JAR_PATH") = 
      localResources("spark.jar").getResource().getScheme.toString() + "://" +
      localResources("spark.jar").getResource().getFile().toString()
    env("SPARK_YARN_JAR_TIMESTAMP") =  localResources("spark.jar").getTimestamp().toString()
    env("SPARK_YARN_JAR_SIZE") =  localResources("spark.jar").getSize().toString()

    env("SPARK_YARN_USERJAR_PATH") =
      localResources("app.jar").getResource().getScheme.toString() + "://" +
      localResources("app.jar").getResource().getFile().toString()
    env("SPARK_YARN_USERJAR_TIMESTAMP") =  localResources("app.jar").getTimestamp().toString()
    env("SPARK_YARN_USERJAR_SIZE") =  localResources("app.jar").getSize().toString()

    if (log4jConfLocalRes != null) {
      env("SPARK_YARN_LOG4J_PATH") =
        log4jConfLocalRes.getResource().getScheme.toString() + "://" + log4jConfLocalRes.getResource().getFile().toString()
      env("SPARK_YARN_LOG4J_TIMESTAMP") =  log4jConfLocalRes.getTimestamp().toString()
      env("SPARK_YARN_LOG4J_SIZE") =  log4jConfLocalRes.getSize().toString()
    }

    // set the environment variables to be passed on to the Workers
    if (distFiles != None) {
      env("SPARK_YARN_CACHE_FILES") = distFiles.get
      env("SPARK_YARN_CACHE_FILES_TIME_STAMPS") = distFilesTimeStamps.get
      env("SPARK_YARN_CACHE_FILES_FILE_SIZES") = distFilesFileSizes.get
    }
    if (distArchives != None) {
      env("SPARK_YARN_CACHE_ARCHIVES") = distArchives.get
      env("SPARK_YARN_CACHE_ARCHIVES_TIME_STAMPS") = distArchivesTimeStamps.get
      env("SPARK_YARN_CACHE_ARCHIVES_FILE_SIZES") = distArchivesFileSizes.get
    }

    // allow users to specify some environment variables
    Apps.setEnvFromInputString(env, System.getenv("SPARK_YARN_USER_ENV"))

    // Add each SPARK-* key to the environment
    System.getenv().filterKeys(_.startsWith("SPARK")).foreach { case (k,v) => env(k) = v }
    return env
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

  def createContainerLaunchContext(newApp: GetNewApplicationResponse,
                                   localResources: HashMap[String, LocalResource],
                                   env: HashMap[String, String]): ContainerLaunchContext = {
    logInfo("Setting up container launch context")
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources)
    amContainer.setEnvironment(env)

    val minResMemory: Int = newApp.getMinimumResourceCapability().getMemory()

    var amMemory = ((args.amMemory / minResMemory) * minResMemory) +
        (if (0 != (args.amMemory % minResMemory)) minResMemory else 0) - YarnAllocationHandler.MEMORY_OVERHEAD

    // Extra options for the JVM
    var JAVA_OPTS = ""

    // Add Xmx for am memory
    JAVA_OPTS += "-Xmx" + amMemory + "m "

    JAVA_OPTS += " -Djava.io.tmpdir=" + 
      new Path(Environment.PWD.$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR) + " "


    // Commenting it out for now - so that people can refer to the properties if required. Remove it once cpuset version is pushed out.
    // The context is, default gc for server class machines end up using all cores to do gc - hence if there are multiple containers in same
    // node, spark gc effects all other containers performance (which can also be other spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce spark behaves 'properly' in multi-tenant environments. Not sure how default java gc behaves if it is
    // limited to subset of cores on a node.
    if (env.isDefinedAt("SPARK_USE_CONC_INCR_GC") && java.lang.Boolean.parseBoolean(env("SPARK_USE_CONC_INCR_GC"))) {
      // In our expts, using (default) throughput collector has severe perf ramnifications in multi-tenant machines
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
    var javaCommand = "java";
    val javaHome = System.getenv("JAVA_HOME")
    if ((javaHome != null && !javaHome.isEmpty()) || env.isDefinedAt("JAVA_HOME")) {
      javaCommand = Environment.JAVA_HOME.$() + "/bin/java"
    }

    val commands = List[String](javaCommand + 
      " -server " +
      JAVA_OPTS +
      " org.apache.spark.deploy.yarn.ApplicationMaster" +
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
    // Memory for the ApplicationMaster
    capability.setMemory(args.amMemory + YarnAllocationHandler.MEMORY_OVERHEAD)
    amContainer.setResource(capability)

    // Setup security tokens
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    amContainer.setContainerTokens(ByteBuffer.wrap(dob.getData()))

    return amContainer
  }
  
  def submitApp(appContext: ApplicationSubmissionContext) = {
    // Submit the application to the applications manager
    logInfo("Submitting application to ASM")
    super.submitApplication(appContext)
  }
  
  def monitorApplication(appId: ApplicationId): Boolean = {  
    while(true) {
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
    return true
  }
}

object Client {
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
}
