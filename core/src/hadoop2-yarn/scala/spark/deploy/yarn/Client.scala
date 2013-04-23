package spark.deploy.yarn

import java.net.{InetSocketAddress, URI}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.client.YarnClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import spark.{Logging, Utils}
import org.apache.hadoop.yarn.util.{Apps, Records, ConverterUtils}
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import spark.deploy.SparkHadoopUtil

class Client(conf: Configuration, args: ClientArguments) extends YarnClientImpl with Logging {
  
  def this(args: ClientArguments) = this(new Configuration(), args)
  
  var rpc: YarnRPC = YarnRPC.create(conf)
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)
  
  def run() {
    init(yarnConf)
    start()
    logClusterResourceDetails()

    val newApp = super.getNewApplication()
    val appId = newApp.getApplicationId()

    verifyClusterResources(newApp)
    val appContext = createApplicationSubmissionContext(appId)
    val localResources = prepareLocalResources(appId, "spark")
    val env = setupLaunchEnv(localResources)
    val amContainer = createContainerLaunchContext(newApp, localResources, env)

    appContext.setQueue(args.amQueue)
    appContext.setAMContainerSpec(amContainer)
    appContext.setUser(args.amUser)
    
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
    logInfo("Max mem capabililty of resources in this cluster " + maxMem)
    
    // If the cluster does not have enough memory resources, exit.
    val requestedMem = (args.amMemory + YarnAllocationHandler.MEMORY_OVERHEAD) + args.numWorkers * args.workerMemory
    if (requestedMem > maxMem) {
      logError("Cluster cannot satisfy memory resource request of " + requestedMem)
      System.exit(1)
    }
  }
  
  def createApplicationSubmissionContext(appId: ApplicationId): ApplicationSubmissionContext = {
    logInfo("Setting up application submission context for ASM")
    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    appContext.setApplicationId(appId)
    appContext.setApplicationName("Spark")
    return appContext
  }
  
  def prepareLocalResources(appId: ApplicationId, appName: String): HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    val locaResources = HashMap[String, LocalResource]()
    // Upload Spark and the application JAR to the remote file system
    // Add them as local resources to the AM
    val fs = FileSystem.get(conf)
    Map("spark.jar" -> System.getenv("SPARK_JAR"), "app.jar" -> args.userJar, "log4j.properties" -> System.getenv("SPARK_LOG4J_CONF"))
    .foreach { case(destName, _localPath) =>
      val localPath: String = if (_localPath != null) _localPath.trim() else ""
      if (! localPath.isEmpty()) {
        val src = new Path(localPath)
        val pathSuffix = appName + "/" + appId.getId() + destName
        val dst = new Path(fs.getHomeDirectory(), pathSuffix)
        logInfo("Uploading " + src + " to " + dst)
        fs.copyFromLocalFile(false, true, src, dst)
        val destStatus = fs.getFileStatus(dst)

        val amJarRsrc = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
        amJarRsrc.setType(LocalResourceType.FILE)
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION)
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst))
        amJarRsrc.setTimestamp(destStatus.getModificationTime())
        amJarRsrc.setSize(destStatus.getLen())
        locaResources(destName) = amJarRsrc
      }
    }
    return locaResources
  }
  
  def setupLaunchEnv(localResources: HashMap[String, LocalResource]): HashMap[String, String] = {
    logInfo("Setting up the launch environment")
    val log4jConfLocalRes = localResources.getOrElse("log4j.properties", null)

    val env = new HashMap[String, String]()
    Apps.addToEnvironment(env, Environment.USER.name, args.amUser)

    // If log4j present, ensure ours overrides all others
    if (log4jConfLocalRes != null) Apps.addToEnvironment(env, Environment.CLASSPATH.name, "./")

    Apps.addToEnvironment(env, Environment.CLASSPATH.name, "./*")
    Apps.addToEnvironment(env, Environment.CLASSPATH.name, "$CLASSPATH")
    Client.populateHadoopClasspath(yarnConf, env)
    SparkHadoopUtil.setYarnMode(env)
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
    val commands = List[String]("java " +
      " -server " +
      JAVA_OPTS +
      " spark.deploy.yarn.ApplicationMaster" +
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
    val args = new ClientArguments(argStrings)
    SparkHadoopUtil.setYarnMode()
    new Client(args).run
  }

  // Based on code from org.apache.hadoop.mapreduce.v2.util.MRApps
  def populateHadoopClasspath(conf: Configuration, env: HashMap[String, String]) {
    for (c <- conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(env, Environment.CLASSPATH.name, c.trim)
    }
  }
}
