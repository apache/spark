package spark.deploy.yarn

import java.net.{InetSocketAddress, URI};
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.Records
import org.apache.hadoop.yarn.util.ConverterUtils
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import spark.{Logging, Utils}

class Client(conf: Configuration, args: ClientArguments) extends Logging {
  
  def this(args: ClientArguments) = this(new Configuration(), args)
  
  var applicationsManager : ClientRMProtocol = null
  var rpc : YarnRPC = YarnRPC.create(conf)
  
  def run() {
    connectToASM()
    
    val newApp = getNewApplication()
    val appId = newApp.getApplicationId()
    
    verifyClusterResources(newApp)
    val appContext = createApplicationSubmissionContext(appId)
    val localResources = prepareLocalResources(appId, "spark")
    val env = setupLaunchEnv(localResources)
    val amContainer = createContainerLaunchContext(localResources, env)
    
    appContext.setAMContainerSpec(amContainer)
    appContext.setUser(args.amUser)
    
    submitApp(appContext)  
    
    monitorApplication(appId)
    System.exit(0)
  }
  
  
  def connectToASM() {
    val yarnConf : YarnConfiguration = new YarnConfiguration()
    val rmAddress : InetSocketAddress = NetUtils.createSocketAddr(
      yarnConf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS)
    )
    logInfo("Connecting to ResourceManager at" + rmAddress)
    applicationsManager = rpc.getProxy(classOf[ClientRMProtocol], rmAddress, conf)
      .asInstanceOf[ClientRMProtocol]
  }
  
  def getNewApplication() : GetNewApplicationResponse = {
    logInfo("Requesting new Application")
    val request = Records.newRecord(classOf[GetNewApplicationRequest])
    val response = applicationsManager.getNewApplication(request)
    logInfo("Got new ApplicationId: " + response.getApplicationId())
    return response
  }
  
  def verifyClusterResources(app: GetNewApplicationResponse) = { 
    val maxMem = app.getMaximumResourceCapability().getMemory()
    logInfo("Max mem capabililty of resources in this cluster " + maxMem)
    
    // If the cluster does not have enough memory resources, exit.
    val requestedMem = args.amMemory + args.numWorkers * args.workerMemory
    if (requestedMem > maxMem) {
      logError("Cluster cannot satisfy memory resource request of " + requestedMem)
      System.exit(1)
    }
  }
  
  def createApplicationSubmissionContext(appId : ApplicationId) : ApplicationSubmissionContext = {
    logInfo("Setting up application submission context for ASM")
    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    appContext.setApplicationId(appId)
    appContext.setApplicationName("Spark")
    return appContext
  }
  
  def prepareLocalResources(appId : ApplicationId, appName : String) 
    : HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    val locaResources = HashMap[String, LocalResource]()
    // Upload Spark and the application JAR to the remote file system
    // Add them as local resources to the AM
    val fs = FileSystem.get(conf)
    Map("spark.jar" -> System.getenv("SPARK_JAR"), "app.jar" -> args.userJar)
    .foreach { case(destName, localPath) => 
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
    return locaResources
  }
  
  def setupLaunchEnv(localResources: HashMap[String, LocalResource]) : HashMap[String, String] = {
    logInfo("Setting up the launch environment")
    val env = new HashMap[String, String]()
    env("CLASSPATH") = "$CLASSPATH:./*:"
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
    // Add each SPARK-* key to the environment
    System.getenv().filterKeys(_.startsWith("SPARK")).foreach { case (k,v) => env(k) = v }
    return env
  }
  
  def createContainerLaunchContext(
      localResources: HashMap[String, LocalResource], 
      env: HashMap[String, String]) : ContainerLaunchContext = {
    logInfo("Setting up container launch context")
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources)
    amContainer.setEnvironment(env)
    
    // Extra options for the JVM
    var JAVA_OPTS = ""
    if (env.isDefinedAt("SPARK_JAVA_OPTS")) {
      JAVA_OPTS += env("SPARK_JAVA_OPTS") + " "
    }
    
    // Command for the ApplicationMaster
    val commands = List[String]("java spark.deploy.yarn.ApplicationMaster" + 
      " --class " + args.userClass + 
      " --jar " + args.userJar +
      " --args " + args.userArgs +
      " --worker-memory " + args.workerMemory +
      " --num-workers " + args.numWorkers +
      JAVA_OPTS +
      " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
    logInfo("Command for the ApplicationMaster: " + commands(0))
    amContainer.setCommands(commands)
    
    val capability = Records.newRecord(classOf[Resource]).asInstanceOf[Resource]
    // Memory for the ApplicationMaster
    capability.setMemory(args.amMemory)
    amContainer.setResource(capability)
    
    return amContainer
  }
  
  def submitApp(appContext: ApplicationSubmissionContext) = {
    // Create the request to send to the applications manager 
    val appRequest = Records.newRecord(classOf[SubmitApplicationRequest])
      .asInstanceOf[SubmitApplicationRequest]
    appRequest.setApplicationSubmissionContext(appContext)
    // Submit the application to the applications manager
    logInfo("Submitting application to ASM")
    applicationsManager.submitApplication(appRequest)
  }
  
  def monitorApplication(appId: ApplicationId) : Boolean = {  
    while(true) {
      Thread.sleep(1000)
      val reportRequest = Records.newRecord(classOf[GetApplicationReportRequest])
        .asInstanceOf[GetApplicationReportRequest]
      reportRequest.setApplicationId(appId)
      val reportResponse = applicationsManager.getApplicationReport(reportRequest)
      val report = reportResponse.getApplicationReport()
      
      logInfo("Application report from ASM: \n" +
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
    new Client(args).run
  }
}