package spark.deploy.yarn

import java.net.{InetSocketAddress, URI};
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords.{GetNewApplicationRequest, GetNewApplicationResponse, SubmitApplicationRequest}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.Records
import org.apache.hadoop.yarn.util.ConverterUtils
import spark.{Logging, Utils}

class Client(conf: Configuration, args: ClientArguments) extends Logging {
  
  def this(args: ClientArguments) = this(new Configuration(), args)
  
  var applicationsManager : ClientRMProtocol = null
  var rpc : YarnRPC = YarnRPC.create(conf)
  
  
  def run = {
    connectToASM
    
    val newApp = getNewApplication
    val appId = newApp.getApplicationId
    
    verifyClusterResources(newApp)
    val appContext = createApplicationSubmissionContext(appId)
    val localResources = prepareLocalResources(appId, "spark")
    val env = setupLaunchEnvironment(localResources)
    val amContainer = createContainerLaunchContext(localResources, env)
    
    appContext.setAMContainerSpec(amContainer)
    // TODO: Get the user from the command line
    appContext.setUser("dennybritz")
    
    // Submit the application to the applications manager
    submitApp(appContext)  
    
    System.exit(0)
  }
  
  
  def connectToASM  = {
    val yarnConf : YarnConfiguration = new YarnConfiguration()
    val rmAddress : InetSocketAddress = NetUtils.createSocketAddr(
      yarnConf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS)
    )
    println("Connecting to ResourceManager at" + rmAddress)
    applicationsManager = rpc.getProxy(classOf[ClientRMProtocol], rmAddress, conf)
      .asInstanceOf[ClientRMProtocol]
  }
  
  def getNewApplication : GetNewApplicationResponse = {
    println("Requesting new Application")
    val request = Records.newRecord(classOf[GetNewApplicationRequest])
    val response = applicationsManager.getNewApplication(request)
    println("Got new ApplicationId: " + response.getApplicationId())
    return response
  }
  
  def verifyClusterResources(app: GetNewApplicationResponse) = { 
    val maxMem = app.getMaximumResourceCapability().getMemory()
    println("Max mem capabililty of resources in this cluster " + maxMem)
    
    // If the cluster does not have enough memory resources, exit.
    val requestedMem = args.amMemory + args.numWorkers * args.workerMemory
    if (requestedMem > maxMem) {
      logError("Cluster cannot satisfy memory resource request of " + requestedMem)
      System.exit(1)
    }
  }
  
  def createApplicationSubmissionContext(appId : ApplicationId) : ApplicationSubmissionContext = {
    println("Setting up application submission context for ASM")
    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    appContext.setApplicationId(appId)
    appContext.setApplicationName("Spark")
    return appContext
  }
  
  def prepareLocalResources(appId : ApplicationId, appName : String) : HashMap[String, LocalResource] = {
    println("Preparing Local resources")
    val locaResources = HashMap[String, LocalResource]()
    // Upload Spark and the application JAR to the remote file system
    // Add them as local resources to the AM
    val fs = FileSystem.get(conf)
    Map("spark.jar" -> System.getenv("SPARK_JAR"), "app.jar" -> args.userJar).foreach { case(destName, localPath) => 
      val src = new Path(localPath)
      val pathSuffix = appName + "/" + appId.getId() + destName
      val dst = new Path(fs.getHomeDirectory(), pathSuffix)
      println("Uploading " + src + " to " + dst)
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
  
  def setupLaunchEnvironment(localResources: HashMap[String, LocalResource]) : HashMap[String, String] = {
    println("Setting up the launch environment")
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
    return env
  }
  
  def createContainerLaunchContext(localResources: HashMap[String, LocalResource], env: HashMap[String, String]) : ContainerLaunchContext = {
    println("Setting up container launch context")
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources)
    amContainer.setEnvironment(env)

    // Command for the ApplicationMaster
    val commands = List[String]("java spark.deploy.yarn.ApplicationMaster" + 
      " --class " + args.userClass + 
      " --jar " + args.userJar +
      " --args " + args.userArgs +
      " --worker-memory " + args.workerMemory +
      " --num-workers " + args.numWorkers +
      " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
    println("Command for the ApplicationMaster: " + commands(0))
    amContainer.setCommands(commands)
    
    val capability = Records.newRecord(classOf[Resource]).asInstanceOf[Resource]
    // Memory for the ApplicationMaster
    capability.setMemory(args.amMemory)
    amContainer.setResource(capability)
    
    return amContainer
  }
  
  def submitApp(appContext: ApplicationSubmissionContext) = {
    // Create the request to send to the applications manager 
    val appRequest = Records.newRecord(classOf[SubmitApplicationRequest]).asInstanceOf[SubmitApplicationRequest]
    appRequest.setApplicationSubmissionContext(appContext)
    // Submit the application to the applications manager
    println("Submitting application to ASM")
    applicationsManager.submitApplication(appRequest)
  }
  
}

object Client {
  def main(argStrings: Array[String]) {
    val args = new ClientArguments(argStrings)
    new Client(args).run
  }
}