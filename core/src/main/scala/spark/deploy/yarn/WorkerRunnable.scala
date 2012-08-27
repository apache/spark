package spark.deploy.yarn

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import spark.{Logging, Utils}

class WorkerRunnable(container: Container, conf: Configuration, masterAddress: String, 
    slaveId: String, hostname: String, workerMemory: Int) 
    extends Runnable with Logging {
  
  var rpc : YarnRPC = YarnRPC.create(conf)
  var cm : ContainerManager = null
  
  def run = {
    logInfo("Starting Worker Container")
    cm = connectToCM
    startContainer
  }
  
  def startContainer = {
    logInfo("Setting up ContainerLaunchContext")
    
    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
      .asInstanceOf[ContainerLaunchContext]
    
    ctx.setContainerId(container.getId())
    ctx.setResource(container.getResource())
    val localResources = prepareLocalResources
    ctx.setLocalResources(localResources)
    
    val env = prepareEnvironment
    ctx.setEnvironment(env)
    
    // Extra options for the JVM
    var JAVA_OPTS = ""
    // Set the JVM memory
    val workerMemoryString = workerMemory + "m"
    JAVA_OPTS += "-Xms" + workerMemoryString + " -Xmx" + workerMemoryString + " "
    if (env.isDefinedAt("SPARK_JAVA_OPTS")) {
      JAVA_OPTS += env("SPARK_JAVA_OPTS") + " "
    }
    
    ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName())
    val commands = List[String]("java " +
      JAVA_OPTS + 
      "spark.executor.StandaloneExecutorBackend " +
      masterAddress + " " +
      slaveId + " " +
      hostname + " " +
      "default " +
      
      " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
    logInfo("Setting up worker with commands: " + commands)
    ctx.setCommands(commands)
    
    // Send the start request to the ContainerManager
    val startReq = Records.newRecord(classOf[StartContainerRequest])
    .asInstanceOf[StartContainerRequest]
    startReq.setContainerLaunchContext(ctx)
    cm.startContainer(startReq)
  }
  
  
  def prepareLocalResources: HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    val locaResources = HashMap[String, LocalResource]()
    
    // Spark JAR
    val sparkJarResource = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
    sparkJarResource.setType(LocalResourceType.FILE)
    sparkJarResource.setVisibility(LocalResourceVisibility.APPLICATION)
    sparkJarResource.setResource(ConverterUtils.getYarnUrlFromURI(
      new URI(System.getenv("SPARK_YARN_JAR_PATH"))))
    sparkJarResource.setTimestamp(System.getenv("SPARK_YARN_JAR_TIMESTAMP").toLong)
    sparkJarResource.setSize(System.getenv("SPARK_YARN_JAR_SIZE").toLong)
    locaResources("spark.jar") = sparkJarResource
    // User JAR
    val userJarResource = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
    userJarResource.setType(LocalResourceType.FILE)
    userJarResource.setVisibility(LocalResourceVisibility.APPLICATION)
    userJarResource.setResource(ConverterUtils.getYarnUrlFromURI(
      new URI(System.getenv("SPARK_YARN_USERJAR_PATH"))))
    userJarResource.setTimestamp(System.getenv("SPARK_YARN_USERJAR_TIMESTAMP").toLong)
    userJarResource.setSize(System.getenv("SPARK_YARN_USERJAR_SIZE").toLong)
    locaResources("app.jar") = userJarResource
    
    logInfo("Prepared Local resources " + locaResources)
    return locaResources
  }
  
  def prepareEnvironment : HashMap[String, String] = {
    val env = new HashMap[String, String]()
    env("CLASSPATH") = "$CLASSPATH:./*:"
    System.getenv().filterKeys(_.startsWith("SPARK")).foreach { case (k,v) => env(k) = v }
    return env
  }
  
  def connectToCM : ContainerManager = {
    val cmIpPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
    val cmAddress = NetUtils.createSocketAddr(cmIpPortStr)
    logInfo("Connecting to ContainerManager at " + cmIpPortStr)
    return rpc.getProxy(classOf[ContainerManager], cmAddress, conf).asInstanceOf[ContainerManager]
  }
  
} 