package spark.deploy.yarn

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap



class WorkerRunnable(container: Container, conf: Configuration, masterAddress: String, workerMemory: Int) extends Runnable {
  
  var rpc : YarnRPC = YarnRPC.create(conf)
  var cm : ContainerManager = null
  
  def run = {
    println("Starting Worker Container")
    cm = conntectToCM
    startContainer
  }
  
  def startContainer = {
    println("Setting up ContainerLaunchContext")
    
    val ctx = Records.newRecord(classOf[ContainerLaunchContext]).asInstanceOf[ContainerLaunchContext]
    
    ctx.setContainerId(container.getId())
    ctx.setResource(container.getResource())
    val localResources = prepareLocalResources
    ctx.setLocalResources(localResources)
    ctx.setEnvironment(Map("CLASSPATH" -> "$CLASSPATH:./*:"))
    
    // TODO Get the user
    ctx.setUser("dennybritz")
    val commands = List[String]("java spark.deploy.worker.Worker " +
      masterAddress +
      " --memory " + workerMemory +  
      " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
    println("Setting up worker with commands: " + commands)
    ctx.setCommands(commands)
    
    // Send the start request to the ContainerManager
    val startReq = Records.newRecord(classOf[StartContainerRequest]).asInstanceOf[StartContainerRequest]
    startReq.setContainerLaunchContext(ctx)
    cm.startContainer(startReq)
  }
  
  
  def prepareLocalResources: HashMap[String, LocalResource] = {
    println("Preparing Local resources")
    val locaResources = HashMap[String, LocalResource]()
    
    // Spark JAR
    val sparkJarResource = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
    sparkJarResource.setType(LocalResourceType.FILE)
    sparkJarResource.setVisibility(LocalResourceVisibility.APPLICATION)
    sparkJarResource.setResource(ConverterUtils.getYarnUrlFromURI(new URI(System.getenv("SPARK_YARN_JAR_PATH"))))
    sparkJarResource.setTimestamp(System.getenv("SPARK_YARN_JAR_TIMESTAMP").toLong)
    sparkJarResource.setSize(System.getenv("SPARK_YARN_JAR_SIZE").toLong)
    locaResources("spark.jar") = sparkJarResource
    // User JAR
    val userJarResource = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
    userJarResource.setType(LocalResourceType.FILE)
    userJarResource.setVisibility(LocalResourceVisibility.APPLICATION)
    userJarResource.setResource(ConverterUtils.getYarnUrlFromURI(new URI(System.getenv("SPARK_YARN_USERJAR_PATH"))))
    userJarResource.setTimestamp(System.getenv("SPARK_YARN_USERJAR_TIMESTAMP").toLong)
    userJarResource.setSize(System.getenv("SPARK_YARN_USERJAR_SIZE").toLong)
    locaResources("app.jar") = userJarResource
    
    println("Prepared Local resources " + locaResources)
    return locaResources
  }
  
  
  def conntectToCM : ContainerManager = {
    val cmIpPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
    val cmAddress = NetUtils.createSocketAddr(cmIpPortStr)
    println("Connecting to ContainerManager at " + cmIpPortStr)
    return rpc.getProxy(classOf[ContainerManager], cmAddress, conf).asInstanceOf[ContainerManager]
  }
  
} 