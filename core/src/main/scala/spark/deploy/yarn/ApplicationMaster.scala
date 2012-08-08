package spark.deploy.yarn

import akka.actor._
import java.net.{InetSocketAddress, URI}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}
import spark.{Logging, Utils}
import spark.deploy.master._
import spark.util.AkkaUtils

class ApplicationMaster(userJar: String, userClass: String, userArgs: String, numWorkers : Int, workerMemory: Int, conf : Configuration) extends Logging {
  
  def this(userJar: String, userClass: String, userArgs: String, numWorkers : Int, workerMemory: Int) = this(userJar, userClass, userArgs, numWorkers, workerMemory, new Configuration())
  
  var rpc : YarnRPC = YarnRPC.create(conf)
  var resourceManager : AMRMProtocol = null
  var appAttemptId : ApplicationAttemptId = null
  val numWorkersRunning = new AtomicInteger()
  val numWorkersCompleted = new AtomicInteger()
  val lastResponseId = new AtomicInteger()
  
  def run = {
    
    // Initialization
    appAttemptId = getApplicationAttemptId
    resourceManager = registerWithResourceManager
    registerApplicationMaster
    
    // Start a standalone Spark Master
    startStandaloneSparkMaster
    
    // Keep polling the Resource Manager for containers
    // TODO: Can we make this nicer? Use actors?
    while(numWorkersRunning.intValue < numWorkers) {
      // Allocating resources for the Workers
      val amResp = allocateWorkerResources(numWorkers - numWorkersRunning.intValue).getAMResponse()
      val allocatedContainers = amResp.getAllocatedContainers
      println("Allocated " + allocatedContainers.size + " containers and cluster has resources: " + amResp.getAvailableResources)
      // Run each of the allocated containers
      for (container <- allocatedContainers) {
        // TODO: Best way to get master url and port?
        new Thread(new WorkerRunnable(container, conf, "spark://" + Utils.localIpAddress() + ":7077", workerMemory)).start
      }
      numWorkersRunning.addAndGet(allocatedContainers.size)
      Thread.sleep(1000)
    }
    
    // Wait until all containers have finished
    // TODO: Handle container failure
    var isDone = false
    while(!isDone) {
      println("Checking for completed containers")
      val amResp = allocateWorkerResources(0).getAMResponse()
      val completedContainers = amResp.getCompletedContainersStatuses()
      println("Found " + completedContainers.size + " completed containers")
      for(status <- completedContainers) {
        numWorkersCompleted.incrementAndGet()
      }
      if (numWorkersCompleted.get() == numWorkers) {
        isDone = true
      }
      Thread.sleep(100)
    }
    
    // Finish the Application
    finishApplicationMaster
    
  }
  
  def getApplicationAttemptId : ApplicationAttemptId = {
    val envs = System.getenv()
    val containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV)
    val containerId = ConverterUtils.toContainerId(containerIdString)
    val appAttemptId = containerId.getApplicationAttemptId()
    println("ApplicationAttemptId: " + appAttemptId)
    return appAttemptId
  }
  
  def registerWithResourceManager : AMRMProtocol = {
    val yarnConf = new YarnConfiguration(conf)
    val rmAddress = NetUtils.createSocketAddr(yarnConf.get(
      YarnConfiguration.RM_SCHEDULER_ADDRESS,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS))
    println("Connecting to ResourceManager at " + rmAddress)
    return rpc.getProxy(classOf[AMRMProtocol], rmAddress, conf).asInstanceOf[AMRMProtocol]
  }
  
  def registerApplicationMaster : RegisterApplicationMasterResponse = {
    println("Registering the ApplicationMaster")
    val appMasterRequest = Records.newRecord(classOf[RegisterApplicationMasterRequest]).asInstanceOf[RegisterApplicationMasterRequest]
    appMasterRequest.setApplicationAttemptId(appAttemptId)
    appMasterRequest.setHost("")
    appMasterRequest.setRpcPort(0)
    appMasterRequest.setTrackingUrl("")
    return resourceManager.registerApplicationMaster(appMasterRequest)
  }
  
  def startStandaloneSparkMaster = {
    val args = new MasterArguments(Array())
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", args.ip, args.port)
    val actor = actorSystem.actorOf(
      Props(new Master(args.ip, boundPort, args.webUiPort)), name = "Master")
  }
  
  def allocateWorkerResources(numWorkers: Int) : AllocateResponse = {
    println("Allocating " + numWorkers + " worker containers with " + workerMemory + " of memory each.")
    // We assume the client has already checked the cluster capabilities
    // Request numWorkers containers, each with workerMemory memory
    val rsrcRequest = Records.newRecord(classOf[ResourceRequest]).asInstanceOf[ResourceRequest]
    // Set the required memory
    val memCapability = Records.newRecord(classOf[Resource]).asInstanceOf[Resource]
    memCapability.setMemory(workerMemory)
    rsrcRequest.setCapability(memCapability)
    // Set the Priority
    // TODO: Make priority a command-line argument
    val pri = Records.newRecord(classOf[Priority]).asInstanceOf[Priority];
    pri.setPriority(0);
    rsrcRequest.setPriority(pri)
    rsrcRequest.setHostName("*")
    rsrcRequest.setNumContainers(numWorkers)
    
    
    val requestedContainers = List[ResourceRequest](rsrcRequest)
    val releasedContainers = List[ContainerId]()
    val req = Records.newRecord(classOf[AllocateRequest]).asInstanceOf[AllocateRequest]
    req.setResponseId(lastResponseId.incrementAndGet)
    req.addAllAsks(requestedContainers)
    req.addAllReleases(releasedContainers)
    req.setApplicationAttemptId(appAttemptId)
    val resp = resourceManager.allocate(req)
    println(resp.getAMResponse)
    return resp
  }
  
  def printContainers(containers : List[Container]) = {
    for (container <- containers) {
      println("Launching shell command on a new container."
        + ", containerId=" + container.getId()
        + ", containerNode=" + container.getNodeId().getHost() 
        + ":" + container.getNodeId().getPort()
        + ", containerNodeURI=" + container.getNodeHttpAddress()
        + ", containerState" + container.getState()
        + ", containerResourceMemory"  
        + container.getResource().getMemory())
    }
  }
  
  def finishApplicationMaster = { 
    val finishReq = Records.newRecord(classOf[FinishApplicationMasterRequest]).asInstanceOf[FinishApplicationMasterRequest]
    finishReq.setAppAttemptId(appAttemptId)
    // TODO: Check if the application failed or succeeded
    finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED)
    resourceManager.finishApplicationMaster(finishReq)
  }
  
 
}

object ApplicationMaster {
  def main(argStrings: Array[String]) {
    val args = new ApplicationMasterArguments(argStrings)
    new ApplicationMaster(args.userJar, args.userClass, args.userArgs, args.numWorkers, args.workerMemory).run
  }
}