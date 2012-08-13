package spark.deploy.yarn

import java.net.{InetSocketAddress, URI}
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import spark.{Logging, Utils}
import spark.scheduler.cluster.StandaloneSchedulerBackend

class ApplicationMaster(args: ApplicationMasterArguments, conf : Configuration) extends Logging {
  
  def this(args: ApplicationMasterArguments) = this(args, new Configuration())
  
  var rpc : YarnRPC = YarnRPC.create(conf)
  var resourceManager : AMRMProtocol = null
  var appAttemptId : ApplicationAttemptId = null
  val numWorkersRunning = new AtomicInteger()
  val numWorkersConnected = new AtomicInteger()
  val lastResponseId = new AtomicInteger()
  
  def run = {
    
    // Initialization
    appAttemptId = getApplicationAttemptId()
    resourceManager = registerWithResourceManager()
    registerApplicationMaster()
    
    // Start the user's JAR
    val userThread = startUserClass()
    
    // This is pretty hacky, but we need to wait until the spark.master.port property has
    // been set by the Thread executing the user class.
    while (System.getProperty("spark.master.port") == null 
      || System.getProperty("spark.master.port").toInt == 0) {
      Thread.sleep(10)
    }
    logInfo("Port: " + System.getProperty("spark.master.port"))
    
    // Allocate all containers
    allocateWorkers()
    
    // Wait for the user class to Finish     
    userThread.join()
     
    // Finish the ApplicationMaster
    finishApplicationMaster()
    // TODO: Exit based on success/failure
    System.exit(0)
  }
  
  def getApplicationAttemptId() : ApplicationAttemptId = {
    val envs = System.getenv()
    val containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV)
    val containerId = ConverterUtils.toContainerId(containerIdString)
    val appAttemptId = containerId.getApplicationAttemptId()
    logInfo("ApplicationAttemptId: " + appAttemptId)
    return appAttemptId
  }
  
  def registerWithResourceManager() : AMRMProtocol = {
    val yarnConf = new YarnConfiguration(conf)
    val rmAddress = NetUtils.createSocketAddr(yarnConf.get(
      YarnConfiguration.RM_SCHEDULER_ADDRESS,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS))
    logInfo("Connecting to ResourceManager at " + rmAddress)
    return rpc.getProxy(classOf[AMRMProtocol], rmAddress, conf).asInstanceOf[AMRMProtocol]
  }
  
  def registerApplicationMaster() : RegisterApplicationMasterResponse = {
    logInfo("Registering the ApplicationMaster")
    val appMasterRequest = Records.newRecord(classOf[RegisterApplicationMasterRequest])
      .asInstanceOf[RegisterApplicationMasterRequest]
    appMasterRequest.setApplicationAttemptId(appAttemptId)
    appMasterRequest.setHost("")
    appMasterRequest.setRpcPort(0)
    appMasterRequest.setTrackingUrl("")
    return resourceManager.registerApplicationMaster(appMasterRequest)
  }
  
  def startUserClass() : Thread  = {
    logInfo("Starting the user JAR in a separate Thread")
    val mainMethod = Class.forName(args.userClass, false, Thread.currentThread.getContextClassLoader)
      .getMethod("main", classOf[Array[String]])
    val t = new Thread {
      override def run() {
        mainMethod.invoke(null, Array[String]("standalone"))
      }
    }
    t.start()
    return t
  }
  
  def allocateWorkers() {
    logInfo("Allocating " + args.numWorkers + " workers.")
    // Wait until all containers have finished
    // TODO: This is a bit ugly. Can we make it nicer?
    // TODO: Handle container failure
    while(numWorkersRunning.intValue < args.numWorkers) {
      // Keep polling the Resource Manager for containers
      val workersToRequest = math.max(args.numWorkers - numWorkersRunning.intValue, 0)
      val amResp = allocateWorkerResources(workersToRequest).getAMResponse()
      val allocatedContainers = amResp.getAllocatedContainers
      if (allocatedContainers.size > 0) {
       logInfo("Allocated " + allocatedContainers.size + " containers.")
       logInfo("Cluster Resources: " + amResp.getAvailableResources)
        // Run each of the allocated containers
        for (container <- allocatedContainers) {
          val masterUrl = "akka://spark@%s:%s/user/%s".format(
            System.getProperty("spark.master.host"), System.getProperty("spark.master.port"),
            StandaloneSchedulerBackend.ACTOR_NAME)
          val workerId = numWorkersRunning.intValue.toString
          val workerHostname = container.getNodeId().getHost()
          // YARN does not support requesting resources by the number of cores yet.
          // TODO: How do we handle this?
          // val workerCores = 1
          new Thread(
            new WorkerRunnable(container, conf, masterUrl, workerId, 
              workerHostname, args.workerMemory)
          ).start()
          numWorkersRunning.incrementAndGet()
        }
        
      }
      Thread.sleep(100)
    }
    logInfo("All workers have launched.")
  }
  
  def allocateWorkerResources(numWorkers: Int) : AllocateResponse = {
    logInfo("Allocating " + numWorkers + " worker containers with " 
      + args.workerMemory + " of memory each.")
    // We assume the client has already checked the cluster capabilities
    // Request numWorkers containers, each with workerMemory memory
    val rsrcRequest = Records.newRecord(classOf[ResourceRequest]).asInstanceOf[ResourceRequest]
    // Set the required memory
    val memCapability = Records.newRecord(classOf[Resource]).asInstanceOf[Resource]
    // There probably is some overhead here, let's reserve a bit more memory.
    memCapability.setMemory(args.workerMemory + 128)
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
    return resp
  }
  
  def printContainers(containers : List[Container]) = {
    for (container <- containers) {
      logInfo("Launching shell command on a new container."
        + ", containerId=" + container.getId()
        + ", containerNode=" + container.getNodeId().getHost() 
        + ":" + container.getNodeId().getPort()
        + ", containerNodeURI=" + container.getNodeHttpAddress()
        + ", containerState" + container.getState()
        + ", containerResourceMemory"  
        + container.getResource().getMemory())
    }
  }
  
  def finishApplicationMaster() { 
    val finishReq = Records.newRecord(classOf[FinishApplicationMasterRequest])
      .asInstanceOf[FinishApplicationMasterRequest]
    finishReq.setAppAttemptId(appAttemptId)
    // TODO: Check if the application has failed or succeeded
    finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED)
    resourceManager.finishApplicationMaster(finishReq)
  }
 
}

object ApplicationMaster {
  def main(argStrings: Array[String]) {
    val args = new ApplicationMasterArguments(argStrings)
    new ApplicationMaster(args).run
  }
}