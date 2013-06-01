package spark.deploy.master

import akka.actor._
import akka.actor.Terminated
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientDisconnected, RemoteClientShutdown}
import akka.util.duration._

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import spark.deploy._
import spark.{Logging, SparkException, Utils}
import spark.util.AkkaUtils


private[spark] class Master(host: String, port: Int, webUiPort: Int) extends Actor with Logging {
  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For application IDs
  val WORKER_TIMEOUT = System.getProperty("spark.worker.timeout", "60").toLong * 1000

  var nextAppNumber = 0
  val workers = new HashSet[WorkerInfo]
  val idToWorker = new HashMap[String, WorkerInfo]
  val actorToWorker = new HashMap[ActorRef, WorkerInfo]
  val addressToWorker = new HashMap[Address, WorkerInfo]

  val apps = new HashSet[ApplicationInfo]
  val idToApp = new HashMap[String, ApplicationInfo]
  val actorToApp = new HashMap[ActorRef, ApplicationInfo]
  val addressToApp = new HashMap[Address, ApplicationInfo]

  val waitingApps = new ArrayBuffer[ApplicationInfo]
  val completedApps = new ArrayBuffer[ApplicationInfo]

  var firstApp: Option[ApplicationInfo] = None

  Utils.checkHost(host, "Expected hostname")

  val masterPublicAddress = {
    val envVar = System.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  val spreadOutApps = System.getProperty("spark.deploy.spreadOut", "true").toBoolean

  override def preStart() {
    logInfo("Starting Spark master at spark://" + host + ":" + port)
    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
    startWebUi()
    context.system.scheduler.schedule(0 millis, WORKER_TIMEOUT millis)(timeOutDeadWorkers())
  }

  def startWebUi() {
    val webUi = new MasterWebUI(context.system, self)
    try {
      AkkaUtils.startSprayServer(context.system, "0.0.0.0", webUiPort, webUi.handler)
    } catch {
      case e: Exception =>
        logError("Failed to create web UI", e)
        System.exit(1)
    }
  }

  override def receive = {
    case RegisterWorker(id, host, workerPort, cores, memory, worker_webUiPort, publicAddress) => {
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        host, workerPort, cores, Utils.memoryMegabytesToString(memory)))
      if (idToWorker.contains(id)) {
        sender ! RegisterWorkerFailed("Duplicate worker ID")
      } else {
        addWorker(id, host, workerPort, cores, memory, worker_webUiPort, publicAddress)
        context.watch(sender)  // This doesn't work with remote actors but helps for testing
        sender ! RegisteredWorker("http://" + masterPublicAddress + ":" + webUiPort)
        schedule()
      }
    }

    case RegisterApplication(description) => {
      logInfo("Registering app " + description.name)
      val app = addApplication(description, sender)
      logInfo("Registered app " + description.name + " with ID " + app.id)
      waitingApps += app
      context.watch(sender)  // This doesn't work with remote actors but helps for testing
      sender ! RegisteredApplication(app.id)
      schedule()
    }

    case ExecutorStateChanged(appId, execId, state, message, exitStatus) => {
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        case Some(exec) => {
          exec.state = state
          exec.application.driver ! ExecutorUpdated(execId, state, message, exitStatus)
          if (ExecutorState.isFinished(state)) {
            val appInfo = idToApp(appId)
            // Remove this executor from the worker and app
            logInfo("Removing executor " + exec.fullId + " because it is " + state)
            appInfo.removeExecutor(exec)
            exec.worker.removeExecutor(exec)

            // Only retry certain number of times so we don't go into an infinite loop.
            if (appInfo.incrementRetryCount < ApplicationState.MAX_NUM_RETRY) {
              schedule()
            } else {
              logError("Application %s with ID %s failed %d times, removing it".format(
                appInfo.desc.name, appInfo.id, appInfo.retryCount))
              removeApplication(appInfo, ApplicationState.FAILED)
            }
          }
        }
        case None =>
          logWarning("Got status update for unknown executor " + appId + "/" + execId)
      }
    }

    case Heartbeat(workerId) => {
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          logWarning("Got heartbeat from unregistered worker " + workerId)
      }
    }

    case Terminated(actor) => {
      // The disconnected actor could've been either a worker or an app; remove whichever of
      // those we have an entry for in the corresponding actor hashmap
      actorToWorker.get(actor).foreach(removeWorker)
      actorToApp.get(actor).foreach(finishApplication)
    }

    case RemoteClientDisconnected(transport, address) => {
      // The disconnected client could've been either a worker or an app; remove whichever it was
      addressToWorker.get(address).foreach(removeWorker)
      addressToApp.get(address).foreach(finishApplication)
    }

    case RemoteClientShutdown(transport, address) => {
      // The disconnected client could've been either a worker or an app; remove whichever it was
      addressToWorker.get(address).foreach(removeWorker)
      addressToApp.get(address).foreach(finishApplication)
    }

    case RequestMasterState => {
      sender ! MasterState(host, port, workers.toArray, apps.toArray, completedApps.toArray)
    }
  }

  /**
   * Can an app use the given worker? True if the worker has enough memory and we haven't already
   * launched an executor for the app on it (right now the standalone backend doesn't like having
   * two executors on the same worker).
   */
  def canUse(app: ApplicationInfo, worker: WorkerInfo): Boolean = {
    worker.memoryFree >= app.desc.memoryPerSlave && !worker.hasExecutor(app)
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   */
  def schedule() {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    if (spreadOutApps) {
      // Try to spread out each app among all the nodes, until it has all its cores
      for (app <- waitingApps if app.coresLeft > 0) {
        val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
                                   .filter(canUse(app, _)).sortBy(_.coresFree).reverse
        val numUsable = usableWorkers.length
        val assigned = new Array[Int](numUsable) // Number of cores to give on each node
        var toAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
        var pos = 0
        while (toAssign > 0) {
          if (usableWorkers(pos).coresFree - assigned(pos) > 0) {
            toAssign -= 1
            assigned(pos) += 1
          }
          pos = (pos + 1) % numUsable
        }
        // Now that we've decided how many cores to give on each node, let's actually give them
        for (pos <- 0 until numUsable) {
          if (assigned(pos) > 0) {
            val exec = app.addExecutor(usableWorkers(pos), assigned(pos))
            launchExecutor(usableWorkers(pos), exec, app.desc.sparkHome)
            app.state = ApplicationState.RUNNING
          }
        }
      }
    } else {
      // Pack each app into as few nodes as possible until we've assigned all its cores
      for (worker <- workers if worker.coresFree > 0 && worker.state == WorkerState.ALIVE) {
        for (app <- waitingApps if app.coresLeft > 0) {
          if (canUse(app, worker)) {
            val coresToUse = math.min(worker.coresFree, app.coresLeft)
            if (coresToUse > 0) {
              val exec = app.addExecutor(worker, coresToUse)
              launchExecutor(worker, exec, app.desc.sparkHome)
              app.state = ApplicationState.RUNNING
            }
          }
        }
      }
    }
  }

  def launchExecutor(worker: WorkerInfo, exec: ExecutorInfo, sparkHome: String) {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    worker.addExecutor(exec)
    worker.actor ! LaunchExecutor(exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory, sparkHome)
    exec.application.driver ! ExecutorAdded(exec.id, worker.id, worker.hostPort, exec.cores, exec.memory)
  }

  def addWorker(id: String, host: String, port: Int, cores: Int, memory: Int, webUiPort: Int,
    publicAddress: String): WorkerInfo = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's), remove them.
    workers.filter(w => (w.host == host && w.port == port) && (w.state == WorkerState.DEAD)).foreach(workers -= _)
    val worker = new WorkerInfo(id, host, port, cores, memory, sender, webUiPort, publicAddress)
    workers += worker
    idToWorker(worker.id) = worker
    actorToWorker(sender) = worker
    addressToWorker(sender.path.address) = worker
    return worker
  }

  def removeWorker(worker: WorkerInfo) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    actorToWorker -= worker.actor
    addressToWorker -= worker.actor.path.address
    for (exec <- worker.executors.values) {
      logInfo("Telling app of lost executor: " + exec.id)
      exec.application.driver ! ExecutorUpdated(exec.id, ExecutorState.LOST, Some("worker lost"), None)
      exec.application.removeExecutor(exec)
    }
  }

  def addApplication(desc: ApplicationDescription, driver: ActorRef): ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val app = new ApplicationInfo(now, newApplicationId(date), desc, date, driver, desc.appUiUrl)
    apps += app
    idToApp(app.id) = app
    actorToApp(driver) = app
    addressToApp(driver.path.address) = app
    if (firstApp == None) {
      firstApp = Some(app)
    }
    val workersAlive = workers.filter(_.state == WorkerState.ALIVE).toArray
    if (workersAlive.size > 0 && !workersAlive.exists(_.memoryFree >= desc.memoryPerSlave)) {
      logWarning("Could not find any workers with enough memory for " + firstApp.get.id)
    }
    return app
  }

  def finishApplication(app: ApplicationInfo) {
    removeApplication(app, ApplicationState.FINISHED)
  }

  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      actorToApp -= app.driver
      addressToApp -= app.driver.path.address
      completedApps += app   // Remember it in our history
      waitingApps -= app
      for (exec <- app.executors.values) {
        exec.worker.removeExecutor(exec)
        exec.worker.actor ! KillExecutor(exec.application.id, exec.id)
        exec.state = ExecutorState.KILLED
      }
      app.markFinished(state)
      app.driver ! ApplicationRemoved(state.toString)
      schedule()
    }
  }

  /** Generate a new app ID given a app's submission date */
  def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(DATE_FORMAT.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers */
  def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val expirationTime = System.currentTimeMillis() - WORKER_TIMEOUT
    val toRemove = workers.filter(_.lastHeartbeat < expirationTime).toArray
    for (worker <- toRemove) {
      logWarning("Removing %s because we got no heartbeat in %d seconds".format(
        worker.id, WORKER_TIMEOUT))
      removeWorker(worker)
    }
  }
}

private[spark] object Master {
  private val systemName = "sparkMaster"
  private val actorName = "Master"
  private val sparkUrlRegex = "spark://([^:]+):([0-9]+)".r

  def main(argStrings: Array[String]) {
    val args = new MasterArguments(argStrings)
    val (actorSystem, _) = startSystemAndActor(args.host, args.port, args.webUiPort)
    actorSystem.awaitTermination()
  }

  /** Returns an `akka://...` URL for the Master actor given a sparkUrl `spark://host:ip`. */
  def toAkkaUrl(sparkUrl: String): String = {
    sparkUrl match {
      case sparkUrlRegex(host, port) =>
        "akka://%s@%s:%s/user/%s".format(systemName, host, port, actorName)
      case _ =>
        throw new SparkException("Invalid master URL: " + sparkUrl)
    }
  }

  def startSystemAndActor(host: String, port: Int, webUiPort: Int): (ActorSystem, Int) = {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port)
    val actor = actorSystem.actorOf(Props(new Master(host, boundPort, webUiPort)), name = actorName)
    (actorSystem, boundPort)
  }
}
