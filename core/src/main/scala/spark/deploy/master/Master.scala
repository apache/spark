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


private[spark] class Master(ip: String, port: Int, webUiPort: Int) extends Actor with Logging {
  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For job IDs
  val WORKER_TIMEOUT = System.getProperty("spark.worker.timeout", "60").toLong * 1000

  var nextJobNumber = 0
  val workers = new HashSet[WorkerInfo]
  val idToWorker = new HashMap[String, WorkerInfo]
  val actorToWorker = new HashMap[ActorRef, WorkerInfo]
  val addressToWorker = new HashMap[Address, WorkerInfo]

  val jobs = new HashSet[JobInfo]
  val idToJob = new HashMap[String, JobInfo]
  val actorToJob = new HashMap[ActorRef, JobInfo]
  val addressToJob = new HashMap[Address, JobInfo]

  val waitingJobs = new ArrayBuffer[JobInfo]
  val completedJobs = new ArrayBuffer[JobInfo]

  val masterPublicAddress = {
    val envVar = System.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else ip
  }

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each job
  // among all the nodes) instead of trying to consolidate each job onto a small # of nodes.
  val spreadOutJobs = System.getProperty("spark.deploy.spreadOut", "false").toBoolean

  override def preStart() {
    logInfo("Starting Spark master at spark://" + ip + ":" + port)
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

    case RegisterJob(description) => {
      logInfo("Registering job " + description.name)
      val job = addJob(description, sender)
      logInfo("Registered job " + description.name + " with ID " + job.id)
      waitingJobs += job
      context.watch(sender)  // This doesn't work with remote actors but helps for testing
      sender ! RegisteredJob(job.id)
      schedule()
    }

    case ExecutorStateChanged(jobId, execId, state, message) => {
      val execOption = idToJob.get(jobId).flatMap(job => job.executors.get(execId))
      execOption match {
        case Some(exec) => {
          exec.state = state
          exec.job.actor ! ExecutorUpdated(execId, state, message)
          if (ExecutorState.isFinished(state)) {
            val jobInfo = idToJob(jobId)
            // Remove this executor from the worker and job
            logInfo("Removing executor " + exec.fullId + " because it is " + state)
            jobInfo.removeExecutor(exec)
            exec.worker.removeExecutor(exec)

            // Only retry certain number of times so we don't go into an infinite loop.
            if (jobInfo.incrementRetryCount <= JobState.MAX_NUM_RETRY) {
              schedule()
            } else {
              val e = new SparkException("Job %s wth ID %s failed %d times.".format(
                jobInfo.desc.name, jobInfo.id, jobInfo.retryCount))
              logError(e.getMessage, e)
              throw e
              //System.exit(1)
            }
          }
        }
        case None =>
          logWarning("Got status update for unknown executor " + jobId + "/" + execId)
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
      // The disconnected actor could've been either a worker or a job; remove whichever of
      // those we have an entry for in the corresponding actor hashmap
      actorToWorker.get(actor).foreach(removeWorker)
      actorToJob.get(actor).foreach(removeJob)
    }

    case RemoteClientDisconnected(transport, address) => {
      // The disconnected client could've been either a worker or a job; remove whichever it was
      addressToWorker.get(address).foreach(removeWorker)
      addressToJob.get(address).foreach(removeJob)
    }

    case RemoteClientShutdown(transport, address) => {
      // The disconnected client could've been either a worker or a job; remove whichever it was
      addressToWorker.get(address).foreach(removeWorker)
      addressToJob.get(address).foreach(removeJob)
    }

    case RequestMasterState => {
      sender ! MasterState(ip + ":" + port, workers.toArray, jobs.toArray, completedJobs.toArray)
    }
  }

  /**
   * Can a job use the given worker? True if the worker has enough memory and we haven't already
   * launched an executor for the job on it (right now the standalone backend doesn't like having
   * two executors on the same worker).
   */
  def canUse(job: JobInfo, worker: WorkerInfo): Boolean = {
    worker.memoryFree >= job.desc.memoryPerSlave && !worker.hasExecutor(job)
  }

  /**
   * Schedule the currently available resources among waiting jobs. This method will be called
   * every time a new job joins or resource availability changes.
   */
  def schedule() {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first job
    // in the queue, then the second job, etc.
    if (spreadOutJobs) {
      // Try to spread out each job among all the nodes, until it has all its cores
      for (job <- waitingJobs if job.coresLeft > 0) {
        val usableWorkers = workers.toArray.filter(canUse(job, _)).sortBy(_.coresFree).reverse
        val numUsable = usableWorkers.length
        val assigned = new Array[Int](numUsable) // Number of cores to give on each node
        var toAssign = math.min(job.coresLeft, usableWorkers.map(_.coresFree).sum)
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
            val exec = job.addExecutor(usableWorkers(pos), assigned(pos))
            launchExecutor(usableWorkers(pos), exec, job.desc.sparkHome)
            job.state = JobState.RUNNING
          }
        }
      }
    } else {
      // Pack each job into as few nodes as possible until we've assigned all its cores
      for (worker <- workers if worker.coresFree > 0) {
        for (job <- waitingJobs if job.coresLeft > 0) {
          if (canUse(job, worker)) {
            val coresToUse = math.min(worker.coresFree, job.coresLeft)
            if (coresToUse > 0) {
              val exec = job.addExecutor(worker, coresToUse)
              launchExecutor(worker, exec, job.desc.sparkHome)
              job.state = JobState.RUNNING
            }
          }
        }
      }
    }
  }

  def launchExecutor(worker: WorkerInfo, exec: ExecutorInfo, sparkHome: String) {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    worker.addExecutor(exec)
    worker.actor ! LaunchExecutor(exec.job.id, exec.id, exec.job.desc, exec.cores, exec.memory, sparkHome)
    exec.job.actor ! ExecutorAdded(exec.id, worker.id, worker.host, exec.cores, exec.memory)
  }

  def addWorker(id: String, host: String, port: Int, cores: Int, memory: Int, webUiPort: Int,
    publicAddress: String): WorkerInfo = {
    val worker = new WorkerInfo(id, host, port, cores, memory, sender, webUiPort, publicAddress)
    workers += worker
    idToWorker(worker.id) = worker
    actorToWorker(sender) = worker
    addressToWorker(sender.path.address) = worker
    return worker
  }

  def removeWorker(worker: WorkerInfo) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    workers -= worker
    idToWorker -= worker.id
    actorToWorker -= worker.actor
    addressToWorker -= worker.actor.path.address
    for (exec <- worker.executors.values) {
      logInfo("Telling job of lost executor: " + exec.id)
      exec.job.actor ! ExecutorUpdated(exec.id, ExecutorState.LOST, Some("worker lost"))
      exec.job.removeExecutor(exec)
    }
  }

  def addJob(desc: JobDescription, actor: ActorRef): JobInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val job = new JobInfo(now, newJobId(date), desc, date, actor)
    jobs += job
    idToJob(job.id) = job
    actorToJob(sender) = job
    addressToJob(sender.path.address) = job
    return job
  }

  def removeJob(job: JobInfo) {
    if (jobs.contains(job)) {
      logInfo("Removing job " + job.id)
      jobs -= job
      idToJob -= job.id
      actorToJob -= job.actor
      addressToWorker -= job.actor.path.address
      completedJobs += job   // Remember it in our history
      waitingJobs -= job
      for (exec <- job.executors.values) {
        exec.worker.removeExecutor(exec)
        exec.worker.actor ! KillExecutor(exec.job.id, exec.id)
      }
      job.markFinished(JobState.FINISHED)  // TODO: Mark it as FAILED if it failed
      schedule()
    }
  }

  /** Generate a new job ID given a job's submission date */
  def newJobId(submitDate: Date): String = {
    val jobId = "job-%s-%04d".format(DATE_FORMAT.format(submitDate), nextJobNumber)
    nextJobNumber += 1
    jobId
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
  def main(argStrings: Array[String]) {
    val args = new MasterArguments(argStrings)
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", args.ip, args.port)
    val actor = actorSystem.actorOf(
      Props(new Master(args.ip, boundPort, args.webUiPort)), name = "Master")
    actorSystem.awaitTermination()
  }
}
