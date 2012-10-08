package spark.deploy.master

import akka.actor._
import akka.actor.Terminated
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientDisconnected, RemoteClientShutdown}

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import spark.deploy._
import spark.{Logging, SparkException, Utils}
import spark.util.AkkaUtils


private[spark] class Master(ip: String, port: Int, webUiPort: Int) extends Actor with Logging {
  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For job IDs

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

  override def preStart() {
    logInfo("Starting Spark master at spark://" + ip + ":" + port)
    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
    startWebUi()
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
    case RegisterWorker(id, host, workerPort, cores, memory, worker_webUiPort) => {
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        host, workerPort, cores, Utils.memoryMegabytesToString(memory)))
      if (idToWorker.contains(id)) {
        sender ! RegisterWorkerFailed("Duplicate worker ID")
      } else {
        addWorker(id, host, workerPort, cores, memory, worker_webUiPort)
        context.watch(sender)  // This doesn't work with remote actors but helps for testing
        sender ! RegisteredWorker("http://" + ip + ":" + webUiPort)
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
      sender ! MasterState(ip + ":" + port, workers.toList, jobs.toList, completedJobs.toList)
    }
  }

  /**
   * Schedule the currently available resources among waiting jobs. This method will be called
   * every time a new job joins or resource availability changes.
   */
  def schedule() {
    // Right now this is a very simple FIFO scheduler. We keep looking through the jobs
    // in order of submission time and launching the first one that fits on each node.
    for (worker <- workers if worker.coresFree > 0) {
      for (job <- waitingJobs.clone()) {
        val jobMemory = job.desc.memoryPerSlave
        if (worker.memoryFree >= jobMemory) {
          val coresToUse = math.min(worker.coresFree, job.coresLeft)
          val exec = job.addExecutor(worker, coresToUse)
          launchExecutor(worker, exec)
        }
        if (job.coresLeft == 0) {
          waitingJobs -= job
          job.state = JobState.RUNNING
        }
      }
    }
  }

  def launchExecutor(worker: WorkerInfo, exec: ExecutorInfo) {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    worker.addExecutor(exec)
    worker.actor ! LaunchExecutor(exec.job.id, exec.id, exec.job.desc, exec.cores, exec.memory)
    exec.job.actor ! ExecutorAdded(exec.id, worker.id, worker.host, exec.cores, exec.memory)
  }

  def addWorker(id: String, host: String, port: Int, cores: Int, memory: Int, webUiPort: Int): WorkerInfo = {
    val worker = new WorkerInfo(id, host, port, cores, memory, sender, webUiPort)
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
      exec.job.actor ! ExecutorStateChanged(exec.job.id, exec.id, ExecutorState.LOST, None)
      exec.job.executors -= exec.id
    }
  }

  def addJob(desc: JobDescription, actor: ActorRef): JobInfo = {
    val date = new Date
    val job = new JobInfo(newJobId(date), desc, date, actor)
    jobs += job
    idToJob(job.id) = job
    actorToJob(sender) = job
    addressToJob(sender.path.address) = job
    return job
  }

  def removeJob(job: JobInfo) {
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
    job.state = JobState.FINISHED
    schedule()
  }

  /** Generate a new job ID given a job's submission date */
  def newJobId(submitDate: Date): String = {
    val jobId = "job-%s-%04d".format(DATE_FORMAT.format(submitDate), nextJobNumber)
    nextJobNumber += 1
    jobId
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
