package spark.scheduler.cluster

import spark.{Utils, Logging, SparkContext}
import spark.deploy.client.{Client, ClientListener}
import spark.deploy.{Command, ApplicationDescription}
import scala.collection.mutable.HashMap

private[spark] class SparkDeploySchedulerBackend(
    scheduler: ClusterScheduler,
    sc: SparkContext,
    master: String,
    appName: String)
  extends StandaloneSchedulerBackend(scheduler, sc.env.actorSystem)
  with ClientListener
  with Logging {

  var client: Client = null
  var stopping = false
  var shutdownCallback : (SparkDeploySchedulerBackend) => Unit = _

  val maxCores = System.getProperty("spark.cores.max", Int.MaxValue.toString).toInt

  override def start() {
    super.start()

    // The endpoint for executors to talk to us
    val driverUrl = "akka://spark@%s:%s/user/%s".format(
      System.getProperty("spark.driver.host"), System.getProperty("spark.driver.port"),
      StandaloneSchedulerBackend.ACTOR_NAME)
    val args = Seq(driverUrl, "{{EXECUTOR_ID}}", "{{HOSTNAME}}", "{{CORES}}")
    val command = Command("spark.executor.StandaloneExecutorBackend", args, sc.executorEnvs)
    val sparkHome = sc.getSparkHome().getOrElse(
      throw new IllegalArgumentException("must supply spark home for spark standalone"))
    val appDesc = new ApplicationDescription(appName, maxCores, executorMemory, command, sparkHome,
        sc.ui.appUIAddress)

    client = new Client(sc.env.actorSystem, master, appDesc, this)
    client.start()
  }

  override def stop() {
    stopping = true
    super.stop()
    client.stop()
    if (shutdownCallback != null) {
      shutdownCallback(this)
    }
  }

  override def connected(appId: String) {
    logInfo("Connected to Spark cluster with app ID " + appId)
  }

  override def disconnected() {
    if (!stopping) {
      logError("Disconnected from Spark cluster!")
      scheduler.error("Disconnected from Spark cluster")
    }
  }

  override def executorAdded(executorId: String, workerId: String, hostPort: String, cores: Int, memory: Int) {
    logInfo("Granted executor ID %s on hostPort %s with %d cores, %s RAM".format(
       executorId, hostPort, cores, Utils.memoryMegabytesToString(memory)))
  }

  override def executorRemoved(executorId: String, message: String, exitStatus: Option[Int]) {
    val reason: ExecutorLossReason = exitStatus match {
      case Some(code) => ExecutorExited(code)
      case None => SlaveLost(message)
    }
    logInfo("Executor %s removed: %s".format(executorId, message))
    removeExecutor(executorId, reason.toString)
  }
}
