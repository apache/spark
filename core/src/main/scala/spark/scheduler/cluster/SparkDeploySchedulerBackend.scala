package spark.scheduler.cluster

import spark.{Utils, Logging, SparkContext}
import spark.deploy.client.{Client, ClientListener}
import spark.deploy.{Command, JobDescription}
import scala.collection.mutable.HashMap

private[spark] class SparkDeploySchedulerBackend(
    scheduler: ClusterScheduler,
    sc: SparkContext,
    master: String,
    jobName: String)
  extends StandaloneSchedulerBackend(scheduler, sc.env.actorSystem)
  with ClientListener
  with Logging {

  var client: Client = null
  var stopping = false
  var shutdownCallback : (SparkDeploySchedulerBackend) => Unit = _

  val maxCores = System.getProperty("spark.cores.max", Int.MaxValue.toString).toInt

  // Memory used by each executor (in megabytes)
  val executorMemory = {
    if (System.getenv("SPARK_MEM") != null) {
      Utils.memoryStringToMb(System.getenv("SPARK_MEM"))
      // TODO: Might need to add some extra memory for the non-heap parts of the JVM
    } else {
      512
    }
  }

  override def start() {
    super.start()

    val masterUrl = "akka://spark@%s:%s/user/%s".format(
      System.getProperty("spark.master.host"), System.getProperty("spark.master.port"),
      StandaloneSchedulerBackend.ACTOR_NAME)
    val args = Seq(masterUrl, "{{SLAVEID}}", "{{HOSTNAME}}", "{{CORES}}")
    val command = Command("spark.executor.StandaloneExecutorBackend", args, sc.executorEnvs)
    val jobDesc = new JobDescription(jobName, maxCores, executorMemory, command)

    client = new Client(sc.env.actorSystem, master, jobDesc, this)
    client.start()
  }

  override def stop() {
    stopping = true;
    super.stop()
    client.stop()
    if (shutdownCallback != null) {
      shutdownCallback(this)
    }
  }

  def connected(jobId: String) {
    logInfo("Connected to Spark cluster with job ID " + jobId)
  }

  def disconnected() {
    if (!stopping) {
      logError("Disconnected from Spark cluster!")
      scheduler.error("Disconnected from Spark cluster")
    }
  }

  def executorAdded(id: String, workerId: String, host: String, cores: Int, memory: Int) {
    logInfo("Granted executor ID %s on host %s with %d cores, %s RAM".format(
       id, host, cores, Utils.memoryMegabytesToString(memory)))
  }

  def executorRemoved(id: String, message: String) {}
}
