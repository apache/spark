package spark.executor

import java.nio.ByteBuffer
import spark.Logging
import spark.TaskState.TaskState
import spark.util.AkkaUtils
import akka.actor.{ActorRef, Actor, Props, Terminated}
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientShutdown, RemoteClientDisconnected}
import java.util.concurrent.{TimeUnit, ThreadPoolExecutor, SynchronousQueue}
import spark.scheduler.cluster._
import spark.scheduler.cluster.RegisteredExecutor
import spark.scheduler.cluster.LaunchTask
import spark.scheduler.cluster.RegisterExecutorFailed
import spark.scheduler.cluster.RegisterExecutor
import spark.Utils
import spark.deploy.SparkHadoopUtil

private[spark] class StandaloneExecutorBackend(
    driverUrl: String,
    executorId: String,
    hostPort: String,
    cores: Int)
  extends Actor
  with ExecutorBackend
  with Logging {

  Utils.checkHostPort(hostPort, "Expected hostport")

  var executor: Executor = null
  var driver: ActorRef = null

  override def preStart() {
    logInfo("Connecting to driver: " + driverUrl)
    driver = context.actorFor(driverUrl)
    driver ! RegisterExecutor(executorId, hostPort, cores)
    context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
    context.watch(driver) // Doesn't work with remote actors, but useful for testing
  }

  override def receive = {
    case RegisteredExecutor(sparkProperties) =>
      logInfo("Successfully registered with driver")
      // Make this host instead of hostPort ?
      executor = new Executor(executorId, Utils.parseHostPort(hostPort)._1, sparkProperties)

    case RegisterExecutorFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)

    case LaunchTask(taskDesc) =>
      logInfo("Got assigned task " + taskDesc.taskId)
      if (executor == null) {
        logError("Received launchTask but executor was null")
        System.exit(1)
      } else {
        executor.launchTask(this, taskDesc.taskId, taskDesc.serializedTask)
      }

    case Terminated(_) | RemoteClientDisconnected(_, _) | RemoteClientShutdown(_, _) =>
      logError("Driver terminated or disconnected! Shutting down.")
      System.exit(1)
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    driver ! StatusUpdate(executorId, taskId, state, data)
  }
}

private[spark] object StandaloneExecutorBackend {
  def run(driverUrl: String, executorId: String, hostname: String, cores: Int) {
    SparkHadoopUtil.runAsUser(run0, Tuple4[Any, Any, Any, Any] (driverUrl, executorId, hostname, cores))
  }

  // This will be run 'as' the user
  def run0(args: Product) {
    assert(4 == args.productArity)
    runImpl(args.productElement(0).asInstanceOf[String], 
      args.productElement(1).asInstanceOf[String],
      args.productElement(2).asInstanceOf[String],
      args.productElement(3).asInstanceOf[Int])
  }
  
  private def runImpl(driverUrl: String, executorId: String, hostname: String, cores: Int) {
    // Debug code
    Utils.checkHost(hostname)

    // Create a new ActorSystem to run the backend, because we can't create a SparkEnv / Executor
    // before getting started with all our system properties, etc
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("sparkExecutor", hostname, 0)
    // set it
    val sparkHostPort = hostname + ":" + boundPort
    System.setProperty("spark.hostPort", sparkHostPort)
    val actor = actorSystem.actorOf(
      Props(new StandaloneExecutorBackend(driverUrl, executorId, sparkHostPort, cores)),
      name = "Executor")
    actorSystem.awaitTermination()
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      //the reason we allow the last frameworkId argument is to make it easy to kill rogue executors
      System.err.println("Usage: StandaloneExecutorBackend <driverUrl> <executorId> <hostname> <cores> [<appid>]")
      System.exit(1)
    }
    run(args(0), args(1), args(2), args(3).toInt)
  }
}
