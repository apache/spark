package spark.executor

import java.nio.ByteBuffer
import spark.Logging
import spark.TaskState.TaskState
import spark.util.AkkaUtils
import akka.actor.{ActorRef, Actor, Props}
import java.util.concurrent.{TimeUnit, ThreadPoolExecutor, SynchronousQueue}
import akka.remote.RemoteClientLifeCycleEvent
import spark.scheduler.cluster._
import spark.scheduler.cluster.RegisteredExecutor
import spark.scheduler.cluster.LaunchTask
import spark.scheduler.cluster.RegisterExecutorFailed
import spark.scheduler.cluster.RegisterExecutor


private[spark] class StandaloneExecutorBackend(
    executor: Executor,
    masterUrl: String,
    executorId: String,
    hostname: String,
    cores: Int)
  extends Actor
  with ExecutorBackend
  with Logging {

  var master: ActorRef = null

  override def preStart() {
    try {
      logInfo("Connecting to master: " + masterUrl)
      master = context.actorFor(masterUrl)
      master ! RegisterExecutor(executorId, hostname, cores)
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
      context.watch(master) // Doesn't work with remote actors, but useful for testing
    } catch {
      case e: Exception =>
        logError("Failed to connect to master", e)
        System.exit(1)
    }
  }

  override def receive = {
    case RegisteredExecutor(sparkProperties) =>
      logInfo("Successfully registered with master")
      executor.initialize(executorId, hostname, sparkProperties)

    case RegisterExecutorFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)

    case LaunchTask(taskDesc) =>
      logInfo("Got assigned task " + taskDesc.taskId)
      executor.launchTask(this, taskDesc.taskId, taskDesc.serializedTask)
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    master ! StatusUpdate(executorId, taskId, state, data)
  }
}

private[spark] object StandaloneExecutorBackend {
  def run(masterUrl: String, executorId: String, hostname: String, cores: Int) {
    // Create a new ActorSystem to run the backend, because we can't create a SparkEnv / Executor
    // before getting started with all our system properties, etc
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("sparkExecutor", hostname, 0)
    val actor = actorSystem.actorOf(
      Props(new StandaloneExecutorBackend(new Executor, masterUrl, executorId, hostname, cores)),
      name = "Executor")
    actorSystem.awaitTermination()
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: StandaloneExecutorBackend <master> <executorId> <hostname> <cores>")
      System.exit(1)
    }
    run(args(0), args(1), args(2), args(3).toInt)
  }
}
