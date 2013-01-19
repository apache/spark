package spark.streaming.receivers

import akka.actor.{ Actor, PoisonPill, Props, SupervisorStrategy }
import akka.actor.{ actorRef2Scala, ActorRef }
import akka.actor.{ PossiblyHarmful, OneForOneStrategy }

import spark.storage.StorageLevel
import spark.streaming.dstream.NetworkReceiver

import java.util.concurrent.atomic.AtomicInteger

/** A helper with set of defaults for supervisor strategy **/
object ReceiverSupervisorStrategy {

  import akka.util.duration._
  import akka.actor.SupervisorStrategy._

  val defaultStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange =
    15 millis) {
    case _: RuntimeException ⇒ Restart
    case _: Exception ⇒ Escalate
  }
}

/**
 * Settings for configuring the actor creation or defining supervisor strategy
 */
case class Settings(props: Props,
  name: String,
  storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER_2,
  supervisorStrategy: SupervisorStrategy = ReceiverSupervisorStrategy.defaultStrategy)

/**
 * Statistcs for querying the supervisor about state of workers
 */
case class Statistcs(numberOfMsgs: Int,
  numberOfWorkers: Int,
  numberOfHiccups: Int,
  otherInfo: String)

/** Case class to receive data sent by child actors **/
case class Data[T: ClassManifest](data: T)

/**
 * Provides Actors as receivers for receiving stream.
 *
 * As Actors can also be used to receive data from almost any stream source.
 * A nice set of abstraction(s) for actors as receivers is already provided for
 * a few general cases. It is thus exposed as an API where user may come with
 * his own Actor to run as receiver for Spark Streaming input source.
 */
class ActorReceiver[T: ClassManifest](settings: Settings)
  extends NetworkReceiver[T] {

  protected lazy val blocksGenerator: BlockGenerator =
    new BlockGenerator(settings.storageLevel)

  protected lazy val supervisor = env.actorSystem.actorOf(Props(new Supervisor),
    "Supervisor" + streamId)

  private class Supervisor extends Actor {

    override val supervisorStrategy = settings.supervisorStrategy
    val worker = context.actorOf(settings.props, settings.name)
    logInfo("Started receiver worker at:" + worker.path)

    val n: AtomicInteger = new AtomicInteger(0)
    val hiccups: AtomicInteger = new AtomicInteger(0)

    def receive = {

      case props: Props =>
        val worker = context.actorOf(props)
        logInfo("Started receiver worker at:" + worker.path)
        sender ! worker

      case (props: Props, name: String) =>
        val worker = context.actorOf(props, name)
        logInfo("Started receiver worker at:" + worker.path)
        sender ! worker

      case _: PossiblyHarmful => hiccups.incrementAndGet()

      case _: Statistcs =>
        val workers = context.children
        sender ! Statistcs(n.get, workers.size, hiccups.get, workers.mkString("\n"))

      case Data(iter: Iterator[_]) => push(iter.asInstanceOf[Iterator[T]])

      case Data(msg) =>
        blocksGenerator += msg.asInstanceOf[T]
        n.incrementAndGet
    }
  }

  protected def push(iter: Iterator[T]) {
    pushBlock("block-" + streamId + "-" + System.nanoTime(),
      iter, null, settings.storageLevel)
  }

  protected def onStart() = {
    blocksGenerator.start()
    supervisor
    logInfo("Supervision tree for receivers initialized at:" + supervisor.path)
  }

  protected def onStop() = {
    supervisor ! PoisonPill
  }

}
