/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

// Must be in akka.actor package as ActorSystemImpl is protected[akka].
package akka.actor

import scala.util.control.{ControlThrowable, NonFatal}

import com.typesafe.config.Config

/**
 * An [[akka.actor.ActorSystem]] which refuses to shut down in the event of a fatal exception.
 * This is necessary as Spark Executors are allowed to recover from fatal exceptions
 * (see [[org.apache.spark.executor.Executor]]).
 */
object IndestructibleActorSystem {
  def apply(name: String, config: Config): ActorSystem =
    apply(name, config, ActorSystem.findClassLoader())

  def apply(name: String, config: Config, classLoader: ClassLoader): ActorSystem =
    new IndestructibleActorSystemImpl(name, config, classLoader).start()
}

private[akka] class IndestructibleActorSystemImpl(
    override val name: String,
    applicationConfig: Config,
    classLoader: ClassLoader)
  extends ActorSystemImpl(name, applicationConfig, classLoader) {

  protected override def uncaughtExceptionHandler: Thread.UncaughtExceptionHandler = {
    val fallbackHandler = super.uncaughtExceptionHandler

    new Thread.UncaughtExceptionHandler() {
      def uncaughtException(thread: Thread, cause: Throwable): Unit = {
        if (isFatalError(cause) && !settings.JvmExitOnFatalError) {
          log.error(cause, "Uncaught fatal error from thread [{}] not shutting down " +
            "ActorSystem [{}] tolerating and continuing.... ", thread.getName, name)
          //shutdown()                 //TODO make it configurable
        } else {
          fallbackHandler.uncaughtException(thread, cause)
        }
      }
    }
  }

  def isFatalError(e: Throwable): Boolean = {
    e match {
      case NonFatal(_) | _: InterruptedException | _: NotImplementedError | _: ControlThrowable =>
        false
      case _ =>
        true
    }
  }
}
