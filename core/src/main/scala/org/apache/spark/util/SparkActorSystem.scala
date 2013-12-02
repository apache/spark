/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.actor

import com.typesafe.config.Config
import akka.util._
import scala.util.control.{NonFatal, ControlThrowable}

/**
 * An actorSystem specific to spark. It has an additional feature of letting spark tolerate
 * fatal exceptions.
 */
object SparkActorSystem {

  def apply(name: String, config: Config): ActorSystem = apply(name, config, findClassLoader())

  def apply(name: String, config: Config, classLoader: ClassLoader): ActorSystem =
    new SparkActorSystemImpl(name, config, classLoader).start()

  /**
   * INTERNAL API
   */
  private[akka] def findClassLoader(): ClassLoader = {
    def findCaller(get: Int ⇒ Class[_]): ClassLoader =
      Iterator.from(2 /*is the magic number, promise*/).map(get) dropWhile {
        c ⇒
          c != null &&
            (c.getName.startsWith("akka.actor.ActorSystem") ||
              c.getName.startsWith("scala.Option") ||
              c.getName.startsWith("scala.collection.Iterator") ||
              c.getName.startsWith("akka.util.Reflect"))
      } next() match {
        case null ⇒ getClass.getClassLoader
        case c ⇒ c.getClassLoader
      }

    Option(Thread.currentThread.getContextClassLoader) orElse
      (Reflect.getCallerClass map findCaller) getOrElse
      getClass.getClassLoader
  }
}

private[akka] class SparkActorSystemImpl(override val name: String,
                                         applicationConfig: Config,
                                         classLoader: ClassLoader)
  extends ActorSystemImpl(name, applicationConfig, classLoader) {

  protected override def uncaughtExceptionHandler: Thread.UncaughtExceptionHandler =
    new Thread.UncaughtExceptionHandler() {
      def uncaughtException(thread: Thread, cause: Throwable): Unit = {
        cause match {
          case NonFatal(_) | _: InterruptedException | _: NotImplementedError | _: ControlThrowable
          ⇒ log.error(cause, "Uncaught error from thread [{}]", thread.getName)
          case _ ⇒
            if (settings.JvmExitOnFatalError) {
              try {
                log.error(cause, "Uncaught error from thread [{}] shutting down JVM since " +
                  "'akka.jvm-exit-on-fatal-error' is enabled", thread.getName)
                import System.err
                err.print("Uncaught error from thread [")
                err.print(thread.getName)
                err.print("] shutting down JVM since 'akka.jvm-exit-on-fatal-error' is enabled for " +
                  "ActorSystem[")
                err.print(name)
                err.println("]")
                cause.printStackTrace(System.err)
                System.err.flush()
              } finally {
                System.exit(-1)
              }
            } else {
              log.error(cause, "Uncaught fatal error from thread [{}] not shutting down " +
                "ActorSystem tolerating and continuing.... [{}]", thread.getName, name)
              //shutdown()                 //TODO make it configurable
              if (thread.isAlive) log.error("Thread is still alive")
              else {
                log.error("Thread is dead")
              }
            }
        }
      }
    }

  override def stop(actor: ActorRef): Unit = {
    val path = actor.path
    val guard = guardian.path
    val sys = systemGuardian.path
    path.parent match {
      case `guard` ⇒ guardian ! StopChild(actor)
      case `sys` ⇒ systemGuardian ! StopChild(actor)
      case _ ⇒ actor.asInstanceOf[InternalActorRef].stop()
    }
  }


  override def /(actorName: String): ActorPath = guardian.path / actorName

  override def /(path: Iterable[String]): ActorPath = guardian.path / path

  private lazy val _start: this.type = {
    // the provider is expected to start default loggers, LocalActorRefProvider does this
    provider.init(this)
    this
  }

  override def start(): this.type = _start

  override def toString: String = lookupRoot.path.root.address.toString

}
