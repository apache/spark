/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
