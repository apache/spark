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

package org.apache.spark.scheduler

import java.lang.reflect.{InvocationHandler, Method, Proxy}

import org.apache.spark.util.Utils

/**
 * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
 * results to the given handler function.
 */
private[spark] class JobWaiter[T](
    dagScheduler: DAGScheduler,
    val jobId: Int,
    totalTasks: Int,
    resultHandler: (Int, T) => Unit)
  extends JobListener {

  // Original signal handler that is overridden.
  @volatile
  private var _originalHandler: Object = _

  // Override default signal handler for ctrl-c if sun.misc Signal handler classes exist.
  private def attachSigintHandler(): Object = {
    try {
      val signalClazz = Utils.classForName("sun.misc.Signal")
      val signalHandlerClazz = Utils.classForName("sun.misc.SignalHandler")
      val newHandler = Proxy.newProxyInstance(Utils.getContextOrSparkClassLoader,
        Array(signalHandlerClazz), new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            // scalastyle:off println
            println("Cancelling running job.. This might take some time, so be patient.\n" +
              "Press Ctrl-C again to kill JVM.")
            // scalastyle:on println
            // Detach sigint handler so that pressing ctrl-c again will interrupt jvm.
            detachSigintHandler()
            cancel()
            null
          }
        })
      signalClazz.getMethod("handle", signalClazz, signalHandlerClazz)
        .invoke(
          null,
          signalClazz.getConstructor(classOf[String]).newInstance("INT").asInstanceOf[Object],
          newHandler)
    } catch {
      // Ignore. sun.misc Signal handler classes don't exist.
      case _: ClassNotFoundException => null
      case e: Exception => throw e
    }
  }

  // Reset signal handler to default
  private def detachSigintHandler(): Unit = {
    try {
      val signalClazz = Utils.classForName("sun.misc.Signal")
      val signalHandlerClazz = Utils.classForName("sun.misc.SignalHandler")
      signalClazz.getMethod("handle", signalClazz, signalHandlerClazz)
        .invoke(
          null,
          signalClazz.getConstructor(classOf[String]).newInstance("INT").asInstanceOf[Object],
          _originalHandler)
    } catch {
      // Ignore. sun.misc Signal handler classes don't exist.
      case _: ClassNotFoundException =>
      case e: Exception => throw e
    }
  }

  private var finishedTasks = 0

  // Is the job as a whole finished (succeeded or failed)?
  @volatile
  private var _jobFinished = totalTasks == 0

  def jobFinished: Boolean = _jobFinished

  // If the job is finished, this will be its result. In the case of 0 task jobs (e.g. zero
  // partition RDDs), we set the jobResult directly to JobSucceeded.
  private var jobResult: JobResult = if (jobFinished) JobSucceeded else null

  /**
   * Sends a signal to the DAGScheduler to cancel the job. The cancellation itself is handled
   * asynchronously. After the low level scheduler cancels all the tasks belonging to this job, it
   * will fail this job with a SparkException.
   */
  def cancel() {
    dagScheduler.cancelJob(jobId)
  }

  override def taskSucceeded(index: Int, result: Any): Unit = synchronized {
    if (_jobFinished) {
      throw new UnsupportedOperationException("taskSucceeded() called on a finished JobWaiter")
    }
    resultHandler(index, result.asInstanceOf[T])
    finishedTasks += 1
    if (finishedTasks == totalTasks) {
      _jobFinished = true
      jobResult = JobSucceeded
      this.notifyAll()
    }
  }

  override def jobFailed(exception: Exception): Unit = synchronized {
    _jobFinished = true
    jobResult = JobFailed(exception)
    this.notifyAll()
  }

  def awaitResult(): JobResult = synchronized {
    _originalHandler = attachSigintHandler()
    while (!_jobFinished) {
      this.wait()
    }
    detachSigintHandler()
    return jobResult
  }
}
