/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.internal

import java.util
import java.util.Random

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.remoteshuffle.clients.{MultiServerHeartbeatClient, NotifyClient}
import org.apache.spark.remoteshuffle.metrics.M3Stats
import org.apache.spark.remoteshuffle.util.ServerHostAndPort
import org.apache.spark.scheduler.{JobFailed, _}

object RssSparkListener extends Logging {

  private val lock = new Object()

  @volatile private var instance: RssSparkListener = null

  /** *
   * Register RssSparkListener if it is not registered. This method may be called multiple times,
   * and will only register
   * the listener once.
   *
   * @param sparkContext
   * @param creator
   * @return
   */
  def registerSparkListenerOnlyOnce(sparkContext: SparkContext,
                                    creator: () => RssSparkListener): RssSparkListener = {
    if (instance != null) {
      return instance
    }

    lock.synchronized {
      if (instance != null) {
        return instance
      }

      instance = creator()
      sparkContext.addSparkListener(instance)
      logInfo("Created and registered RssSparkListener instance")
      return instance
    }
  }
}

/** *
 * This class implements Spark listeners to listen to several events (e.g. onJobEnd,
 * onApplicationEnd, etc.). It will
 * invoke remote shuffle control servers to notify them these events.
 *
 * @param user
 * @param appId
 * @param attemptId
 * @param notifyServers
 */
class RssSparkListener(val user: String, val appId: String, val attemptId: String,
                       val notifyServers: Array[String], val networkTimeoutMillis: Int)
  extends SparkListener with Logging {

  private val m3Tags: util.Map[String, String] = new util.HashMap[String, String]
  if (user != null && !user.isEmpty) {
    m3Tags.put("user", user)
  } else {
    m3Tags.put("user", "unknown")
  }

  private val numInAppJobs = M3Stats.createSubScope(m3Tags).counter("numInAppJobs")
  private val numFailedInAppJobs = M3Stats.createSubScope(m3Tags).counter("numFailedInAppJobs")

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    try {
      numInAppJobs.inc(1)
    } catch {
      case e: Throwable => {
        logWarning("Failed to run onJobStart", e)
      }
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    try {
      val jobResult = jobEnd.jobResult
      if (jobResult != null && jobResult.isInstanceOf[JobFailed]) {
        numFailedInAppJobs.inc(1)
      }
    } catch {
      case e: Throwable => {
        logWarning("Failed to run onJobEnd", e)
      }
    }

    if (notifyServers == null || notifyServers.length == 0) {
      return
    }

    invokeRandomNotifyServer(client => {
      val jobResult = jobEnd.jobResult
      var jobStatus = ""
      var exceptionName = ""
      var exceptionDetail = ""
      if (jobResult != null) {
        jobStatus = jobResult.getClass().getSimpleName()
        if (jobResult.isInstanceOf[JobFailed]) {
          val jobFailed = jobResult.asInstanceOf[JobFailed]
          if (jobFailed.exception != null) {
            exceptionName = jobFailed.exception.getClass().getSimpleName
            exceptionDetail = ExceptionUtils.getStackTrace(jobFailed.exception)
          }
        }
      }
      client.finishApplicationJob(appId, attemptId, jobEnd.jobId, jobStatus, exceptionName,
        exceptionDetail)
    })
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (notifyServers == null || notifyServers.length == 0) {
      return
    }

    MultiServerHeartbeatClient.getInstance().clearServers()

    invokeRandomNotifyServer(client => {
      client.finishApplicationAttempt(appId, attemptId)
    })
  }

  private def invokeRandomNotifyServer(run: NotifyClient => Unit) = {
    var client: NotifyClient = null
    try {
      val server = getRandomNotifyServer()
      logInfo(s"Invoking on random control server $server")

      client = new NotifyClient(server.getHost, server.getPort, networkTimeoutMillis, user)
      client.connect()
      run(client)
    } catch {
      case e: Throwable =>
        logWarning("Failed to invoke control server", e)
        M3Stats.addException(e, this.getClass().getSimpleName())
    } finally {
      client.close()
    }
  }

  private def getRandomNotifyServer() = {
    val random = new Random()
    val randomIndex = random.nextInt(notifyServers.length)
    ServerHostAndPort.fromString(notifyServers(randomIndex))
  }

}
