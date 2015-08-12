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

package org.apache.spark.deploy.master

import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.util.{Comparator, PriorityQueue}

import scala.xml.XML

import org.apache.spark.{Logging, SparkException}

/**
 * An interface for sort algorithm
 */
private[master] trait SchedulingAlgorithm {
  def master: Master

  /**
   * Schedule and launch executors on workers
   */
  def startExecutorsOnWorkers(
      waitingApps: Array[ApplicationInfo],
      workers: Array[WorkerInfo]): Unit

  def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int]

  def registerApplication(app: ApplicationInfo): Unit = {}

  def removeApplication(app: ApplicationInfo): Unit = {}

  /**
   * Allocate a worker's resources to one or more executors.
   * @param app the info of the application which the executors belong to
   * @param assignedCores number of cores on this worker for this application
   * @param coresPerExecutor number of cores per executor
   * @param worker the worker info
   */
  def allocateWorkerResourceToExecutors(
      app: ApplicationInfo,
      assignedCores: Int,
      coresPerExecutor: Option[Int],
      worker: WorkerInfo): Unit = {
    // If the number of cores per executor is specified, we divide the cores assigned
    // to this worker evenly among the executors with no remainder.
    // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
    val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
    for (i <- 1 to numExecutors) {
      val exec = app.addExecutor(worker, coresToAssign)
      master.launchExecutor(worker, exec)
      app.state = ApplicationState.RUNNING
    }
  }
}

/**
 * FIFO algorithm between Spark applications
 */
private[master] class FIFOSchedulingAlgorithm(val master: Master) extends SchedulingAlgorithm {
  /**
   * This is a very simple FIFO scheduler. We keep trying to fit in the first app
   * in the queue, then the second app, etc.
   */
  def startExecutorsOnWorkers(
      waitingApps: Array[ApplicationInfo],
      workers: Array[WorkerInfo]): Unit = {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    for (app <- waitingApps if app.coresLeft > 0) {
      val coresPerExecutor: Option[Int] = app.desc.coresPerExecutor
      // Filter out workers that don't have enough resources to launch an executor
      val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
        .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
          worker.coresFree >= coresPerExecutor.getOrElse(1))
        .sortBy(_.coresFree).reverse
      val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, master.spreadOutApps)

      // Now that we've decided how many cores to allocate on each worker, let's allocate them
      for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
        allocateWorkerResourceToExecutors(
          app, assignedCores(pos), coresPerExecutor, usableWorkers(pos))
      }
    }
  }

  /**
   * Schedule executors to be launched on the workers.
   * Returns an array containing number of cores assigned to each worker.
   *
   * There are two modes of launching executors. The first attempts to spread out an application's
   * executors on as many workers as possible, while the second does the opposite (i.e. launch them
   * on as few workers as possible). The former is usually better for data locality purposes and is
   * the default.
   *
   * The number of cores assigned to each executor is configurable. When this is explicitly set,
   * multiple executors from the same application may be launched on the same worker if the worker
   * has enough cores and memory. Otherwise, each executor grabs all the cores available on the
   * worker by default, in which case only one executor may be launched on each worker.
   *
   * It is important to allocate coresPerExecutor on each worker at a time (instead of 1 core
   * at a time). Consider the following example: cluster has 4 workers with 16 cores each.
   * User requests 3 executors (spark.cores.max = 48, spark.executor.cores = 16). If 1 core is
   * allocated at a time, 12 cores from each worker would be assigned to each executor.
   * Since 12 < 16, no executors would launch [SPARK-8881].
   */
  def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int] = {
    val coresPerExecutor = app.desc.coresPerExecutor
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    val numUsable = usableWorkers.length
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app. */
    def canLaunchExecutor(pos: Int): Boolean = {
      val keepScheduling = coresToAssign >= minCoresPerExecutor
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

      // If we allow multiple executors per worker, then we can always launch new executors.
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        keepScheduling && enoughCores && enoughMemory && underLimit
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutor(pos)) {
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutor)
    }
    assignedCores
  }
}

case class ApplicationSubmission(val appInfo: ApplicationInfo, val submittedTime: LocalDateTime)

private[master] class PrioritySchedulingAlgorithm(
    val master: Master,
    val schedulingSetting: SchedulingSetting) extends SchedulingAlgorithm with Logging {

  val DEFAULT_PRIORITY = 1
  val DEFAULT_CORES = 1
  val POOLS_PROPERTY = "pool"
  val POOL_NAME_PROPERTY = "@name"
  val PRIORITY_PROPERTY = "priority"
  val CORES_PROPERTY = "cores"

  val initAppNumberPerPool = 100
  val initPoolNumber = 5

  private final val applicationComparator: Comparator[ApplicationSubmission] =
    new Comparator[ApplicationSubmission]() {
      override def compare(left: ApplicationSubmission, right: ApplicationSubmission): Int = {
        left.submittedTime.compareTo(right.submittedTime)
      }
    }

  private final val poolComparator: Comparator[Pool] = new Comparator[Pool]() {
    override def compare(left: Pool, right: Pool): Int = {
      -Ordering[Int].compare(left.priority, right.priority)
    }
  }

  class Pool(val poolName: String, val priority: Int, val cores: Int) {
    private val appQueue: PriorityQueue[ApplicationSubmission] =
      new PriorityQueue[ApplicationSubmission](initAppNumberPerPool, applicationComparator)


    def addApplication(app: ApplicationSubmission): Unit = {
      appQueue.add(app)
    }

    def addApplication(app: ApplicationInfo): Unit = {
      val submission = ApplicationSubmission(app, LocalDateTime.now())
      addApplication(submission)
    }

    def nextApplication(): Option[ApplicationInfo] = {
      val nextOne = appQueue.poll()
      if (nextOne == null) {
        None
      } else {
        Some(nextOne.appInfo)
      }
    }

    def removeApplication(app: ApplicationInfo): Boolean = {
      val submitted = appQueue.toArray().find(_.asInstanceOf[ApplicationSubmission].appInfo == app)
      if (submitted != None) {
        appQueue.remove(submitted)
      } else {
        false
      }
    }

    def getApplications(): Seq[ApplicationInfo] = {
      appQueue.toArray().map(_.asInstanceOf[ApplicationSubmission].appInfo)
        .map(_.asInstanceOf[ApplicationInfo])
    }

    def size: Int = appQueue.size()
  }

  private final val poolQueue: PriorityQueue[Pool] =
    new PriorityQueue[Pool](initPoolNumber, poolComparator)

  private var queueIndex: Int = 0

  private var currentPool: Pool = _

  def queueSize(): Int = poolQueue.size()

  def firstPool(): Option[Pool] = {
    if (queueSize() == 0) {
      None
    } else {
      Some(poolQueue.peek())
    }
  }

  def nextPool(): Option[Pool] = {
    if (queueSize() == 0) {
      None
    } else {
      queueIndex %= queueSize()
      val nextOne = poolQueue.toArray()(queueIndex)
      queueIndex += 1
      Some(nextOne.asInstanceOf[Pool])
    }
  }

  def nonEmptyPools(): Seq[Pool] = {
    poolQueue.toArray().filter(_.asInstanceOf[Pool].size > 0).map(_.asInstanceOf[Pool])
  }

  def nextNonEmptyPool(): Option[Pool] = {
    if (queueSize() == 0) {
      None
    } else {
      poolQueue.toArray().find(_.asInstanceOf[Pool].size > 0).map(_.asInstanceOf[Pool])
    }
  }

  override def registerApplication(app: ApplicationInfo): Unit = {
    if (app.desc.assignedPool == None) {
      throw new SparkException(s"Application ${app.desc.name} hasn't been assigned to any pool")
    } else {
      val pool = poolQueue.toArray().find { p =>
        p.asInstanceOf[Pool].poolName == app.desc.assignedPool.get
      }.getOrElse {
          throw new SparkException(s"Application ${app.desc.name} has been assigned to " +
            s"unknown pool ${app.desc.assignedPool.get}")
      }.asInstanceOf[Pool]
      pool.addApplication(app)
    }
  }

  override def removeApplication(app: ApplicationInfo): Unit =  {
    if (app.desc.assignedPool == None) {
      throw new SparkException(s"Application ${app.desc.name} hasn't been assigned to any pool")
    } else {
      val pool = poolQueue.toArray().find { p =>
        p.asInstanceOf[Pool].poolName == app.desc.assignedPool.get
      }.getOrElse {
          throw new SparkException(s"Application ${app.desc.name} has been assigned to " +
            s"unknown pool ${app.desc.assignedPool.get}")
      }.asInstanceOf[Pool]
      if (!pool.removeApplication(app)) {
        throw new SparkException(s"Can't remove application ${app.desc.name} " +
          s"from pool ${app.desc.assignedPool.get}")
      }
    }
  }

  def startExecutorsOnWorkers(
      waitingApps: Array[ApplicationInfo],
      workers: Array[WorkerInfo]): Unit = {
    nonEmptyPools().map { pool => pool.getApplications().map { app =>
      // The allowed cores setting overwrites app.requestedCores, so we check it here
      if (pool.cores - app.coresGranted > 0) {
        val coresPerExecutor: Option[Int] = app.desc.coresPerExecutor
        // Filter out workers that don't have enough resources to launch an executor
        val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
          .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
            worker.coresFree >= coresPerExecutor.getOrElse(1))
          .sortBy(_.coresFree).reverse
        currentPool = pool
        val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, master.spreadOutApps)

        // Now that we've decided how many cores to allocate on each worker, let's allocate them
        for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
          allocateWorkerResourceToExecutors(
            app, assignedCores(pos), coresPerExecutor, usableWorkers(pos))
        }
      }
    }}
  }

  def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int] = {
    val coresPerExecutor = app.desc.coresPerExecutor
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    val numUsable = usableWorkers.length
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    var coresToAssign = math.min(currentPool.cores - app.coresGranted,
      usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app. */
    def canLaunchExecutor(pos: Int): Boolean = {
      val keepScheduling = coresToAssign >= minCoresPerExecutor
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

      // If we allow multiple executors per worker, then we can always launch new executors.
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        keepScheduling && enoughCores && enoughMemory && underLimit
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutor(pos)) {
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutor)
    }
    assignedCores
  }

  def loadDefault(): InputStream = {
    val exampleXML = """<?xml version="1.0"?>
                        <allocations>
                          <pool name="production">
                            <priority>10</priority>
                            <cores>5</cores>
                          </pool>
                          <pool name="test">
                            <priority>2</priority>
                            <cores>1</cores>
                          </pool>
                        </allocations>"""
    new ByteArrayInputStream(exampleXML.getBytes(StandardCharsets.UTF_8))
  }

  def buildPools(): PrioritySchedulingAlgorithm = {
    var is: Option[InputStream] = None
    try {
      is = Option {
        schedulingSetting.configFile.map { f =>
          new FileInputStream(f)
        }.getOrElse {
          logError("Must specify configuration file for priority scheduling")
          loadDefault()
        }
      }
      is.foreach { i => buildFairSchedulerPool(i) }
    } finally {
      is.foreach(_.close())
    }
    this
  }

  def buildFairSchedulerPool(is: InputStream): Unit = {
    val xml = XML.load(is)
    for (poolNode <- (xml \\ POOLS_PROPERTY)) {

      val poolName = (poolNode \ POOL_NAME_PROPERTY).text
      var priority = DEFAULT_PRIORITY
      var cores = DEFAULT_CORES

      val xmlPriority = (poolNode \ PRIORITY_PROPERTY).text
      if (xmlPriority != "") {
        priority = xmlPriority.toInt
      }

      val xmlCores = (poolNode \ CORES_PROPERTY).text
      if (xmlCores != "") {
        cores = xmlCores.toInt
      }

      val pool = new Pool(poolName, priority, cores)
      poolQueue.add(pool)
      logInfo("Created pool %s, priority: %d, cores: %d".format(
        poolName, priority, cores))
    }
  }
}
