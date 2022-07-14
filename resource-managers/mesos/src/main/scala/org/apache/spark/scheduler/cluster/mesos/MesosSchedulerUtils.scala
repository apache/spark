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

package org.apache.spark.scheduler.cluster.mesos

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.{List => JList}
import java.util.concurrent.CountDownLatch

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.google.common.base.Splitter
import com.google.common.io.Files
import org.apache.mesos.{MesosSchedulerDriver, Protos, Scheduler, SchedulerDriver}
import org.apache.mesos.Protos.{TaskState => MesosTaskState, _}
import org.apache.mesos.Protos.FrameworkInfo.Capability
import org.apache.mesos.Protos.Resource.ReservationInfo
import org.apache.mesos.protobuf.GeneratedMessageV3

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.TaskState
import org.apache.spark.deploy.mesos.{config => mesosConfig, MesosDriverDescription}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{Status => _, _}
import org.apache.spark.util.Utils

/**
 * Shared trait for implementing a Mesos Scheduler. This holds common state and helper
 * methods and Mesos scheduler will use.
 */
trait MesosSchedulerUtils extends Logging {
  // Lock used to wait for scheduler to be registered
  private final val registerLatch = new CountDownLatch(1)

  private final val ANY_ROLE = "*"

  /**
   * Creates a new MesosSchedulerDriver that communicates to the Mesos master.
   *
   * @param masterUrl The url to connect to Mesos master
   * @param scheduler the scheduler class to receive scheduler callbacks
   * @param sparkUser User to impersonate with when running tasks
   * @param appName The framework name to display on the Mesos UI
   * @param conf Spark configuration
   * @param webuiUrl The WebUI url to link from Mesos UI
   * @param checkpoint Option to checkpoint tasks for failover
   * @param failoverTimeout Duration Mesos master expect scheduler to reconnect on disconnect
   * @param frameworkId The id of the new framework
   */
  protected def createSchedulerDriver(
      masterUrl: String,
      scheduler: Scheduler,
      sparkUser: String,
      appName: String,
      conf: SparkConf,
      webuiUrl: Option[String] = None,
      checkpoint: Option[Boolean] = None,
      failoverTimeout: Option[Double] = None,
      frameworkId: Option[String] = None): SchedulerDriver = {
    val fwInfoBuilder = FrameworkInfo.newBuilder().setUser(sparkUser).setName(appName)
    fwInfoBuilder.setHostname(Option(conf.getenv("SPARK_PUBLIC_DNS")).getOrElse(
      conf.get(DRIVER_HOST_ADDRESS)))
    webuiUrl.foreach { url => fwInfoBuilder.setWebuiUrl(url) }
    checkpoint.foreach { checkpoint => fwInfoBuilder.setCheckpoint(checkpoint) }
    failoverTimeout.foreach { timeout => fwInfoBuilder.setFailoverTimeout(timeout) }
    frameworkId.foreach { id =>
      fwInfoBuilder.setId(FrameworkID.newBuilder().setValue(id).build())
    }

    conf.get(mesosConfig.ROLE).foreach { role =>
      fwInfoBuilder.setRole(role)
    }
    val maxGpus = conf.get(mesosConfig.MAX_GPUS)
    if (maxGpus > 0) {
      fwInfoBuilder.addCapabilities(Capability.newBuilder().setType(Capability.Type.GPU_RESOURCES))
    }
    val credBuilder = buildCredentials(conf, fwInfoBuilder)
    if (credBuilder.hasPrincipal) {
      new MesosSchedulerDriver(
        scheduler, fwInfoBuilder.build(), masterUrl, credBuilder.build())
    } else {
      new MesosSchedulerDriver(scheduler, fwInfoBuilder.build(), masterUrl)
    }
  }

  def buildCredentials(
      conf: SparkConf,
      fwInfoBuilder: Protos.FrameworkInfo.Builder): Protos.Credential.Builder = {
    val credBuilder = Credential.newBuilder()
    conf.get(mesosConfig.CREDENTIAL_PRINCIPAL)
      .orElse(Option(conf.getenv("SPARK_MESOS_PRINCIPAL")))
      .orElse(
        conf.get(mesosConfig.CREDENTIAL_PRINCIPAL_FILE)
          .orElse(Option(conf.getenv("SPARK_MESOS_PRINCIPAL_FILE")))
          .map { principalFile =>
            Files.toString(new File(principalFile), StandardCharsets.UTF_8)
          }
      ).foreach { principal =>
        fwInfoBuilder.setPrincipal(principal)
        credBuilder.setPrincipal(principal)
      }
    conf.get(mesosConfig.CREDENTIAL_SECRET)
      .orElse(Option(conf.getenv("SPARK_MESOS_SECRET")))
      .orElse(
        conf.get(mesosConfig.CREDENTIAL_SECRET_FILE)
         .orElse(Option(conf.getenv("SPARK_MESOS_SECRET_FILE")))
         .map { secretFile =>
           Files.toString(new File(secretFile), StandardCharsets.UTF_8)
         }
      ).foreach { secret =>
        credBuilder.setSecret(secret)
      }
    if (credBuilder.hasSecret && !fwInfoBuilder.hasPrincipal) {
      throw new SparkException(
        s"${mesosConfig.CREDENTIAL_PRINCIPAL} must be configured when " +
          s"${mesosConfig.CREDENTIAL_SECRET} is set")
    }
    credBuilder
  }

  /**
   * Starts the MesosSchedulerDriver and stores the current running driver to this new instance.
   * This driver is expected to not be running.
   * This method returns only after the scheduler has registered with Mesos.
   */
  def startScheduler(newDriver: SchedulerDriver): Unit = {
    synchronized {
      @volatile
      var error: Option[Exception] = None

      // We create a new thread that will block inside `mesosDriver.run`
      // until the scheduler exists
      new Thread(Utils.getFormattedClassName(this) + "-mesos-driver") {
        setDaemon(true)
        override def run(): Unit = {
          try {
            val ret = newDriver.run()
            logInfo("driver.run() returned with code " + ret)
            if (ret != null && ret.equals(Status.DRIVER_ABORTED)) {
              error = Some(new SparkException("Error starting driver, DRIVER_ABORTED"))
              markErr()
            }
          } catch {
            case e: Exception =>
              logError("driver.run() failed", e)
              error = Some(e)
              markErr()
          }
        }
      }.start()

      registerLatch.await()

      // propagate any error to the calling thread. This ensures that SparkContext creation fails
      // without leaving a broken context that won't be able to schedule any tasks
      error.foreach(throw _)
    }
  }

  def getResource(res: JList[Resource], name: String): Double = {
    // A resource can have multiple values in the offer since it can either be from
    // a specific role or wildcard.
    res.asScala.filter(_.getName == name).map(_.getScalar.getValue).sum
  }

  /**
   * Transforms a range resource to a list of ranges
   *
   * @param res the mesos resource list
   * @param name the name of the resource
   * @return the list of ranges returned
   */
  protected def getRangeResource(res: JList[Resource], name: String): List[(Long, Long)] = {
    // A resource can have multiple values in the offer since it can either be from
    // a specific role or wildcard.
    res.asScala.filter(_.getName == name).flatMap(_.getRanges.getRangeList.asScala
      .map(r => (r.getBegin, r.getEnd)).toList).toList
  }

  /**
   * Signal that the scheduler has registered with Mesos.
   */
  protected def markRegistered(): Unit = {
    registerLatch.countDown()
  }

  protected def markErr(): Unit = {
    registerLatch.countDown()
  }

  private def setReservationInfo(
       reservationInfo: Option[ReservationInfo],
       role: Option[String],
       builder: Resource.Builder): Unit = {
    if (!role.contains(ANY_ROLE)) {
      reservationInfo.foreach { res => builder.setReservation(res) }
    }
  }

  def createResource(
       name: String,
       amount: Double,
       role: Option[String] = None,
       reservationInfo: Option[ReservationInfo] = None): Resource = {
    val builder = Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(amount).build())
    role.foreach { r => builder.setRole(r) }
    setReservationInfo(reservationInfo, role, builder)
    builder.build()
  }

  private def getReservation(resource: Resource): Option[ReservationInfo] = {
    if (resource.hasReservation) {
      Some(resource.getReservation)
    } else {
      None
    }
  }
  /**
   * Partition the existing set of resources into two groups, those remaining to be
   * scheduled and those requested to be used for a new task.
   *
   * @param resources The full list of available resources
   * @param resourceName The name of the resource to take from the available resources
   * @param amountToUse The amount of resources to take from the available resources
   * @return The remaining resources list and the used resources list.
   */
  def partitionResources(
      resources: JList[Resource],
      resourceName: String,
      amountToUse: Double): (List[Resource], List[Resource]) = {
    var remain = amountToUse
    var requestedResources = new ArrayBuffer[Resource]
    val remainingResources = resources.asScala.map {
      case r =>
        val reservation = getReservation(r)
        if (remain > 0 &&
          r.getType == Value.Type.SCALAR &&
          r.getScalar.getValue > 0.0 &&
          r.getName == resourceName) {
          val usage = Math.min(remain, r.getScalar.getValue)
          requestedResources += createResource(resourceName, usage,
            Option(r.getRole), reservation)
          remain -= usage
          createResource(resourceName, r.getScalar.getValue - usage,
            Option(r.getRole), reservation)
        } else {
          r
        }
    }

    // Filter any resource that has depleted.
    val filteredResources =
      remainingResources.filter(r => r.getType != Value.Type.SCALAR || r.getScalar.getValue > 0.0)

    (filteredResources.toList, requestedResources.toList)
  }

  /** Helper method to get the key,value-set pair for a Mesos Attribute protobuf */
  protected def getAttribute(attr: Attribute): (String, Set[String]) = {
    (attr.getName, attr.getText.getValue.split(',').toSet)
  }

  /**
   * Converts the attributes from the resource offer into a Map of name to Attribute Value
   * The attribute values are the mesos attribute types and they are
   *
   * @param offerAttributes the attributes offered
   */
  protected def toAttributeMap(offerAttributes: JList[Attribute])
    : Map[String, GeneratedMessageV3] = {
    offerAttributes.asScala.map { attr =>
      val attrValue = attr.getType match {
        case Value.Type.SCALAR => attr.getScalar
        case Value.Type.RANGES => attr.getRanges
        case Value.Type.SET => attr.getSet
        case Value.Type.TEXT => attr.getText
      }
      (attr.getName, attrValue)
    }.toMap
  }


  /**
   * Match the requirements (if any) to the offer attributes.
   * if attribute requirements are not specified - return true
   * else if attribute is defined and no values are given, simple attribute presence is performed
   * else if attribute name and value is specified, subset match is performed on agent attributes
   */
  def matchesAttributeRequirements(
      agentOfferConstraints: Map[String, Set[String]],
      offerAttributes: Map[String, GeneratedMessageV3]): Boolean = {
    agentOfferConstraints.forall {
      // offer has the required attribute and subsumes the required values for that attribute
      case (name, requiredValues) =>
        offerAttributes.get(name) match {
          case Some(_) if requiredValues.isEmpty => true // empty value matches presence
          case Some(scalarValue: Value.Scalar) =>
            // check if provided values is less than equal to the offered values
            requiredValues.map(_.toDouble).exists(_ <= scalarValue.getValue)
          case Some(rangeValue: Value.Range) =>
            val offerRange = rangeValue.getBegin to rangeValue.getEnd
            // Check if there is some required value that is between the ranges specified
            // Note: We only support the ability to specify discrete values, in the future
            // we may expand it to subsume ranges specified with a XX..YY value or something
            // similar to that.
            requiredValues.map(_.toLong).exists(offerRange.contains(_))
          case Some(offeredValue: Value.Set) =>
            // check if the specified required values is a subset of offered set
            requiredValues.subsetOf(offeredValue.getItemList.asScala.toSet)
          case Some(textValue: Value.Text) =>
            // check if the specified value is equal, if multiple values are specified
            // we succeed if any of them match.
            requiredValues.contains(textValue.getValue)
          case _ => false
        }
    }
  }

  /**
   * Parses the attributes constraints provided to spark and build a matching data struct:
   *  {@literal Map[<attribute-name>, Set[values-to-match]}
   *  The constraints are specified as ';' separated key-value pairs where keys and values
   *  are separated by ':'. The ':' implies equality (for singular values) and "is one of" for
   *  multiple values (comma separated). For example:
   *  {{{
   *  parseConstraintString("os:centos7;zone:us-east-1a,us-east-1b")
   *  // would result in
   *  <code>
   *  Map(
   *    "os" -> Set("centos7"),
   *    "zone":   -> Set("us-east-1a", "us-east-1b")
   *  )
   *  }}}
   *
   *  Mesos documentation: http://mesos.apache.org/documentation/attributes-resources/
   *                       https://github.com/apache/mesos/blob/master/src/common/values.cpp
   *                       https://github.com/apache/mesos/blob/master/src/common/attributes.cpp
   *
   * @param constraintsVal contains string consisting of ';' separated key-value pairs (separated
   *                       by ':')
   * @return  Map of constraints to match resources offers.
   */
  def parseConstraintString(constraintsVal: String): Map[String, Set[String]] = {
    /*
      Based on mesos docs:
      attributes : attribute ( ";" attribute )*
      attribute : labelString ":" ( labelString | "," )+
      labelString : [a-zA-Z0-9_/.-]
    */
    val splitter = Splitter.on(';').trimResults().withKeyValueSeparator(':')
    // kv splitter
    if (constraintsVal.isEmpty) {
      Map()
    } else {
      try {
        splitter.split(constraintsVal).asScala.toMap.mapValues(v =>
          if (v == null || v.isEmpty) {
            Set.empty[String]
          } else {
            v.split(',').toSet
          }
        ).toMap
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(s"Bad constraint string: $constraintsVal", e)
      }
    }
  }

  // This default copied from YARN
  private val MEMORY_OVERHEAD_MINIMUM = 384

  /**
   * Return the amount of memory to allocate to each executor, taking into account
   * container overheads.
   *
   * @param sc SparkContext to use to get `spark.mesos.executor.memoryOverhead` value
   * @return memory requirement as (0.1 * memoryOverhead) or MEMORY_OVERHEAD_MINIMUM
   *         (whichever is larger)
   */
  def executorMemory(sc: SparkContext): Int = {
    val memoryOverheadFactor = sc.conf.get(EXECUTOR_MEMORY_OVERHEAD_FACTOR)
    sc.conf.get(mesosConfig.EXECUTOR_MEMORY_OVERHEAD).getOrElse(
      math.max(memoryOverheadFactor * sc.executorMemory, MEMORY_OVERHEAD_MINIMUM).toInt) +
      sc.executorMemory
  }

  /**
   * Return the amount of memory to allocate to each driver, taking into account
   * container overheads.
   *
   * @param driverDesc used to get driver memory
   * @return memory requirement defined as `DRIVER_MEMORY_OVERHEAD` if set in the config,
   *         otherwise the larger of `MEMORY_OVERHEAD_MINIMUM (=384MB)` or
   *         `MEMORY_OVERHEAD_FRACTION (=0.1) * driverMemory`
   */
  def driverContainerMemory(driverDesc: MesosDriverDescription): Int = {
    val memoryOverheadFactor = driverDesc.conf.get(DRIVER_MEMORY_OVERHEAD_FACTOR)
    val defaultMem = math.max(memoryOverheadFactor * driverDesc.mem, MEMORY_OVERHEAD_MINIMUM)
    driverDesc.conf.get(mesosConfig.DRIVER_MEMORY_OVERHEAD).getOrElse(defaultMem.toInt) +
      driverDesc.mem
  }

  def setupUris(uris: Seq[String],
                builder: CommandInfo.Builder,
                useFetcherCache: Boolean = false): Unit = {
    uris.foreach { uri =>
      builder.addUris(CommandInfo.URI.newBuilder().setValue(uri.trim()).setCache(useFetcherCache))
    }
  }

  protected def getRejectOfferDuration(conf: SparkConf): Long = {
    conf.get(mesosConfig.REJECT_OFFER_DURATION)
  }

  protected def getRejectOfferDurationForUnmetConstraints(conf: SparkConf): Long = {
    conf.get(mesosConfig.REJECT_OFFER_DURATION_FOR_UNMET_CONSTRAINTS)
      .getOrElse(getRejectOfferDuration(conf))
  }

  protected def getRejectOfferDurationForReachedMaxCores(conf: SparkConf): Long = {
    conf.get(mesosConfig.REJECT_OFFER_DURATION_FOR_REACHED_MAX_CORES)
      .getOrElse(getRejectOfferDuration(conf))
  }

  /**
   * Checks executor ports if they are within some range of the offered list of ports ranges,
   *
   * @param conf the Spark Config
   * @param ports the list of ports to check
   * @return true if ports are within range false otherwise
   */
  protected def checkPorts(conf: SparkConf, ports: List[(Long, Long)]): Boolean = {

    def checkIfInRange(port: Long, ps: List[(Long, Long)]): Boolean = {
      ps.exists{case (rangeStart, rangeEnd) => rangeStart <= port & rangeEnd >= port }
    }

    val portsToCheck = nonZeroPortValuesFromConfig(conf)
    val withinRange = portsToCheck.forall(p => checkIfInRange(p, ports))
    // make sure we have enough ports to allocate per offer
    val enoughPorts =
    ports.map{case (rangeStart, rangeEnd) => rangeEnd - rangeStart + 1}.sum >= portsToCheck.size
    enoughPorts && withinRange
  }

  /**
   * Partitions port resources.
   *
   * @param requestedPorts non-zero ports to assign
   * @param offeredResources the resources offered
   * @return resources left, port resources to be used.
   */
  def partitionPortResources(requestedPorts: List[Long], offeredResources: List[Resource])
    : (List[Resource], List[Resource]) = {
    if (requestedPorts.isEmpty) {
      (offeredResources, List[Resource]())
    } else {
      // partition port offers
      val (resourcesWithoutPorts, portResources) = filterPortResources(offeredResources)

      val portsAndResourceInfo = requestedPorts.
        map { x => (x, findPortAndGetAssignedResourceInfo(x, portResources)) }

      val assignedPortResources = createResourcesFromPorts(portsAndResourceInfo)

      // ignore non-assigned port resources, they will be declined implicitly by mesos
      // no need for splitting port resources.
      (resourcesWithoutPorts, assignedPortResources)
    }
  }

  val managedPortNames = List(BLOCK_MANAGER_PORT.key)

  /**
   * The values of the non-zero ports to be used by the executor process.
 *
   * @param conf the spark config to use
   * @return the ono-zero values of the ports
   */
  def nonZeroPortValuesFromConfig(conf: SparkConf): List[Long] = {
    managedPortNames.map(conf.getLong(_, 0)).filter( _ != 0)
  }

  private case class RoleResourceInfo(
      role: String,
      resInfo: Option[ReservationInfo])

  /** Creates a mesos resource for a specific port number. */
  private def createResourcesFromPorts(
      portsAndResourcesInfo: List[(Long, RoleResourceInfo)])
    : List[Resource] = {
    portsAndResourcesInfo.flatMap { case (port, rInfo) =>
      createMesosPortResource(List((port, port)), Option(rInfo.role), rInfo.resInfo)}
  }

  /** Helper to create mesos resources for specific port ranges. */
  private def createMesosPortResource(
      ranges: List[(Long, Long)],
      role: Option[String] = None,
      reservationInfo: Option[ReservationInfo] = None): List[Resource] = {
    // for ranges we are going to use (user defined ports fall in there) create mesos resources
    // for each range there is a role associated with it.
    ranges.map { case (rangeStart, rangeEnd) =>
      val rangeValue = Value.Range.newBuilder()
        .setBegin(rangeStart)
        .setEnd(rangeEnd)
      val builder = Resource.newBuilder()
        .setName("ports")
        .setType(Value.Type.RANGES)
        .setRanges(Value.Ranges.newBuilder().addRange(rangeValue))
      role.foreach { r => builder.setRole(r) }
      setReservationInfo(reservationInfo, role, builder)
      builder.build()
    }
  }

 /**
  * Helper to assign a port to an offered range and get the latter's role
  * info to use it later on.
  */
  private def findPortAndGetAssignedResourceInfo(port: Long, portResources: List[Resource])
    : RoleResourceInfo = {

    val ranges = portResources.
      map { resource =>
        val reservation = getReservation(resource)
        (RoleResourceInfo(resource.getRole, reservation),
          resource.getRanges.getRangeList.asScala.map(r => (r.getBegin, r.getEnd)).toList)
      }

    val rangePortResourceInfo = ranges
      .find { case (resourceInfo, rangeList) => rangeList
        .exists{ case (rangeStart, rangeEnd) => rangeStart <= port & rangeEnd >= port}}
    // this is safe since we have previously checked about the ranges (see checkPorts method)
    rangePortResourceInfo.map{ case (resourceInfo, rangeList) => resourceInfo}.get
  }

  /** Retrieves the port resources from a list of mesos offered resources */
  private def filterPortResources(resources: List[Resource]): (List[Resource], List[Resource]) = {
    resources.partition { r => !(r.getType == Value.Type.RANGES && r.getName == "ports") }
  }

  /**
   * spark.mesos.driver.frameworkId is set by the cluster dispatcher to correlate driver
   * submissions with frameworkIDs.  However, this causes issues when a driver process launches
   * more than one framework (more than one SparkContext(, because they all try to register with
   * the same frameworkID.  To enforce that only the first driver registers with the configured
   * framework ID, the driver calls this method after the first registration.
   */
  def unsetFrameworkID(sc: SparkContext): Unit = {
    sc.conf.remove(mesosConfig.DRIVER_FRAMEWORK_ID)
    System.clearProperty(mesosConfig.DRIVER_FRAMEWORK_ID.key)
  }

  def mesosToTaskState(state: MesosTaskState): TaskState.TaskState = state match {
    case MesosTaskState.TASK_STAGING |
         MesosTaskState.TASK_STARTING => TaskState.LAUNCHING
    case MesosTaskState.TASK_RUNNING |
         MesosTaskState.TASK_KILLING => TaskState.RUNNING
    case MesosTaskState.TASK_FINISHED => TaskState.FINISHED
    case MesosTaskState.TASK_FAILED |
         MesosTaskState.TASK_GONE |
         MesosTaskState.TASK_GONE_BY_OPERATOR => TaskState.FAILED
    case MesosTaskState.TASK_KILLED => TaskState.KILLED
    case MesosTaskState.TASK_LOST |
         MesosTaskState.TASK_ERROR |
         MesosTaskState.TASK_DROPPED |
         MesosTaskState.TASK_UNKNOWN |
         MesosTaskState.TASK_UNREACHABLE => TaskState.LOST
  }

  protected def declineOffer(
    driver: org.apache.mesos.SchedulerDriver,
    offer: Offer,
    reason: Option[String] = None,
    refuseSeconds: Option[Long] = None): Unit = {

    val id = offer.getId.getValue
    val offerAttributes = toAttributeMap(offer.getAttributesList)
    val mem = getResource(offer.getResourcesList, "mem")
    val cpus = getResource(offer.getResourcesList, "cpus")
    val ports = getRangeResource(offer.getResourcesList, "ports")

    logDebug(s"Declining offer: $id with " +
      s"attributes: $offerAttributes " +
      s"mem: $mem " +
      s"cpu: $cpus " +
      s"port: $ports " +
      refuseSeconds.map(s => s"for ${s} seconds ").getOrElse("") +
      reason.map(r => s" (reason: $r)").getOrElse(""))

    refuseSeconds match {
      case Some(seconds) =>
        val filters = Filters.newBuilder().setRefuseSeconds(seconds).build()
        driver.declineOffer(offer.getId, filters)
      case _ =>
        driver.declineOffer(offer.getId)
    }
  }
}
