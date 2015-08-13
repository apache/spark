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

import java.util.{List => JList}
import java.util.concurrent.CountDownLatch

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.google.common.base.Splitter
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver, Scheduler, Protos}
import org.apache.mesos.Protos._
import org.apache.mesos.protobuf.{ByteString, GeneratedMessage}
import org.apache.spark.{SparkException, SparkConf, Logging, SparkContext}
import org.apache.spark.util.Utils


/**
 * Shared trait for implementing a Mesos Scheduler. This holds common state and helper
 * methods and Mesos scheduler will use.
 */
private[mesos] trait MesosSchedulerUtils extends Logging {
  // Lock used to wait for scheduler to be registered
  private final val registerLatch = new CountDownLatch(1)

  // Driver for talking to Mesos
  protected var mesosDriver: SchedulerDriver = null

  /**
   * Creates a new MesosSchedulerDriver that communicates to the Mesos master.
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
    val credBuilder = Credential.newBuilder()
    webuiUrl.foreach { url => fwInfoBuilder.setWebuiUrl(url) }
    checkpoint.foreach { checkpoint => fwInfoBuilder.setCheckpoint(checkpoint) }
    failoverTimeout.foreach { timeout => fwInfoBuilder.setFailoverTimeout(timeout) }
    frameworkId.foreach { id =>
      fwInfoBuilder.setId(FrameworkID.newBuilder().setValue(id).build())
    }
    conf.getOption("spark.mesos.principal").foreach { principal =>
      fwInfoBuilder.setPrincipal(principal)
      credBuilder.setPrincipal(principal)
    }
    conf.getOption("spark.mesos.secret").foreach { secret =>
      credBuilder.setSecret(ByteString.copyFromUtf8(secret))
    }
    if (credBuilder.hasSecret && !fwInfoBuilder.hasPrincipal) {
      throw new SparkException(
        "spark.mesos.principal must be configured when spark.mesos.secret is set")
    }
    conf.getOption("spark.mesos.role").foreach { role =>
      fwInfoBuilder.setRole(role)
    }
    if (credBuilder.hasPrincipal) {
      new MesosSchedulerDriver(
        scheduler, fwInfoBuilder.build(), masterUrl, credBuilder.build())
    } else {
      new MesosSchedulerDriver(scheduler, fwInfoBuilder.build(), masterUrl)
    }
  }

  /**
   * Starts the MesosSchedulerDriver and stores the current running driver to this new instance.
   * This driver is expected to not be running.
   * This method returns only after the scheduler has registered with Mesos.
   */
  def startScheduler(newDriver: SchedulerDriver): Unit = {
    synchronized {
      if (mesosDriver != null) {
        registerLatch.await()
        return
      }

      new Thread(Utils.getFormattedClassName(this) + "-mesos-driver") {
        setDaemon(true)

        override def run() {
          mesosDriver = newDriver
          try {
            val ret = mesosDriver.run()
            logInfo("driver.run() returned with code " + ret)
            if (ret != null && ret.equals(Status.DRIVER_ABORTED)) {
              System.exit(1)
            }
          } catch {
            case e: Exception => {
              logError("driver.run() failed", e)
              System.exit(1)
            }
          }
        }
      }.start()

      registerLatch.await()
    }
  }

  /**
   * Signal that the scheduler has registered with Mesos.
   */
  protected def getResource(res: JList[Resource], name: String): Double = {
    // A resource can have multiple values in the offer since it can either be from
    // a specific role or wildcard.
    res.filter(_.getName == name).map(_.getScalar.getValue).sum
  }

  protected def markRegistered(): Unit = {
    registerLatch.countDown()
  }

  def createResource(name: String, amount: Double, role: Option[String] = None): Resource = {
    val builder = Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(amount).build())

    role.foreach { r => builder.setRole(r) }

    builder.build()
  }

  /**
   * Partition the existing set of resources into two groups, those remaining to be
   * scheduled and those requested to be used for a new task.
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
    val remainingResources = resources.map {
      case r => {
        if (remain > 0 &&
          r.getType == Value.Type.SCALAR &&
          r.getScalar.getValue > 0.0 &&
          r.getName == resourceName) {
          val usage = Math.min(remain, r.getScalar.getValue)
          requestedResources += createResource(resourceName, usage, Some(r.getRole))
          remain -= usage
          createResource(resourceName, r.getScalar.getValue - usage, Some(r.getRole))
        } else {
          r
        }
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


  /** Build a Mesos resource protobuf object */
  protected def createResource(resourceName: String, quantity: Double): Protos.Resource = {
    Resource.newBuilder()
      .setName(resourceName)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(quantity).build())
      .build()
  }

  /**
   * Converts the attributes from the resource offer into a Map of name -> Attribute Value
   * The attribute values are the mesos attribute types and they are
   * @param offerAttributes
   * @return
   */
  protected def toAttributeMap(offerAttributes: JList[Attribute]): Map[String, GeneratedMessage] = {
    offerAttributes.map(attr => {
      val attrValue = attr.getType match {
        case Value.Type.SCALAR => attr.getScalar
        case Value.Type.RANGES => attr.getRanges
        case Value.Type.SET => attr.getSet
        case Value.Type.TEXT => attr.getText
      }
      (attr.getName, attrValue)
    }).toMap
  }


  /**
   * Match the requirements (if any) to the offer attributes.
   * if attribute requirements are not specified - return true
   * else if attribute is defined and no values are given, simple attribute presence is performed
   * else if attribute name and value is specified, subset match is performed on slave attributes
   */
  def matchesAttributeRequirements(
      slaveOfferConstraints: Map[String, Set[String]],
      offerAttributes: Map[String, GeneratedMessage]): Boolean = {
    slaveOfferConstraints.forall {
      // offer has the required attribute and subsumes the required values for that attribute
      case (name, requiredValues) =>
        offerAttributes.get(name) match {
          case None => false
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
            requiredValues.subsetOf(offeredValue.getItemList.toSet)
          case Some(textValue: Value.Text) =>
            // check if the specified value is equal, if multiple values are specified
            // we succeed if any of them match.
            requiredValues.contains(textValue.getValue)
        }
    }
  }

  /**
   * Parses the attributes constraints provided to spark and build a matching data struct:
   *  Map[<attribute-name>, Set[values-to-match]]
   *  The constraints are specified as ';' separated key-value pairs where keys and values
   *  are separated by ':'. The ':' implies equality (for singular values) and "is one of" for
   *  multiple values (comma separated). For example:
   *  {{{
   *  parseConstraintString("tachyon:true;zone:us-east-1a,us-east-1b")
   *  // would result in
   *  <code>
   *  Map(
   *    "tachyon" -> Set("true"),
   *    "zone":   -> Set("us-east-1a", "us-east-1b")
   *  )
   *  }}}
   *
   *  Mesos documentation: http://mesos.apache.org/documentation/attributes-resources/
   *                       https://github.com/apache/mesos/blob/master/src/common/values.cpp
   *                       https://github.com/apache/mesos/blob/master/src/common/attributes.cpp
   *
   * @param constraintsVal constaints string consisting of ';' separated key-value pairs (separated
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
        Map() ++ mapAsScalaMap(splitter.split(constraintsVal)).map {
          case (k, v) =>
            if (v == null || v.isEmpty) {
              (k, Set[String]())
            } else {
              (k, v.split(',').toSet)
            }
        }
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(s"Bad constraint string: $constraintsVal", e)
      }
    }
  }

  // These defaults copied from YARN
  private val MEMORY_OVERHEAD_FRACTION = 0.10
  private val MEMORY_OVERHEAD_MINIMUM = 384

  /**
   * Return the amount of memory to allocate to each executor, taking into account
   * container overheads.
   * @param sc SparkContext to use to get `spark.mesos.executor.memoryOverhead` value
   * @return memory requirement as (0.1 * <memoryOverhead>) or MEMORY_OVERHEAD_MINIMUM
   *         (whichever is larger)
   */
  def calculateTotalMemory(sc: SparkContext): Int = {
    sc.conf.getInt("spark.mesos.executor.memoryOverhead",
      math.max(MEMORY_OVERHEAD_FRACTION * sc.executorMemory, MEMORY_OVERHEAD_MINIMUM).toInt) +
      sc.executorMemory
  }

  def setupUris(uris: String, builder: CommandInfo.Builder): Unit = {
    uris.split(",").foreach { uri =>
      builder.addUris(CommandInfo.URI.newBuilder().setValue(uri.trim()))
    }
  }

}
