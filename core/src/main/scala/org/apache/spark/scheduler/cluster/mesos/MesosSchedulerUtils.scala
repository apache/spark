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

import com.google.common.base.Splitter

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.mesos.Protos._
import org.apache.mesos.{Protos, MesosSchedulerDriver, Scheduler}
import org.apache.spark.Logging
import org.apache.spark.util.Utils


/**
 * Shared trait for implementing a Mesos Scheduler. This holds common state and helper
 * methods and Mesos scheduler will use.
 */
private[mesos] trait MesosSchedulerUtils extends Logging {
  // Lock used to wait for scheduler to be registered
  private final val registerLatch = new CountDownLatch(1)

  // Driver for talking to Mesos
  protected var mesosDriver: MesosSchedulerDriver = null

  /**
   * Starts the MesosSchedulerDriver with the provided information. This method returns
   * only after the scheduler has registered with Mesos.
   * @param masterUrl Mesos master connection URL
   * @param scheduler Scheduler object
   * @param fwInfo FrameworkInfo to pass to the Mesos master
   */
  def startScheduler(masterUrl: String, scheduler: Scheduler, fwInfo: FrameworkInfo): Unit = {
    synchronized {
      if (mesosDriver != null) {
        registerLatch.await()
        return
      }

      new Thread(Utils.getFormattedClassName(this) + "-mesos-driver") {
        setDaemon(true)

        override def run() {
          mesosDriver = new MesosSchedulerDriver(scheduler, fwInfo, masterUrl)
          try {
            val ret = mesosDriver.run()
            logInfo("driver.run() returned with code " + ret)
            if (ret.equals(Status.DRIVER_ABORTED)) {
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
  protected def markRegistered(): Unit = {
    registerLatch.countDown()
  }

  /**
   * Get the amount of resources for the specified type from the resource list
   */
  protected def getResource(res: JList[Resource], name: String): Double = {
    for (r <- res if r.getName == name) {
      return r.getScalar.getValue
    }
    0.0
  }


  /** Helper method to get the key,value-set pair for a Mesos Attribute protobuf */
  private[mesos] def getAttribute(attr: Attribute): (String, Set[String]) =
    (attr.getName, attr.getText.getValue.split(',').toSet)


  /** Build a Mesos resource protobuf object */
  private[mesos] def createResource(resourceName: String, quantity: Double): Protos.Resource = {
    Resource.newBuilder()
      .setName(resourceName)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(quantity).build())
      .build()
  }


  /**
   * Match the requirements (if any) to the offer attributes.
   * if attribute requirements are not specified - return true
   * else if attribute is defined and no values are given, simple attribute presence is preformed
   * else if attribute name and value is specified, subset match is performed on slave attributes
   */
  private[mesos] def matchesAttributeRequirements(
    slaveOfferConstraints: Map[String, Set[String]],
    offerAttributes: Map[String, Set[String]]): Boolean =
    if (slaveOfferConstraints.isEmpty) {
      true
    } else {
      slaveOfferConstraints.forall {
        // offer has the required attribute and subsumes the required values for that attribute
        case (name, requiredValues) =>
          // The attributes and their values are case sensitive during comparison
          // i.e tachyon -> true != Tachyon -> true != tachyon -> True
          offerAttributes.contains(name) && requiredValues.subsetOf(offerAttributes(name))

      }
    }

  /**
   * Parses the attributes constraints provided to spark and build a matching data struct:
   *  Map[<attribute-name>, Set[values-to-match]
   *  The constraints are specified as ';' separated key-value pairs where keys and values
   *  are separated by ':'. The ':' implies equality. For example:
   *  {{{
   *  parseConstraintString("tachyon:true;zone:us-east-1a,us-east-1b")
   *  // would result in
   *  <code>
   *  Map(
   *    "tachyon" -> Set("true"),
   *    "zone":   -> Set("us-east-1a", "us-east-1b")
   *  )
   *  }}}
   * @param constraintsVal constaints string consisting of ';' separated key-value pairs (separated
   *                       by ':')
   * @return  Map of constraints to match resources offers.
   */
  private[mesos] def parseConstraintString(constraintsVal: String): Map[String, Set[String]] = {
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
      Map() ++ mapAsScalaMap(splitter.split(constraintsVal)).map {
        case (k, v) =>
          if (v == null) {
            (k, Set[String]())
          } else {
            (k, v.split(',').toSet)
          }
      }
    }
  }

  /**
   * For the list of offers received, find the ones that match the offer constraints (if specified)
   * @param offers set of all offers received
   * @return Offers that match the constraints
   */
  private[mesos] def filterOffersByConstraints(
    offers: JList[Offer],
    offerConstraints: Map[String, Set[String]]): mutable.Buffer[Offer] = offers.filter { o =>
    matchesAttributeRequirements(offerConstraints, (o.getAttributesList map getAttribute).toMap)
  }

}
