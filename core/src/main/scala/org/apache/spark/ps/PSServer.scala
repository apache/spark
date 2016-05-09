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

package org.apache.spark.ps

import scala.reflect.ClassTag
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import akka.actor.ActorContext

import org.apache.spark.SparkConf
import org.apache.spark.ps.CoarseGrainedParameterServerMessage.NotifyClient
import org.apache.spark.ps.storage.PSStorage

/**
 * Server Role in `Parameter Server`
 */
private[spark] class PSServer(
    context: ActorContext,
    sparkConf: SparkConf,
    serverId: Long,
    agg: ArrayBuffer[Array[Double]] => Array[Double],
    func: (Array[Double], Array[Double]) => Array[Double]) {


  private val psKVStorage: PSStorage = PSStorage.getKVStorage(sparkConf)

  private val inValidV = new Array[Double](0)
  private var globalClock: Int = 0
  private val staleClock: Int = sparkConf.get("spark.ps.stale.clock", "0").toInt
  private val allTaskClientClock = new mutable.HashMap[String, Int]()
  private val allTaskClients = new mutable.HashSet[String]()

  def getParameter[K: ClassTag, V: ClassTag](key: K, clock: Int): (Boolean, V) = {
    if (checkValidity(clock)) {
      (true, psKVStorage.get[K, V](key).get)
    } else {
      (false, inValidV.asInstanceOf[V])
    }
  }

  def setParameter[K: ClassTag, V: ClassTag](key: K, value: V, clock: Int): Boolean = {
    if (checkValidity(clock)) {
      psKVStorage.put[K, V](key, value)
      true
    } else {
      false
    }
  }

  def updateParameter[K: ClassTag, V: ClassTag](key: K, value: V, clock: Int): Boolean = {
    if (checkValidity(clock)) {
      psKVStorage.update[K, V](key, value)
      true
    } else {
      false
    }
  }

  /**
   * batch update parameter from PSTask
   * every k-v pair has same array index
   * @param keys ordered keys
   * @param values  ordered values
   */
  def batchUpdateParameter[K: ClassTag, V: ClassTag](keys: Array[K], values: Array[V]): Unit = {

  }

  def updateClock(clientId: String, clock: Int): Boolean = {
    println(s"global clock: $globalClock, clock: $clock")
    allTaskClientClock(clientId) = clock
    allTaskClientClock.synchronized {
      val slower = allTaskClientClock.filter(_._2 < globalClock)
      if (slower.size == 0) {
        update()
        globalClock += 1
        notifyAllClients()
      }
    }

    if ((clock + 1) > globalClock + staleClock) {
      true
    } else {
      false
    }
  }

  def initPSClient(clientId: String): Unit = {
    allTaskClientClock(clientId) = -1
  }

  def addPSClient(executorUrl: String): Unit = {
    allTaskClients.add(executorUrl)
  }

  def checkValidity(clock: Int): Boolean = {
    if (clock <= globalClock + staleClock) {
      true
    } else {
      false
    }
  }

  def notifyAllClients(): Unit = {
    println("notify all ps clients")
    val message = "notify all ps clients"
    allTaskClients.foreach(e => {
      val executorRef = context.actorSelection(e)
      executorRef ! NotifyClient(message)
    })
  }

  private def update(): Unit = {
    psKVStorage.applyDelta[String, Array[Double]](agg, func)
  }
}
