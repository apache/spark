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

import scala.collection.mutable._
import scala.concurrent.Await

import akka.actor.{ActorContext, ActorSelection}
import akka.pattern.ask

import org.apache.spark.ps.CoarseGrainedParameterServerMessage._
import org.apache.spark.util.AkkaUtils
import org.apache.spark.{SparkConf, SparkEnv}

/**
 * Client Role in `Parameter Server`
 */
class PSClient(clientId: String, context: ActorContext, conf: SparkConf) {
  val interval = conf.getInt("spark.ps.server.heartbeatInterval", 1000)
  val timeout = AkkaUtils.lookupTimeout(conf)
  val retryAttempts = AkkaUtils.numRetries(conf)
  val retryIntervalMs = AkkaUtils.retryWaitMs(conf)
  val serverId2ActorRef = new HashMap[Long, ActorSelection]()
  var currentClock: ThreadLocal[Int] = new ThreadLocal[Int] {
    override def initialValue(): Int = 0
  }
  val waiting: String = "waiting"
  
  def addServer(serverInfo: ServerInfo) {
    val serverRef = context.actorSelection(serverInfo.serverUrl)
    serverId2ActorRef(serverInfo.serverId) = serverRef
    serverRef ! InitPSClient(clientId)
  }

  def get(key: String): Array[Double] = {
    val message = GetParameter(key, currentClock.get())
    val serverRef = serverId2ActorRef.head._2
    val future = serverRef.ask(message)(timeout)
    val response = Await.result(future, timeout)
    response.asInstanceOf[Parameter].value
  }

  def set(key: String, value: Array[Double]): Unit = {
    val serverRef = serverId2ActorRef.head._2
    serverRef ! SetParameter(key, value, currentClock.get())
  }

  def update(key: String, value: Array[Double]): Unit = {
    val serverRef = serverId2ActorRef.head._2
    serverRef ! UpdateParameter(key, value, currentClock.get())
  }

  def serverActorRef(url: String) = {
    SparkEnv.get.actorSystem.actorSelection(url)
  }

  def clock(): Unit = {
    val cc = currentClock.get()
    val serverRef = serverId2ActorRef.head._2
    val message = UpdateClock(clientId, cc)
    val future = serverRef.ask(message)(timeout)
    val pause = Await.result(future, timeout).asInstanceOf[Boolean]
    if (pause) {
      waiting.synchronized {
        waiting.wait()
      }
    }
    currentClock.set(cc + 1)
  }

  def initClock(clock: Int): Unit = {
    currentClock.set(clock)
  }

  def notifyTasks(): Unit = {
    waiting.synchronized {
      waiting.notifyAll()
    }
  }
}
