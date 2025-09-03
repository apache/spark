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
package org.apache.spark.sql.rabbitmq.common

import java.nio.file.{Path, Paths}

case class RmqPropsHolder(props: Map[String, String], path: String = "/opt/etc") {

  private val queueName = req("rmq.queuename")
  private val host = req("rmq.host")
  private val port = opt("rmq.port").map(_.toInt).getOrElse(5552)
  private val vhost = opt("rmq.vhost").getOrElse("/")
  private val username = opt("rmq.username").getOrElse("guest")
  private val password = opt("rmq.password").getOrElse("guest")
  private val fetchSize = opt("rmq.fetchsize").map(_.toLong).getOrElse(2000L)
  private val readTimeoutSec = opt("rmq.readtimeout").map(_.toLong).getOrElse(300L)
  private val maxBatch = opt("rmq.maxbatchsize").map(_.toLong).getOrElse(1000L)
  private val checkpointPath = opt("rmq.offsetcheckpointpath")
    .getOrElse(path)
  private val checkpointFile = {
    Paths.get(checkpointPath).resolve("customOffset")
  }

  def getPort: Int = port

  def getVhost: String = vhost

  def getUsername: String = username

  def getPassword: String = password

  def getFetchSize: Long = fetchSize

  def getReadTimeoutSec: Long = readTimeoutSec

  def getMaxBatch: Long = maxBatch

  def getQueueName: String = queueName

  def getHost: String = host

  def getCheckpointFile: Path = checkpointFile

  def getCheckpointPath: String = checkpointPath

  def getProps: Map[String, String] = props


  private def req(k: String): String = {
    val v = props.getOrElse(k, null)
    if (v == null) throw new IllegalArgumentException(s"Missing option: $k")
    v
  }

  private def opt(k: String): Option[String] = props.get(k)
}
