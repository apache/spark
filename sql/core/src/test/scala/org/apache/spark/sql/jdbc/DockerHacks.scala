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

package org.apache.spark.sql.jdbc

import scala.collection.mutable.MutableList

import com.spotify.docker.client._

/**
 * A factory and morgue for DockerClient objects.  In the DockerClient we use,
 * calling close() closes the desired DockerClient but also renders all other
 * DockerClients inoperable.  This is inconvenient if we have more than one
 * open, such as during tests.
 */
object DockerClientFactory {
  var numClients: Int = 0
  val zombies = new MutableList[DockerClient]()

  def get(): DockerClient = {
    this.synchronized {
      numClients = numClients + 1
      DefaultDockerClient.fromEnv.build()
    }
  }

  def close(dc: DockerClient) {
    this.synchronized {
      numClients = numClients - 1
      zombies += dc
      if (numClients == 0) {
        zombies.foreach(_.close())
        zombies.clear()
      }
    }
  }
}
