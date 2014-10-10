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

package org.apache.spark.network

private[spark] case class ConnectionId(connectionManagerId: ConnectionManagerId, uniqId: Int) {
  override def toString = connectionManagerId.host + "_" + connectionManagerId.port + "_" + uniqId
}

private[spark] object ConnectionId {

  def createConnectionIdFromString(connectionIdString: String): ConnectionId = {
    val res = connectionIdString.split("_").map(_.trim())
    if (res.size != 3) {
      throw new Exception("Error converting ConnectionId string: " + connectionIdString +
        " to a ConnectionId Object")
    }
    new ConnectionId(new ConnectionManagerId(res(0), res(1).toInt), res(2).toInt)
  }
}
