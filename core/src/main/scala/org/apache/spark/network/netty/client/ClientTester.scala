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

package org.apache.spark.network.netty.client

import org.apache.spark.SparkConf

object ClientTester {
  def main(args: Array[String]): Unit = {
    val remoteHost = args(0)
    val remotePort = args(1).toInt
    val blocks = args.drop(2)

    val clientFactory = new BlockFetchingClientFactory(new SparkConf)
    val client = clientFactory.createClient(remoteHost, remotePort)

    client.fetchBlocks(
      blocks,
      (blockId, data) => {
        println("got block id " + blockId)
        val bytes = new Array[Byte](data.byteBuffer().remaining())
        data.byteBuffer().get(bytes)
        println("data in string: " + new String(bytes))
      },
      (blockId, errorMessage) => {
        println(s"failed to fetch $blockId, error message: $errorMessage")
      }
    )
    client.waitForClose()
  }
}
