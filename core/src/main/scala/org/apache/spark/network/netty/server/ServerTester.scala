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

package org.apache.spark.network.netty.server

import org.apache.spark.SparkConf
import org.apache.spark.network.netty.PathResolver
import org.apache.spark.storage.{TestBlockId, FileSegment, BlockId}

/** A simple main function for testing the server. */
object ServerTester {
  def main(args: Array[String]): Unit = {
    new BlockServer(new SparkConf, new PathResolver {
      override def getBlockLocation(blockId: BlockId): FileSegment = {
        val file = new java.io.File(blockId.asInstanceOf[TestBlockId].id)
        new FileSegment(file, 0, file.length())
      }
    })
    Thread.sleep(1000000)
  }
}
