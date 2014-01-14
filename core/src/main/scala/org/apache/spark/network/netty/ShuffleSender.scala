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

package org.apache.spark.network.netty

import java.io.File

import org.apache.spark.Logging
import org.apache.spark.util.Utils
import org.apache.spark.storage.{BlockId, FileSegment}


private[spark] class ShuffleSender(portIn: Int, val pResolver: PathResolver) extends Logging {

  val server = new FileServer(pResolver, portIn)
  server.start()

  def stop() {
    server.stop()
  }

  def port: Int = server.getPort()
}


/**
 * An application for testing the shuffle sender as a standalone program.
 */
private[spark] object ShuffleSender {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Usage: ShuffleSender <port> <subDirsPerLocalDir> <list of shuffle_block_directories>")
      System.exit(1)
    }

    val port = args(0).toInt
    val subDirsPerLocalDir = args(1).toInt
    val localDirs = args.drop(2).map(new File(_))

    val pResovler = new PathResolver {
      override def getBlockLocation(blockId: BlockId): FileSegment = {
        if (!blockId.isShuffle) {
          throw new Exception("Block " + blockId + " is not a shuffle block")
        }
        // Figure out which local directory it hashes to, and which subdirectory in that
        val hash = Utils.nonNegativeHash(blockId)
        val dirId = hash % localDirs.length
        val subDirId = (hash / localDirs.length) % subDirsPerLocalDir
        val subDir = new File(localDirs(dirId), "%02x".format(subDirId))
        val file = new File(subDir, blockId.name)
        new FileSegment(file, 0, file.length())
      }
    }
    val sender = new ShuffleSender(port, pResovler)
  }
}
