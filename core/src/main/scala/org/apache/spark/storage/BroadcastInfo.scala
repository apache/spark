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

package org.apache.spark.storage

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.{TorrentBroadcast, Broadcast}
import org.apache.spark.util.Utils
import org.apache.spark.util.Utils._

@DeveloperApi
class BroadcastInfo(
    val id: Long,
    val name: String,
    val numPartitions: Int) extends Ordered[BroadcastInfo] {

  var memSize = 0L
  var diskSize = 0L
  var tachyonSize = 0L

  override def toString = {
    import Utils.bytesToString
    ("%s\" (%d) ; " +
      "MemorySize: %s; TachyonSize: %s; DiskSize: %s").format(
        name, id, bytesToString(memSize), bytesToString(tachyonSize), bytesToString(diskSize))
  }

  override def compare(that: BroadcastInfo) = {
    if (this.id > that.id) {
      1
    } else {
      if (this.id == that.id) {
        0
      }
      -1
    }
  }
}
