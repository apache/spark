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

package org.apache.spark.scheduler

import org.apache.spark.storage.BlockManagerId
import java.io.{ObjectOutput, ObjectInput, Externalizable}

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task ran on as well as the sizes of outputs for each reducer, for passing on to the reduce tasks.
 * The map output sizes are compressed using MapOutputTracker.compressSize.
 */
private[spark] class MapStatus(var location: BlockManagerId, var compressedSizes: Array[Byte])
  extends Externalizable {

  def this() = this(null, null)  // For deserialization only

  def writeExternal(out: ObjectOutput) {
    location.writeExternal(out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
  }

  def readExternal(in: ObjectInput) {
    location = BlockManagerId(in)
    compressedSizes = new Array[Byte](in.readInt())
    in.readFully(compressedSizes)
  }
}
