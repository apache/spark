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

import org.apache.spark.util.Utils

class StoreInfo(
    val id: String,
    val store: DiskStore,
    val totalSize: Long = 0L) {

  private var currentSize = 0L
  private val currentSizeLock = new AnyRef

  def freeSize = totalSize - currentSize

  override def toString = {
    import Utils.bytesToString
    ("Store %s : TotalSize: %s; CurrentSize: %s").format(
        id, bytesToString(totalSize), bytesToString(currentSize))
  }

  def tryUse(size: Long): Boolean = {
    // 0 for totalSize means unlimited volume
    if (totalSize == 0) {
      return true
    }

    currentSizeLock.synchronized {
      if (freeSize >= size) {
        currentSize += size
        true
      } else {
        false
      }
    }
  }

  def use(size: Long): Unit = {
    if (totalSize == 0) {
      return
    }

    currentSizeLock.synchronized {
        currentSize += size
    }
  }

  def free(size: Long): Unit = {
    if (totalSize == 0) {
      return
    }

    currentSizeLock.synchronized {
      currentSize -= size
    }
  }

}


