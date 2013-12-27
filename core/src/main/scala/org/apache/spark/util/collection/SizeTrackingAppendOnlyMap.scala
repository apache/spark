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

package org.apache.spark.util.collection

import org.apache.spark.util.SamplingSizeTracker

/** Append-only map that keeps track of its estimated size in bytes. */
private[spark] class SizeTrackingAppendOnlyMap[K, V] extends AppendOnlyMap[K, V] {

  private val sizeTracker = new SamplingSizeTracker(this)

  def estimateSize() = sizeTracker.estimateSize()

  override def update(key: K, value: V): Unit = {
    super.update(key, value)
    sizeTracker.updateMade()
  }

  override def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    val newValue = super.changeValue(key, updateFunc)
    sizeTracker.updateMade()
    newValue
  }

  override protected def growTable() {
    super.growTable()
    sizeTracker.flushSamples()
  }
}
