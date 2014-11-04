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

package org.apache.spark.shuffle.sort

import org.apache.spark.{TaskContext, Logging}
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader}
import org.apache.spark.shuffle.hash.HashShuffleReader

private[spark] class MixedShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext)
  extends ShuffleReader[K, C] with Logging {

  private val shuffleReader = if (handle.dependency.keyOrdering.isDefined) {
    new SortShuffleReader[K, C](handle, startPartition, endPartition, context)
  } else {
    new HashShuffleReader[K, C](handle, startPartition, endPartition, context)
  }

  override def read(): Iterator[Product2[K, C]] = shuffleReader.read()

  override def stop(): Unit = shuffleReader.stop()
}
