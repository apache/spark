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

import scala.reflect.ClassTag

/**
 * CompactBuffer that keeps track of its size via SizeTracker.
 */
private[spark] class SizeTrackingCompactBuffer[T: ClassTag] extends CompactBuffer[T]
  with SizeTracker {

  override def +=(t: T): SizeTrackingCompactBuffer[T] = {
    super.+=(t)
    super.afterUpdate()
    this
  }

  override def ++=(t: TraversableOnce[T]): SizeTrackingCompactBuffer[T] = {
    super.++=(t)
    super.afterUpdate()
    this
  }
}

private[spark] object SizeTrackingCompactBuffer {
  def apply[T: ClassTag](): SizeTrackingCompactBuffer[T] = new SizeTrackingCompactBuffer[T]

  def apply[T: ClassTag](value: T): SizeTrackingCompactBuffer[T] = {
    val buf = new SizeTrackingCompactBuffer[T]
    buf += value
  }
}
