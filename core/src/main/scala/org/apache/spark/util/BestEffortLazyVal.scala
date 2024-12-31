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
package org.apache.spark.util

import java.io.{IOException, ObjectOutputStream}
import java.util.concurrent.atomic.AtomicReference

/**
 * A util class to lazily initialize a variable, re-computation is allowed if multiple
 * threads are trying to initialize it concurrently.
 * This may be helpful for avoiding deadlocks in certain scenarios while extract-once
 * is not a hard requirement.
 *
 * @note
 * This helper class has additional requirements on the compute function:
 *   1) The compute function MUST not return null;
 *   2) The computation failure is not cached.
 *
 * @note
 *   Scala 3 uses a different implementation of lazy vals which doesn't have this problem.
 *   Please refer to <a
 *   href="https://docs.scala-lang.org/scala3/reference/changed-features/lazy-vals-init.html">Lazy
 *   Vals Initialization</a> for more details.
 */
private[spark] class BestEffortLazyVal[T <: AnyRef](
    private[this] var compute: () => T) extends Serializable {

  private[this] val cached: AtomicReference[T] = new AtomicReference(null.asInstanceOf[T])

  def apply(): T = {
    val value = cached.get()
    if (value != null) {
      // The value has been cached, so null the compute function
      this.compute = null
      value
    } else {
      val newValue = compute()
      assert(newValue != null, "compute function cannot return null.")
      cached.compareAndSet(null.asInstanceOf[T], newValue)
      cached.get()
    }
  }

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    if (this.compute != null && cached.get() != null) {
      // The value has been cached, so null the compute function
      this.compute = null
    }
    oos.defaultWriteObject()
  }
}
