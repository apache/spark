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

import java.io.{IOException, ObjectInputStream}
import java.util.concurrent.atomic.AtomicReference

/**
 * A lock-free implementation of a lazily-initialized variable.
 * If there are concurrent initializations then the `compute()` function may be invoked
 * multiple times. However, only a single `compute()` result will be stored and all readers
 * will receive the same result object instance.
 *
 * This may be helpful for avoiding deadlocks in certain scenarios where exactly-once
 * value computation is not a hard requirement.
 *
 * The main difference between this and [[BestEffortLazyVal]] is that:
 * [[BestEffortLazyVal]] serializes the cached value after computation, while
 * [[TransientBestEffortLazyVal]] always serializes the compute function.
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
private[spark] class TransientBestEffortLazyVal[T <: AnyRef](
    private[this] val compute: () => T) extends Serializable {

  @transient
  private[this] var cached: AtomicReference[T] = new AtomicReference(null.asInstanceOf[T])

  def apply(): T = {
    val value = cached.get()
    if (value != null) {
      value
    } else {
      val newValue = compute()
      assert(newValue != null, "compute function cannot return null.")
      cached.compareAndSet(null.asInstanceOf[T], newValue)
      cached.get()
    }
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    cached = new AtomicReference(null.asInstanceOf[T])
  }
}
