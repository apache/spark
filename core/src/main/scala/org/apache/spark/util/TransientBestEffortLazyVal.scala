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

/**
 * A util class to lazily initialize a variable, re-computation is allowed if multiple
 * threads are trying to initialize it concurrently.
 * This may be helpful for avoiding deadlocks in certain scenarios while extract-once
 * is not a hard requirement.
 * The main difference between this and [[BestEffortLazyVal]] is that:
 * [[BestEffortLazyVal]] serializes the cached value after computation, while
 * [[TransientBestEffortLazyVal]] always serializes the compute function.
 *
 * @note
 * This helper class has additional requirements on the compute function:
 *   1) The compute function MUST not return null;
 *   2) The compute function MUST be deterministic;
 *   3) This class won't cache the failure.
 * @note
 *   Scala 3 uses a different implementation of lazy vals which doesn't have this problem.
 *   Please refer to <a
 *   href="https://docs.scala-lang.org/scala3/reference/changed-features/lazy-vals-init.html">Lazy
 *   Vals Initialization</a> for more details.
 */
private[spark] class TransientBestEffortLazyVal[T <: AnyRef](
    private[this] val compute: () => T) extends Serializable {

  @transient @volatile private[this] var cached: T = null.asInstanceOf[T]

  def apply(): T = {
    if (cached == null) {
      val result = compute()
      assert(result != null, "Computed value cannot be null.")
      cached = result
    }
    cached
  }
}
