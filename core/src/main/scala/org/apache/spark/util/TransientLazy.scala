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
 * Construct to lazily initialize a variable.
 * This may be helpful for avoiding deadlocks in certain scenarios. For example,
 *   a) Thread 1 entered a synchronized method, grabbing a coarse lock on the parent object.
 *   b) Thread 2 gets spawned off, and tries to initialize a lazy value on the same parent object
 *      (in our case, this was the logger). This causes scala to also try to grab a coarse lock on
 *      the parent object.
 *   c) If thread 1 waits for thread 2 to join, a deadlock occurs.
 * The main difference between this and [[LazyTry]] is that this does not cache failures.
 *
 * @note
 *   Scala 3 uses a different implementation of lazy vals which doesn't have this problem.
 *   Please refer to <a
 *   href="https://docs.scala-lang.org/scala3/reference/changed-features/lazy-vals-init.html">Lazy
 *   Vals Initialization</a> for more details.
 */
private[spark] class TransientLazy[T](initializer: => T) extends Serializable {

  @transient
  private[this] lazy val value: T = initializer

  def apply(): T = {
    value
  }
}
