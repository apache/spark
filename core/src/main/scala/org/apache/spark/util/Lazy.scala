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

import java.io.ObjectOutputStream

/**
 * Construct to lazily initialize a variable.
 * This may be helpful for avoiding deadlocks in certain scenarios. For example,
 *   a) Thread 1 entered a synchronized method, grabbing a coarse lock on the parent object.
 *   b) Thread 2 gets spawned off, and tries to initialize a lazy value on the same parent object
 *      (in our case, this was the logger). This causes scala to also try to grab a coarse lock on
 *      the parent object.
 *   c) If thread 1 waits for thread 2 to join, a deadlock occurs.
 */
@SerialVersionUID(7964587975756091988L)
private[spark] class Lazy[T](initializer: => T) extends Serializable {

  private[this] lazy val value: T = initializer

  def apply(): T = {
    value
  }

  private def writeObject(stream: ObjectOutputStream): Unit = {
    this.value // ensure value is initialized
    stream.defaultWriteObject()
  }
}
