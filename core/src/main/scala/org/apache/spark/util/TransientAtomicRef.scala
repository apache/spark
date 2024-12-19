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

private[spark] class TransientAtomicRef[T <: AnyRef](compute: => T) extends Serializable {

  @transient
  private var atomicRef: AtomicReference[T] = new AtomicReference(null.asInstanceOf[T])

  def apply(): T = {
    val ref = atomicRef.get()
    if (ref == null) {
      val newRef: T = compute
      assert(newRef != null, "computed value cannot be null.")
      atomicRef.compareAndSet(null.asInstanceOf[T], newRef)
      atomicRef.get()
    } else {
      ref
    }
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    atomicRef = new AtomicReference(null.asInstanceOf[T])
  }
}
