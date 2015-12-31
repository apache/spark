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

package org.apache.spark

import org.apache.spark.annotation.DeveloperApi

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * :: DeveloperApi ::
 * An iterator that wraps around an existing iterator to provide task killing functionality.
 * It works by checking the interrupted flag in [[TaskContext]].
 */
@DeveloperApi
class InterruptibleIterator[+T](val context: TaskContext, val delegate: Iterator[T])
  extends Iterator[T] {

  def hasNext: Boolean = {
    // TODO(aarondav/rxin): Check Thread.interrupted instead of context.interrupted if interrupt
    // is allowed. The assumption is that Thread.interrupted does not have a memory fence in read
    // (just a volatile field in C), while context.interrupted is a volatile in the JVM, which
    // introduces an expensive read fence.
    if (context.isInterrupted) {
      throw new TaskKilledException
    } else {
      delegate.hasNext
    }
  }

  def next(): T = delegate.next()

  /** implement efficient linear-sequence drop until scala includes fix for jira SI-8835. */
  override def drop(n: Int): Iterator[T] = {
    implicit val ctag: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
    val arrayClass = Array.empty[T].iterator.getClass
    val arrayBufferClass = ArrayBuffer.empty[T].iterator.getClass
    delegate.getClass match {
      case `arrayClass` => new InterruptibleIterator(context, delegate.drop(n))
      case `arrayBufferClass` => new InterruptibleIterator(context, delegate.drop(n))
      case _ => {
        var j = 0
        while (j < n && this.hasNext) {
          this.next()
          j += 1
        }
        this
      }
    }
  }
}
