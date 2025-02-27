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
 * Helper trait for defining thread locals with lexical scoping. With this helper, the thread local
 * is private and can only be set by the [[Handle]]. The [[Handle]] only exposes the thread local
 * value to functions passed into its runWith method. This pattern allows for
 * the lifetime of the thread local value to be strictly controlled.
 *
 * Rather than calling `tl.set(...)` and `tl.remove()` you would get a handle and execute your code
 * in `handle.runWith { ... }`.
 *
 * Example:
 * {{{
 *   object Credentials extends LexicalThreadLocal[Int] {
 *     def create(creds: Map[String, String]) = new Handle(Some(creds))
 *   }
 *   ...
 *   val handle = Credentials.create(Map("key" -> "value"))
 *   assert(Credentials.get() == None)
 *   handle.runWith {
 *     assert(Credentials.get() == Some(Map("key" -> "value")))
 *   }
 * }}}
 */
trait LexicalThreadLocal[T] {
  private val tl = new ThreadLocal[T]

  private def set(opt: Option[T]): Unit = {
    opt match {
      case Some(x) => tl.set(x)
      case None => tl.remove()
    }
  }

  protected def createHandle(opt: Option[T]): Handle = new Handle(opt)

  def get(): Option[T] = Option(tl.get)

  /** Final class representing a handle to a thread local value. */
  final class Handle private[LexicalThreadLocal] (private val opt: Option[T]) {
    def runWith[R](f: => R): R = {
      val old = get()
      set(opt)
      try f finally {
        set(old)
      }
    }
  }
}
