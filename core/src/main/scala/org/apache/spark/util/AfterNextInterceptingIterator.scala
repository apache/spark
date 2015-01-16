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
 * A Wrapper for iterators where a caller can get a hook after next()
 * has been called.
 *
 * @param sub The iterator being wrapped.
 * @tparam A the iterable type
 */
private[spark]
class AfterNextInterceptingIterator[A](sub: Iterator[A]) extends Iterator[A] {

  def hasNext: Boolean = sub.hasNext
  override def next() :A = {
    val next = sub.next()
    afterNext(next)
  }

  def afterNext(next : A) : A = next
}

/**
 * A shortcut if running a closure is all you need to do.
 */
private[spark] object AfterNextInterceptingIterator {
  def apply[A](sub: Iterator[A], function: (A) => A) : AfterNextInterceptingIterator[A] = {
    new AfterNextInterceptingIterator[A](sub) {
      override def afterNext(next : A) = function(next)
    }
  }
}

