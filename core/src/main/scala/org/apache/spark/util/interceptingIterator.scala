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
 * A Wrapper for iterators where a caller can get hooks before/after
 * iterator methods.
 * @param sub The iterator being wrapped.
 * @tparam A the iterable type
 */
private[spark]
class InterceptingIterator[A](sub: Iterator[A]) extends Iterator[A] {

  override def next() :A = {
    beforeNext()
    val next = sub.next()
    afterNext(next)
  }

  override def hasNext: Boolean = {
    beforeHasNext()
    val r = sub.hasNext
    afterHasNext(r)
  }

  def beforeNext() = {}
  def afterNext(next : A) : A = next

  def beforeHasNext() = {}
  def afterHasNext(hasNext : Boolean) : Boolean = hasNext
}

/**
 * Helpers if only needing to intercept one of the methods.
 */

private[spark] object BeforeNextInterceptingIterator {
  def apply[A](sub: Iterator[A], function: => Unit) : InterceptingIterator[A] = {
    new InterceptingIterator[A](sub) {
      override def beforeNext() = function
    }
  }
}

private[spark] object AfterNextInterceptingIterator {
  def apply[A](sub: Iterator[A], function: (A) => A) : InterceptingIterator[A] = {
    new InterceptingIterator[A](sub) {
      override def afterNext(next : A) = function(next)
    }
  }
}

private[spark] object BeforeHasNextInterceptingIterator {
  def apply[A](sub: Iterator[A], function: => Unit) : InterceptingIterator[A] = {
    new InterceptingIterator[A](sub) {
      override def beforeHasNext() = function
    }
  }
}

private[spark] object AfterHasNextInterceptingIterator {
  def apply[A](sub: Iterator[A], function: (Boolean) => Boolean) : InterceptingIterator[A] = {
    new InterceptingIterator[A](sub) {
      override def afterHasNext(hasNext : Boolean) = function(hasNext)
    }
  }
}
