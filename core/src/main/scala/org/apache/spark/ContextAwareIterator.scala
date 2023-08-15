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

/**
 * :: DeveloperApi ::
 * A TaskContext aware iterator.
 *
 * As the Python evaluation consumes the parent iterator in a separate thread,
 * it could consume more data from the parent even after the task ends and the parent is closed.
 * If an off-heap access exists in the parent iterator, it could cause segmentation fault
 * which crashes the executor.
 * Thus, we should use [[ContextAwareIterator]] to stop consuming after the task ends.
 *
 * @since 3.1.0
 * @deprecated since 4.0.0 as its only usage for Python evaluation is now extinct
 */
@DeveloperApi
@deprecated("Only usage for Python evaluation is now extinct", "4.0.0")
class ContextAwareIterator[+T](val context: TaskContext, val delegate: Iterator[T])
  extends Iterator[T] {

  override def hasNext: Boolean =
    !context.isCompleted() && !context.isInterrupted() && delegate.hasNext

  override def next(): T = delegate.next()
}
