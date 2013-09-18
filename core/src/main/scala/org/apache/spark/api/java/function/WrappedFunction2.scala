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

package org.apache.spark.api.java.function

import scala.runtime.AbstractFunction2

/**
 * Subclass of Function2 for ease of calling from Java. The main thing it does is re-expose the
 * apply() method as call() and declare that it can throw Exception (since AbstractFunction2.apply
 * isn't marked to allow that).
 */
private[spark] abstract class WrappedFunction2[T1, T2, R] extends AbstractFunction2[T1, T2, R] {
  @throws(classOf[Exception])
  def call(t1: T1, t2: T2): R

  final def apply(t1: T1, t2: T2): R = call(t1, t2)
}
