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

package org.apache.spark.sql.catalyst.encoders

import java.util.concurrent.ConcurrentMap

import com.google.common.collect.MapMaker

object OuterScopes {
  @transient
  lazy val outerScopes: ConcurrentMap[String, AnyRef] =
    new MapMaker().weakValues().makeMap()

  /**
   * Adds a new outer scope to this context that can be used when instantiating an `inner class`
   * during deserialialization. Inner classes are created when a case class is defined in the
   * Spark REPL and registering the outer scope that this class was defined in allows us to create
   * new instances on the spark executors.  In normal use, users should not need to call this
   * function.
   *
   * Warning: this function operates on the assumption that there is only ever one instance of any
   * given wrapper class.
   */
  def addOuterScope(outer: AnyRef): Unit = {
    outerScopes.putIfAbsent(outer.getClass.getName, outer)
  }
}
