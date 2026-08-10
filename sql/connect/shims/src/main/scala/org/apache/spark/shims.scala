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

import scala.reflect.ClassTag

class SparkContext
class SparkConf {
  def getAll: Array[(String, String)] = Array.empty
}

package api.java {
  class JavaRDD[T]
}

package rdd {
  class RDD[T]
}

package broadcast {
  // (SPARK-51705) Compile-time shim mirroring org.apache.spark.broadcast.Broadcast's surface so
  // that the JVM-less Connect client (connect-common, which has no spark-core) can define a
  // ConnectBroadcast[T] subclass and expose Broadcast[T] on the client API. Modules with real
  // spark-core (connect/server, catalyst, sql/core) exclude spark-connect-shims, so the identical
  // FQN binds to the real Broadcast there -- same swap already used for RDD/SparkContext above.
  // Must match the real abstract surface exactly (getValue/doUnpersist/doDestroy + id + value).
  abstract class Broadcast[T: ClassTag](val id: Long) extends Serializable {
    def value: T = getValue()
    def unpersist(): Unit = doUnpersist(blocking = false)
    def unpersist(blocking: Boolean): Unit = doUnpersist(blocking)
    def destroy(): Unit = doDestroy(blocking = false)
    protected def getValue(): T
    protected def doUnpersist(blocking: Boolean): Unit
    protected def doDestroy(blocking: Boolean): Unit
  }
}

package sql {
  class ExperimentalMethods
  class SparkSessionExtensions

  package execution {
    class QueryExecution
  }
  package internal {
    class SharedState
    class SessionState
  }
  package util {
    class ExecutionListenerManager
  }
  package sources {
    class BaseRelation
  }
}
