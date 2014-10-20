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

import org.apache.spark.ExtensionState.ExtensionState
import org.apache.spark.InterceptionPoints.InterceptionPoints


/** Notion of Spark application platform extension. */
trait PlatformExtension extends Serializable {

  /** Method to start extension */
  def start(conf: SparkConf):Unit
  /** Method to stop extension */
  def stop (conf: SparkConf):Unit

  /** Method to query actual state of extension */
  def state : ExtensionState = ExtensionState.STOPPED

  /* Point in Spark infrastructure which will be intercepted by this extension. */
  def intercept: InterceptionPoints = InterceptionPoints.EXECUTOR_LC

  /* User-friendly description of extension */
  def desc:String

  override def toString = s"$desc@$intercept"
}

/** Supported interception points.
  *
  * Currently only Executor life cycle is supported. */
object InterceptionPoints extends Enumeration {
  type InterceptionPoints = Value
  val EXECUTOR_LC /* Inject into executor lifecycle */
      = Value
}
/** Possible state of extension.
  */
object ExtensionState extends Enumeration {
  type ExtensionState = Value
  val STOPPED, STARTED, STARTING, STOPPING = Value
}
