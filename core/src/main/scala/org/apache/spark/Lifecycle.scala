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

trait Lifecycle extends Service {

  import Service.State._

  protected var state_ = Uninitialized

  def conf: SparkConf

  def uninitialized = state_ == Uninitialized

  def initialized = state_ == Initialized

  def started = state_ == Started

  def stopped = state_ == Stopped

  def state: Service.State = state_

  def initialize(): Unit = synchronized {
    if (!uninitialized) {
      throw new SparkException(s"Can't move to initialized state when $state_")
    }
    doInitialize
    state_ = Initialized
  }

  override def start(): Unit = synchronized {
    if (uninitialized) initialize()
    if (started) {
      throw new SparkException(s"Can't move to started state when $state_")
    }
    doStart()
    state_ = Started
  }

  override def stop(): Unit = synchronized {
    if (!started) {
      throw new SparkException(s"Can't move to stopped state when $state_")
    }
    doStop
    state_ = Stopped
  }

  override def close(): Unit = synchronized {
    stop()
  }

  protected def doInitialize(): Unit = {}

  protected def doStart(): Unit

  protected def doStop(): Unit
}
