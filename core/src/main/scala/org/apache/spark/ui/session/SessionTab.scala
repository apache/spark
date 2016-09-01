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

package org.apache.spark.ui.session

import scala.collection.Map

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.{SparkListener, SparkListenerSessionUpdate, _}
import org.apache.spark.ui.{SparkUI, SparkUITab, _}

private[ui] class SessionTab(parent: SparkUI) extends SparkUITab(parent, "session") {
  val listener = parent.sessionListener
  attachPage(new SessionPage(this))
}

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the SessionTab
 */
@DeveloperApi
class SessionListener extends SparkListener {
  var sessionDetails = Map[String, String]()

  override def onSessionUpdate(sessionUpdate: SparkListenerSessionUpdate) {
    synchronized {
      sessionDetails = sessionUpdate.sessionDetails
    }
  }
}

