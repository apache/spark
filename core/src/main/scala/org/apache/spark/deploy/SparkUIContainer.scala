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

package org.apache.spark.deploy

import org.apache.spark.ui.{SparkUI, WebUI}

private[spark] abstract class SparkUIContainer(name: String) extends WebUI(name) {

  /** Attach a SparkUI to this container. Only valid after bind(). */
  def attachUI(ui: SparkUI) {
    assert(serverInfo.isDefined,
      "%s must be bound to a server before attaching SparkUIs".format(name))
    val rootHandler = serverInfo.get.rootHandler
    for (handler <- ui.handlers) {
      rootHandler.addHandler(handler)
      if (!handler.isStarted) {
        handler.start()
      }
    }
  }

  /** Detach a SparkUI from this container. Only valid after bind(). */
  def detachUI(ui: SparkUI) {
    assert(serverInfo.isDefined,
      "%s must be bound to a server before detaching SparkUIs".format(name))
    val rootHandler = serverInfo.get.rootHandler
    for (handler <- ui.handlers) {
      if (handler.isStarted) {
        handler.stop()
      }
      rootHandler.removeHandler(handler)
    }
  }

}
