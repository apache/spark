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
package org.apache.spark.ui.storage

import javax.servlet.http.HttpServletRequest

import org.apache.spark.status.api._
import org.apache.spark.status.{JsonRequestHandler, StatusJsonRoute}

class AllRDDJsonRoute(parent: JsonRequestHandler) extends StatusJsonRoute[Seq[RDDStorageInfo]] {

  override def renderJson(route: HttpServletRequest): Seq[RDDStorageInfo] = {
    parent.withSparkUI(route) { case (ui, route) =>
      //should all access on storageListener also be synchronized?
      val storageStatusList = ui.storageListener.storageStatusList
      val rddInfos = ui.storageListener.rddInfoList
      rddInfos.map{rddInfo =>
        RDDJsonRoute.getRDDStorageInfo(rddInfo.id, rddInfo, storageStatusList, includeDetails = false)
      }
    }
  }
}
