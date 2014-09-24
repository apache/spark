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

import scala.collection._


object MergedPropertyMap {

  /**
   * Flatten a map of maps out into a single map, later maps in the propList
   * have priority over older ones
   * @param propList Vector of property maps[PropName->PropValue] to merge
   */
  def mergePropertyMaps( propList: Vector[Map[String, String]]): mutable.Map[String, String] = {
    val propMap = new mutable.HashMap[String, String]()
    // loop through each entry of each map in order of priority
    // and add it to our propMap
    propList.foreach {
      _.foreach{ case(k,v) => propMap.getOrElseUpdate(k,v)}
    }
    propMap
  }
}
