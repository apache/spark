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

package org.apache.spark.ui

import scala.xml.Node

object TimelineViewUtils {

  def executorsLegend: Seq[Node] = {
    <div class="legend-area"><svg width="200px" height="55px">
      <rect x="5px" y="5px" width="20px" height="15px"
            rx="2px" ry="2px" stroke="#97B0F8" fill="#D5DDF6"></rect>
      <text x="35px" y="17px">Executor Added</text>
      <rect x="5px" y="35px" width="20px" height="15px"
            rx="2px" ry="2px" stroke="#97B0F8" fill="#EBCA59"></rect>
      <text x="35px" y="47px">Executor Removed</text>
    </svg></div>
  }

  def jobsLegend: Seq[Node] = {
    <div class="legend-area"><svg width="200px" height="85px">
      <rect x="5px" y="5px" width="20px" height="15px"
            rx="2px" ry="2px" stroke="#97B0F8" fill="#D5DDF6"></rect>
      <text x="35px" y="17px">Succeeded Job</text>
      <rect x="5px" y="35px" width="20px" height="15px"
            rx="2px" ry="2px" stroke="#97B0F8" fill="#FF5475"></rect>
      <text x="35px" y="47px">Failed Job</text>
      <rect x="5px" y="65px" width="20px" height="15px"
            rx="2px" ry="2px" stroke="#97B0F8" fill="#FDFFCA"></rect>
      <text x="35px" y="77px">Running Job</text>
    </svg></div>
  }

  def stagesLegend: Seq[Node] = {
    <div class="legend-area"><svg width="200px" height="85px">
      <rect x="5px" y="5px" width="20px" height="15px"
            rx="2px" ry="2px" stroke="#97B0F8" fill="#D5DDF6"></rect>
      <text x="35px" y="17px">Completed Stage </text>
      <rect x="5px" y="35px" width="20px" height="15px"
            rx="2px" ry="2px" stroke="#97B0F8" fill="#FF5475"></rect>
      <text x="35px" y="47px">Failed Stage</text>
      <rect x="5px" y="65px" width="20px" height="15px"
            rx="2px" ry="2px" stroke="#97B0F8" fill="#FDFFCA"></rect>
      <text x="35px" y="77px">Active Stage</text>
    </svg></div>
  }

  def nodesToFlatString(nodes: Seq[Node]): String = {
    nodes.toString.filter(_ != '\n')
  }
}
