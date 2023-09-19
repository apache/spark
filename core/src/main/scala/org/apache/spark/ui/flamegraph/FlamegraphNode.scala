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
package org.apache.spark.ui.flamegraph

import scala.collection.mutable.HashMap

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.status.api.v1.ThreadStackTrace

case class FlamegraphNode(name: String) {
  private val children = new HashMap[String, FlamegraphNode]()
  private var value: Int = 0
  def toJsonString: String = {
    // scalastyle:off line.size.limit
    s"""{"name":"$name","value":$value,"children":[${children.map(_._2.toJsonString).mkString(",")}]}"""
    // scalastyle:on line.size.limit
  }
}

object FlamegraphNode {
  def apply(stacks: Array[ThreadStackTrace]): FlamegraphNode = {
    val root = FlamegraphNode("root")
    stacks.foreach { stack =>
      root.value += 1
      var cur = root
      stack.stackTrace.elems.reverse.foreach { e =>
        val head = e.split("\n").head
        val name = StringEscapeUtils.escapeJson(head)
        cur = cur.children.getOrElseUpdate(name, FlamegraphNode(name))
        cur.value += 1
      }
    }
    root
  }
}
