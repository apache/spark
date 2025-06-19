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

package org.apache.spark.sql.catalyst

/**
 * This class records the paths the serializer and deserializer walk through to reach current
 * path. Note that this class adds new path in prior to recorded paths so it maintains the paths
 * as reverse order.
 */
case class WalkedTypePath(private val walkedPaths: Seq[String] = Nil) {
  def recordRoot(className: String): WalkedTypePath =
    newInstance(s"""- root class: "$className"""")

  def recordOption(className: String): WalkedTypePath =
    newInstance(s"""- option value class: "$className"""")

  def recordArray(elementClassName: String): WalkedTypePath =
    newInstance(s"""- array element class: "$elementClassName"""")

  def recordMap(keyClassName: String, valueClassName: String): WalkedTypePath = {
    newInstance(
      s"""- map key class: "$keyClassName"""" +
        s""", value class: "$valueClassName"""")
  }

  def recordKeyForMap(keyClassName: String): WalkedTypePath =
    newInstance(s"""- map key class: "$keyClassName"""")

  def recordValueForMap(valueClassName: String): WalkedTypePath =
    newInstance(s"""- map value class: "$valueClassName"""")

  def recordField(className: String, fieldName: String): WalkedTypePath =
    newInstance(s"""- field (class: "$className", name: "$fieldName")""")

  override def toString: String = {
    walkedPaths.mkString("\n")
  }

  def getPaths: Seq[String] = walkedPaths

  private def newInstance(newRecord: String): WalkedTypePath =
    WalkedTypePath(newRecord +: walkedPaths)
}
