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

import scala.collection.mutable

case class WalkedTypePath() extends Serializable {
  val walkedPaths: mutable.ArrayBuffer[String] = new mutable.ArrayBuffer[String]()

  def recordRoot(className: String): Unit =
    record(s"""- root class: "$className"""")

  def recordOption(className: String): Unit =
    record(s"""- option value class: "$className"""")

  def recordArray(elementClassName: String): Unit =
    record(s"""- array element class: "$elementClassName"""")

  def recordMap(keyClassName: String, valueClassName: String): Unit = {
    record(s"""- map key class: "$keyClassName"""" +
        s""", value class: "$valueClassName"""")
  }

  def recordKeyForMap(keyClassName: String): Unit =
    record(s"""- map key class: "$keyClassName"""")

  def recordValueForMap(valueClassName: String): Unit =
    record(s"""- map value class: "$valueClassName"""")

  def recordField(className: String, fieldName: String): Unit =
    record(s"""- field (class: "$className", name: "$fieldName")""")

  def copy(): WalkedTypePath = {
    val copied = WalkedTypePath()
    copied.walkedPaths ++= walkedPaths
    copied
  }

  override def toString: String = {
    // to speed up appending element we are adding element at last and apply reverse
    // just before printing it out
    walkedPaths.reverse.mkString("\n")
  }

  private def record(newRecord: String): Unit = {
    walkedPaths += newRecord
  }
}
