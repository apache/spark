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

package org.apache.spark.sql.catalyst.test

import org.apache.spark.sql.catalyst.CatalystConf

import scala.collection.immutable
import scala.collection.mutable

/** A CatalystConf that can be used for local testing. */
class SimpleConf extends CatalystConf{
  val map = mutable.Map[String, String]()

  def setConf(key: String, value: String) : Unit = {
    map.put(key, value)
  }
  def getConf(key: String) : String ={
    map.get(key).get
  }
  def getConf(key: String, defaultValue: String) : String = {
    map.getOrElse(key, defaultValue)
  }
  def getAllConfs: immutable.Map[String, String] = {
    map.toMap
  }
}
