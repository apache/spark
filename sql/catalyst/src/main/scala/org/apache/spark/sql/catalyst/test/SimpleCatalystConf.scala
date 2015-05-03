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

import scala.collection.immutable
import scala.collection.JavaConversions._

import org.apache.spark.sql.catalyst.CatalystConf

/** A CatalystConf that can be used for local testing. */
class SimpleCatalystConf(caseSensitive: Boolean) extends CatalystConf {
  val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  override def caseSensitiveAnalysis: Boolean = caseSensitive

  override def setConf(key: String, value: String) : Unit = {
    settings.put(key, value)
  }

  override def getConf(key: String) : String ={
    settings.get(key)
  }

  override def getConf(key: String, defaultValue: String) : String = {
    Option(settings.get(key)).getOrElse(defaultValue)
  }

  override def getAllConfs: immutable.Map[String, String] = settings.synchronized {
    settings.toMap
  }
}
