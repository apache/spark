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

package org.apache.spark.sql.hive.client

import java.lang.reflect.{Method, Modifier}
import java.net.URI
import java.util.{ArrayList => JArrayList, List => JList, Set => JSet}

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table}
import org.apache.hadoop.hive.ql.processors.{CommandProcessor, CommandProcessorFactory}
import org.apache.hadoop.hive.ql.session.SessionState

private[client] sealed trait Shim {

  def setCurrentSessionState(state: SessionState): Unit

  def getDataLocation(table: Table): String

  def setDataLocation(table: Table, loc: String): Unit

  def getAllPartitions(hive: Hive, table: Table): Seq[Partition]

  def getCommandProcessor(token: String, conf: HiveConf): CommandProcessor

  def getDriverResults(driver: Driver): Seq[String]

  protected def findStaticMethod(klass: Class[_], name: String, args: Class[_]*): Method = {
    val method = findMethod(klass, name, args:_*)
    require(Modifier.isStatic(method.getModifiers()),
      s"Method $name of class $klass is not static.")
    method
  }

  protected def findMethod(klass: Class[_], name: String, args: Class[_]*): Method = {
    klass.getMethod(name, args:_*)
  }

}

private[client] class Shim_v0_12 extends Shim {

  private lazy val startMethod = findStaticMethod(classOf[SessionState], "start",
    classOf[SessionState])
  private lazy val getDataLocationMethod = findMethod(classOf[Table], "getDataLocation")
  private lazy val setDataLocationMethod = findMethod(classOf[Table], "setDataLocation",
    classOf[URI])
  private lazy val getAllPartitionsMethod = findMethod(classOf[Hive], "getAllPartitionsForPruner",
    classOf[Table])
  private lazy val getCommandProcessorMethod = findStaticMethod(classOf[CommandProcessorFactory],
    "get", classOf[String], classOf[HiveConf])
  private lazy val getDriverResultsMethod = findMethod(classOf[Driver], "getResults",
    classOf[JArrayList[String]])

  override def setCurrentSessionState(state: SessionState): Unit = startMethod.invoke(null, state)

  override def getDataLocation(table: Table): String =
    getDataLocationMethod.invoke(table).toString()

  override def setDataLocation(table: Table, loc: String): Unit =
    setDataLocationMethod.invoke(table, new URI(loc))

  override def getAllPartitions(hive: Hive, table: Table): Seq[Partition] =
    getAllPartitionsMethod.invoke(hive, table).asInstanceOf[JSet[Partition]].toSeq

  override def getCommandProcessor(token: String, conf: HiveConf): CommandProcessor =
    getCommandProcessorMethod.invoke(null, token, conf).asInstanceOf[CommandProcessor]

  override def getDriverResults(driver: Driver): Seq[String] = {
    val res = new JArrayList[String]()
    getDriverResultsMethod.invoke(driver, res)
    res.toSeq
  }

}

private[client] class Shim_v0_13 extends Shim_v0_12 {

  private lazy val setCurrentSessionStateMethod =
    findStaticMethod(classOf[SessionState], "setCurrentSessionState", classOf[SessionState])
  private lazy val setDataLocationMethod = findMethod(classOf[Table], "setDataLocation",
    classOf[Path])
  private lazy val getAllPartitionsMethod = findMethod(classOf[Hive], "getAllPartitionsOf",
    classOf[Table])
  private lazy val getCommandProcessorMethod = findStaticMethod(classOf[CommandProcessorFactory],
    "get", classOf[Array[String]], classOf[HiveConf])
  private lazy val getDriverResultsMethod = findMethod(classOf[Driver], "getResults",
    classOf[JList[Object]])

  override def setCurrentSessionState(state: SessionState): Unit =
    setCurrentSessionStateMethod.invoke(null, state)

  override def setDataLocation(table: Table, loc: String): Unit =
    setDataLocationMethod.invoke(table, new Path(loc))

  override def getAllPartitions(hive: Hive, table: Table): Seq[Partition] =
    getAllPartitionsMethod.invoke(hive, table).asInstanceOf[JSet[Partition]].toSeq

  override def getCommandProcessor(token: String, conf: HiveConf): CommandProcessor =
    getCommandProcessorMethod.invoke(null, Array(token), conf).asInstanceOf[CommandProcessor]

  override def getDriverResults(driver: Driver): Seq[String] = {
    val res = new JArrayList[Object]()
    getDriverResultsMethod.invoke(driver, res)
    res.map { r =>
      r match {
        case s: String => s
        case a: Array[Object] => a(0).asInstanceOf[String]
      }
    }
  }

}
