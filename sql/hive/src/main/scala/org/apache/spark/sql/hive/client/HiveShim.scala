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

import java.lang.{Boolean => JBoolean, Integer => JInteger, Long => JLong}
import java.lang.reflect.{Method, Modifier}
import java.net.URI
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap, Set => JSet}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table}
import org.apache.hadoop.hive.ql.processors.{CommandProcessor, CommandProcessorFactory}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde.serdeConstants

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StringType, IntegralType}

/**
 * A shim that defines the interface between ClientWrapper and the underlying Hive library used to
 * talk to the metastore. Each Hive version has its own implementation of this class, defining
 * version-specific version of needed functions.
 *
 * The guideline for writing shims is:
 * - always extend from the previous version unless really not possible
 * - initialize methods in lazy vals, both for quicker access for multiple invocations, and to
 *   avoid runtime errors due to the above guideline.
 */
private[client] sealed abstract class Shim {

  /**
   * Set the current SessionState to the given SessionState. Also, set the context classloader of
   * the current thread to the one set in the HiveConf of this given `state`.
   * @param state
   */
  def setCurrentSessionState(state: SessionState): Unit

  /**
   * This shim is necessary because the return type is different on different versions of Hive.
   * All parameters are the same, though.
   */
  def getDataLocation(table: Table): Option[String]

  def setDataLocation(table: Table, loc: String): Unit

  def getAllPartitions(hive: Hive, table: Table): Seq[Partition]

  def getPartitionsByFilter(hive: Hive, table: Table, predicates: Seq[Expression]): Seq[Partition]

  def getCommandProcessor(token: String, conf: HiveConf): CommandProcessor

  def getDriverResults(driver: Driver): Seq[String]

  def getMetastoreClientConnectRetryDelayMillis(conf: HiveConf): Long

  def loadPartition(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean): Unit

  def loadTable(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      replace: Boolean,
      holdDDLTime: Boolean): Unit

  def loadDynamicPartitions(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean,
      listBucketingEnabled: Boolean): Unit

  def dropIndex(hive: Hive, dbName: String, tableName: String, indexName: String): Unit

  protected def findStaticMethod(klass: Class[_], name: String, args: Class[_]*): Method = {
    val method = findMethod(klass, name, args: _*)
    require(Modifier.isStatic(method.getModifiers()),
      s"Method $name of class $klass is not static.")
    method
  }

  protected def findMethod(klass: Class[_], name: String, args: Class[_]*): Method = {
    klass.getMethod(name, args: _*)
  }

}

private[client] class Shim_v0_12 extends Shim with Logging {

  private lazy val startMethod =
    findStaticMethod(
      classOf[SessionState],
      "start",
      classOf[SessionState])
  private lazy val getDataLocationMethod = findMethod(classOf[Table], "getDataLocation")
  private lazy val setDataLocationMethod =
    findMethod(
      classOf[Table],
      "setDataLocation",
      classOf[URI])
  private lazy val getAllPartitionsMethod =
    findMethod(
      classOf[Hive],
      "getAllPartitionsForPruner",
      classOf[Table])
  private lazy val getCommandProcessorMethod =
    findStaticMethod(
      classOf[CommandProcessorFactory],
      "get",
      classOf[String],
      classOf[HiveConf])
  private lazy val getDriverResultsMethod =
    findMethod(
      classOf[Driver],
      "getResults",
      classOf[JArrayList[String]])
  private lazy val loadPartitionMethod =
    findMethod(
      classOf[Hive],
      "loadPartition",
      classOf[Path],
      classOf[String],
      classOf[JMap[String, String]],
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE)
  private lazy val loadTableMethod =
    findMethod(
      classOf[Hive],
      "loadTable",
      classOf[Path],
      classOf[String],
      JBoolean.TYPE,
      JBoolean.TYPE)
  private lazy val loadDynamicPartitionsMethod =
    findMethod(
      classOf[Hive],
      "loadDynamicPartitions",
      classOf[Path],
      classOf[String],
      classOf[JMap[String, String]],
      JBoolean.TYPE,
      JInteger.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE)
  private lazy val dropIndexMethod =
    findMethod(
      classOf[Hive],
      "dropIndex",
      classOf[String],
      classOf[String],
      classOf[String],
      JBoolean.TYPE)

  override def setCurrentSessionState(state: SessionState): Unit = {
    // Starting from Hive 0.13, setCurrentSessionState will internally override
    // the context class loader of the current thread by the class loader set in
    // the conf of the SessionState. So, for this Hive 0.12 shim, we add the same
    // behavior and make shim.setCurrentSessionState of all Hive versions have the
    // consistent behavior.
    Thread.currentThread().setContextClassLoader(state.getConf.getClassLoader)
    startMethod.invoke(null, state)
  }

  override def getDataLocation(table: Table): Option[String] =
    Option(getDataLocationMethod.invoke(table)).map(_.toString())

  override def setDataLocation(table: Table, loc: String): Unit =
    setDataLocationMethod.invoke(table, new URI(loc))

  override def getAllPartitions(hive: Hive, table: Table): Seq[Partition] =
    getAllPartitionsMethod.invoke(hive, table).asInstanceOf[JSet[Partition]].toSeq

  override def getPartitionsByFilter(
      hive: Hive,
      table: Table,
      predicates: Seq[Expression]): Seq[Partition] = {
    // getPartitionsByFilter() doesn't support binary comparison ops in Hive 0.12.
    // See HIVE-4888.
    logDebug("Hive 0.12 doesn't support predicate pushdown to metastore. " +
      "Please use Hive 0.13 or higher.")
    getAllPartitions(hive, table)
  }

  override def getCommandProcessor(token: String, conf: HiveConf): CommandProcessor =
    getCommandProcessorMethod.invoke(null, token, conf).asInstanceOf[CommandProcessor]

  override def getDriverResults(driver: Driver): Seq[String] = {
    val res = new JArrayList[String]()
    getDriverResultsMethod.invoke(driver, res)
    res.toSeq
  }

  override def getMetastoreClientConnectRetryDelayMillis(conf: HiveConf): Long = {
    conf.getIntVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY) * 1000
  }

  override def loadPartition(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean): Unit = {
    loadPartitionMethod.invoke(hive, loadPath, tableName, partSpec, replace: JBoolean,
      holdDDLTime: JBoolean, inheritTableSpecs: JBoolean, isSkewedStoreAsSubdir: JBoolean)
  }

  override def loadTable(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      replace: Boolean,
      holdDDLTime: Boolean): Unit = {
    loadTableMethod.invoke(hive, loadPath, tableName, replace: JBoolean, holdDDLTime: JBoolean)
  }

  override def loadDynamicPartitions(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean,
      listBucketingEnabled: Boolean): Unit = {
    loadDynamicPartitionsMethod.invoke(hive, loadPath, tableName, partSpec, replace: JBoolean,
      numDP: JInteger, holdDDLTime: JBoolean, listBucketingEnabled: JBoolean)
  }

  override def dropIndex(hive: Hive, dbName: String, tableName: String, indexName: String): Unit = {
    dropIndexMethod.invoke(hive, dbName, tableName, indexName, true: JBoolean)
  }

}

private[client] class Shim_v0_13 extends Shim_v0_12 {

  private lazy val setCurrentSessionStateMethod =
    findStaticMethod(
      classOf[SessionState],
      "setCurrentSessionState",
      classOf[SessionState])
  private lazy val setDataLocationMethod =
    findMethod(
      classOf[Table],
      "setDataLocation",
      classOf[Path])
  private lazy val getAllPartitionsMethod =
    findMethod(
      classOf[Hive],
      "getAllPartitionsOf",
      classOf[Table])
  private lazy val getPartitionsByFilterMethod =
    findMethod(
      classOf[Hive],
      "getPartitionsByFilter",
      classOf[Table],
      classOf[String])
  private lazy val getCommandProcessorMethod =
    findStaticMethod(
      classOf[CommandProcessorFactory],
      "get",
      classOf[Array[String]],
      classOf[HiveConf])
  private lazy val getDriverResultsMethod =
    findMethod(
      classOf[Driver],
      "getResults",
      classOf[JList[Object]])

  override def setCurrentSessionState(state: SessionState): Unit =
    setCurrentSessionStateMethod.invoke(null, state)

  override def setDataLocation(table: Table, loc: String): Unit =
    setDataLocationMethod.invoke(table, new Path(loc))

  override def getAllPartitions(hive: Hive, table: Table): Seq[Partition] =
    getAllPartitionsMethod.invoke(hive, table).asInstanceOf[JSet[Partition]].toSeq

  /**
   * Converts catalyst expression to the format that Hive's getPartitionsByFilter() expects, i.e.
   * a string that represents partition predicates like "str_key=\"value\" and int_key=1 ...".
   *
   * Unsupported predicates are skipped.
   */
  def convertFilters(table: Table, filters: Seq[Expression]): String = {
    // hive varchar is treated as catalyst string, but hive varchar can't be pushed down.
    val varcharKeys = table.getPartitionKeys
      .filter(col => col.getType.startsWith(serdeConstants.VARCHAR_TYPE_NAME))
      .map(col => col.getName).toSet

    filters.collect {
      case op @ BinaryComparison(a: Attribute, Literal(v, _: IntegralType)) =>
        s"${a.name} ${op.symbol} $v"
      case op @ BinaryComparison(Literal(v, _: IntegralType), a: Attribute) =>
        s"$v ${op.symbol} ${a.name}"
      case op @ BinaryComparison(a: Attribute, Literal(v, _: StringType))
          if !varcharKeys.contains(a.name) =>
        s"""${a.name} ${op.symbol} "$v""""
      case op @ BinaryComparison(Literal(v, _: StringType), a: Attribute)
          if !varcharKeys.contains(a.name) =>
        s""""$v" ${op.symbol} ${a.name}"""
    }.mkString(" and ")
  }

  override def getPartitionsByFilter(
      hive: Hive,
      table: Table,
      predicates: Seq[Expression]): Seq[Partition] = {

    // Hive getPartitionsByFilter() takes a string that represents partition
    // predicates like "str_key=\"value\" and int_key=1 ..."
    val filter = convertFilters(table, predicates)
    val partitions =
      if (filter.isEmpty) {
        getAllPartitionsMethod.invoke(hive, table).asInstanceOf[JSet[Partition]]
      } else {
        logDebug(s"Hive metastore filter is '$filter'.")
        getPartitionsByFilterMethod.invoke(hive, table, filter).asInstanceOf[JArrayList[Partition]]
      }

    partitions.toSeq
  }

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

private[client] class Shim_v0_14 extends Shim_v0_13 {

  private lazy val loadPartitionMethod =
    findMethod(
      classOf[Hive],
      "loadPartition",
      classOf[Path],
      classOf[String],
      classOf[JMap[String, String]],
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE)
  private lazy val loadTableMethod =
    findMethod(
      classOf[Hive],
      "loadTable",
      classOf[Path],
      classOf[String],
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE)
  private lazy val loadDynamicPartitionsMethod =
    findMethod(
      classOf[Hive],
      "loadDynamicPartitions",
      classOf[Path],
      classOf[String],
      classOf[JMap[String, String]],
      JBoolean.TYPE,
      JInteger.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE)
  private lazy val getTimeVarMethod =
    findMethod(
      classOf[HiveConf],
      "getTimeVar",
      classOf[HiveConf.ConfVars],
      classOf[TimeUnit])

  override def loadPartition(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean): Unit = {
    loadPartitionMethod.invoke(hive, loadPath, tableName, partSpec, replace: JBoolean,
      holdDDLTime: JBoolean, inheritTableSpecs: JBoolean, isSkewedStoreAsSubdir: JBoolean,
      isSrcLocal(loadPath, hive.getConf()): JBoolean, JBoolean.FALSE)
  }

  override def loadTable(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      replace: Boolean,
      holdDDLTime: Boolean): Unit = {
    loadTableMethod.invoke(hive, loadPath, tableName, replace: JBoolean, holdDDLTime: JBoolean,
      isSrcLocal(loadPath, hive.getConf()): JBoolean, JBoolean.FALSE, JBoolean.FALSE)
  }

  override def loadDynamicPartitions(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean,
      listBucketingEnabled: Boolean): Unit = {
    loadDynamicPartitionsMethod.invoke(hive, loadPath, tableName, partSpec, replace: JBoolean,
      numDP: JInteger, holdDDLTime: JBoolean, listBucketingEnabled: JBoolean, JBoolean.FALSE)
  }

  override def getMetastoreClientConnectRetryDelayMillis(conf: HiveConf): Long = {
    getTimeVarMethod.invoke(
      conf,
      HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY,
      TimeUnit.MILLISECONDS).asInstanceOf[Long]
  }

  protected def isSrcLocal(path: Path, conf: HiveConf): Boolean = {
    val localFs = FileSystem.getLocal(conf)
    val pathFs = FileSystem.get(path.toUri(), conf)
    localFs.getUri() == pathFs.getUri()
  }

}

private[client] class Shim_v1_0 extends Shim_v0_14 {

}

private[client] class Shim_v1_1 extends Shim_v1_0 {

  private lazy val dropIndexMethod =
    findMethod(
      classOf[Hive],
      "dropIndex",
      classOf[String],
      classOf[String],
      classOf[String],
      JBoolean.TYPE,
      JBoolean.TYPE)

  override def dropIndex(hive: Hive, dbName: String, tableName: String, indexName: String): Unit = {
    dropIndexMethod.invoke(hive, dbName, tableName, indexName, true: JBoolean, true: JBoolean)
  }

}

private[client] class Shim_v1_2 extends Shim_v1_1 {

  private lazy val loadDynamicPartitionsMethod =
    findMethod(
      classOf[Hive],
      "loadDynamicPartitions",
      classOf[Path],
      classOf[String],
      classOf[JMap[String, String]],
      JBoolean.TYPE,
      JInteger.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JLong.TYPE)

  override def loadDynamicPartitions(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean,
      listBucketingEnabled: Boolean): Unit = {
    loadDynamicPartitionsMethod.invoke(hive, loadPath, tableName, partSpec, replace: JBoolean,
      numDP: JInteger, holdDDLTime: JBoolean, listBucketingEnabled: JBoolean, JBoolean.FALSE,
      0L: JLong)
  }

}
