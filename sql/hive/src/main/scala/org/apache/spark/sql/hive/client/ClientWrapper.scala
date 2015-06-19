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

import java.io.{BufferedReader, InputStreamReader, File, PrintStream}
import java.net.URI
import java.util.{ArrayList => JArrayList, Map => JMap, List => JList, Set => JSet}

import scala.collection.JavaConversions._
import scala.language.reflectiveCalls

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.metadata
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.Driver

import org.apache.spark.Logging
import org.apache.spark.sql.execution.QueryExecutionException


/**
 * A class that wraps the HiveClient and converts its responses to externally visible classes.
 * Note that this class is typically loaded with an internal classloader for each instantiation,
 * allowing it to interact directly with a specific isolated version of Hive.  Loading this class
 * with the isolated classloader however will result in it only being visible as a ClientInterface,
 * not a ClientWrapper.
 *
 * This class needs to interact with multiple versions of Hive, but will always be compiled with
 * the 'native', execution version of Hive.  Therefore, any places where hive breaks compatibility
 * must use reflection after matching on `version`.
 *
 * @param version the version of hive used when pick function calls that are not compatible.
 * @param config  a collection of configuration options that will be added to the hive conf before
 *                opening the hive client.
 * @param initClassLoader the classloader used when creating the `state` field of
 *                        this ClientWrapper.
 */
private[hive] class ClientWrapper(
    version: HiveVersion,
    config: Map[String, String],
    initClassLoader: ClassLoader)
  extends ClientInterface
  with Logging
  with ReflectionMagic {

  // Circular buffer to hold what hive prints to STDOUT and ERR.  Only printed when failures occur.
  private val outputBuffer = new java.io.OutputStream {
    var pos: Int = 0
    var buffer = new Array[Int](10240)
    def write(i: Int): Unit = {
      buffer(pos) = i
      pos = (pos + 1) % buffer.size
    }

    override def toString: String = {
      val (end, start) = buffer.splitAt(pos)
      val input = new java.io.InputStream {
        val iterator = (start ++ end).iterator

        def read(): Int = if (iterator.hasNext) iterator.next() else -1
      }
      val reader = new BufferedReader(new InputStreamReader(input))
      val stringBuilder = new StringBuilder
      var line = reader.readLine()
      while(line != null) {
        stringBuilder.append(line)
        stringBuilder.append("\n")
        line = reader.readLine()
      }
      stringBuilder.toString()
    }
  }

  // Create an internal session state for this ClientWrapper.
  val state = {
    val original = Thread.currentThread().getContextClassLoader
    // Switch to the initClassLoader.
    Thread.currentThread().setContextClassLoader(initClassLoader)
    val ret = try {
      val oldState = SessionState.get()
      if (oldState == null) {
        val initialConf = new HiveConf(classOf[SessionState])
        // HiveConf is a Hadoop Configuration, which has a field of classLoader and
        // the initial value will be the current thread's context class loader
        // (i.e. initClassLoader at here).
        // We call initialConf.setClassLoader(initClassLoader) at here to make
        // this action explicit.
        initialConf.setClassLoader(initClassLoader)
        config.foreach { case (k, v) =>
          logDebug(s"Hive Config: $k=$v")
          initialConf.set(k, v)
        }
        val newState = new SessionState(initialConf)
        SessionState.start(newState)
        newState.out = new PrintStream(outputBuffer, true, "UTF-8")
        newState.err = new PrintStream(outputBuffer, true, "UTF-8")
        newState
      } else {
        oldState
      }
    } finally {
      Thread.currentThread().setContextClassLoader(original)
    }
    ret
  }

  /** Returns the configuration for the current session. */
  def conf: HiveConf = SessionState.get().getConf

  // TODO: should be a def?s
  // When we create this val client, the HiveConf of it (conf) is the one associated with state.
  private val client = Hive.get(conf)

  /**
   * Runs `f` with ThreadLocal session state and classloaders configured for this version of hive.
   */
  private def withHiveState[A](f: => A): A = synchronized {
    val original = Thread.currentThread().getContextClassLoader
    // Set the thread local metastore client to the client associated with this ClientWrapper.
    Hive.set(client)
    version match {
      case hive.v12 =>
        // Starting from Hive 0.13.0, setCurrentSessionState will use the classLoader associated
        // with the HiveConf in `state` to override the context class loader of the current
        // thread. So, for Hive 0.12, we add the same behavior.
        Thread.currentThread().setContextClassLoader(state.getConf.getClassLoader)
        classOf[SessionState]
          .callStatic[SessionState, SessionState]("start", state)
      case hive.v13 =>
        classOf[SessionState]
          .callStatic[SessionState, SessionState]("setCurrentSessionState", state)
    }
    val ret = try f finally {
      Thread.currentThread().setContextClassLoader(original)
    }
    ret
  }

  def setOut(stream: PrintStream): Unit = withHiveState {
    state.out = stream
  }

  def setInfo(stream: PrintStream): Unit = withHiveState {
    state.info = stream
  }

  def setError(stream: PrintStream): Unit = withHiveState {
    state.err = stream
  }

  override def currentDatabase: String = withHiveState {
    state.getCurrentDatabase
  }

  override def createDatabase(database: HiveDatabase): Unit = withHiveState {
    client.createDatabase(
      new Database(
        database.name,
        "",
        new File(database.location).toURI.toString,
        new java.util.HashMap),
        true)
  }

  override def getDatabaseOption(name: String): Option[HiveDatabase] = withHiveState {
    Option(client.getDatabase(name)).map { d =>
      HiveDatabase(
        name = d.getName,
        location = d.getLocationUri)
    }
  }

  override def getTableOption(
      dbName: String,
      tableName: String): Option[HiveTable] = withHiveState {

    logDebug(s"Looking up $dbName.$tableName")

    val hiveTable = Option(client.getTable(dbName, tableName, false))
    val converted = hiveTable.map { h =>

      HiveTable(
        name = h.getTableName,
        specifiedDatabase = Option(h.getDbName),
        schema = h.getCols.map(f => HiveColumn(f.getName, f.getType, f.getComment)),
        partitionColumns = h.getPartCols.map(f => HiveColumn(f.getName, f.getType, f.getComment)),
        properties = h.getParameters.toMap,
        serdeProperties = h.getTTable.getSd.getSerdeInfo.getParameters.toMap,
        tableType = h.getTableType match {
          case TableType.MANAGED_TABLE => ManagedTable
          case TableType.EXTERNAL_TABLE => ExternalTable
          case TableType.VIRTUAL_VIEW => VirtualView
          case TableType.INDEX_TABLE => IndexTable
        },
        location = version match {
          case hive.v12 => Option(h.call[URI]("getDataLocation")).map(_.toString)
          case hive.v13 => Option(h.call[Path]("getDataLocation")).map(_.toString)
        },
        inputFormat = Option(h.getInputFormatClass).map(_.getName),
        outputFormat = Option(h.getOutputFormatClass).map(_.getName),
        serde = Option(h.getSerializationLib),
        viewText = Option(h.getViewExpandedText)).withClient(this)
    }
    converted
  }

  private def toInputFormat(name: String) =
    Class.forName(name).asInstanceOf[Class[_ <: org.apache.hadoop.mapred.InputFormat[_, _]]]

  private def toOutputFormat(name: String) =
    Class.forName(name)
      .asInstanceOf[Class[_ <: org.apache.hadoop.hive.ql.io.HiveOutputFormat[_, _]]]

  private def toQlTable(table: HiveTable): metadata.Table = {
    val qlTable = new metadata.Table(table.database, table.name)

    qlTable.setFields(table.schema.map(c => new FieldSchema(c.name, c.hiveType, c.comment)))
    qlTable.setPartCols(
      table.partitionColumns.map(c => new FieldSchema(c.name, c.hiveType, c.comment)))
    table.properties.foreach { case (k, v) => qlTable.setProperty(k, v) }
    table.serdeProperties.foreach { case (k, v) => qlTable.setSerdeParam(k, v) }

    // set owner
    qlTable.setOwner(conf.getUser)
    // set create time
    qlTable.setCreateTime((System.currentTimeMillis() / 1000).asInstanceOf[Int])

    version match {
      case hive.v12 =>
        table.location.map(new URI(_)).foreach(u => qlTable.call[URI, Unit]("setDataLocation", u))
      case hive.v13 =>
        table.location
          .map(new org.apache.hadoop.fs.Path(_))
          .foreach(qlTable.call[Path, Unit]("setDataLocation", _))
    }
    table.inputFormat.map(toInputFormat).foreach(qlTable.setInputFormatClass)
    table.outputFormat.map(toOutputFormat).foreach(qlTable.setOutputFormatClass)
    table.serde.foreach(qlTable.setSerializationLib)

    qlTable
  }

  override def createTable(table: HiveTable): Unit = withHiveState {
    val qlTable = toQlTable(table)
    client.createTable(qlTable)
  }

  override def alterTable(table: HiveTable): Unit = withHiveState {
    val qlTable = toQlTable(table)
    client.alterTable(table.qualifiedName, qlTable)
  }

  private def toHivePartition(partition: metadata.Partition): HivePartition = {
    val apiPartition = partition.getTPartition
    HivePartition(
      values = Option(apiPartition.getValues).map(_.toSeq).getOrElse(Seq.empty),
      storage = HiveStorageDescriptor(
        location = apiPartition.getSd.getLocation,
        inputFormat = apiPartition.getSd.getInputFormat,
        outputFormat = apiPartition.getSd.getOutputFormat,
        serde = apiPartition.getSd.getSerdeInfo.getSerializationLib,
        serdeProperties = apiPartition.getSd.getSerdeInfo.getParameters.toMap))
  }

  override def getPartitionOption(
      table: HiveTable,
      partitionSpec: JMap[String, String]): Option[HivePartition] = withHiveState {

    val qlTable = toQlTable(table)
    val qlPartition = client.getPartition(qlTable, partitionSpec, false)
    Option(qlPartition).map(toHivePartition)
  }

  override def getAllPartitions(hTable: HiveTable): Seq[HivePartition] = withHiveState {
    val qlTable = toQlTable(hTable)
    val qlPartitions = version match {
      case hive.v12 =>
        client.call[metadata.Table, JSet[metadata.Partition]]("getAllPartitionsForPruner", qlTable)
      case hive.v13 =>
        client.call[metadata.Table, JSet[metadata.Partition]]("getAllPartitionsOf", qlTable)
    }
    qlPartitions.toSeq.map(toHivePartition)
  }

  override def listTables(dbName: String): Seq[String] = withHiveState {
    client.getAllTables(dbName)
  }

  /**
   * Runs the specified SQL query using Hive.
   */
  override def runSqlHive(sql: String): Seq[String] = {
    val maxResults = 100000
    val results = runHive(sql, maxResults)
    // It is very confusing when you only get back some of the results...
    if (results.size == maxResults) sys.error("RESULTS POSSIBLY TRUNCATED")
    results
  }

  /**
   * Execute the command using Hive and return the results as a sequence. Each element
   * in the sequence is one row.
   */
  protected def runHive(cmd: String, maxRows: Int = 1000): Seq[String] = withHiveState {
    logDebug(s"Running hiveql '$cmd'")
    if (cmd.toLowerCase.startsWith("set")) { logDebug(s"Changing config: $cmd") }
    try {
      val cmd_trimmed: String = cmd.trim()
      val tokens: Array[String] = cmd_trimmed.split("\\s+")
      // The remainder of the command.
      val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
      val proc: CommandProcessor = version match {
        case hive.v12 =>
          classOf[CommandProcessorFactory]
            .callStatic[String, HiveConf, CommandProcessor]("get", tokens(0), conf)
        case hive.v13 =>
          classOf[CommandProcessorFactory]
            .callStatic[Array[String], HiveConf, CommandProcessor]("get", Array(tokens(0)), conf)
      }

      proc match {
        case driver: Driver =>
          val response: CommandProcessorResponse = driver.run(cmd)
          // Throw an exception if there is an error in query processing.
          if (response.getResponseCode != 0) {
            driver.close()
            throw new QueryExecutionException(response.getErrorMessage)
          }
          driver.setMaxRows(maxRows)

          val results = version match {
            case hive.v12 =>
              val res = new JArrayList[String]
              driver.call[JArrayList[String], Boolean]("getResults", res)
              res.toSeq
            case hive.v13 =>
              val res = new JArrayList[Object]
              driver.call[JList[Object], Boolean]("getResults", res)
              res.map { r =>
                r match {
                  case s: String => s
                  case a: Array[Object] => a(0).asInstanceOf[String]
                }
              }
          }
          driver.close()
          results

        case _ =>
          if (state.out != null) {
            state.out.println(tokens(0) + " " + cmd_1)
          }
          Seq(proc.run(cmd_1).getResponseCode.toString)
      }
    } catch {
      case e: Exception =>
        logError(
          s"""
            |======================
            |HIVE FAILURE OUTPUT
            |======================
            |${outputBuffer.toString}
            |======================
            |END HIVE FAILURE OUTPUT
            |======================
          """.stripMargin)
        throw e
    }
  }

  def loadPartition(
      loadPath: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String],
      replace: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean): Unit = withHiveState {

    client.loadPartition(
      new Path(loadPath), // TODO: Use URI
      tableName,
      partSpec,
      replace,
      holdDDLTime,
      inheritTableSpecs,
      isSkewedStoreAsSubdir)
  }

  def loadTable(
      loadPath: String, // TODO URI
      tableName: String,
      replace: Boolean,
      holdDDLTime: Boolean): Unit = withHiveState {
    client.loadTable(
      new Path(loadPath),
      tableName,
      replace,
      holdDDLTime)
  }

  def loadDynamicPartitions(
      loadPath: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String],
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean,
      listBucketingEnabled: Boolean): Unit = withHiveState {
    client.loadDynamicPartitions(
      new Path(loadPath),
      tableName,
      partSpec,
      replace,
      numDP,
      holdDDLTime,
      listBucketingEnabled)
  }

  def reset(): Unit = withHiveState {
    client.getAllTables("default").foreach { t =>
        logDebug(s"Deleting table $t")
        val table = client.getTable("default", t)
        client.getIndexes("default", t, 255).foreach { index =>
          client.dropIndex("default", t, index.getIndexName, true)
        }
        if (!table.isIndexTable) {
          client.dropTable("default", t)
        }
      }
      client.getAllDatabases.filterNot(_ == "default").foreach { db =>
        logDebug(s"Dropping Database: $db")
        client.dropDatabase(db, true, false, true)
      }
  }
}
