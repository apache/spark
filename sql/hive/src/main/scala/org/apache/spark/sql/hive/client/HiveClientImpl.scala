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

import java.io.{File, PrintStream}

import scala.collection.JavaConverters._
import scala.language.reflectiveCalls

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.{TableType => HiveTableType}
import org.apache.hadoop.hive.metastore.api.{Database => HiveDatabase, FieldSchema, Function => HiveFunction, FunctionType, PrincipalType, ResourceUri}
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition => HivePartition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.{Logging, SparkConf, SparkException}
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchPartitionException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.util.{CircularBuffer, Utils}

/**
 * A class that wraps the HiveClient and converts its responses to externally visible classes.
 * Note that this class is typically loaded with an internal classloader for each instantiation,
 * allowing it to interact directly with a specific isolated version of Hive.  Loading this class
 * with the isolated classloader however will result in it only being visible as a [[HiveClient]],
 * not a [[HiveClientImpl]].
 *
 * This class needs to interact with multiple versions of Hive, but will always be compiled with
 * the 'native', execution version of Hive.  Therefore, any places where hive breaks compatibility
 * must use reflection after matching on `version`.
 *
 * @param version the version of hive used when pick function calls that are not compatible.
 * @param config  a collection of configuration options that will be added to the hive conf before
 *                opening the hive client.
 * @param initClassLoader the classloader used when creating the `state` field of
 *                        this [[HiveClientImpl]].
 */
private[hive] class HiveClientImpl(
    override val version: HiveVersion,
    config: Map[String, String],
    initClassLoader: ClassLoader,
    val clientLoader: IsolatedClientLoader)
  extends HiveClient
  with Logging {

  // Circular buffer to hold what hive prints to STDOUT and ERR.  Only printed when failures occur.
  private val outputBuffer = new CircularBuffer()

  private val shim = version match {
    case hive.v12 => new Shim_v0_12()
    case hive.v13 => new Shim_v0_13()
    case hive.v14 => new Shim_v0_14()
    case hive.v1_0 => new Shim_v1_0()
    case hive.v1_1 => new Shim_v1_1()
    case hive.v1_2 => new Shim_v1_2()
  }

  // Create an internal session state for this HiveClientImpl.
  val state = {
    val original = Thread.currentThread().getContextClassLoader
    // Switch to the initClassLoader.
    Thread.currentThread().setContextClassLoader(initClassLoader)

    // Set up kerberos credentials for UserGroupInformation.loginUser within
    // current class loader
    // Instead of using the spark conf of the current spark context, a new
    // instance of SparkConf is needed for the original value of spark.yarn.keytab
    // and spark.yarn.principal set in SparkSubmit, as yarn.Client resets the
    // keytab configuration for the link name in distributed cache
    val sparkConf = new SparkConf
    if (sparkConf.contains("spark.yarn.principal") && sparkConf.contains("spark.yarn.keytab")) {
      val principalName = sparkConf.get("spark.yarn.principal")
      val keytabFileName = sparkConf.get("spark.yarn.keytab")
      if (!new File(keytabFileName).exists()) {
        throw new SparkException(s"Keytab file: ${keytabFileName}" +
          " specified in spark.yarn.keytab does not exist")
      } else {
        logInfo("Attempting to login to Kerberos" +
          s" using principal: ${principalName} and keytab: ${keytabFileName}")
        UserGroupInformation.loginUserFromKeytab(principalName, keytabFileName)
      }
    }

    val ret = try {
      // originState will be created if not exists, will never be null
      val originalState = SessionState.get()
      if (originalState.isInstanceOf[CliSessionState]) {
        // In `SparkSQLCLIDriver`, we have already started a `CliSessionState`,
        // which contains information like configurations from command line. Later
        // we call `SparkSQLEnv.init()` there, which would run into this part again.
        // so we should keep `conf` and reuse the existing instance of `CliSessionState`.
        originalState
      } else {
        val initialConf = new HiveConf(classOf[SessionState])
        // HiveConf is a Hadoop Configuration, which has a field of classLoader and
        // the initial value will be the current thread's context class loader
        // (i.e. initClassLoader at here).
        // We call initialConf.setClassLoader(initClassLoader) at here to make
        // this action explicit.
        initialConf.setClassLoader(initClassLoader)
        config.foreach { case (k, v) =>
          if (k.toLowerCase.contains("password")) {
            logDebug(s"Hive Config: $k=xxx")
          } else {
            logDebug(s"Hive Config: $k=$v")
          }
          initialConf.set(k, v)
        }
        val state = new SessionState(initialConf)
        if (clientLoader.cachedHive != null) {
          Hive.set(clientLoader.cachedHive.asInstanceOf[Hive])
        }
        SessionState.start(state)
        state.out = new PrintStream(outputBuffer, true, "UTF-8")
        state.err = new PrintStream(outputBuffer, true, "UTF-8")
        state
      }
    } finally {
      Thread.currentThread().setContextClassLoader(original)
    }
    ret
  }

  /** Returns the configuration for the current session. */
  def conf: HiveConf = SessionState.get().getConf

  override def getConf(key: String, defaultValue: String): String = {
    conf.get(key, defaultValue)
  }

  // We use hive's conf for compatibility.
  private val retryLimit = conf.getIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES)
  private val retryDelayMillis = shim.getMetastoreClientConnectRetryDelayMillis(conf)

  /**
   * Runs `f` with multiple retries in case the hive metastore is temporarily unreachable.
   */
  private def retryLocked[A](f: => A): A = clientLoader.synchronized {
    // Hive sometimes retries internally, so set a deadline to avoid compounding delays.
    val deadline = System.nanoTime + (retryLimit * retryDelayMillis * 1e6).toLong
    var numTries = 0
    var caughtException: Exception = null
    do {
      numTries += 1
      try {
        return f
      } catch {
        case e: Exception if causedByThrift(e) =>
          caughtException = e
          logWarning(
            "HiveClient got thrift exception, destroying client and retrying " +
              s"(${retryLimit - numTries} tries remaining)", e)
          clientLoader.cachedHive = null
          Thread.sleep(retryDelayMillis)
      }
    } while (numTries <= retryLimit && System.nanoTime < deadline)
    if (System.nanoTime > deadline) {
      logWarning("Deadline exceeded")
    }
    throw caughtException
  }

  private def causedByThrift(e: Throwable): Boolean = {
    var target = e
    while (target != null) {
      val msg = target.getMessage()
      if (msg != null && msg.matches("(?s).*(TApplication|TProtocol|TTransport)Exception.*")) {
        return true
      }
      target = target.getCause()
    }
    false
  }

  def client: Hive = {
    if (clientLoader.cachedHive != null) {
      clientLoader.cachedHive.asInstanceOf[Hive]
    } else {
      val c = Hive.get(conf)
      clientLoader.cachedHive = c
      c
    }
  }

  /**
   * Runs `f` with ThreadLocal session state and classloaders configured for this version of hive.
   */
  def withHiveState[A](f: => A): A = retryLocked {
    val original = Thread.currentThread().getContextClassLoader
    // Set the thread local metastore client to the client associated with this HiveClientImpl.
    Hive.set(client)
    // The classloader in clientLoader could be changed after addJar, always use the latest
    // classloader
    state.getConf.setClassLoader(clientLoader.classLoader)
    // setCurrentSessionState will use the classLoader associated
    // with the HiveConf in `state` to override the context class loader of the current
    // thread.
    shim.setCurrentSessionState(state)
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

  override def setCurrentDatabase(databaseName: String): Unit = withHiveState {
    if (getDatabaseOption(databaseName).isDefined) {
      state.setCurrentDatabase(databaseName)
    } else {
      throw new NoSuchDatabaseException(databaseName)
    }
  }

  override def createDatabase(
      database: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = withHiveState {
    client.createDatabase(
      new HiveDatabase(
        database.name,
        database.description,
        database.locationUri,
        database.properties.asJava),
        ignoreIfExists)
  }

  override def dropDatabase(
      name: String,
      ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = withHiveState {
    client.dropDatabase(name, true, ignoreIfNotExists, cascade)
  }

  override def alterDatabase(database: CatalogDatabase): Unit = withHiveState {
    client.alterDatabase(
      database.name,
      new HiveDatabase(
        database.name,
        database.description,
        database.locationUri,
        database.properties.asJava))
  }

  override def getDatabaseOption(name: String): Option[CatalogDatabase] = withHiveState {
    Option(client.getDatabase(name)).map { d =>
      CatalogDatabase(
        name = d.getName,
        description = d.getDescription,
        locationUri = d.getLocationUri,
        properties = d.getParameters.asScala.toMap)
    }
  }

  override def listDatabases(pattern: String): Seq[String] = withHiveState {
    client.getDatabasesByPattern(pattern).asScala.toSeq
  }

  override def getTableOption(
      dbName: String,
      tableName: String): Option[CatalogTable] = withHiveState {
    logDebug(s"Looking up $dbName.$tableName")
    Option(client.getTable(dbName, tableName, false)).map { h =>
      CatalogTable(
        specifiedDatabase = Option(h.getDbName),
        name = h.getTableName,
        tableType = h.getTableType match {
          case HiveTableType.EXTERNAL_TABLE => CatalogTableType.EXTERNAL_TABLE
          case HiveTableType.MANAGED_TABLE => CatalogTableType.MANAGED_TABLE
          case HiveTableType.INDEX_TABLE => CatalogTableType.INDEX_TABLE
          case HiveTableType.VIRTUAL_VIEW => CatalogTableType.VIRTUAL_VIEW
        },
        schema = h.getCols.asScala.map(fromHiveColumn),
        partitionColumns = h.getPartCols.asScala.map(fromHiveColumn),
        sortColumns = Seq(),
        numBuckets = h.getNumBuckets,
        createTime = h.getTTable.getCreateTime.toLong * 1000,
        lastAccessTime = h.getLastAccessTime.toLong * 1000,
        storage = CatalogStorageFormat(
          locationUri = shim.getDataLocation(h),
          inputFormat = Option(h.getInputFormatClass).map(_.getName),
          outputFormat = Option(h.getOutputFormatClass).map(_.getName),
          serde = Option(h.getSerializationLib),
          serdeProperties = h.getTTable.getSd.getSerdeInfo.getParameters.asScala.toMap
        ),
        properties = h.getParameters.asScala.toMap,
        viewOriginalText = Option(h.getViewOriginalText),
        viewText = Option(h.getViewExpandedText))
    }
  }

  override def createView(view: CatalogTable): Unit = withHiveState {
    client.createTable(toHiveViewTable(view))
  }

  override def alertView(view: CatalogTable): Unit = withHiveState {
    client.alterTable(view.qualifiedName, toHiveViewTable(view))
  }

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = withHiveState {
    client.createTable(toHiveTable(table), ignoreIfExists)
  }

  override def dropTable(
      dbName: String,
      tableName: String,
      ignoreIfNotExists: Boolean): Unit = withHiveState {
    client.dropTable(dbName, tableName, true, ignoreIfNotExists)
  }

  override def alterTable(tableName: String, table: CatalogTable): Unit = withHiveState {
    val hiveTable = toHiveTable(table)
    // Do not use `table.qualifiedName` here because this may be a rename
    val qualifiedTableName = s"${table.database}.$tableName"
    client.alterTable(qualifiedTableName, hiveTable)
  }

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = withHiveState {
    val addPartitionDesc = new AddPartitionDesc(db, table, ignoreIfExists)
    parts.foreach { s =>
      addPartitionDesc.addPartition(s.spec.asJava, s.storage.locationUri.orNull)
    }
    client.createPartitions(addPartitionDesc)
  }

  override def dropPartitions(
      db: String,
      table: String,
      specs: Seq[ExternalCatalog.TablePartitionSpec]): Unit = withHiveState {
    // TODO: figure out how to drop multiple partitions in one call
    specs.foreach { s => client.dropPartition(db, table, s.values.toList.asJava, true) }
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[ExternalCatalog.TablePartitionSpec],
      newSpecs: Seq[ExternalCatalog.TablePartitionSpec]): Unit = withHiveState {
    require(specs.size == newSpecs.size, "number of old and new partition specs differ")
    val catalogTable = getTable(db, table)
    val hiveTable = toHiveTable(catalogTable)
    specs.zip(newSpecs).foreach { case (oldSpec, newSpec) =>
      val hivePart = getPartitionOption(catalogTable, oldSpec)
        .map { p => toHivePartition(p.copy(spec = newSpec), hiveTable) }
        .getOrElse { throw new NoSuchPartitionException(db, table, oldSpec) }
      client.renamePartition(hiveTable, oldSpec.asJava, hivePart)
    }
  }

  override def alterPartitions(
      db: String,
      table: String,
      newParts: Seq[CatalogTablePartition]): Unit = withHiveState {
    val hiveTable = toHiveTable(getTable(db, table))
    client.alterPartitions(table, newParts.map { p => toHivePartition(p, hiveTable) }.asJava)
  }

  override def getPartitionOption(
      table: CatalogTable,
      spec: ExternalCatalog.TablePartitionSpec): Option[CatalogTablePartition] = withHiveState {
    val hiveTable = toHiveTable(table)
    val hivePartition = client.getPartition(hiveTable, spec.asJava, false)
    Option(hivePartition).map(fromHivePartition)
  }

  override def getAllPartitions(table: CatalogTable): Seq[CatalogTablePartition] = withHiveState {
    val hiveTable = toHiveTable(table)
    shim.getAllPartitions(client, hiveTable).map(fromHivePartition)
  }

  override def getPartitionsByFilter(
      table: CatalogTable,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = withHiveState {
    val hiveTable = toHiveTable(table)
    shim.getPartitionsByFilter(client, hiveTable, predicates).map(fromHivePartition)
  }

  override def listTables(dbName: String): Seq[String] = withHiveState {
    client.getAllTables(dbName).asScala
  }

  override def listTables(dbName: String, pattern: String): Seq[String] = withHiveState {
    client.getTablesByPattern(dbName, pattern).asScala
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
      val proc = shim.getCommandProcessor(tokens(0), conf)
      proc match {
        case driver: Driver =>
          val response: CommandProcessorResponse = driver.run(cmd)
          // Throw an exception if there is an error in query processing.
          if (response.getResponseCode != 0) {
            driver.close()
            throw new QueryExecutionException(response.getErrorMessage)
          }
          driver.setMaxRows(maxRows)

          val results = shim.getDriverResults(driver)
          driver.close()
          results

        case _ =>
          if (state.out != null) {
            // scalastyle:off println
            state.out.println(tokens(0) + " " + cmd_1)
            // scalastyle:on println
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
    shim.loadPartition(
      client,
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
    shim.loadTable(
      client,
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
    shim.loadDynamicPartitions(
      client,
      new Path(loadPath),
      tableName,
      partSpec,
      replace,
      numDP,
      holdDDLTime,
      listBucketingEnabled)
  }

  override def createFunction(db: String, func: CatalogFunction): Unit = withHiveState {
    client.createFunction(toHiveFunction(func, db))
  }

  override def dropFunction(db: String, name: String): Unit = withHiveState {
    client.dropFunction(db, name)
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = withHiveState {
    val catalogFunc = getFunction(db, oldName).copy(name = newName)
    val hiveFunc = toHiveFunction(catalogFunc, db)
    client.alterFunction(db, oldName, hiveFunc)
  }

  override def alterFunction(db: String, func: CatalogFunction): Unit = withHiveState {
    client.alterFunction(db, func.name, toHiveFunction(func, db))
  }

  override def getFunctionOption(
      db: String,
      name: String): Option[CatalogFunction] = withHiveState {
    Option(client.getFunction(db, name)).map(fromHiveFunction)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = withHiveState {
    client.getFunctions(db, pattern).asScala
  }

  def addJar(path: String): Unit = {
    val uri = new Path(path).toUri
    val jarURL = if (uri.getScheme == null) {
      // `path` is a local file path without a URL scheme
      new File(path).toURI.toURL
    } else {
      // `path` is a URL with a scheme
      uri.toURL
    }
    clientLoader.addJar(jarURL)
    runSqlHive(s"ADD JAR $path")
  }

  def newSession(): HiveClientImpl = {
    clientLoader.createClient().asInstanceOf[HiveClientImpl]
  }

  def reset(): Unit = withHiveState {
    client.getAllTables("default").asScala.foreach { t =>
        logDebug(s"Deleting table $t")
        val table = client.getTable("default", t)
        client.getIndexes("default", t, 255).asScala.foreach { index =>
          shim.dropIndex(client, "default", t, index.getIndexName)
        }
        if (!table.isIndexTable) {
          client.dropTable("default", t)
        }
      }
      client.getAllDatabases.asScala.filterNot(_ == "default").foreach { db =>
        logDebug(s"Dropping Database: $db")
        client.dropDatabase(db, true, false, true)
      }
  }


  /* -------------------------------------------------------- *
   |  Helper methods for converting to and from Hive classes  |
   * -------------------------------------------------------- */

  private def toInputFormat(name: String) =
    Utils.classForName(name).asInstanceOf[Class[_ <: org.apache.hadoop.mapred.InputFormat[_, _]]]

  private def toOutputFormat(name: String) =
    Utils.classForName(name)
      .asInstanceOf[Class[_ <: org.apache.hadoop.hive.ql.io.HiveOutputFormat[_, _]]]

  private def toHiveFunction(f: CatalogFunction, db: String): HiveFunction = {
    new HiveFunction(
      f.name,
      db,
      f.className,
      null,
      PrincipalType.USER,
      (System.currentTimeMillis / 1000).toInt,
      FunctionType.JAVA,
      List.empty[ResourceUri].asJava)
  }

  private def fromHiveFunction(hf: HiveFunction): CatalogFunction = {
    new CatalogFunction(hf.getFunctionName, hf.getClassName)
  }

  private def toHiveColumn(c: CatalogColumn): FieldSchema = {
    new FieldSchema(c.name, c.dataType, c.comment.orNull)
  }

  private def fromHiveColumn(hc: FieldSchema): CatalogColumn = {
    new CatalogColumn(
      name = hc.getName,
      dataType = hc.getType,
      nullable = true,
      comment = Option(hc.getComment))
  }

  private def toHiveTable(table: CatalogTable): HiveTable = {
    val hiveTable = new HiveTable(table.database, table.name)
    hiveTable.setTableType(table.tableType match {
      case CatalogTableType.EXTERNAL_TABLE => HiveTableType.EXTERNAL_TABLE
      case CatalogTableType.MANAGED_TABLE => HiveTableType.MANAGED_TABLE
      case CatalogTableType.INDEX_TABLE => HiveTableType.INDEX_TABLE
      case CatalogTableType.VIRTUAL_VIEW => HiveTableType.VIRTUAL_VIEW
    })
    hiveTable.setFields(table.schema.map(toHiveColumn).asJava)
    hiveTable.setPartCols(table.partitionColumns.map(toHiveColumn).asJava)
    // TODO: set sort columns here too
    hiveTable.setOwner(conf.getUser)
    hiveTable.setNumBuckets(table.numBuckets)
    hiveTable.setCreateTime((table.createTime / 1000).toInt)
    hiveTable.setLastAccessTime((table.lastAccessTime / 1000).toInt)
    table.storage.locationUri.foreach { loc => shim.setDataLocation(hiveTable, loc) }
    table.storage.inputFormat.map(toInputFormat).foreach(hiveTable.setInputFormatClass)
    table.storage.outputFormat.map(toOutputFormat).foreach(hiveTable.setOutputFormatClass)
    table.storage.serde.foreach(hiveTable.setSerializationLib)
    table.storage.serdeProperties.foreach { case (k, v) => hiveTable.setSerdeParam(k, v) }
    table.properties.foreach { case (k, v) => hiveTable.setProperty(k, v) }
    table.viewOriginalText.foreach { t => hiveTable.setViewOriginalText(t) }
    table.viewText.foreach { t => hiveTable.setViewExpandedText(t) }
    hiveTable
  }

  private def toHiveViewTable(view: CatalogTable): HiveTable = {
    val tbl = toHiveTable(view)
    tbl.setTableType(HiveTableType.VIRTUAL_VIEW)
    tbl.setSerializationLib(null)
    tbl.clearSerDeInfo()
    tbl
  }

  private def toHivePartition(
      p: CatalogTablePartition,
      ht: HiveTable): HivePartition = {
    new HivePartition(ht, p.spec.asJava, p.storage.locationUri.map { l => new Path(l) }.orNull)
  }

  private def fromHivePartition(hp: HivePartition): CatalogTablePartition = {
    val apiPartition = hp.getTPartition
    CatalogTablePartition(
      spec = Option(hp.getSpec).map(_.asScala.toMap).getOrElse(Map.empty),
      storage = CatalogStorageFormat(
        locationUri = Option(apiPartition.getSd.getLocation),
        inputFormat = Option(apiPartition.getSd.getInputFormat),
        outputFormat = Option(apiPartition.getSd.getOutputFormat),
        serde = Option(apiPartition.getSd.getSerdeInfo.getSerializationLib),
        serdeProperties = apiPartition.getSd.getSerdeInfo.getParameters.asScala.toMap))
  }

}
