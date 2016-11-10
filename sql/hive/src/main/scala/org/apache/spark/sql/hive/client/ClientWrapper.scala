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
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.language.reflectiveCalls

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.{Database, FieldSchema}
import org.apache.hadoop.hive.metastore.{TableType => HTableType}
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.ql.{Driver, metadata}
import org.apache.hadoop.hive.shims.{HadoopShims, ShimLoader}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.VersionInfo

import org.apache.spark.{SparkConf, SparkException, Logging}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.util.{CircularBuffer, Utils}

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
    override val version: HiveVersion,
    config: Map[String, String],
    initClassLoader: ClassLoader,
    val clientLoader: IsolatedClientLoader)
  extends ClientInterface
  with Logging {

  overrideHadoopShims()

  // !! HACK ALERT !!
  //
  // Internally, Hive `ShimLoader` tries to load different versions of Hadoop shims by checking
  // major version number gathered from Hadoop jar files:
  //
  // - For major version number 1, load `Hadoop20SShims`, where "20S" stands for Hadoop 0.20 with
  //   security.
  // - For major version number 2, load `Hadoop23Shims`, where "23" stands for Hadoop 0.23.
  //
  // However, APIs in Hadoop 2.0.x and 2.1.x versions were in flux due to historical reasons. It
  // turns out that Hadoop 2.0.x versions should also be used together with `Hadoop20SShims`, but
  // `Hadoop23Shims` is chosen because the major version number here is 2.
  //
  // To fix this issue, we try to inspect Hadoop version via `org.apache.hadoop.utils.VersionInfo`
  // and load `Hadoop20SShims` for Hadoop 1.x and 2.0.x versions.  If Hadoop version information is
  // not available, we decide whether to override the shims or not by checking for existence of a
  // probe method which doesn't exist in Hadoop 1.x or 2.0.x versions.
  private def overrideHadoopShims(): Unit = {
    val hadoopVersion = VersionInfo.getVersion
    val VersionPattern = """(\d+)\.(\d+).*""".r

    hadoopVersion match {
      case null =>
        logError("Failed to inspect Hadoop version")

        // Using "Path.getPathWithoutSchemeAndAuthority" as the probe method.
        val probeMethod = "getPathWithoutSchemeAndAuthority"
        if (!classOf[Path].getDeclaredMethods.exists(_.getName == probeMethod)) {
          logInfo(
            s"Method ${classOf[Path].getCanonicalName}.$probeMethod not found, " +
              s"we are probably using Hadoop 1.x or 2.0.x")
          loadHadoop20SShims()
        }

      case VersionPattern(majorVersion, minorVersion) =>
        logInfo(s"Inspected Hadoop version: $hadoopVersion")

        // Loads Hadoop20SShims for 1.x and 2.0.x versions
        val (major, minor) = (majorVersion.toInt, minorVersion.toInt)
        if (major < 2 || (major == 2 && minor == 0)) {
          loadHadoop20SShims()
        }
    }

    // Logs the actual loaded Hadoop shims class
    val loadedShimsClassName = ShimLoader.getHadoopShims.getClass.getCanonicalName
    logInfo(s"Loaded $loadedShimsClassName for Hadoop version $hadoopVersion")
  }

  private def loadHadoop20SShims(): Unit = {
    val hadoop20SShimsClassName = "org.apache.hadoop.hive.shims.Hadoop20SShims"
    logInfo(s"Loading Hadoop shims $hadoop20SShimsClassName")

    try {
      val shimsField = classOf[ShimLoader].getDeclaredField("hadoopShims")
      // scalastyle:off classforname
      val shimsClass = Class.forName(hadoop20SShimsClassName)
      // scalastyle:on classforname
      val shims = classOf[HadoopShims].cast(shimsClass.newInstance())
      shimsField.setAccessible(true)
      shimsField.set(null, shims)
    } catch { case cause: Throwable =>
      throw new RuntimeException(s"Failed to load $hadoop20SShimsClassName", cause)
    }
  }

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

  // Create an internal session state for this ClientWrapper.
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
  def conf: HiveConf = state.getConf

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
            "HiveClientWrapper got thrift exception, destroying client and retrying " +
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
    // Set the thread local metastore client to the client associated with this ClientWrapper.
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
        schema = h.getCols.asScala.map(f => HiveColumn(f.getName, f.getType, f.getComment)),
        partitionColumns = h.getPartCols.asScala.map(f =>
          HiveColumn(f.getName, f.getType, f.getComment)),
        properties = h.getParameters.asScala.toMap,
        serdeProperties = h.getTTable.getSd.getSerdeInfo.getParameters.asScala.toMap,
        tableType = h.getTableType match {
          case HTableType.MANAGED_TABLE => ManagedTable
          case HTableType.EXTERNAL_TABLE => ExternalTable
          case HTableType.VIRTUAL_VIEW => VirtualView
          case HTableType.INDEX_TABLE => IndexTable
        },
        location = shim.getDataLocation(h),
        inputFormat = Option(h.getInputFormatClass).map(_.getName),
        outputFormat = Option(h.getOutputFormatClass).map(_.getName),
        serde = Option(h.getSerializationLib),
        viewText = Option(h.getViewExpandedText)).withClient(this)
    }
    converted
  }

  private def toInputFormat(name: String) =
    Utils.classForName(name).asInstanceOf[Class[_ <: org.apache.hadoop.mapred.InputFormat[_, _]]]

  private def toOutputFormat(name: String) =
    Utils.classForName(name)
      .asInstanceOf[Class[_ <: org.apache.hadoop.hive.ql.io.HiveOutputFormat[_, _]]]

  private def toQlTable(table: HiveTable): metadata.Table = {
    val qlTable = new metadata.Table(table.database, table.name)

    qlTable.setFields(table.schema.map(c => new FieldSchema(c.name, c.hiveType, c.comment)).asJava)
    qlTable.setPartCols(
      table.partitionColumns.map(c => new FieldSchema(c.name, c.hiveType, c.comment)).asJava)
    table.properties.foreach { case (k, v) => qlTable.setProperty(k, v) }
    table.serdeProperties.foreach { case (k, v) => qlTable.setSerdeParam(k, v) }

    // set owner
    qlTable.setOwner(conf.getUser)
    // set create time
    qlTable.setCreateTime((System.currentTimeMillis() / 1000).asInstanceOf[Int])

    table.location.foreach { loc => shim.setDataLocation(qlTable, loc) }
    table.inputFormat.map(toInputFormat).foreach(qlTable.setInputFormatClass)
    table.outputFormat.map(toOutputFormat).foreach(qlTable.setOutputFormatClass)
    table.serde.foreach(qlTable.setSerializationLib)

    qlTable
  }

  private def toViewTable(view: HiveTable): metadata.Table = {
    // TODO: this is duplicated with `toQlTable` except the table type stuff.
    val tbl = new metadata.Table(view.database, view.name)
    tbl.setTableType(HTableType.VIRTUAL_VIEW)
    tbl.setSerializationLib(null)
    tbl.clearSerDeInfo()

    // TODO: we will save the same SQL string to original and expanded text, which is different
    // from Hive.
    tbl.setViewOriginalText(view.viewText.get)
    tbl.setViewExpandedText(view.viewText.get)

    tbl.setFields(view.schema.map(c => new FieldSchema(c.name, c.hiveType, c.comment)).asJava)
    view.properties.foreach { case (k, v) => tbl.setProperty(k, v) }

    // set owner
    tbl.setOwner(conf.getUser)
    // set create time
    tbl.setCreateTime((System.currentTimeMillis() / 1000).asInstanceOf[Int])

    tbl
  }

  override def createView(view: HiveTable): Unit = withHiveState {
    client.createTable(toViewTable(view))
  }

  override def alertView(view: HiveTable): Unit = withHiveState {
    client.alterTable(view.qualifiedName, toViewTable(view))
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
      values = Option(apiPartition.getValues).map(_.asScala).getOrElse(Seq.empty),
      storage = HiveStorageDescriptor(
        location = apiPartition.getSd.getLocation,
        inputFormat = apiPartition.getSd.getInputFormat,
        outputFormat = apiPartition.getSd.getOutputFormat,
        serde = apiPartition.getSd.getSerdeInfo.getSerializationLib,
        serdeProperties = apiPartition.getSd.getSerdeInfo.getParameters.asScala.toMap))
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
    shim.getAllPartitions(client, qlTable).map(toHivePartition)
  }

  override def getPartitionsByFilter(
      hTable: HiveTable,
      predicates: Seq[Expression]): Seq[HivePartition] = withHiveState {
    val qlTable = toQlTable(hTable)
    shim.getPartitionsByFilter(client, qlTable, predicates).map(toHivePartition)
  }

  override def listTables(dbName: String): Seq[String] = withHiveState {
    client.getAllTables(dbName).asScala
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
            CommandProcessorFactory.clean(conf)
            throw new QueryExecutionException(response.getErrorMessage)
          }
          driver.setMaxRows(maxRows)

          val results = shim.getDriverResults(driver)
          driver.close()
          CommandProcessorFactory.clean(conf)
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

  def newSession(): ClientWrapper = {
    clientLoader.createClient().asInstanceOf[ClientWrapper]
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
}
