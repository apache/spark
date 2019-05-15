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
import java.lang.{Iterable => JIterable}
import java.util.{Locale, Map => JMap}
import java.util.concurrent.TimeUnit._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.{TableType => HiveTableType}
import org.apache.hadoop.hive.metastore.api.{Database => HiveDatabase, FieldSchema, Order}
import org.apache.hadoop.hive.metastore.api.{SerDeInfo, StorageDescriptor}
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition => HivePartition, Table => HiveTable}
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchPartitionException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hive.HiveExternalCatalog.{DATASOURCE_SCHEMA, DATASOURCE_SCHEMA_NUMPARTS, DATASOURCE_SCHEMA_PART_PREFIX}
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.client.HiveClientImpl._
import org.apache.spark.sql.types._
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
 * Every HiveClientImpl creates an internal HiveConf object. This object is using the given
 * `hadoopConf` as the base. All options set in the `sparkConf` will be applied to the HiveConf
 * object and overrides any exiting options. Then, options in extraConfig will be applied
 * to the HiveConf object and overrides any existing options.
 *
 * @param version the version of hive used when pick function calls that are not compatible.
 * @param sparkConf all configuration options set in SparkConf.
 * @param hadoopConf the base Configuration object used by the HiveConf created inside
 *                   this HiveClientImpl.
 * @param extraConfig a collection of configuration options that will be added to the
 *                hive conf before opening the hive client.
 * @param initClassLoader the classloader used when creating the `state` field of
 *                        this [[HiveClientImpl]].
 */
private[hive] class HiveClientImpl(
    override val version: HiveVersion,
    warehouseDir: Option[String],
    sparkConf: SparkConf,
    hadoopConf: JIterable[JMap.Entry[String, String]],
    extraConfig: Map[String, String],
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
    case hive.v2_0 => new Shim_v2_0()
    case hive.v2_1 => new Shim_v2_1()
    case hive.v2_2 => new Shim_v2_2()
    case hive.v2_3 => new Shim_v2_3()
    case hive.v3_1 => new Shim_v3_1()
  }

  // Create an internal session state for this HiveClientImpl.
  val state: SessionState = {
    val original = Thread.currentThread().getContextClassLoader
    if (clientLoader.isolationOn) {
      // Switch to the initClassLoader.
      Thread.currentThread().setContextClassLoader(initClassLoader)
      try {
        newState()
      } finally {
        Thread.currentThread().setContextClassLoader(original)
      }
    } else {
      // Isolation off means we detect a CliSessionState instance in current thread.
      // 1: Inside the spark project, we have already started a CliSessionState in
      // `SparkSQLCLIDriver`, which contains configurations from command lines. Later, we call
      // `SparkSQLEnv.init()` there, which would new a hive client again. so we should keep those
      // configurations and reuse the existing instance of `CliSessionState`. In this case,
      // SessionState.get will always return a CliSessionState.
      // 2: In another case, a user app may start a CliSessionState outside spark project with built
      // in hive jars, which will turn off isolation, if SessionSate.detachSession is
      // called to remove the current state after that, hive client created later will initialize
      // its own state by newState()
      val ret = SessionState.get
      if (ret != null) {
        // hive.metastore.warehouse.dir is determined in SharedState after the CliSessionState
        // instance constructed, we need to follow that change here.
        warehouseDir.foreach { dir =>
          ret.getConf.setVar(ConfVars.METASTOREWAREHOUSE, dir)
        }
        ret
      } else {
        newState()
      }
    }
  }

  // Log the default warehouse location.
  logInfo(
    s"Warehouse location for Hive client " +
      s"(version ${version.fullVersion}) is ${conf.getVar(ConfVars.METASTOREWAREHOUSE)}")

  private def newState(): SessionState = {
    val hiveConf = new HiveConf(classOf[SessionState])
    // HiveConf is a Hadoop Configuration, which has a field of classLoader and
    // the initial value will be the current thread's context class loader
    // (i.e. initClassLoader at here).
    // We call initialConf.setClassLoader(initClassLoader) at here to make
    // this action explicit.
    hiveConf.setClassLoader(initClassLoader)

    // 1: Take all from the hadoopConf to this hiveConf.
    // This hadoopConf contains user settings in Hadoop's core-site.xml file
    // and Hive's hive-site.xml file. Note, we load hive-site.xml file manually in
    // SharedState and put settings in this hadoopConf instead of relying on HiveConf
    // to load user settings. Otherwise, HiveConf's initialize method will override
    // settings in the hadoopConf. This issue only shows up when spark.sql.hive.metastore.jars
    // is not set to builtin. When spark.sql.hive.metastore.jars is builtin, the classpath
    // has hive-site.xml. So, HiveConf will use that to override its default values.
    // 2: we set all spark confs to this hiveConf.
    // 3: we set all entries in config to this hiveConf.
    (hadoopConf.iterator().asScala.map(kv => kv.getKey -> kv.getValue)
      ++ sparkConf.getAll.toMap ++ extraConfig).foreach { case (k, v) =>
      logDebug(
        s"""
           |Applying Hadoop/Hive/Spark and extra properties to Hive Conf:
           |$k=${if (k.toLowerCase(Locale.ROOT).contains("password")) "xxx" else v}
         """.stripMargin)
      hiveConf.set(k, v)
    }
    // Disable CBO because we removed the Calcite dependency.
    hiveConf.setBoolean("hive.cbo.enable", false)
    val state = new SessionState(hiveConf)
    if (clientLoader.cachedHive != null) {
      Hive.set(clientLoader.cachedHive.asInstanceOf[Hive])
    }
    SessionState.start(state)
    state.out = new PrintStream(outputBuffer, true, "UTF-8")
    state.err = new PrintStream(outputBuffer, true, "UTF-8")
    state
  }

  /** Returns the configuration for the current session. */
  def conf: HiveConf = if (!HiveUtils.isHive23) {
    state.getConf
  } else {
    val hiveConf = state.getConf
    // Hive changed the default of datanucleus.schema.autoCreateAll from true to false
    // and hive.metastore.schema.verification from false to true since Hive 2.0.
    // For details, see the JIRA HIVE-6113, HIVE-12463 and HIVE-1841.
    // isEmbeddedMetaStore should not be true in the production environment.
    // We hard-code hive.metastore.schema.verification and datanucleus.schema.autoCreateAll to allow
    // bin/spark-shell, bin/spark-sql and sbin/start-thriftserver.sh to automatically create the
    // Derby Metastore when running Spark in the non-production environment.
    val isEmbeddedMetaStore = {
      val msUri = hiveConf.getVar(ConfVars.METASTOREURIS)
      val msConnUrl = hiveConf.getVar(ConfVars.METASTORECONNECTURLKEY)
      (msUri == null || msUri.trim().isEmpty) &&
        (msConnUrl != null && msConnUrl.startsWith("jdbc:derby"))
    }
    if (isEmbeddedMetaStore) {
      hiveConf.setBoolean("hive.metastore.schema.verification", false)
      hiveConf.setBoolean("datanucleus.schema.autoCreateAll", true)
    }
    hiveConf
  }

  private val userName = conf.getUser

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

  private def client: Hive = {
    if (clientLoader.cachedHive != null) {
      clientLoader.cachedHive.asInstanceOf[Hive]
    } else {
      val c = Hive.get(conf)
      clientLoader.cachedHive = c
      c
    }
  }

  /** Return the associated Hive [[SessionState]] of this [[HiveClientImpl]] */
  override def getState: SessionState = withHiveState(state)

  /**
   * Runs `f` with ThreadLocal session state and classloaders configured for this version of hive.
   */
  def withHiveState[A](f: => A): A = retryLocked {
    val original = Thread.currentThread().getContextClassLoader
    val originalConfLoader = state.getConf.getClassLoader
    // The classloader in clientLoader could be changed after addJar, always use the latest
    // classloader. We explicitly set the context class loader since "conf.setClassLoader" does
    // not do that, and the Hive client libraries may need to load classes defined by the client's
    // class loader.
    Thread.currentThread().setContextClassLoader(clientLoader.classLoader)
    state.getConf.setClassLoader(clientLoader.classLoader)
    // Set the thread local metastore client to the client associated with this HiveClientImpl.
    Hive.set(client)
    // Replace conf in the thread local Hive with current conf
    Hive.get(conf)
    // setCurrentSessionState will use the classLoader associated
    // with the HiveConf in `state` to override the context class loader of the current
    // thread.
    shim.setCurrentSessionState(state)
    val ret = try f finally {
      state.getConf.setClassLoader(originalConfLoader)
      Thread.currentThread().setContextClassLoader(original)
      HiveCatalogMetrics.incrementHiveClientCalls(1)
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

  private def setCurrentDatabaseRaw(db: String): Unit = {
    if (state.getCurrentDatabase != db) {
      if (databaseExists(db)) {
        state.setCurrentDatabase(db)
      } else {
        throw new NoSuchDatabaseException(db)
      }
    }
  }

  override def setCurrentDatabase(databaseName: String): Unit = withHiveState {
    setCurrentDatabaseRaw(databaseName)
  }

  override def createDatabase(
      database: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = withHiveState {
    client.createDatabase(
      new HiveDatabase(
        database.name,
        database.description,
        CatalogUtils.URIToString(database.locationUri),
        Option(database.properties).map(_.asJava).orNull),
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
        CatalogUtils.URIToString(database.locationUri),
        Option(database.properties).map(_.asJava).orNull))
  }

  override def getDatabase(dbName: String): CatalogDatabase = withHiveState {
    Option(client.getDatabase(dbName)).map { d =>
      CatalogDatabase(
        name = d.getName,
        description = Option(d.getDescription).getOrElse(""),
        locationUri = CatalogUtils.stringToURI(d.getLocationUri),
        properties = Option(d.getParameters).map(_.asScala.toMap).orNull)
    }.getOrElse(throw new NoSuchDatabaseException(dbName))
  }

  override def databaseExists(dbName: String): Boolean = withHiveState {
    client.databaseExists(dbName)
  }

  override def listDatabases(pattern: String): Seq[String] = withHiveState {
    client.getDatabasesByPattern(pattern).asScala
  }

  private def getRawTableOption(dbName: String, tableName: String): Option[HiveTable] = {
    Option(client.getTable(dbName, tableName, false /* do not throw exception */))
  }

  override def tableExists(dbName: String, tableName: String): Boolean = withHiveState {
    getRawTableOption(dbName, tableName).nonEmpty
  }

  override def getTableOption(
      dbName: String,
      tableName: String): Option[CatalogTable] = withHiveState {
    logDebug(s"Looking up $dbName.$tableName")
    getRawTableOption(dbName, tableName).map { h =>
      // Note: Hive separates partition columns and the schema, but for us the
      // partition columns are part of the schema
      val cols = h.getCols.asScala.map(fromHiveColumn)
      val partCols = h.getPartCols.asScala.map(fromHiveColumn)
      val schema = StructType(cols ++ partCols)

      val bucketSpec = if (h.getNumBuckets > 0) {
        val sortColumnOrders = h.getSortCols.asScala
        // Currently Spark only supports columns to be sorted in ascending order
        // but Hive can support both ascending and descending order. If all the columns
        // are sorted in ascending order, only then propagate the sortedness information
        // to downstream processing / optimizations in Spark
        // TODO: In future we can have Spark support columns sorted in descending order
        val allAscendingSorted = sortColumnOrders.forall(_.getOrder == HIVE_COLUMN_ORDER_ASC)

        val sortColumnNames = if (allAscendingSorted) {
          sortColumnOrders.map(_.getCol)
        } else {
          Seq.empty
        }
        Option(BucketSpec(h.getNumBuckets, h.getBucketCols.asScala, sortColumnNames))
      } else {
        None
      }

      // Skew spec and storage handler can't be mapped to CatalogTable (yet)
      val unsupportedFeatures = ArrayBuffer.empty[String]

      if (!h.getSkewedColNames.isEmpty) {
        unsupportedFeatures += "skewed columns"
      }

      if (h.getStorageHandler != null) {
        unsupportedFeatures += "storage handler"
      }

      if (h.getTableType == HiveTableType.VIRTUAL_VIEW && partCols.nonEmpty) {
        unsupportedFeatures += "partitioned view"
      }

      val properties = Option(h.getParameters).map(_.asScala.toMap).orNull

      // Hive-generated Statistics are also recorded in ignoredProperties
      val ignoredProperties = scala.collection.mutable.Map.empty[String, String]
      for (key <- HiveStatisticsProperties; value <- properties.get(key)) {
        ignoredProperties += key -> value
      }

      val excludedTableProperties = HiveStatisticsProperties ++ Set(
        // The property value of "comment" is moved to the dedicated field "comment"
        "comment",
        // For EXTERNAL_TABLE, the table properties has a particular field "EXTERNAL". This is added
        // in the function toHiveTable.
        "EXTERNAL"
      )

      val filteredProperties = properties.filterNot {
        case (key, _) => excludedTableProperties.contains(key)
      }
      val comment = properties.get("comment")

      CatalogTable(
        identifier = TableIdentifier(h.getTableName, Option(h.getDbName)),
        tableType = h.getTableType match {
          case HiveTableType.EXTERNAL_TABLE => CatalogTableType.EXTERNAL
          case HiveTableType.MANAGED_TABLE => CatalogTableType.MANAGED
          case HiveTableType.VIRTUAL_VIEW => CatalogTableType.VIEW
          case unsupportedType =>
            val tableTypeStr = unsupportedType.toString.toLowerCase(Locale.ROOT).replace("_", " ")
            throw new AnalysisException(s"Hive $tableTypeStr is not supported.")
        },
        schema = schema,
        partitionColumnNames = partCols.map(_.name),
        // If the table is written by Spark, we will put bucketing information in table properties,
        // and will always overwrite the bucket spec in hive metastore by the bucketing information
        // in table properties. This means, if we have bucket spec in both hive metastore and
        // table properties, we will trust the one in table properties.
        bucketSpec = bucketSpec,
        owner = Option(h.getOwner).getOrElse(""),
        createTime = h.getTTable.getCreateTime.toLong * 1000,
        lastAccessTime = h.getLastAccessTime.toLong * 1000,
        storage = CatalogStorageFormat(
          locationUri = shim.getDataLocation(h).map(CatalogUtils.stringToURI),
          // To avoid ClassNotFound exception, we try our best to not get the format class, but get
          // the class name directly. However, for non-native tables, there is no interface to get
          // the format class name, so we may still throw ClassNotFound in this case.
          inputFormat = Option(h.getTTable.getSd.getInputFormat).orElse {
            Option(h.getStorageHandler).map(_.getInputFormatClass.getName)
          },
          outputFormat = Option(h.getTTable.getSd.getOutputFormat).orElse {
            Option(h.getStorageHandler).map(_.getOutputFormatClass.getName)
          },
          serde = Option(h.getSerializationLib),
          compressed = h.getTTable.getSd.isCompressed,
          properties = Option(h.getTTable.getSd.getSerdeInfo.getParameters)
            .map(_.asScala.toMap).orNull
        ),
        // For EXTERNAL_TABLE, the table properties has a particular field "EXTERNAL". This is added
        // in the function toHiveTable.
        properties = filteredProperties,
        stats = readHiveStats(properties),
        comment = comment,
        // In older versions of Spark(before 2.2.0), we expand the view original text and
        // store that into `viewExpandedText`, that should be used in view resolution.
        // We get `viewExpandedText` as viewText, and also get `viewOriginalText` in order to
        // display the original view text in `DESC [EXTENDED|FORMATTED] table` command for views
        // that created by older versions of Spark.
        viewOriginalText = Option(h.getViewOriginalText),
        viewText = Option(h.getViewExpandedText),
        unsupportedFeatures = unsupportedFeatures,
        ignoredProperties = ignoredProperties.toMap)
    }
  }

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = withHiveState {
    verifyColumnDataType(table.dataSchema)
    client.createTable(toHiveTable(table, Some(userName)), ignoreIfExists)
  }

  override def dropTable(
      dbName: String,
      tableName: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = withHiveState {
    shim.dropTable(client, dbName, tableName, true, ignoreIfNotExists, purge)
  }

  override def alterTable(
      dbName: String,
      tableName: String,
      table: CatalogTable): Unit = withHiveState {
    // getTableOption removes all the Hive-specific properties. Here, we fill them back to ensure
    // these properties are still available to the others that share the same Hive metastore.
    // If users explicitly alter these Hive-specific properties through ALTER TABLE DDL, we respect
    // these user-specified values.
    verifyColumnDataType(table.dataSchema)
    val hiveTable = toHiveTable(
      table.copy(properties = table.ignoredProperties ++ table.properties), Some(userName))
    // Do not use `table.qualifiedName` here because this may be a rename
    val qualifiedTableName = s"$dbName.$tableName"
    shim.alterTable(client, qualifiedTableName, hiveTable)
  }

  override def alterTableDataSchema(
      dbName: String,
      tableName: String,
      newDataSchema: StructType,
      schemaProps: Map[String, String]): Unit = withHiveState {
    val oldTable = client.getTable(dbName, tableName)
    verifyColumnDataType(newDataSchema)
    val hiveCols = newDataSchema.map(toHiveColumn)
    oldTable.setFields(hiveCols.asJava)

    // remove old schema table properties
    val it = oldTable.getParameters.entrySet.iterator
    while (it.hasNext) {
      val entry = it.next()
      val isSchemaProp = entry.getKey.startsWith(DATASOURCE_SCHEMA_PART_PREFIX) ||
        entry.getKey == DATASOURCE_SCHEMA || entry.getKey == DATASOURCE_SCHEMA_NUMPARTS
      if (isSchemaProp) {
        it.remove()
      }
    }

    // set new schema table properties
    schemaProps.foreach { case (k, v) => oldTable.setProperty(k, v) }

    val qualifiedTableName = s"$dbName.$tableName"
    shim.alterTable(client, qualifiedTableName, oldTable)
  }

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = withHiveState {
    shim.createPartitions(client, db, table, parts, ignoreIfExists)
  }

  override def dropPartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = withHiveState {
    // TODO: figure out how to drop multiple partitions in one call
    val hiveTable = client.getTable(db, table, true /* throw exception */)
    // do the check at first and collect all the matching partitions
    val matchingParts =
      specs.flatMap { s =>
        assert(s.values.forall(_.nonEmpty), s"partition spec '$s' is invalid")
        // The provided spec here can be a partial spec, i.e. it will match all partitions
        // whose specs are supersets of this partial spec. E.g. If a table has partitions
        // (b='1', c='1') and (b='1', c='2'), a partial spec of (b='1') will match both.
        val parts = client.getPartitions(hiveTable, s.asJava).asScala
        if (parts.isEmpty && !ignoreIfNotExists) {
          throw new AnalysisException(
            s"No partition is dropped. One partition spec '$s' does not exist in table '$table' " +
            s"database '$db'")
        }
        parts.map(_.getValues)
      }.distinct
    var droppedParts = ArrayBuffer.empty[java.util.List[String]]
    matchingParts.foreach { partition =>
      try {
        shim.dropPartition(client, db, table, partition, !retainData, purge)
      } catch {
        case e: Exception =>
          val remainingParts = matchingParts.toBuffer -- droppedParts
          logError(
            s"""
               |======================
               |Attempt to drop the partition specs in table '$table' database '$db':
               |${specs.mkString("\n")}
               |In this attempt, the following partitions have been dropped successfully:
               |${droppedParts.mkString("\n")}
               |The remaining partitions have not been dropped:
               |${remainingParts.mkString("\n")}
               |======================
             """.stripMargin)
          throw e
      }
      droppedParts += partition
    }
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = withHiveState {
    require(specs.size == newSpecs.size, "number of old and new partition specs differ")
    val catalogTable = getTable(db, table)
    val hiveTable = toHiveTable(catalogTable, Some(userName))
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
    // Note: Before altering table partitions in Hive, you *must* set the current database
    // to the one that contains the table of interest. Otherwise you will end up with the
    // most helpful error message ever: "Unable to alter partition. alter is not possible."
    // See HIVE-2742 for more detail.
    val original = state.getCurrentDatabase
    try {
      setCurrentDatabaseRaw(db)
      val hiveTable = toHiveTable(getTable(db, table), Some(userName))
      shim.alterPartitions(client, table, newParts.map { toHivePartition(_, hiveTable) }.asJava)
    } finally {
      state.setCurrentDatabase(original)
    }
  }

  /**
   * Returns the partition names for the given table that match the supplied partition spec.
   * If no partition spec is specified, all partitions are returned.
   *
   * The returned sequence is sorted as strings.
   */
  override def getPartitionNames(
      table: CatalogTable,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = withHiveState {
    val hivePartitionNames =
      partialSpec match {
        case None =>
          // -1 for result limit means "no limit/return all"
          client.getPartitionNames(table.database, table.identifier.table, -1)
        case Some(s) =>
          assert(s.values.forall(_.nonEmpty), s"partition spec '$s' is invalid")
          client.getPartitionNames(table.database, table.identifier.table, s.asJava, -1)
      }
    hivePartitionNames.asScala.sorted
  }

  override def getPartitionOption(
      table: CatalogTable,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = withHiveState {
    val hiveTable = toHiveTable(table, Some(userName))
    val hivePartition = client.getPartition(hiveTable, spec.asJava, false)
    Option(hivePartition).map(fromHivePartition)
  }

  /**
   * Returns the partitions for the given table that match the supplied partition spec.
   * If no partition spec is specified, all partitions are returned.
   */
  override def getPartitions(
      table: CatalogTable,
      spec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = withHiveState {
    val hiveTable = toHiveTable(table, Some(userName))
    val partSpec = spec match {
      case None => CatalogTypes.emptyTablePartitionSpec
      case Some(s) =>
        assert(s.values.forall(_.nonEmpty), s"partition spec '$s' is invalid")
        s
    }
    val parts = client.getPartitions(hiveTable, partSpec.asJava).asScala.map(fromHivePartition)
    HiveCatalogMetrics.incrementFetchedPartitions(parts.length)
    parts
  }

  override def getPartitionsByFilter(
      table: CatalogTable,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = withHiveState {
    val hiveTable = toHiveTable(table, Some(userName))
    val parts = shim.getPartitionsByFilter(client, hiveTable, predicates).map(fromHivePartition)
    HiveCatalogMetrics.incrementFetchedPartitions(parts.length)
    parts
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
   * Since upgrading the built-in Hive to 2.3, hive-llap-client is needed when
   * running MapReduce jobs with `runHive`.
   */
  protected def runHive(cmd: String, maxRows: Int = 1000): Seq[String] = withHiveState {
    logDebug(s"Running hiveql '$cmd'")
    if (cmd.toLowerCase(Locale.ROOT).startsWith("set")) { logDebug(s"Changing config: $cmd") }
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
      dbName: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String],
      replace: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = withHiveState {
    val hiveTable = client.getTable(dbName, tableName, true /* throw exception */)
    shim.loadPartition(
      client,
      new Path(loadPath), // TODO: Use URI
      s"$dbName.$tableName",
      partSpec,
      replace,
      inheritTableSpecs,
      isSkewedStoreAsSubdir = hiveTable.isStoredAsSubDirectories,
      isSrcLocal = isSrcLocal)
  }

  def loadTable(
      loadPath: String, // TODO URI
      tableName: String,
      replace: Boolean,
      isSrcLocal: Boolean): Unit = withHiveState {
    shim.loadTable(
      client,
      new Path(loadPath),
      tableName,
      replace,
      isSrcLocal)
  }

  def loadDynamicPartitions(
      loadPath: String,
      dbName: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String],
      replace: Boolean,
      numDP: Int): Unit = withHiveState {
    val hiveTable = client.getTable(dbName, tableName, true /* throw exception */)
    shim.loadDynamicPartitions(
      client,
      new Path(loadPath),
      s"$dbName.$tableName",
      partSpec,
      replace,
      numDP,
      listBucketingEnabled = hiveTable.isStoredAsSubDirectories)
  }

  override def createFunction(db: String, func: CatalogFunction): Unit = withHiveState {
    shim.createFunction(client, db, func)
  }

  override def dropFunction(db: String, name: String): Unit = withHiveState {
    shim.dropFunction(client, db, name)
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = withHiveState {
    shim.renameFunction(client, db, oldName, newName)
  }

  override def alterFunction(db: String, func: CatalogFunction): Unit = withHiveState {
    shim.alterFunction(client, db, func)
  }

  override def getFunctionOption(
      db: String, name: String): Option[CatalogFunction] = withHiveState {
    shim.getFunctionOption(client, db, name)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = withHiveState {
    shim.listFunctions(client, db, pattern)
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
    val allTables = client.getAllTables("default")
    val (mvs, others) = allTables.asScala.map(t => client.getTable("default", t))
      .partition(_.getTableType.toString.equals("MATERIALIZED_VIEW"))

    // Remove materialized view first, otherwise caused a violation of foreign key constraint.
    mvs.foreach { table =>
      val t = table.getTableName
      logDebug(s"Deleting materialized view $t")
      client.dropTable("default", t)
    }

    others.foreach { table =>
      val t = table.getTableName
      logDebug(s"Deleting table $t")
      try {
        client.getIndexes("default", t, 255).asScala.foreach { index =>
          shim.dropIndex(client, "default", t, index.getIndexName)
        }
        if (!table.isIndexTable) {
          client.dropTable("default", t)
        }
      } catch {
        case _: NoSuchMethodError =>
          // HIVE-18448 Hive 3.0 remove index APIs
          client.dropTable("default", t)
      }
    }
    client.getAllDatabases.asScala.filterNot(_ == "default").foreach { db =>
      logDebug(s"Dropping Database: $db")
      client.dropDatabase(db, true, false, true)
    }
  }
}

private[hive] object HiveClientImpl {
  /** Converts the native StructField to Hive's FieldSchema. */
  def toHiveColumn(c: StructField): FieldSchema = {
    val typeString = if (c.metadata.contains(HIVE_TYPE_STRING)) {
      c.metadata.getString(HIVE_TYPE_STRING)
    } else {
      c.dataType.catalogString
    }
    new FieldSchema(c.name, typeString, c.getComment().orNull)
  }

  /** Get the Spark SQL native DataType from Hive's FieldSchema. */
  private def getSparkSQLDataType(hc: FieldSchema): DataType = {
    try {
      CatalystSqlParser.parseDataType(hc.getType)
    } catch {
      case e: ParseException =>
        throw new SparkException("Cannot recognize hive type string: " + hc.getType, e)
    }
  }

  /** Builds the native StructField from Hive's FieldSchema. */
  def fromHiveColumn(hc: FieldSchema): StructField = {
    val columnType = getSparkSQLDataType(hc)
    val metadata = if (hc.getType != columnType.catalogString) {
      new MetadataBuilder().putString(HIVE_TYPE_STRING, hc.getType).build()
    } else {
      Metadata.empty
    }

    val field = StructField(
      name = hc.getName,
      dataType = columnType,
      nullable = true,
      metadata = metadata)
    Option(hc.getComment).map(field.withComment).getOrElse(field)
  }

  private def verifyColumnDataType(schema: StructType): Unit = {
    schema.foreach(col => getSparkSQLDataType(toHiveColumn(col)))
  }

  private def toInputFormat(name: String) =
    Utils.classForName[org.apache.hadoop.mapred.InputFormat[_, _]](name)

  private def toOutputFormat(name: String) =
    Utils.classForName[org.apache.hadoop.hive.ql.io.HiveOutputFormat[_, _]](name)

  /**
   * Converts the native table metadata representation format CatalogTable to Hive's Table.
   */
  def toHiveTable(table: CatalogTable, userName: Option[String] = None): HiveTable = {
    val hiveTable = new HiveTable(table.database, table.identifier.table)
    // For EXTERNAL_TABLE, we also need to set EXTERNAL field in the table properties.
    // Otherwise, Hive metastore will change the table to a MANAGED_TABLE.
    // (metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L1095-L1105)
    hiveTable.setTableType(table.tableType match {
      case CatalogTableType.EXTERNAL =>
        hiveTable.setProperty("EXTERNAL", "TRUE")
        HiveTableType.EXTERNAL_TABLE
      case CatalogTableType.MANAGED =>
        HiveTableType.MANAGED_TABLE
      case CatalogTableType.VIEW => HiveTableType.VIRTUAL_VIEW
      case t =>
        throw new IllegalArgumentException(
          s"Unknown table type is found at toHiveTable: $t")
    })
    // Note: In Hive the schema and partition columns must be disjoint sets
    val (partCols, schema) = table.schema.map(toHiveColumn).partition { c =>
      table.partitionColumnNames.contains(c.getName)
    }
    hiveTable.setFields(schema.asJava)
    hiveTable.setPartCols(partCols.asJava)
    userName.foreach(hiveTable.setOwner)
    hiveTable.setCreateTime(MILLISECONDS.toSeconds(table.createTime).toInt)
    hiveTable.setLastAccessTime(MILLISECONDS.toSeconds(table.lastAccessTime).toInt)
    table.storage.locationUri.map(CatalogUtils.URIToString).foreach { loc =>
      hiveTable.getTTable.getSd.setLocation(loc)}
    table.storage.inputFormat.map(toInputFormat).foreach(hiveTable.setInputFormatClass)
    table.storage.outputFormat.map(toOutputFormat).foreach(hiveTable.setOutputFormatClass)
    hiveTable.setSerializationLib(
      table.storage.serde.getOrElse("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    table.storage.properties.foreach { case (k, v) => hiveTable.setSerdeParam(k, v) }
    table.properties.foreach { case (k, v) => hiveTable.setProperty(k, v) }
    table.comment.foreach { c => hiveTable.setProperty("comment", c) }
    // Hive will expand the view text, so it needs 2 fields: viewOriginalText and viewExpandedText.
    // Since we don't expand the view text, but only add table properties, we map the `viewText` to
    // the both fields in hive table.
    table.viewText.foreach { t =>
      hiveTable.setViewOriginalText(t)
      hiveTable.setViewExpandedText(t)
    }

    table.bucketSpec match {
      case Some(bucketSpec) if DDLUtils.isHiveTable(table) =>
        hiveTable.setNumBuckets(bucketSpec.numBuckets)
        hiveTable.setBucketCols(bucketSpec.bucketColumnNames.toList.asJava)

        if (bucketSpec.sortColumnNames.nonEmpty) {
          hiveTable.setSortCols(
            bucketSpec.sortColumnNames
              .map(col => new Order(col, HIVE_COLUMN_ORDER_ASC))
              .toList
              .asJava
          )
        }
      case _ =>
    }

    hiveTable
  }

  /**
   * Converts the native partition metadata representation format CatalogTablePartition to
   * Hive's Partition.
   */
  def toHivePartition(
      p: CatalogTablePartition,
      ht: HiveTable): HivePartition = {
    val tpart = new org.apache.hadoop.hive.metastore.api.Partition
    val partValues = ht.getPartCols.asScala.map { hc =>
      p.spec.getOrElse(hc.getName, throw new IllegalArgumentException(
        s"Partition spec is missing a value for column '${hc.getName}': ${p.spec}"))
    }
    val storageDesc = new StorageDescriptor
    val serdeInfo = new SerDeInfo
    p.storage.locationUri.map(CatalogUtils.URIToString(_)).foreach(storageDesc.setLocation)
    p.storage.inputFormat.foreach(storageDesc.setInputFormat)
    p.storage.outputFormat.foreach(storageDesc.setOutputFormat)
    p.storage.serde.foreach(serdeInfo.setSerializationLib)
    serdeInfo.setParameters(p.storage.properties.asJava)
    storageDesc.setSerdeInfo(serdeInfo)
    tpart.setDbName(ht.getDbName)
    tpart.setTableName(ht.getTableName)
    tpart.setValues(partValues.asJava)
    tpart.setSd(storageDesc)
    tpart.setCreateTime(MILLISECONDS.toSeconds(p.createTime).toInt)
    tpart.setLastAccessTime(MILLISECONDS.toSeconds(p.lastAccessTime).toInt)
    tpart.setParameters(mutable.Map(p.parameters.toSeq: _*).asJava)
    new HivePartition(ht, tpart)
  }

  /**
   * Build the native partition metadata from Hive's Partition.
   */
  def fromHivePartition(hp: HivePartition): CatalogTablePartition = {
    val apiPartition = hp.getTPartition
    val properties: Map[String, String] = if (hp.getParameters != null) {
      hp.getParameters.asScala.toMap
    } else {
      Map.empty
    }
    CatalogTablePartition(
      spec = Option(hp.getSpec).map(_.asScala.toMap).getOrElse(Map.empty),
      storage = CatalogStorageFormat(
        locationUri = Option(CatalogUtils.stringToURI(apiPartition.getSd.getLocation)),
        inputFormat = Option(apiPartition.getSd.getInputFormat),
        outputFormat = Option(apiPartition.getSd.getOutputFormat),
        serde = Option(apiPartition.getSd.getSerdeInfo.getSerializationLib),
        compressed = apiPartition.getSd.isCompressed,
        properties = Option(apiPartition.getSd.getSerdeInfo.getParameters)
          .map(_.asScala.toMap).orNull),
      createTime = apiPartition.getCreateTime.toLong * 1000,
      lastAccessTime = apiPartition.getLastAccessTime.toLong * 1000,
      parameters = properties,
      stats = readHiveStats(properties))
  }

  /**
   * Reads statistics from Hive.
   * Note that this statistics could be overridden by Spark's statistics if that's available.
   */
  private def readHiveStats(properties: Map[String, String]): Option[CatalogStatistics] = {
    val totalSize = properties.get(StatsSetupConst.TOTAL_SIZE).map(BigInt(_))
    val rawDataSize = properties.get(StatsSetupConst.RAW_DATA_SIZE).map(BigInt(_))
    val rowCount = properties.get(StatsSetupConst.ROW_COUNT).map(BigInt(_))
    // NOTE: getting `totalSize` directly from params is kind of hacky, but this should be
    // relatively cheap if parameters for the table are populated into the metastore.
    // Currently, only totalSize, rawDataSize, and rowCount are used to build the field `stats`
    // TODO: stats should include all the other two fields (`numFiles` and `numPartitions`).
    // (see StatsSetupConst in Hive)

    // When table is external, `totalSize` is always zero, which will influence join strategy.
    // So when `totalSize` is zero, use `rawDataSize` instead. When `rawDataSize` is also zero,
    // return None.
    // In Hive, when statistics gathering is disabled, `rawDataSize` and `numRows` is always
    // zero after INSERT command. So they are used here only if they are larger than zero.
    if (totalSize.isDefined && totalSize.get > 0L) {
      Some(CatalogStatistics(sizeInBytes = totalSize.get, rowCount = rowCount.filter(_ > 0)))
    } else if (rawDataSize.isDefined && rawDataSize.get > 0) {
      Some(CatalogStatistics(sizeInBytes = rawDataSize.get, rowCount = rowCount.filter(_ > 0)))
    } else {
      // TODO: still fill the rowCount even if sizeInBytes is empty. Might break anything?
      None
    }
  }

  // Below is the key of table properties for storing Hive-generated statistics
  private val HiveStatisticsProperties = Set(
    StatsSetupConst.COLUMN_STATS_ACCURATE,
    StatsSetupConst.NUM_FILES,
    StatsSetupConst.NUM_PARTITIONS,
    StatsSetupConst.ROW_COUNT,
    StatsSetupConst.RAW_DATA_SIZE,
    StatsSetupConst.TOTAL_SIZE
  )
}
