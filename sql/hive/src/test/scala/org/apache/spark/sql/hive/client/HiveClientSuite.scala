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

import java.io.{ByteArrayOutputStream, File, PrintStream, PrintWriter}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{DatabaseAlreadyExistsException, NoSuchDatabaseException, PartitionsAlreadyExistException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.test.TestHiveVersion
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.tags.SlowHiveTest
import org.apache.spark.util.{MutableURLClassLoader, Utils}

@SlowHiveTest
class HiveClientSuite(version: String) extends HiveVersionSuite(version) {

  private var versionSpark: TestHiveVersion = null

  private val emptyDir = Utils.createTempDir().getCanonicalPath

  private val ver = IsolatedClientLoader.hiveVersion(version)

  override def beforeAll(): Unit = {
    super.beforeAll()
    System.gc() // Hack to avoid SEGV on some JVM versions.
    val hadoopConf = new Configuration()
    hadoopConf.set("test", "success")
    client = buildClient(hadoopConf)
    versionSpark = TestHiveVersion(client)
  }

  override def afterAll(): Unit = {
    try {
      if (versionSpark != null) {
        versionSpark.reset()
        versionSpark = null
      }
      client = null
    } finally {
      super.afterAll()
    }
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        versionSpark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  test("create client") {
    assert(client != null, "client should be initialized in beforeAll")
    assert(versionSpark != null, "versionSpark should be initialized in beforeAll")
    assert(versionSpark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog]
      .client.version.fullVersion.startsWith(version))
  }

  def table(database: String, tableName: String,
      collation: Option[String] = None,
      tableType: CatalogTableType = CatalogTableType.MANAGED): CatalogTable = {
    CatalogTable(
      identifier = TableIdentifier(tableName, Some(database)),
      tableType = tableType,
      schema = new StructType().add("key", "int"),
      collation = collation,
      storage = CatalogStorageFormat(
        locationUri = None,
        inputFormat = Some(classOf[TextInputFormat].getName),
        outputFormat = Some(classOf[HiveIgnoreKeyTextOutputFormat[_, _]].getName),
        serde = Some(classOf[LazySimpleSerDe].getName),
        compressed = false,
        properties = Map.empty
      ))
  }

  ///////////////////////////////////////////////////////////////////////////
  // Database related API
  ///////////////////////////////////////////////////////////////////////////

  private val tempDatabasePath = Utils.createTempDir().toURI

  test("createDatabase") {
    val defaultDB = CatalogDatabase("default", "desc", new URI("loc"), Map())
    client.createDatabase(defaultDB, ignoreIfExists = true)
    val tempDB = CatalogDatabase(
      "temporary", description = "test create", tempDatabasePath, Map())
    client.createDatabase(tempDB, ignoreIfExists = true)

    intercept[DatabaseAlreadyExistsException] {
      client.createDatabase(tempDB, ignoreIfExists = false)
    }
  }

  test("create/get/alter database should pick right user name as owner") {
    val currentUser = UserGroupInformation.getCurrentUser.getUserName
    val ownerName = "SPARK_29425"
    val db1 = "SPARK_29425_1"
    val db2 = "SPARK_29425_2"
    val ownerProps = Map("owner" -> ownerName)

    // create database with owner
    val dbWithOwner = CatalogDatabase(db1, "desc", Utils.createTempDir().toURI, ownerProps)
    client.createDatabase(dbWithOwner, ignoreIfExists = true)
    val getDbWithOwner = client.getDatabase(db1)
    assert(getDbWithOwner.properties("owner") === ownerName)
    // alter database without owner
    client.alterDatabase(getDbWithOwner.copy(properties = Map()))
    assert(client.getDatabase(db1).properties("owner") === "")

    // create database without owner
    val dbWithoutOwner = CatalogDatabase(db2, "desc", Utils.createTempDir().toURI, Map())
    client.createDatabase(dbWithoutOwner, ignoreIfExists = true)
    val getDbWithoutOwner = client.getDatabase(db2)
    assert(getDbWithoutOwner.properties("owner") === currentUser)
    // alter database with owner
    client.alterDatabase(getDbWithoutOwner.copy(properties = ownerProps))
    assert(client.getDatabase(db2).properties("owner") === ownerName)
  }

  test("createDatabase with null description") {
    withTempDir { tmpDir =>
      val dbWithNullDesc =
        CatalogDatabase("dbWithNullDesc", description = null, tmpDir.toURI, Map())
      client.createDatabase(dbWithNullDesc, ignoreIfExists = true)
      assert(client.getDatabase("dbWithNullDesc").description == "")
    }
  }

  test("setCurrentDatabase") {
    client.setCurrentDatabase("default")
  }

  test("getDatabase") {
    // No exception should be thrown
    client.getDatabase("default")
    intercept[NoSuchDatabaseException](client.getDatabase("nonexist"))
  }

  test("databaseExists") {
    assert(client.databaseExists("default"))
    assert(!client.databaseExists("nonexist"))
  }

  test("listDatabases") {
    assert(client.listDatabases("defau.*") == Seq("default"))
  }

  test("alterDatabase") {
    val database = client.getDatabase("temporary").copy(properties = Map("flag" -> "true"))
    client.alterDatabase(database)
    assert(client.getDatabase("temporary").properties.contains("flag"))

    // test alter database location
    val tempDatabasePath2 = Utils.createTempDir().toURI
    // Hive support altering database location since HIVE-8472.
    if (ver.compare(hive.v3_0) >= 0) {
      client.alterDatabase(database.copy(locationUri = tempDatabasePath2))
      val uriInCatalog = client.getDatabase("temporary").locationUri
      assert("file" === uriInCatalog.getScheme)
      assert(new Path(tempDatabasePath2.getPath).toUri.getPath === uriInCatalog.getPath,
        "Failed to alter database location")
    } else {
      val e = intercept[AnalysisException] {
        client.alterDatabase(database.copy(locationUri = tempDatabasePath2))
      }
      assert(e.getMessage.contains("does not support altering database location"))
    }
  }

  test("dropDatabase") {
    assert(client.databaseExists("temporary"))

    client.createTable(table("temporary", tableName = "tbl"), ignoreIfExists = false)
    val ex = intercept[AnalysisException] {
      client.dropDatabase("temporary", ignoreIfNotExists = false, cascade = false)
      assert(false, "dropDatabase should throw HiveException")
    }
    checkError(ex,
      condition = "SCHEMA_NOT_EMPTY",
      parameters = Map("schemaName" -> "`temporary`"))

    client.dropDatabase("temporary", ignoreIfNotExists = false, cascade = true)
    assert(!client.databaseExists("temporary"))
  }

  ///////////////////////////////////////////////////////////////////////////
  // Table related API
  ///////////////////////////////////////////////////////////////////////////

  test("createTable") {
    client.createTable(table("default", tableName = "src"), ignoreIfExists = false)
    client.createTable(table("default", tableName = "temporary"), ignoreIfExists = false)
    client.createTable(table("default", tableName = "view1", tableType = CatalogTableType.VIEW),
      ignoreIfExists = false)
  }

  test("create/alter table with collations") {
    client.createTable(table("default", tableName = "collation_table",
      collation = Some("UNICODE")), ignoreIfExists = false)

    val readBack = client.getTable("default", "collation_table")
    assert(!readBack.properties.contains(TableCatalog.PROP_COLLATION))
    assert(readBack.collation === Some("UNICODE"))

    client.alterTable("default", "collation_table",
      readBack.copy(collation = Some("UNICODE_CI")))
    val alteredTbl = client.getTable("default", "collation_table")
    assert(alteredTbl.collation === Some("UNICODE_CI"))

    client.dropTable("default", "collation_table", ignoreIfNotExists = true, purge = true)
  }

  test("loadTable") {
    client.loadTable(
      emptyDir,
      tableName = "src",
      replace = false,
      isSrcLocal = false)
  }

  test("tableExists") {
    // No exception should be thrown
    assert(client.tableExists("default", "src"))
    assert(!client.tableExists("default", "nonexistent"))
  }

  test("getTable") {
    // No exception should be thrown
    client.getTable("default", "src")
  }

  test("getTableOption") {
    assert(client.getTableOption("default", "src").isDefined)
  }

  test("getTablesByName") {
    assert(client.getTablesByName("default", Seq("src")).head
      == client.getTableOption("default", "src").get)
  }

  test("getTablesByName when multiple tables") {
    assert(client.getTablesByName("default", Seq("src", "temporary"))
      .map(_.identifier.table) == Seq("src", "temporary"))
  }

  test("getTablesByName when some tables do not exist") {
    assert(client.getTablesByName("default", Seq("src", "notexist"))
      .map(_.identifier.table) == Seq("src"))
  }

  test("getTablesByName when contains invalid name") {
    // scalastyle:off
    val name = "砖"
    // scalastyle:on
    assert(client.getTablesByName("default", Seq("src", name))
      .map(_.identifier.table) == Seq("src"))
  }

  test("getTablesByName when empty") {
    assert(client.getTablesByName("default", Seq.empty).isEmpty)
  }

  test("alterTable(table: CatalogTable)") {
    val newTable = client.getTable("default", "src").copy(properties = Map("changed" -> ""))
    client.alterTable(newTable)
    assert(client.getTable("default", "src").properties.contains("changed"))
  }

  test("alterTable - should respect the original catalog table's owner name") {
    val ownerName = "SPARK-29405"
    val originalTable = client.getTable("default", "src")
    // mocking the owner is what we declared
    val newTable = originalTable.copy(owner = ownerName)
    client.alterTable(newTable)
    assert(client.getTable("default", "src").owner === ownerName)
    // mocking the owner is empty
    val newTable2 = originalTable.copy(owner = "")
    client.alterTable(newTable2)
    assert(client.getTable("default", "src").owner === client.userName)
  }

  test("alterTable(dbName: String, tableName: String, table: CatalogTable)") {
    val newTable = client.getTable("default", "src").copy(properties = Map("changedAgain" -> ""))
    client.alterTable("default", "src", newTable)
    assert(client.getTable("default", "src").properties.contains("changedAgain"))
  }

  test("alterTable - rename") {
    val newTable = client.getTable("default", "src")
      .copy(identifier = TableIdentifier("tgt", database = Some("default")))
    assert(!client.tableExists("default", "tgt"))

    client.alterTable("default", "src", newTable)

    assert(client.tableExists("default", "tgt"))
    assert(!client.tableExists("default", "src"))
  }

  test("alterTable - change database") {
    val tempDB = CatalogDatabase(
      "temporary", description = "test create", tempDatabasePath, Map())
    client.createDatabase(tempDB, ignoreIfExists = true)

    val newTable = client.getTable("default", "tgt")
      .copy(identifier = TableIdentifier("tgt", database = Some("temporary")))
    assert(!client.tableExists("temporary", "tgt"))

    client.alterTable("default", "tgt", newTable)

    assert(client.tableExists("temporary", "tgt"))
    assert(!client.tableExists("default", "tgt"))
  }

  test("alterTable - change database and table names") {
    val newTable = client.getTable("temporary", "tgt")
      .copy(identifier = TableIdentifier("src", database = Some("default")))
    assert(!client.tableExists("default", "src"))

    client.alterTable("temporary", "tgt", newTable)

    assert(client.tableExists("default", "src"))
    assert(!client.tableExists("temporary", "tgt"))
  }

  test("listTables(database)") {
    assert((client.listTables("default") diff Seq("src", "temporary", "view1")) === Nil)
  }

  test("listTables(database, pattern)") {
    assert(client.listTables("default", pattern = "src") === Seq("src"))
    assert(client.listTables("default", pattern = "nonexist").isEmpty)
  }

  test("listTablesByType(database, pattern, tableType)") {
    assert(client.listTablesByType("default", pattern = "view1",
      CatalogTableType.VIEW) === Seq("view1"))
    assert(client.listTablesByType("default", pattern = "nonexist",
      CatalogTableType.VIEW).isEmpty)
  }

  test("dropTable") {
    client.dropTable("default", tableName = "temporary", ignoreIfNotExists = false,
      purge = true)
    client.dropTable("default", tableName = "view1", ignoreIfNotExists = false,
      purge = true)
    assert(client.listTables("default") === Seq("src"))
  }

  ///////////////////////////////////////////////////////////////////////////
  // Partition related API
  ///////////////////////////////////////////////////////////////////////////

  private val storageFormat = CatalogStorageFormat(
    locationUri = None,
    inputFormat = None,
    outputFormat = None,
    serde = None,
    compressed = false,
    properties = Map.empty)

  test("sql create partitioned table") {
    val table = CatalogTable(
      identifier = TableIdentifier("src_part", Some("default")),
      tableType = CatalogTableType.MANAGED,
      schema = new StructType().add("value", "int").add("key1", "int").add("key2", "int"),
      partitionColumnNames = Seq("key1", "key2"),
      storage = CatalogStorageFormat(
        locationUri = None,
        inputFormat = Some(classOf[TextInputFormat].getName),
        outputFormat = Some(classOf[HiveIgnoreKeyTextOutputFormat[_, _]].getName),
        serde = Some(classOf[LazySimpleSerDe].getName),
        compressed = false,
        properties = Map.empty
      ))
    client.createTable(table, ignoreIfExists = false)
  }

  val testPartitionCount = 2

  test("createPartitions") {
    val partitions = (1 to testPartitionCount).map { key2 =>
      CatalogTablePartition(Map("key1" -> "1", "key2" -> key2.toString), storageFormat)
    }
    client.createPartitions(
      client.getTable("default", "src_part"), partitions, ignoreIfExists = true)
  }

  test("getPartitionNames(catalogTable)") {
    val partitionNames = (1 to testPartitionCount).map(key2 => s"key1=1/key2=$key2")
    assert(partitionNames == client.getPartitionNames(client.getTable("default", "src_part")))
  }

  test("getPartitions(db, table, spec)") {
    assert(testPartitionCount ==
      client.getPartitions("default", "src_part", None).size)
  }

  test("getPartitionLocations(db, table, spec)") {
    val partitions = client.getPartitionLocations("default", "src_part", None)
    assert(testPartitionCount == partitions.size)
    assert(partitions.forall(_.locationUri.isDefined))

    val prefix = client.getPartitionLocations(
      "default", "src_part", Some(Map("key1" -> "1")))
    assert(testPartitionCount == prefix.size)
    assert(prefix.forall(_.locationUri.isDefined))

    val partial = client.getPartitionLocations(
      "default", "src_part", Some(Map("key2" -> "1")))
    assert(partial.size == 1)
    assert(partial.head.spec == Map("key1" -> "1", "key2" -> "1"))
    assert(partial.head.locationUri.isDefined)

    assert(client.getPartitionLocations("default", "src", None).isEmpty)
  }

  test("getPartitionLocations returns the same specs and locations as getPartitions") {
    // Shims below 4.0 serve getPartitionLocations from the full-fetch path, while
    // Shim_v4_0 and above fulfill it with HMS partition projection. Running per version,
    // this pins the projected specs and locations to the classic full-fetch results.
    Seq(None, Some(Map("key1" -> "1")), Some(Map("key2" -> "1")),
      Some(Map("key1" -> "1", "key2" -> "1")), Some(Map("key2" -> "unknown"))).foreach { spec =>
      val full = client.getPartitions("default", "src_part", spec)
        .map(p => p.spec -> p.storage.locationUri).toSet
      val light = client.getPartitionLocations("default", "src_part", spec)
        .map(p => p.spec -> p.locationUri).toSet
      assert(light == full,
        s"mismatch between getPartitionLocations and getPartitions with spec: $spec")
    }
  }

  test("getPartitionsByFilter") {
    // Only one partition [1, 1] for key2 == 1
    val result = client.getPartitionsByFilter(client.getRawHiveTable("default", "src_part"),
      Seq(EqualTo(AttributeReference("key2", IntegerType)(), Literal(1))))
    assert(result.size == 1)
  }

  test("getPartition") {
    // No exception should be thrown
    client.getPartition("default", "src_part", Map("key1" -> "1", "key2" -> "2"))
  }

  test("getPartitionOption(db: String, table: String, spec: TablePartitionSpec)") {
    val partition = client.getPartitionOption(
      "default", "src_part", Map("key1" -> "1", "key2" -> "2"))
    assert(partition.isDefined)
  }

  test("getPartitionOption(table: CatalogTable, spec: TablePartitionSpec)") {
    val partition = client.getPartitionOption(
      client.getRawHiveTable("default", "src_part"), Map("key1" -> "1", "key2" -> "2"))
    assert(partition.isDefined)
  }

  test("getPartitions(db: String, table: String)") {
    assert(testPartitionCount == client.getPartitions("default", "src_part", None).size)
  }

  test("loadPartition") {
    val partSpec = new java.util.LinkedHashMap[String, String]
    partSpec.put("key1", "1")
    partSpec.put("key2", "2")

    client.loadPartition(
      emptyDir,
      "default",
      "src_part",
      partSpec,
      replace = false,
      inheritTableSpecs = false,
      isSrcLocal = false)
  }

  test("loadDynamicPartitions") {
    val partSpec = new java.util.LinkedHashMap[String, String]
    partSpec.put("key1", "1")
    partSpec.put("key2", "") // Dynamic partition

    client.loadDynamicPartitions(
      emptyDir,
      "default",
      "src_part",
      partSpec,
      replace = false,
      numDP = 1)
  }

  test("renamePartitions") {
    val oldSpec = Map("key1" -> "1", "key2" -> "1")
    val newSpec = Map("key1" -> "1", "key2" -> "3")
    client.renamePartitions("default", "src_part", Seq(oldSpec), Seq(newSpec))

    // Checks the existence of the new partition (key1 = 1, key2 = 3)
    assert(client.getPartitionOption("default", "src_part", newSpec).isDefined)
  }

  test("alterPartitions") {
    val spec = Map("key1" -> "1", "key2" -> "2")
    val parameters = Map(StatsSetupConst.TOTAL_SIZE -> "0", StatsSetupConst.NUM_FILES -> "1")
    val newLocation = new URI(Utils.createTempDir().toURI.toString.stripSuffix("/"))
    val storage = storageFormat.copy(locationUri = Some(newLocation))
    val partition = CatalogTablePartition(spec, storage, parameters)
    client.alterPartitions("default", "src_part", Seq(partition))
    assert(client.getPartition("default", "src_part", spec)
      .storage.locationUri.contains(newLocation))
    assert(client.getPartition("default", "src_part", spec)
      .parameters.get(StatsSetupConst.TOTAL_SIZE).contains("0"))

    val customPartitions = client.getPartitionLocations("default", "src_part", Some(spec))
    assert(customPartitions.length == 1)
    assert(customPartitions.head.locationUri.contains(newLocation))
  }

  test("dropPartitions") {
    val spec = Map("key1" -> "1", "key2" -> "3")
    client.dropPartitions("default", "src_part", Seq(spec), ignoreIfNotExists = true,
      purge = true, retainData = false)
    assert(client.getPartitionOption("default", "src_part", spec).isEmpty)
  }

  test("createPartitions if already exists") {
    val partitions = Seq(CatalogTablePartition(
      Map("key1" -> "101", "key2" -> "102"),
      storageFormat))
    val table = client.getTable("default", "src_part")

    try {
      client.createPartitions(table, partitions, ignoreIfExists = false)
      val e = intercept[PartitionsAlreadyExistException] {
        client.createPartitions(table, partitions, ignoreIfExists = false)
      }
      checkError(e,
        condition = "PARTITIONS_ALREADY_EXIST",
        parameters = Map("partitionList" -> "PARTITION (`key1` = 101, `key2` = 102)",
          "tableName" -> "`default`.`src_part`"))
    } finally {
      client.dropPartitions(
        "default",
        "src_part",
        partitions.map(_.spec),
        ignoreIfNotExists = true,
        purge = false,
        retainData = false)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Function related API
  ///////////////////////////////////////////////////////////////////////////

  def function(name: String, className: String): CatalogFunction = {
    CatalogFunction(
      FunctionIdentifier(name, Some("default")), className, Seq.empty[FunctionResource])
  }

  test("createFunction") {
    val functionClass = "org.apache.spark.MyFunc1"
    client.createFunction("default", function("func1", functionClass))
  }

  test("functionExists") {
    assert(client.functionExists("default", "func1"))
  }

  test("renameFunction") {
    client.renameFunction("default", "func1", "func2")
    assert(client.functionExists("default", "func2"))
  }

  test("alterFunction") {
    val functionClass = "org.apache.spark.MyFunc2"
    client.alterFunction("default", function("func2", functionClass))
  }

  test("getFunction") {
    // No exception should be thrown
    val func = client.getFunction("default", "func2")
    assert(func.className == "org.apache.spark.MyFunc2")
  }

  test("getFunctionOption") {
    assert(client.getFunctionOption("default", "func2").isDefined)
    assert(client.getFunctionOption("default", "the_func_not_exists").isEmpty)
  }

  test("listFunctions") {
    assert(client.listFunctions("default", "fun.*").size == 1)
  }

  test("dropFunction") {
    // No exception should be thrown
    client.dropFunction("default", "func2")
    assert(client.listFunctions("default", "fun.*").isEmpty)
  }

  ///////////////////////////////////////////////////////////////////////////
  // SQL related API
  ///////////////////////////////////////////////////////////////////////////

  test("sql set command") {
    client.runSqlHive("SET spark.sql.test.key=1")
  }

  test("sql create index and reset") {
    // HIVE-18448 Since Hive 3.0, INDEX is not supported.
    if (ver.compare(hive.v3_0) < 0) {
      client.runSqlHive("CREATE TABLE indexed_table (key INT)")
      client.runSqlHive("CREATE INDEX index_1 ON TABLE indexed_table(key) " +
        "as 'COMPACT' WITH DEFERRED REBUILD")
    }
  }

  test("sql read hive materialized view") {
    // HIVE-14249 Since Hive 2.3.0, materialized view is supported.
    // Since Hive 3.0(HIVE-19383), we can not run local MR by `client.runSqlHive` with JDK 11.
    if (version == "2.3") {
      // Since HIVE-18394(Hive 3.1), "Create Materialized View" should default to rewritable ones
      client.runSqlHive("CREATE TABLE materialized_view_tbl (c1 INT)")
      client.runSqlHive(
        s"CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM materialized_view_tbl")
      checkError(
        exception = intercept[AnalysisException] {
          versionSpark.table("mv1").collect()
        },
        condition = "UNSUPPORTED_FEATURE.HIVE_TABLE_TYPE",
        parameters = Map(
          "tableName" -> "`mv1`",
          "tableType" -> "materialized view"
        )
      )
    }
  }

  test("read table written by Hive") {
    // Hive 3.0 and 3.1 don't work with JDK 11+ (HIVE-22097)
    if (ver != hive.v3_0 && ver != hive.v3_1) {
      withTable("test_tbl") {
        client.runSqlHive("CREATE TABLE test_tbl AS SELECT 1")
        assert(versionSpark.sql("SELECT * from test_tbl").collect() === Array(Row(1)))
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Miscellaneous API
  ///////////////////////////////////////////////////////////////////////////

  test("version") {
    assert(client.version.fullVersion.startsWith(version))
  }

  test("getConf") {
    assert("success" === client.getConf("test", null))
  }

  test("setOut") {
    client.setOut(new PrintStream(new ByteArrayOutputStream()))
  }

  test("setInfo") {
    client.setInfo(new PrintStream(new ByteArrayOutputStream()))
  }

  test("setError") {
    client.setError(new PrintStream(new ByteArrayOutputStream()))
  }

  test("newSession") {
    val newClient = client.newSession()
    assert(newClient != null)
  }

  test("withHiveState and addJar") {
    val newClassPath = "."
    client.addJar(newClassPath)
    client.withHiveState {
      // No exception should be thrown.
      // withHiveState changes the classloader to MutableURLClassLoader
      val classLoader = Thread.currentThread().getContextClassLoader
        .asInstanceOf[MutableURLClassLoader]

      val urls = classLoader.getURLs
      urls.contains(new File(newClassPath).toURI.toURL)
    }
  }

  test("reset") {
    // Clears all database, tables, functions...
    client.reset()
    assert(client.listTables("default").isEmpty)
  }

  ///////////////////////////////////////////////////////////////////////////
  // End-To-End tests
  ///////////////////////////////////////////////////////////////////////////

  test("CREATE TABLE AS SELECT") {
    withTable("tbl") {
      versionSpark.sql("CREATE TABLE tbl AS SELECT 1 AS a")
      assert(versionSpark.table("tbl").collect().toSeq == Seq(Row(1)))
      val tableMeta = versionSpark.sessionState.catalog.getTableMetadata(TableIdentifier("tbl"))
      val totalSize = tableMeta.stats.map(_.sizeInBytes)
      assert(totalSize.nonEmpty && totalSize.get > 0)
    }
  }

  test("CREATE Partitioned TABLE AS SELECT") {
    withTable("tbl") {
      versionSpark.sql(
        """
          |CREATE TABLE tbl(c1 string)
          |USING hive
          |PARTITIONED BY (ds STRING)
          """.stripMargin)
      versionSpark.sql("INSERT OVERWRITE TABLE tbl partition (ds='2') SELECT '1'")

      assert(versionSpark.table("tbl").collect().toSeq == Seq(Row("1", "2")))
      val partMeta = versionSpark.sessionState.catalog.getPartition(
        TableIdentifier("tbl"), spec = Map("ds" -> "2")).parameters
      val totalSize = partMeta.get(StatsSetupConst.TOTAL_SIZE).map(_.toLong)
      val numFiles = partMeta.get(StatsSetupConst.NUM_FILES).map(_.toLong)
      assert(totalSize.nonEmpty && numFiles.nonEmpty)

      versionSpark.sql(
        """
          |ALTER TABLE tbl PARTITION (ds='2')
          |SET SERDEPROPERTIES ('newKey' = 'vvv')
          """.stripMargin)
      val newPartMeta = versionSpark.sessionState.catalog.getPartition(
        TableIdentifier("tbl"), spec = Map("ds" -> "2")).parameters

      val newTotalSize = newPartMeta.get(StatsSetupConst.TOTAL_SIZE).map(_.toLong)
      val newNumFiles = newPartMeta.get(StatsSetupConst.NUM_FILES).map(_.toLong)
      assert(newTotalSize.nonEmpty && newNumFiles.nonEmpty)
    }
  }

  test("INSERT OVERWRITE a partition with a custom location uses the location reported by " +
      "getPartitionLocations") {
    // Only Shim_v4_0 (and its 4.1/4.2 descendants) implement partition projection via
    // getPartitionsWithSpecs; earlier shims fall back to the plain getPartitions-based
    // implementation, which this test is not meant to exercise.
    withTable("custom_loc_tbl") {
      withTempDir { partDir =>
        versionSpark.sql(
          "CREATE TABLE custom_loc_tbl(c1 int, p1 int) USING PARQUET PARTITIONED BY (p1)")
        versionSpark.sql("INSERT INTO custom_loc_tbl PARTITION (p1 = 1) VALUES (10)")
        versionSpark.sql(
          s"ALTER TABLE custom_loc_tbl PARTITION (p1 = 1) SET LOCATION '${partDir.toURI}'")

        val parts = versionSpark.sessionState.catalog.listPartitionLocations(
          TableIdentifier("custom_loc_tbl"))
        assert(parts.length == 1)
        assert(parts.head.locationUri.map(_.toString.stripSuffix("/")) ==
          Some(partDir.toURI.toString.stripSuffix("/")))

        // This overwrite must resolve the partition's custom location via
        // getCustomPartitionLocations, which depends on Shim_v4_0.getPartitionLocations
        // correctly reporting the post-ALTER location rather than the table's default path.
        versionSpark.sql("INSERT OVERWRITE TABLE custom_loc_tbl PARTITION (p1 = 1) VALUES (20)")
        assert(versionSpark.table("custom_loc_tbl").collect().toSeq == Seq(Row(20, 1)))
        assert(partDir.listFiles().exists(_.getName.startsWith("part-")))

        // A second overwrite must delete-and-rewrite the same custom directory again, proving
        // the custom location is resolved correctly on every call, not just the first one.
        versionSpark.sql("INSERT OVERWRITE TABLE custom_loc_tbl PARTITION (p1 = 1) VALUES (30)")
        assert(versionSpark.table("custom_loc_tbl").collect().toSeq == Seq(Row(30, 1)))
        assert(partDir.listFiles().count(_.getName.startsWith("part-")) == 1)
      }
    }
  }

  test("getPartitionLocations resolves a partition location that shares a string prefix " +
      "with the table's own location") {
    // Guards against a bug in Shim_v4_0.getPartitionLocations (and, previously, getPartitions):
    // the metastore's MetaStoreServerUtils.getPartitionspecsGroupedByStorageDescriptor groups a
    // partition into a PartitionSpecWithSharedSD whenever its location has the table's own
    // location as a *string* prefix -- a plain String.startsWith check, not path-boundary aware.
    // A sibling directory like "<tableLocation>_external" matches that check even though it is
    // not actually nested under the table, so the metastore encodes it as a relativePath with no
    // leading separator (e.g. "_external/p1=1"). Hive's own client-side reconstruction
    // (PartitionSpecWithSharedSDProxy.Iterator.getCurrent) undoes this with plain string
    // concatenation, not by joining root and relPath as filesystem path segments --
    // normalizeLocation must match that exactly or it silently computes the wrong location.
    withTempDir { baseDir =>
      val tableDir = new File(baseDir, "prefix_collision_tbl")
      val siblingDir = new File(baseDir, "prefix_collision_tbl_external")
      withTable("prefix_collision_tbl") {
        versionSpark.sql(
          s"""
             |CREATE TABLE prefix_collision_tbl(c1 int, p1 int)
             |USING PARQUET PARTITIONED BY (p1)
             |LOCATION '${tableDir.toURI}'
           """.stripMargin)
        versionSpark.sql("INSERT INTO prefix_collision_tbl PARTITION (p1 = 1) VALUES (10)")
        versionSpark.sql(
          s"ALTER TABLE prefix_collision_tbl PARTITION (p1 = 1) SET LOCATION " +
            s"'${siblingDir.toURI}'")

        val parts = versionSpark.sessionState.catalog.listPartitionLocations(
          TableIdentifier("prefix_collision_tbl"))
        assert(parts.length == 1)
        assert(parts.head.locationUri.map(_.toString.stripSuffix("/")) ==
          Some(siblingDir.toURI.toString.stripSuffix("/")))

        // This overwrite must resolve the partition's custom location via
        // getCustomPartitionLocations; if normalizeLocation miscomputed the location, this
        // would either write to the wrong (likely non-existent) directory or throw.
        versionSpark.sql(
          "INSERT OVERWRITE TABLE prefix_collision_tbl PARTITION (p1 = 1) VALUES (20)")
        assert(versionSpark.table("prefix_collision_tbl").collect().toSeq == Seq(Row(20, 1)))
        assert(siblingDir.exists() && siblingDir.listFiles().exists(_.getName.startsWith("part-")))
      }
    }
  }

  test("listPartitionLocations and INSERT OVERWRITE with partition specs") {
    assume(version.startsWith("4."))
    withTable("spec_part_tbl") {
      versionSpark.sql(
        "CREATE TABLE spec_part_tbl(c1 int, p1 int, p2 int) " +
        "USING PARQUET PARTITIONED BY (p1, p2)")
      versionSpark.sql(
        "INSERT INTO spec_part_tbl PARTITION (p1 = 1, p2 = 1) VALUES (10)")
      versionSpark.sql(
        "INSERT INTO spec_part_tbl PARTITION (p1 = 1, p2 = 2) VALUES (20)")
      versionSpark.sql(
        "INSERT INTO spec_part_tbl PARTITION (p1 = 2, p2 = 1) VALUES (30)")
      versionSpark.sql(
        "INSERT INTO spec_part_tbl PARTITION (p1 = 2, p2 = 2) VALUES (40)")
      // Verify integer key p1=1 does not over-match p1=10 via trailing wildcard without slash
      versionSpark.sql(
        "INSERT INTO spec_part_tbl PARTITION (p1 = 10, p2 = 1) VALUES (50)")

      val catalog = versionSpark.sessionState.catalog
      val tableIdent = TableIdentifier("spec_part_tbl")

      // 1. Leading prefix spec (p1 = 1): pushes down BY_VALUES filter in Shim_v4_0
      val leadingPartitions = catalog.listPartitionLocations(
        tableIdent, Some(Map("p1" -> "1")))
      assert(leadingPartitions.map(_.spec).toSet == Set(
        Map("p1" -> "1", "p2" -> "1"),
        Map("p1" -> "1", "p2" -> "2")
      ))

      // 2. Non-leading spec (p2 = 1): pads leading columns with "" to match p1=.*/p2=1
      val nonLeadingPartitions = catalog.listPartitionLocations(
        tableIdent, Some(Map("p2" -> "1")))
      assert(nonLeadingPartitions.map(_.spec).toSet == Set(
        Map("p1" -> "1", "p2" -> "1"),
        Map("p1" -> "2", "p2" -> "1"),
        Map("p1" -> "10", "p2" -> "1")
      ))

      // 3. Full spec (p1 = 2, p2 = 2)
      val fullPartitions = catalog.listPartitionLocations(
        tableIdent, Some(Map("p1" -> "2", "p2" -> "2")))
      assert(fullPartitions.map(_.spec).toSet == Set(
        Map("p1" -> "2", "p2" -> "2")
      ))

      // 4. End-to-end INSERT OVERWRITE with partition spec
      versionSpark.sql(
        "INSERT OVERWRITE TABLE spec_part_tbl PARTITION (p1 = 1, p2 = 1) VALUES (100)")
      val rows = versionSpark.table("spec_part_tbl").collect().map { r =>
        (r.getInt(0), r.getInt(1), r.getInt(2))
      }.toSet
      assert(rows == Set((100, 1, 1), (20, 1, 2), (30, 2, 1), (40, 2, 2), (50, 10, 1)))
    }
  }

  test("listPartitionLocations matches listPartitions across partition location shapes " +
      "for all Hive versions") {
    // For Hive 4.0+ shims the projection RPC may group the table's partitions into every
    // response shape it can produce: partitions inside the table directory form a
    // shared-SD group whose relative path carries the leading separator, a partition
    // under a sibling directory that has the table location as a *string* prefix ends up
    // in a shared-SD group whose relative path has no leading separator, and a partition
    // outside the table directory is returned with its own storage descriptor. Shims
    // below 4.0 serve all shapes from the full-fetch path. The new lightweight listing
    // must report exactly the same specs and locations as the full listPartitions.
    withTempDir { baseDir =>
      val tableDir = new File(baseDir, "projection_parity_tbl")
      val customDir = new File(baseDir, "custom_loc_dir")
      val prefixSiblingDir = new File(baseDir, "projection_parity_tbl_external")
      withTable("projection_parity_tbl") {
        versionSpark.sql(
          s"""
             |CREATE TABLE projection_parity_tbl(c1 int, p1 int, p2 int)
             |USING PARQUET PARTITIONED BY (p1, p2)
             |LOCATION '${tableDir.toURI}'
           """.stripMargin)
        // Partitions at default locations, inside the table directory
        versionSpark.sql(
          "INSERT INTO projection_parity_tbl PARTITION (p1 = 1, p2 = 1) VALUES (10)")
        versionSpark.sql(
          "INSERT INTO projection_parity_tbl PARTITION (p1 = 1, p2 = 2) VALUES (20)")
        versionSpark.sql(
          "INSERT INTO projection_parity_tbl PARTITION (p1 = 10, p2 = 1) VALUES (30)")
        // Custom location outside the table directory
        versionSpark.sql(
          s"ALTER TABLE projection_parity_tbl PARTITION (p1 = 1, p2 = 2) SET LOCATION " +
            s"'${customDir.toURI}'")
        // Location under a sibling directory that shares the table location as a string
        // prefix, without a path separator
        versionSpark.sql(
          s"ALTER TABLE projection_parity_tbl PARTITION (p1 = 10, p2 = 1) SET LOCATION " +
            s"'${prefixSiblingDir.toURI}'")

        val catalog = versionSpark.sessionState.catalog
        val tableIdent = TableIdentifier("projection_parity_tbl")
        val specs = Seq(None, Some(Map("p1" -> "1")), Some(Map("p2" -> "1")),
          Some(Map("p1" -> "1", "p2" -> "2")), Some(Map("p1" -> "10", "p2" -> "1")),
          Some(Map("p1" -> "unknown")))
        specs.foreach { spec =>
          val light = catalog.listPartitionLocations(tableIdent, spec)
            .map(p => (p.spec, p.locationUri)).toSet
          val full = catalog.listPartitions(tableIdent, spec)
            .map(p => (p.spec, p.storage.locationUri)).toSet
          assert(light == full,
            s"listPartitionLocations mismatched listPartitions with the partial spec: $spec")
        }
      }
    }
  }

  test("listPartitionLocations yields identical results with partition projection " +
      "enabled or disabled") {
    // Shim_v4_0 and above apply HMS partition projection only when the flag is on, so
    // for Hive 4.x the results must not depend on the flag. Older shims always take the
    // full-fetch path, where the flag is irrelevant, hence the version assumption.
    assume(version.startsWith("4."))
    withTable("projection_conf_tbl") {
      versionSpark.sql(
        "CREATE TABLE projection_conf_tbl(c1 int, p1 int, p2 int) " +
        "USING PARQUET PARTITIONED BY (p1, p2)")
      versionSpark.sql(
        "INSERT INTO projection_conf_tbl PARTITION (p1 = 1, p2 = 1) VALUES (10)")
      versionSpark.sql(
        "INSERT INTO projection_conf_tbl PARTITION (p1 = 1, p2 = 2) VALUES (20)")

      val catalog = versionSpark.sessionState.catalog
      val tableIdent = TableIdentifier("projection_conf_tbl")
      val specs = Seq(None, Some(Map("p1" -> "1")), Some(Map("p2" -> "1")),
        Some(Map("p1" -> "1", "p2" -> "1")))
      def locations(): Seq[Set[(Map[String, String], Option[URI])]] = specs.map { spec =>
        catalog.listPartitionLocations(tableIdent, spec)
          .map(p => (p.spec, p.locationUri)).toSet
      }

      val withProjection = locations()
      try {
        versionSpark.conf.setConfString(
          SQLConf.HIVE_METASTORE_PARTITION_PROJECTION_ENABLED.key, "false")
        assert(locations() == withProjection)
      } finally {
        versionSpark.conf.unsetConf(SQLConf.HIVE_METASTORE_PARTITION_PROJECTION_ENABLED.key)
      }
    }
  }

  test("Delete the temporary staging directory and files after each insert") {
    withTempDir { tmpDir =>
      withTable("tab") {
        versionSpark.sql(
          s"""
             |CREATE TABLE tab(c1 string)
             |USING HIVE
             |location '${tmpDir.toURI.toString}'
             """.stripMargin)

        (1 to 3).map { i =>
          versionSpark.sql(s"INSERT OVERWRITE TABLE tab SELECT '$i'")
        }
        def listFiles(path: File): List[String] = {
          val dir = path.listFiles()
          val folders = dir.filter(_.isDirectory).toList
          val filePaths = dir.map(_.getName).toList
          folders.flatMap(listFiles) ++: filePaths
        }
        assert(listFiles(tmpDir).length === 2)
      }
    }
  }

  test("SPARK-13709: reading partitioned Avro table with nested schema") {
    withTempDir { dir =>
      val path = dir.toURI.toString
      val tableName = "spark_13709"
      val tempTableName = "spark_13709_temp"

      new File(dir.getAbsolutePath, tableName).mkdir()
      new File(dir.getAbsolutePath, tempTableName).mkdir()

      val avroSchema =
        """{
          |  "name": "test_record",
          |  "type": "record",
          |  "fields": [ {
          |    "name": "f0",
          |    "type": "int"
          |  }, {
          |    "name": "f1",
          |    "type": {
          |      "type": "record",
          |      "name": "inner",
          |      "fields": [ {
          |        "name": "f10",
          |        "type": "int"
          |      }, {
          |        "name": "f11",
          |        "type": "double"
          |      } ]
          |    }
          |  } ]
          |}
          """.stripMargin

      withTable(tableName, tempTableName) {
        // Creates the external partitioned Avro table to be tested.
        versionSpark.sql(
          s"""CREATE EXTERNAL TABLE $tableName
             |PARTITIONED BY (ds STRING)
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
             |STORED AS
             |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
             |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
             |LOCATION '$path/$tableName'
             |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
           """.stripMargin
        )

        // Creates an temporary Avro table used to prepare testing Avro file.
        versionSpark.sql(
          s"""CREATE EXTERNAL TABLE $tempTableName
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
             |STORED AS
             |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
             |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
             |LOCATION '$path/$tempTableName'
             |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
           """.stripMargin
        )

        // Generates Avro data.
        versionSpark.sql(s"INSERT OVERWRITE TABLE $tempTableName SELECT 1, STRUCT(2, 2.5)")

        // Adds generated Avro data as a new partition to the testing table.
        versionSpark.sql(
          s"ALTER TABLE $tableName ADD PARTITION (ds = 'foo') LOCATION '$path/$tempTableName'")

        // The following query fails before SPARK-13709 is fixed. This is because when reading
        // data from table partitions, Avro deserializer needs the Avro schema, which is defined
        // in table property "avro.schema.literal". However, we only initializes the deserializer
        // using partition properties, which doesn't include the wanted property entry. Merging
        // two sets of properties solves the problem.
        assert(versionSpark.sql(s"SELECT * FROM $tableName").collect() ===
          Array(Row(1, Row(2, 2.5D), "foo")))
      }
    }
  }

  test("CTAS for managed data source tables") {
    withTable("t", "t1") {
      versionSpark.range(1).write.saveAsTable("t")
      assert(versionSpark.table("t").collect() === Array(Row(0)))
      versionSpark.sql("create table t1 using parquet as select 2 as a")
      assert(versionSpark.table("t1").collect() === Array(Row(2)))
    }
  }

  test("Decimal support of Avro Hive serde") {
    val tableName = "tab1"
    // TODO: add the other logical types. For details, see the link:
    // https://avro.apache.org/docs/1.12.1/specification/#logical-types
    val avroSchema =
    """{
      |  "name": "test_record",
      |  "type": "record",
      |  "fields": [ {
      |    "name": "f0",
      |    "type": [
      |      "null",
      |      {
      |        "precision": 38,
      |        "scale": 2,
      |        "type": "bytes",
      |        "logicalType": "decimal"
      |      }
      |    ]
      |  } ]
      |}
        """.stripMargin

    Seq(true, false).foreach { isPartitioned =>
      withTable(tableName) {
        val partitionClause = if (isPartitioned) "PARTITIONED BY (ds STRING)" else ""
        // Creates the (non-)partitioned Avro table
        versionSpark.sql(
          s"""
             |CREATE TABLE $tableName
             |$partitionClause
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
             |STORED AS
             |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
             |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
             |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
           """.stripMargin
        )

        if (isPartitioned) {
          val insertStmt = s"INSERT OVERWRITE TABLE $tableName partition (ds='a') SELECT 1.3"
          versionSpark.sql(insertStmt)
          assert(versionSpark.table(tableName).collect() ===
            versionSpark.sql("SELECT 1.30, 'a'").collect())
        } else {
          val insertStmt = s"INSERT OVERWRITE TABLE $tableName SELECT 1.3"
          versionSpark.sql(insertStmt)
          assert(versionSpark.table(tableName).collect() ===
            versionSpark.sql("SELECT 1.30").collect())
        }
      }
    }
  }

  test("read avro file containing decimal") {
    val url = Thread.currentThread().getContextClassLoader.getResource("avroDecimal")
    val location = new File(url.getFile).toURI.toString

    val tableName = "tab1"
    val avroSchema =
      """{
        |  "name": "test_record",
        |  "type": "record",
        |  "fields": [ {
        |    "name": "f0",
        |    "type": [
        |      "null",
        |      {
        |        "precision": 38,
        |        "scale": 2,
        |        "type": "bytes",
        |        "logicalType": "decimal"
        |      }
        |    ]
        |  } ]
        |}
        """.stripMargin
    withTable(tableName) {
      versionSpark.sql(
        s"""
           |CREATE TABLE $tableName
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
           |WITH SERDEPROPERTIES ('respectSparkSchema' = 'true')
           |STORED AS
           |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
           |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
           |LOCATION '$location'
           |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
           """.stripMargin
      )
      assert(versionSpark.table(tableName).collect() ===
        versionSpark.sql("SELECT 1.30").collect())
    }
  }

  test("SPARK-17920: Insert into/overwrite avro table") {
    // skipped because it's failed in the condition on Windows
    assume(!Utils.isWindows)
    withTempDir { dir =>
      val avroSchema =
        """
          |{
          |  "name": "test_record",
          |  "type": "record",
          |  "fields": [{
          |    "name": "f0",
          |    "type": [
          |      "null",
          |      {
          |        "precision": 38,
          |        "scale": 2,
          |        "type": "bytes",
          |        "logicalType": "decimal"
          |      }
          |    ]
          |  }]
          |}
          """.stripMargin
      val schemaFile = new File(dir, "avroDecimal.avsc")
      Utils.tryWithResource(new PrintWriter(schemaFile)) { writer =>
        writer.write(avroSchema)
      }
      val schemaPath = schemaFile.toURI.toString

      val url = Thread.currentThread().getContextClassLoader.getResource("avroDecimal")
      val srcLocation = new File(url.getFile).toURI.toString
      val destTableName = "tab1"
      val srcTableName = "tab2"

      withTable(srcTableName, destTableName) {
        versionSpark.sql(
          s"""
             |CREATE EXTERNAL TABLE $srcTableName
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
             |WITH SERDEPROPERTIES ('respectSparkSchema' = 'true')
             |STORED AS
             |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
             |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
             |LOCATION '$srcLocation'
             |TBLPROPERTIES ('avro.schema.url' = '$schemaPath')
           """.stripMargin
        )

        versionSpark.sql(
          s"""
             |CREATE TABLE $destTableName
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
             |WITH SERDEPROPERTIES ('respectSparkSchema' = 'true')
             |STORED AS
             |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
             |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
             |TBLPROPERTIES ('avro.schema.url' = '$schemaPath')
           """.stripMargin
        )
        versionSpark.sql(
          s"""INSERT OVERWRITE TABLE $destTableName SELECT * FROM $srcTableName""")
        val result = versionSpark.table(srcTableName).collect()
        assert(versionSpark.table(destTableName).collect() === result)
        versionSpark.sql(
          s"""INSERT INTO TABLE $destTableName SELECT * FROM $srcTableName""")
        assert(versionSpark.table(destTableName).collect().toSeq === result ++ result)
      }
    }
  }
  // TODO: add more tests.
}
