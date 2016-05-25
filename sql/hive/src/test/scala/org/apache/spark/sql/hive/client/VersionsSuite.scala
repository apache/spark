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

import java.io.{ByteArrayOutputStream, File, PrintStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.util.VersionInfo

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.NoSuchPermanentFunctionException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.tags.ExtendedHiveTest
import org.apache.spark.util.{MutableURLClassLoader, Utils}

/**
 * A simple set of tests that call the methods of a [[HiveClient]], loading different version
 * of hive from maven central.  These tests are simple in that they are mostly just testing to make
 * sure that reflective calls are not throwing NoSuchMethod error, but the actually functionality
 * is not fully tested.
 */
@ExtendedHiveTest
class VersionsSuite extends SparkFunSuite with Logging {

  private val sparkConf = new SparkConf()

  // In order to speed up test execution during development or in Jenkins, you can specify the path
  // of an existing Ivy cache:
  private val ivyPath: Option[String] = {
    sys.env.get("SPARK_VERSIONS_SUITE_IVY_PATH").orElse(
      Some(new File(sys.props("java.io.tmpdir"), "hive-ivy-cache").getAbsolutePath))
  }

  private def buildConf() = {
    lazy val warehousePath = Utils.createTempDir()
    lazy val metastorePath = Utils.createTempDir()
    metastorePath.delete()
    Map(
      "javax.jdo.option.ConnectionURL" -> s"jdbc:derby:;databaseName=$metastorePath;create=true",
      "hive.metastore.warehouse.dir" -> warehousePath.toString)
  }

  test("success sanity check") {
    val badClient = IsolatedClientLoader.forVersion(
      hiveMetastoreVersion = HiveUtils.hiveExecutionVersion,
      hadoopVersion = VersionInfo.getVersion,
      sparkConf = sparkConf,
      hadoopConf = new Configuration(),
      config = buildConf(),
      ivyPath = ivyPath).createClient()
    val db = new CatalogDatabase("default", "desc", "loc", Map())
    badClient.createDatabase(db, ignoreIfExists = true)
  }

  test("hadoop configuration preserved") {
    val hadoopConf = new Configuration();
    hadoopConf.set("test", "success")
    val client = IsolatedClientLoader.forVersion(
      hiveMetastoreVersion = HiveUtils.hiveExecutionVersion,
      hadoopVersion = VersionInfo.getVersion,
      sparkConf = sparkConf,
      hadoopConf = hadoopConf,
      config = buildConf(),
      ivyPath = ivyPath).createClient()
    assert("success" === client.getConf("test", null))
  }

  private def getNestedMessages(e: Throwable): String = {
    var causes = ""
    var lastException = e
    while (lastException != null) {
      causes += lastException.toString + "\n"
      lastException = lastException.getCause
    }
    causes
  }

  private val emptyDir = Utils.createTempDir().getCanonicalPath

  // Its actually pretty easy to mess things up and have all of your tests "pass" by accidentally
  // connecting to an auto-populated, in-process metastore.  Let's make sure we are getting the
  // versions right by forcing a known compatibility failure.
  // TODO: currently only works on mysql where we manually create the schema...
  ignore("failure sanity check") {
    val e = intercept[Throwable] {
      val badClient = quietly {
        IsolatedClientLoader.forVersion(
          hiveMetastoreVersion = "13",
          hadoopVersion = VersionInfo.getVersion,
          sparkConf = sparkConf,
          hadoopConf = new Configuration(),
          config = buildConf(),
          ivyPath = ivyPath).createClient()
      }
    }
    assert(getNestedMessages(e) contains "Unknown column 'A0.OWNER_NAME' in 'field list'")
  }

  private val versions = Seq("0.12", "0.13", "0.14", "1.0", "1.1", "1.2")

  private var client: HiveClient = null

  versions.foreach { version =>
    test(s"$version: create client") {
      client = null
      System.gc() // Hack to avoid SEGV on some JVM versions.
      val hadoopConf = new Configuration();
      hadoopConf.set("test", "success")
      client =
        IsolatedClientLoader.forVersion(
          hiveMetastoreVersion = version,
          hadoopVersion = VersionInfo.getVersion,
          sparkConf = sparkConf,
          hadoopConf = hadoopConf,
          config = buildConf(),
          ivyPath = ivyPath).createClient()
    }

    def table(database: String, tableName: String): CatalogTable = {
      CatalogTable(
        identifier = TableIdentifier(tableName, Some(database)),
        tableType = CatalogTableType.MANAGED,
        schema = Seq(CatalogColumn("key", "int")),
        storage = CatalogStorageFormat(
          locationUri = None,
          inputFormat = Some(classOf[TextInputFormat].getName),
          outputFormat = Some(classOf[HiveIgnoreKeyTextOutputFormat[_, _]].getName),
          serde = Some(classOf[LazySimpleSerDe].getName()),
          compressed = false,
          serdeProperties = Map.empty
        ))
    }

    ///////////////////////////////////////////////////////////////////////////
    // Database related API
    ///////////////////////////////////////////////////////////////////////////

    val tempDatabasePath = Utils.createTempDir().getCanonicalPath

    test(s"$version: createDatabase") {
      val defaultDB = CatalogDatabase("default", "desc", "loc", Map())
      client.createDatabase(defaultDB, ignoreIfExists = true)
      val tempDB = CatalogDatabase(
        "temporary", description = "test create", tempDatabasePath, Map())
      client.createDatabase(tempDB, ignoreIfExists = true)
    }

    test(s"$version: setCurrentDatabase") {
      client.setCurrentDatabase("default")
    }

    test(s"$version: getDatabase") {
      // No exception should be thrown
      client.getDatabase("default")
    }

    test(s"$version: getDatabaseOption") {
      assert(client.getDatabaseOption("default").isDefined)
      assert(client.getDatabaseOption("nonexist") == None)
    }

    test(s"$version: listDatabases") {
      assert(client.listDatabases("defau.*") == Seq("default"))
    }

    test(s"$version: alterDatabase") {
      val database = client.getDatabase("temporary").copy(properties = Map("flag" -> "true"))
      client.alterDatabase(database)
      assert(client.getDatabase("temporary").properties.contains("flag"))
    }

    test(s"$version: dropDatabase") {
      assert(client.getDatabaseOption("temporary").isDefined)
      client.dropDatabase("temporary", ignoreIfNotExists = false, cascade = true)
      assert(client.getDatabaseOption("temporary").isEmpty)
    }

    ///////////////////////////////////////////////////////////////////////////
    // Table related API
    ///////////////////////////////////////////////////////////////////////////

    test(s"$version: createTable") {
      client.createTable(table("default", tableName = "src"), ignoreIfExists = false)
      client.createTable(table("default", "temporary"), ignoreIfExists = false)
    }

    test(s"$version: loadTable") {
      client.loadTable(
        emptyDir,
        tableName = "src",
        replace = false,
        holdDDLTime = false)
    }

    test(s"$version: getTable") {
      // No exception should be thrown
      client.getTable("default", "src")
    }

    test(s"$version: getTableOption") {
      assert(client.getTableOption("default", "src").isDefined)
    }

    test(s"$version: alterTable(table: CatalogTable)") {
      val newTable = client.getTable("default", "src").copy(properties = Map("changed" -> ""))
      client.alterTable(newTable)
      assert(client.getTable("default", "src").properties.contains("changed"))
    }

    test(s"$version: alterTable(tableName: String, table: CatalogTable)") {
      val newTable = client.getTable("default", "src").copy(properties = Map("changedAgain" -> ""))
      client.alterTable("src", newTable)
      assert(client.getTable("default", "src").properties.contains("changedAgain"))
    }

    test(s"$version: listTables(database)") {
      assert(client.listTables("default") === Seq("src", "temporary"))
    }

    test(s"$version: listTables(database, pattern)") {
      assert(client.listTables("default", pattern = "src") === Seq("src"))
      assert(client.listTables("default", pattern = "nonexist").isEmpty)
    }

    test(s"$version: dropTable") {
      client.dropTable("default", tableName = "temporary", ignoreIfNotExists = false)
      assert(client.listTables("default") === Seq("src"))
    }

    ///////////////////////////////////////////////////////////////////////////
    // Partition related API
    ///////////////////////////////////////////////////////////////////////////

    val storageFormat = CatalogStorageFormat(
      locationUri = None,
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      serdeProperties = Map.empty)

    test(s"$version: sql create partitioned table") {
      client.runSqlHive("CREATE TABLE src_part (value INT) PARTITIONED BY (key1 INT, key2 INT)")
    }

    test(s"$version: createPartitions") {
      val partition1 = CatalogTablePartition(Map("key1" -> "1", "key2" -> "1"), storageFormat)
      val partition2 = CatalogTablePartition(Map("key1" -> "1", "key2" -> "2"), storageFormat)
      client.createPartitions(
        "default", "src_part", Seq(partition1, partition2), ignoreIfExists = true)
    }

    test(s"$version: getPartitions(catalogTable)") {
      assert(2 == client.getPartitions(client.getTable("default", "src_part")).size)
    }

    test(s"$version: getPartitionsByFilter") {
      // Only one partition [1, 1] for key2 == 1
      val result = client.getPartitionsByFilter(client.getTable("default", "src_part"),
        Seq(EqualTo(AttributeReference("key2", IntegerType)(), Literal(1))))

      // Hive 0.12 doesn't support getPartitionsByFilter, it ignores the filter condition.
      if (version != "0.12") {
        assert(result.size == 1)
      }
    }

    test(s"$version: getPartition") {
      // No exception should be thrown
      client.getPartition("default", "src_part", Map("key1" -> "1", "key2" -> "2"))
    }

    test(s"$version: getPartitionOption(db: String, table: String, spec: TablePartitionSpec)") {
      val partition = client.getPartitionOption(
        "default", "src_part", Map("key1" -> "1", "key2" -> "2"))
      assert(partition.isDefined)
    }

    test(s"$version: getPartitionOption(table: CatalogTable, spec: TablePartitionSpec)") {
      val partition = client.getPartitionOption(
        client.getTable("default", "src_part"), Map("key1" -> "1", "key2" -> "2"))
      assert(partition.isDefined)
    }

    test(s"$version: getPartitions(db: String, table: String)") {
      assert(2 == client.getPartitions("default", "src_part", None).size)
    }

    test(s"$version: loadPartition") {
      val partSpec = new java.util.LinkedHashMap[String, String]
      partSpec.put("key1", "1")
      partSpec.put("key2", "2")

      client.loadPartition(
        emptyDir,
        "default.src_part",
        partSpec,
        replace = false,
        holdDDLTime = false,
        inheritTableSpecs = false,
        isSkewedStoreAsSubdir = false)
    }

    test(s"$version: loadDynamicPartitions") {
      val partSpec = new java.util.LinkedHashMap[String, String]
      partSpec.put("key1", "1")
      partSpec.put("key2", "") // Dynamic partition

      client.loadDynamicPartitions(
        emptyDir,
        "default.src_part",
        partSpec,
        replace = false,
        numDP = 1,
        false,
        false)
    }

    test(s"$version: renamePartitions") {
      val oldSpec = Map("key1" -> "1", "key2" -> "1")
      val newSpec = Map("key1" -> "1", "key2" -> "3")
      client.renamePartitions("default", "src_part", Seq(oldSpec), Seq(newSpec))

      // Checks the existence of the new partition (key1 = 1, key2 = 3)
      assert(client.getPartitionOption("default", "src_part", newSpec).isDefined)
    }

    test(s"$version: alterPartitions") {
      val spec = Map("key1" -> "1", "key2" -> "2")
      val newLocation = Utils.createTempDir().getPath()
      val storage = storageFormat.copy(locationUri = Some(newLocation))
      val partition = CatalogTablePartition(spec, storage)
      client.alterPartitions("default", "src_part", Seq(partition))
      assert(client.getPartition("default", "src_part", spec)
        .storage.locationUri == Some(newLocation))
    }

    test(s"$version: dropPartitions") {
      val spec = Map("key1" -> "1", "key2" -> "3")
      client.dropPartitions("default", "src_part", Seq(spec), ignoreIfNotExists = true)
      assert(client.getPartitionOption("default", "src_part", spec).isEmpty)
    }

    ///////////////////////////////////////////////////////////////////////////
    // Function related API
    ///////////////////////////////////////////////////////////////////////////

    def function(name: String, className: String): CatalogFunction = {
      CatalogFunction(
        FunctionIdentifier(name, Some("default")), className, Seq.empty[FunctionResource])
    }

    test(s"$version: createFunction") {
      val functionClass = "org.apache.spark.MyFunc1"
      if (version == "0.12") {
        // Hive 0.12 doesn't support creating permanent functions
        intercept[AnalysisException] {
          client.createFunction("default", function("func1", functionClass))
        }
      } else {
        client.createFunction("default", function("func1", functionClass))
      }
    }

    test(s"$version: functionExists") {
      if (version == "0.12") {
        // Hive 0.12 doesn't allow customized permanent functions
        assert(client.functionExists("default", "func1") == false)
      } else {
        assert(client.functionExists("default", "func1") == true)
      }
    }

    test(s"$version: renameFunction") {
      if (version == "0.12") {
        // Hive 0.12 doesn't allow customized permanent functions
        intercept[NoSuchPermanentFunctionException] {
          client.renameFunction("default", "func1", "func2")
        }
      } else {
        client.renameFunction("default", "func1", "func2")
        assert(client.functionExists("default", "func2") == true)
      }
    }

    test(s"$version: alterFunction") {
      val functionClass = "org.apache.spark.MyFunc2"
      if (version == "0.12") {
        // Hive 0.12 doesn't allow customized permanent functions
        intercept[NoSuchPermanentFunctionException] {
          client.alterFunction("default", function("func2", functionClass))
        }
      } else {
        client.alterFunction("default", function("func2", functionClass))
      }
    }

    test(s"$version: getFunction") {
      if (version == "0.12") {
        // Hive 0.12 doesn't allow customized permanent functions
        intercept[NoSuchPermanentFunctionException] {
          client.getFunction("default", "func2")
        }
      } else {
        // No exception should be thrown
        val func = client.getFunction("default", "func2")
        assert(func.className == "org.apache.spark.MyFunc2")
      }
    }

    test(s"$version: getFunctionOption") {
      if (version == "0.12") {
        // Hive 0.12 doesn't allow customized permanent functions
        assert(client.getFunctionOption("default", "func2").isEmpty)
      } else {
        assert(client.getFunctionOption("default", "func2").isDefined)
        assert(client.getFunctionOption("default", "the_func_not_exists").isEmpty)
      }
    }

    test(s"$version: listFunctions") {
      if (version == "0.12") {
        // Hive 0.12 doesn't allow customized permanent functions
        assert(client.listFunctions("default", "fun.*").isEmpty)
      } else {
        assert(client.listFunctions("default", "fun.*").size == 1)
      }
    }

    test(s"$version: dropFunction") {
      if (version == "0.12") {
        // Hive 0.12 doesn't support creating permanent functions
        intercept[NoSuchPermanentFunctionException] {
          client.dropFunction("default", "func2")
        }
      } else {
        // No exception should be thrown
        client.dropFunction("default", "func2")
        assert(client.listFunctions("default", "fun.*").size == 0)
      }
    }

    ///////////////////////////////////////////////////////////////////////////
    // SQL related API
    ///////////////////////////////////////////////////////////////////////////

    test(s"$version: sql set command") {
      client.runSqlHive("SET spark.sql.test.key=1")
    }

    test(s"$version: sql create index and reset") {
      client.runSqlHive("CREATE TABLE indexed_table (key INT)")
      client.runSqlHive("CREATE INDEX index_1 ON TABLE indexed_table(key) " +
        "as 'COMPACT' WITH DEFERRED REBUILD")
    }

    ///////////////////////////////////////////////////////////////////////////
    // Miscellaneous API
    ///////////////////////////////////////////////////////////////////////////

    test(s"$version: version") {
      assert(client.version.fullVersion.startsWith(version))
    }

    test(s"$version: getConf") {
      assert("success" === client.getConf("test", null))
    }

    test(s"$version: setOut") {
      client.setOut(new PrintStream(new ByteArrayOutputStream()))
    }

    test(s"$version: setInfo") {
      client.setInfo(new PrintStream(new ByteArrayOutputStream()))
    }

    test(s"$version: setError") {
      client.setError(new PrintStream(new ByteArrayOutputStream()))
    }

    test(s"$version: newSession") {
      val newClient = client.newSession()
      assert(newClient != null)
    }

    test(s"$version: withHiveState and addJar") {
      val newClassPath = "."
      client.addJar(newClassPath)
      client.withHiveState {
        // No exception should be thrown.
        // withHiveState changes the classloader to MutableURLClassLoader
        val classLoader = Thread.currentThread().getContextClassLoader
          .asInstanceOf[MutableURLClassLoader]

        val urls = classLoader.getURLs()
        urls.contains(new File(newClassPath).toURI.toURL)
      }
    }

    test(s"$version: reset") {
      // Clears all database, tables, functions...
      client.reset()
      assert(client.listTables("default").isEmpty)
    }
  }
}
