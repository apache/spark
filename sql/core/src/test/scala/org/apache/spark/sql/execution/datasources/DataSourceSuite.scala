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

package org.apache.spark.sql.execution.datasources

import java.io.File
import java.net.URI
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}
import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkIllegalArgumentException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class DataSourceSuite extends SharedSparkSession with PrivateMethodTester {
  import TestPaths._

  test("test glob and non glob paths") {
    val resultPaths = DataSource.checkAndGlobPathIfNecessary(
      Seq(
        path1.toString,
        path2.toString,
        globPath1.toString,
        globPath2.toString
      ),
      hadoopConf,
      checkEmptyGlobPath = true,
      checkFilesExist = true,
      enableGlobbing = true
    )

    assert(resultPaths.toSet === allPathsInFs.toSet)
  }

  test("test glob paths") {
    val resultPaths = DataSource.checkAndGlobPathIfNecessary(
      Seq(
        globPath1.toString,
        globPath2.toString
      ),
      hadoopConf,
      checkEmptyGlobPath = true,
      checkFilesExist = true,
      enableGlobbing = true
    )

    assert(
      resultPaths.toSet === Set(
        globPath1Result1,
        globPath1Result2,
        globPath2Result1,
        globPath2Result2
      )
    )
  }

  test("test non glob paths") {
    val resultPaths = DataSource.checkAndGlobPathIfNecessary(
      Seq(
        path1.toString,
        path2.toString
      ),
      hadoopConf,
      checkEmptyGlobPath = true,
      checkFilesExist = true,
      enableGlobbing = true
    )

    assert(
      resultPaths.toSet === Set(
        path1,
        path2
      )
    )
  }

  test("test non glob paths checkFilesExist=false") {
    val resultPaths = DataSource.checkAndGlobPathIfNecessary(
      Seq(
        path1.toString,
        path2.toString,
        nonExistentPath.toString
      ),
      hadoopConf,
      checkEmptyGlobPath = true,
      checkFilesExist = false,
      enableGlobbing = true
    )

    assert(
      resultPaths.toSet === Set(
        path1,
        path2,
        nonExistentPath
      )
    )
  }

  test("test non existent paths") {
    checkError(
      exception = intercept[AnalysisException](
        DataSource.checkAndGlobPathIfNecessary(
          Seq(
            path1.toString,
            path2.toString,
            nonExistentPath.toString
          ),
          hadoopConf,
          checkEmptyGlobPath = true,
          checkFilesExist = true,
          enableGlobbing = true
        )
      ),
      condition = "PATH_NOT_FOUND",
      parameters = Map("path" -> nonExistentPath.toString)
    )
  }

  test("test non existent glob paths") {
    assertThrows[AnalysisException](
      DataSource.checkAndGlobPathIfNecessary(
        Seq(
          globPath1.toString,
          globPath2.toString,
          nonExistentGlobPath.toString
        ),
        hadoopConf,
        checkEmptyGlobPath = true,
        checkFilesExist = true,
        enableGlobbing = true
      )
    )
  }

  test("Data source options should be propagated in method checkAndGlobPathIfNecessary") {
    val dataSourceOptions = Map("fs.defaultFS" -> "nonexistentFs://nonexistentFs")
    val dataSource = DataSource(spark, "parquet", Seq("/path3"), options = dataSourceOptions)
    val checkAndGlobPathIfNecessary =
      PrivateMethod[Seq[Path]](Symbol("checkAndGlobPathIfNecessary"))

    val message = intercept[java.io.IOException] {
      dataSource invokePrivate checkAndGlobPathIfNecessary(false, false)
    }.getMessage
    val expectMessage = "No FileSystem for scheme nonexistentFs"
    assert(message.filterNot(Set(':', '"').contains) == expectMessage)
  }

  test("SPARK-13774: Check error message for non existent path without globbed paths") {
    val uuid = UUID.randomUUID().toString
    val baseDir = Utils.createTempDir()
    checkError(
      exception = intercept[AnalysisException] {
        spark.read.format("csv").load(
          new File(baseDir, "file").getAbsolutePath,
          new File(baseDir, "file2").getAbsolutePath,
          new File(uuid, "file3").getAbsolutePath,
          uuid).rdd
      },
      condition = "PATH_NOT_FOUND",
      parameters = Map("path" -> "file:.*"),
      matchPVals = true
    )
  }

  test("SPARK-13774: Check error message for not existent globbed paths") {
    // Non-existent initial path component:
    val nonExistentBasePath = "/" + UUID.randomUUID().toString
    assert(!new File(nonExistentBasePath).exists())
    checkError(
      exception = intercept[AnalysisException] {
        spark.read.format("text").load(s"$nonExistentBasePath/*")
      },
      condition = "PATH_NOT_FOUND",
      parameters = Map("path" -> s"file:$nonExistentBasePath/*")
    )

    // Existent initial path component, but no matching files:
    val baseDir = Utils.createTempDir()
    val childDir = Utils.createTempDir(baseDir.getAbsolutePath)
    assert(childDir.exists())
    try {
      checkError(
        exception = intercept[AnalysisException] {
          spark.read.json(s"${baseDir.getAbsolutePath}/*/*-xyz.json").rdd
        },
        condition = "PATH_NOT_FOUND",
        parameters = Map("path" -> s"file:${baseDir.getAbsolutePath}/*/*-xyz.json")
      )
    } finally {
      Utils.deleteRecursively(baseDir)
    }
  }

  test("SPARK-50458: Proper error handling for unsupported file system") {
    val loc = "https://raw.githubusercontent.com/apache/spark/refs/heads/master/examples/" +
      "src/main/resources/employees.json"
    checkError(exception = intercept[SparkUnsupportedOperationException](
      sql(s"CREATE TABLE HTTP USING JSON LOCATION '$loc'")),
      condition = "FAILED_READ_FILE.UNSUPPORTED_FILE_SYSTEM",
      parameters = Map(
        "path" -> loc,
        "fileSystemClass" -> "org.apache.hadoop.fs.http.HttpsFileSystem",
        "method" -> "listStatus"))
  }

  test("SPARK-51182: DataFrameWriter should throw dataPathNotSpecifiedError when path is not " +
    "specified") {
    val df = new DataSource(spark, "parquet")
    checkError(exception = intercept[SparkIllegalArgumentException](
      df.planForWriting(SaveMode.ErrorIfExists, spark.range(0).logicalPlan)),
      condition = "_LEGACY_ERROR_TEMP_2047")
  }

  test("SPARK-51182: DataFrameWriter should throw multiplePathsSpecifiedError when more than " +
    "one path is specified") {
    val dataSources: List[DataSource] = List(
      new DataSource(spark, "parquet", Seq("/path1"), options = Map("path" -> "/path2")),
      new DataSource(spark, "parquet", Seq("/path1", "/path2")))
    dataSources.foreach(df => checkError(exception = intercept[SparkIllegalArgumentException](
      df.planForWriting(SaveMode.ErrorIfExists, spark.range(0).logicalPlan)),
      condition = "_LEGACY_ERROR_TEMP_2050",
      parameters = Map("paths" -> "/path1, /path2"))
    )
  }

  test("SPARK-60887: newHadoopConfWithOptions preserves key casing for CaseInsensitiveMap") {
    val options = CaseInsensitiveMap(Map(
      "fs.azure.skipUserGroupMetadataDuringInitialization" -> "true",
      "Path" -> "/tmp/a",
      "PATHS" -> "/tmp/b"
    ))
    val conf = spark.sessionState.newHadoopConfWithOptions(options)
    assert(conf.get("fs.azure.skipUserGroupMetadataDuringInitialization") === "true")
    assert(conf.get("fs.azure.skipusergroupmetadataduringinitialization") === null)
    assert(conf.get("path") === null)
    assert(conf.get("Path") === null)
    assert(conf.get("paths") === null)
    assert(conf.get("PATHS") === null)

    val plainConf = spark.sessionState.newHadoopConfWithOptions(Map(
      "fs.azure.skipUserGroupMetadataDuringInitialization" -> "true",
      "Path" -> "/tmp/a",
      "PATHS" -> "/tmp/b"
    ))
    assert(plainConf.get("fs.azure.skipUserGroupMetadataDuringInitialization") === "true")
    assert(plainConf.get("Path") === null)
    assert(plainConf.get("PATHS") === null)

    val dupCaseOptions = CaseInsensitiveMap(new scala.collection.immutable.ListMap() +
      ("parquet.filter.columnindex.enabled" -> "true") +
      ("PARQUET.FILTER.COLUMNINDEX.ENABLED" -> "false"))
    val dupConf = spark.sessionState.newHadoopConfWithOptions(dupCaseOptions)
    assert(dupConf.get("parquet.filter.columnindex.enabled") === null)
    assert(dupConf.get("PARQUET.FILTER.COLUMNINDEX.ENABLED") === "false")

    withSQLConf(SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF.key -> "true") {
      val legacyConf = spark.sessionState.newHadoopConfWithOptions(options)
      assert(legacyConf.get("fs.azure.skipUserGroupMetadataDuringInitialization") === null)
      assert(legacyConf.get("fs.azure.skipusergroupmetadataduringinitialization") === "true")
    }
  }

  test("SPARK-60887: V2 file table insert fallback and catalog table write options with legacy " +
    "flag") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.parquet(path)

      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        withTempView("v2_view") {
          withSQLConf(
            "fs.file.impl" -> classOf[org.apache.spark.sql.FakeFileSystemRequiringDSOption].getName,
            "fs.file.impl.disable.cache" -> "true",
            SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF.key -> "false") {
            spark.read.format("parquet").option("ds_Option", "value").load(path)
              .createOrReplaceTempView("v2_view")
            val insertDf = sql("INSERT INTO v2_view VALUES (1L)")
            val cmd = insertDf.queryExecution.analyzed.collectFirst {
              case c: InsertIntoHadoopFsRelationCommand => c
            }.get
            assert(cmd.options.get("ds_Option").contains("value"))
          }

          withSQLConf(
            SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF.key -> "true") {
            spark.read.format("parquet").option("ds_Option", "value").load(path)
              .createOrReplaceTempView("v2_view")
            withSQLConf(
              "fs.file.impl" -> classOf[FakeFileSystemRequiringLowercaseDSOption].getName,
              "fs.file.impl.disable.cache" -> "true") {
              val insertDf = sql("INSERT INTO v2_view VALUES (2L)")
              val cmd = insertDf.queryExecution.analyzed.collectFirst {
                case c: InsertIntoHadoopFsRelationCommand => c
              }.get
              assert(!cmd.options.contains("ds_Option"))
              assert(cmd.options.get("ds_option").contains("value"))
            }
          }
        }
      }
    }

    // Verify catalog table write option deduplication and Path filtering under both flag values.
    val writeOpts = CaseInsensitiveMap(new scala.collection.immutable.ListMap() +
      ("parquet.enable.dictionary" -> "false") +
      ("PARQUET.ENABLE.DICTIONARY" -> "true") +
      ("Path" -> "/tmp/custom"))

    val defaultJobConf = spark.sessionState.newHadoopConfWithOptions(writeOpts)
    DataSourceUtils.mergeOptionsIntoHadoopConf(
      writeOpts, defaultJobConf, legacyLowercaseKeys = false, legacyPreserveOriginalMap = false)
    assert(defaultJobConf.get("parquet.enable.dictionary") === null)
    assert(defaultJobConf.get("PARQUET.ENABLE.DICTIONARY") === "true")
    assert(defaultJobConf.get("Path") === null)

    withSQLConf(SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF.key -> "true") {
      val legacyJobConf = spark.sessionState.newHadoopConfWithOptions(writeOpts)
      DataSourceUtils.mergeOptionsIntoHadoopConf(
        writeOpts, legacyJobConf, legacyLowercaseKeys = true, legacyPreserveOriginalMap = true)
      assert(legacyJobConf.get("parquet.enable.dictionary") === "false")
      assert(legacyJobConf.get("PARQUET.ENABLE.DICTIONARY") === "true")
      assert(legacyJobConf.get("Path") === "/tmp/custom")
    }

    withTempDir { dir =>
      withTable("dict_tbl") {
        val loc = dir.getCanonicalPath
        val orderedProps = new scala.collection.immutable.ListMap[String, String]() +
          ("parquet.enable.dictionary" -> "false") +
          ("PARQUET.ENABLE.DICTIONARY" -> "true") +
          ("Path" -> "/tmp/custom")
        val tableDesc = org.apache.spark.sql.catalyst.catalog.CatalogTable(
          identifier = org.apache.spark.sql.catalyst.TableIdentifier("dict_tbl"),
          tableType = org.apache.spark.sql.catalyst.catalog.CatalogTableType.EXTERNAL,
          storage = org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat.empty.copy(
            locationUri = Some(dir.toURI),
            properties = orderedProps),
          schema = new org.apache.spark.sql.types.StructType()
            .add("s", org.apache.spark.sql.types.StringType),
          provider = Some("parquet"))
        spark.sessionState.catalog.createTable(tableDesc, ignoreIfExists = false)

        def hasDictionaryPage(tableDir: java.io.File): Boolean = {
          val parquetFile = tableDir.listFiles()
            .find(f => f.isFile && f.getName.endsWith(".parquet")).get
          val footer = org.apache.parquet.hadoop.ParquetFileReader.open(
            org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
              new Path(parquetFile.toURI), spark.sessionState.newHadoopConf()))
          try {
            footer.getFooter.getBlocks.get(0).getColumns.get(0).hasDictionaryPage
          } finally {
            footer.close()
          }
        }

        withSQLConf(
          "fs.file.impl" -> classOf[CapturingLocalFileSystem].getName,
          "fs.file.impl.disable.cache" -> "true",
          SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF.key -> "true") {
          CapturingLocalFileSystem.lastConf = null
          sql("INSERT OVERWRITE dict_tbl SELECT 'repeated_val' FROM range(100)")
          val jobConf = CapturingLocalFileSystem.lastConf
          assert(jobConf !== null)
          assert(jobConf.get("parquet.enable.dictionary") === "false")
          assert(jobConf.get("PARQUET.ENABLE.DICTIONARY") === "true")
          assert(jobConf.get("Path") === "/tmp/custom")
          assert(!hasDictionaryPage(dir))
        }

        spark.sessionState.catalog.invalidateAllCachedTables()
        withSQLConf(
          "fs.file.impl" -> classOf[CapturingLocalFileSystem].getName,
          "fs.file.impl.disable.cache" -> "true",
          SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF.key -> "false") {
          CapturingLocalFileSystem.lastConf = null
          sql("INSERT OVERWRITE dict_tbl SELECT 'repeated_val' FROM range(100)")
          val jobConf = CapturingLocalFileSystem.lastConf
          assert(jobConf !== null)
          assert(jobConf.get("parquet.enable.dictionary") === null)
          assert(jobConf.get("PARQUET.ENABLE.DICTIONARY") === "true")
          assert(jobConf.get("Path") === null)
          assert(hasDictionaryPage(dir))
        }
      }
    }
  }

  test("SPARK-60887: Data Source V2 schema inference, streaming sink fileIndex, and " +
    "TextBasedFileScan codecFactory respect legacy flag") {
    import scala.jdk.CollectionConverters._
    import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
    import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
    import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
    import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
    import org.apache.spark.sql.execution.datasources.v2.{FileTable, TextBasedFileScan}
    import org.apache.spark.sql.execution.datasources.v2.csv.{CSVScan, CSVTable}
    import org.apache.spark.sql.execution.datasources.v2.json.{JsonScan, JsonTable}
    import org.apache.spark.sql.execution.datasources.v2.orc.OrcTable
    import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
    import org.apache.spark.sql.execution.datasources.v2.text.TextScan
    import org.apache.spark.sql.execution.streaming.runtime.MetadataLogFileIndex
    import org.apache.spark.sql.types.{StringType, StructType}
    import org.apache.spark.sql.util.CaseInsensitiveStringMap

    val dsOptions = new CaseInsensitiveStringMap(
      Map("ds_Option" -> "value", "mergeSchema" -> "true").asJava)

    Seq("parquet", "orc", "json", "csv").foreach { format =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.range(1).selectExpr("CAST(id AS STRING) AS v").write.format(format).save(path)
        val fs = new Path(path).getFileSystem(spark.sessionState.newHadoopConf())
        val files = fs.listStatus(new Path(path)).filterNot(_.getPath.getName.startsWith("_")).toSeq

        def createTable(): FileTable = format match {
          case "parquet" =>
            new ParquetTable("t", spark, dsOptions, Seq(path), None, classOf[ParquetFileFormat])
          case "orc" =>
            new OrcTable("t", spark, dsOptions, Seq(path), None, classOf[OrcFileFormat])
          case "json" =>
            new JsonTable("t", spark, dsOptions, Seq(path), None, classOf[JsonFileFormat])
          case "csv" =>
            new CSVTable("t", spark, dsOptions, Seq(path), None, classOf[CSVFileFormat])
        }

        withSQLConf(
          SQLConf.USE_V1_SOURCE_LIST.key -> "",
          SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF.key -> "true",
          "fs.file.impl" -> classOf[FakeFileSystemRequiringLowercaseDSOption].getName,
          "fs.file.impl.disable.cache" -> "true") {
          assert(createTable().inferSchema(files).nonEmpty)
        }
        withSQLConf(
          SQLConf.USE_V1_SOURCE_LIST.key -> "",
          SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF.key -> "true",
          "fs.file.impl" -> classOf[org.apache.spark.sql.FakeFileSystemRequiringDSOption].getName,
          "fs.file.impl.disable.cache" -> "true") {
          val err = intercept[Throwable] {
            createTable().inferSchema(files)
          }
          assert(org.apache.spark.util.SparkErrorUtils.getRootCause(err)
            .isInstanceOf[IllegalArgumentException])
        }
      }
    }

    // V2 streaming sink output directory (`MetadataLogFileIndex`)
    withTempDir { dir =>
      val sinkPath = new java.io.File(dir, "sink").getCanonicalPath
      val checkpointPath = new java.io.File(dir, "ckpt").getCanonicalPath
      val q = spark.readStream.format("rate").load()
        .writeStream.format("parquet")
        .option("checkpointLocation", checkpointPath)
        .start(sinkPath)
      try {
        q.processAllAvailable()
      } finally {
        q.stop()
      }

      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "",
        SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF.key -> "true") {
        val table = new ParquetTable(
          "t", spark, dsOptions, Seq(sinkPath), None, classOf[ParquetFileFormat])
        assert(table.fileIndex.isInstanceOf[MetadataLogFileIndex])
        val hadoopConfMethod =
          classOf[PartitioningAwareFileIndex].getDeclaredMethod("hadoopConf")
        hadoopConfMethod.setAccessible(true)
        val conf = hadoopConfMethod.invoke(table.fileIndex).asInstanceOf[Configuration]
        assert(conf.get("ds_option") === "value")
        assert(conf.get("ds_Option") === null)
        withSQLConf(
          "fs.file.impl" -> classOf[FakeFileSystemRequiringLowercaseDSOption].getName,
          "fs.file.impl.disable.cache" -> "true") {
          new Path(sinkPath).getFileSystem(conf)
        }
      }
    }

    // V2 CSV, JSON, and text scan `codecFactory` split decisions
    val codecOpts = new CaseInsensitiveStringMap(
      Map(
        "Io.Compression.Codecs" -> classOf[FakeUnsplittableCodec].getName,
        "ds_Option" -> "value").asJava)
    val dummyIndex = new InMemoryFileIndex(spark, Seq.empty, Map.empty, None)
    val schema = new StructType().add("v", StringType)

    def createScans(): Seq[TextBasedFileScan] = Seq(
      CSVScan(spark, dummyIndex, schema, schema, new StructType(), codecOpts, Array.empty),
      JsonScan(spark, dummyIndex, schema, schema, new StructType(), codecOpts, Array.empty),
      TextScan(spark, dummyIndex, schema, schema, new StructType(), codecOpts))

    withSQLConf(SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF.key -> "false") {
      createScans().foreach { scan =>
        assert(scan.isSplitable(new Path("test.fakegz")))
      }
    }
    withSQLConf(SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF.key -> "true") {
      createScans().foreach { scan =>
        FakeUnsplittableCodec.lastConf = null
        assert(!scan.isSplitable(new Path("test.fakegz")))
        assert(FakeUnsplittableCodec.lastConf.get("ds_option") === "value")
        assert(FakeUnsplittableCodec.lastConf.get("ds_Option") === null)
      }
    }

    val codecOptsLowerKey = new CaseInsensitiveStringMap(
      Map(
        "io.compression.codecs" -> classOf[FakeUnsplittableCodec].getName,
        "ds_Option" -> "value").asJava)
    withSQLConf(SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF.key -> "false") {
      Seq(
        CSVScan(spark, dummyIndex, schema, schema, new StructType(), codecOptsLowerKey,
          Array.empty),
        JsonScan(spark, dummyIndex, schema, schema, new StructType(), codecOptsLowerKey,
          Array.empty),
        TextScan(spark, dummyIndex, schema, schema, new StructType(), codecOptsLowerKey)
      ).foreach { scan =>
        FakeUnsplittableCodec.lastConf = null
        assert(!scan.isSplitable(new Path("test.fakegz")))
        assert(FakeUnsplittableCodec.lastConf.get("ds_Option") === "value")
        assert(FakeUnsplittableCodec.lastConf.get("ds_option") === null)
      }
    }
  }
}

class FakeFileSystemRequiringLowercaseDSOption extends org.apache.hadoop.fs.LocalFileSystem {
  override def initialize(name: java.net.URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    require(conf.get("ds_option", "") == "value" && conf.get("ds_Option") == null)
  }
}

class CapturingLocalFileSystem extends org.apache.hadoop.fs.LocalFileSystem {
  override def initialize(name: java.net.URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    if (conf.get("spark.sql.sources.writeJobUUID") != null) {
      CapturingLocalFileSystem.lastConf = conf
    }
  }
}

object CapturingLocalFileSystem {
  @volatile var lastConf: Configuration = _
}

class FakeUnsplittableCodec extends org.apache.hadoop.io.compress.GzipCodec {
  override def getDefaultExtension: String = ".fakegz"
  override def setConf(conf: Configuration): Unit = {
    super.setConf(conf)
    if (conf != null) {
      FakeUnsplittableCodec.lastConf = conf
    }
  }
}

object FakeUnsplittableCodec {
  @volatile var lastConf: Configuration = _
}

object TestPaths {
  val hadoopConf = new Configuration()
  hadoopConf.set("fs.mockFs.impl", classOf[MockFileSystem].getName)

  val path1 = new Path("mockFs://mockFs/somepath1")
  val path2 = new Path("mockFs://mockFs/somepath2")
  val globPath1 = new Path("mockFs://mockFs/globpath1*")
  val globPath2 = new Path("mockFs://mockFs/globpath2*")

  val nonExistentPath = new Path("mockFs://mockFs/nonexistentpath")
  val nonExistentGlobPath = new Path("mockFs://mockFs/nonexistentpath*")

  val globPath1Result1 = new Path("mockFs://mockFs/globpath1/path1")
  val globPath1Result2 = new Path("mockFs://mockFs/globpath1/path2")
  val globPath2Result1 = new Path("mockFs://mockFs/globpath2/path1")
  val globPath2Result2 = new Path("mockFs://mockFs/globpath2/path2")

  val allPathsInFs = Seq(
    path1,
    path2,
    globPath1Result1,
    globPath1Result2,
    globPath2Result1,
    globPath2Result2
  )

  val mockGlobResults: Map[Path, Array[FileStatus]] = Map(
    globPath1 ->
      Array(
        createMockFileStatus(globPath1Result1.toString),
        createMockFileStatus(globPath1Result2.toString)
      ),
    globPath2 ->
      Array(
        createMockFileStatus(globPath2Result1.toString),
        createMockFileStatus(globPath2Result2.toString)
      )
  )

  def createMockFileStatus(path: String): FileStatus = {
    val fileStatus = new FileStatus()
    fileStatus.setPath(new Path(path))
    fileStatus
  }
}

class MockFileSystem extends RawLocalFileSystem {
  import TestPaths._

  override def exists(f: Path): Boolean = {
    allPathsInFs.contains(f)
  }

  override def globStatus(pathPattern: Path): Array[FileStatus] = {
    mockGlobResults.getOrElse(pathPattern, Array())
  }

  override def getUri: URI = URI.create("mockFs://mockFs/")
}
