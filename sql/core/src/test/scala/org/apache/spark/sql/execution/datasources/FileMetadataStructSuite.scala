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
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.TestUtils
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

class FileMetadataStructSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  val data0: Seq[Row] = Seq(Row("jack", 24, Row(12345L, "uom")))

  val data1: Seq[Row] = Seq(Row("lily", 31, Row(54321L, "ucb")))

  val schema: StructType = new StructType()
    .add(StructField("name", StringType))
    .add(StructField("age", IntegerType))
    .add(StructField("info", new StructType()
      .add(StructField("id", LongType))
      .add(StructField("university", StringType))))

  val schemaWithNameConflicts: StructType = new StructType()
    .add(StructField("name", StringType))
    .add(StructField("age", IntegerType))
    .add(StructField("_METADATA", new StructType()
      .add(StructField("id", LongType))
      .add(StructField("university", StringType))))

  private val METADATA_FILE_PATH = "_metadata.file_path"

  private val METADATA_FILE_NAME = "_metadata.file_name"

  private val METADATA_FILE_SIZE = "_metadata.file_size"

  private val METADATA_FILE_BLOCK_START = "_metadata.file_block_start"

  private val METADATA_FILE_BLOCK_LENGTH = "_metadata.file_block_length"

  private val METADATA_FILE_MODIFICATION_TIME = "_metadata.file_modification_time"

  private val METADATA_ROW_INDEX = "_metadata.row_index"

  private val FILE_FORMAT = "fileFormat"

  private def getMetadataRow(f: Map[String, Any]): Row = f(FILE_FORMAT) match {
    case "parquet" =>
      Row(f(METADATA_FILE_PATH), f(METADATA_FILE_NAME),
        f(METADATA_FILE_SIZE), f(METADATA_FILE_BLOCK_START), f(METADATA_FILE_BLOCK_LENGTH),
        f(METADATA_FILE_MODIFICATION_TIME), f(METADATA_ROW_INDEX))
    case _ =>
      Row(f(METADATA_FILE_PATH), f(METADATA_FILE_NAME),
        f(METADATA_FILE_SIZE), f(METADATA_FILE_BLOCK_START), f(METADATA_FILE_BLOCK_LENGTH),
        f(METADATA_FILE_MODIFICATION_TIME))
  }

  private def getMetadataForFile(f: File): Map[String, Any] = {
    Map(
      METADATA_FILE_PATH -> f.toURI.toString,
      METADATA_FILE_NAME -> f.getName,
      METADATA_FILE_SIZE -> f.length(),
      // test file is small enough so we would not do splitting files,
      // then the file block start is always 0 and file block length is same with file size
      METADATA_FILE_BLOCK_START -> 0,
      METADATA_FILE_BLOCK_LENGTH -> f.length(),
      METADATA_FILE_MODIFICATION_TIME -> new Timestamp(f.lastModified()),
      METADATA_ROW_INDEX -> 0,
      FILE_FORMAT -> f.getName.split("\\.").last
    )
  }

  /**
   * This test wrapper will test for both row-based and column-based file formats:
   * (json and parquet) with nested schema:
   * 1. create df0 and df1 and save them as testFileFormat under /data/f0 and /data/f1
   * 2. read the path /data, return the df for further testing
   * 3. create actual metadata maps for both files under /data/f0 and /data/f1 for further testing
   *
   * The final df will have data:
   * jack | 24 | {12345, uom}
   * lily | 31 | {54321, ucb}
   *
   * The schema of the df will be the `fileSchema` provided to this method
   *
   * This test wrapper will provide a `df` and actual metadata map `f0`, `f1`
   */
  private def metadataColumnsTest(
      testName: String, fileSchema: StructType)
    (f: (DataFrame, Map[String, Any], Map[String, Any]) => Unit): Unit = {
    Seq("json", "parquet").foreach { testFileFormat =>
      test(s"metadata struct ($testFileFormat): " + testName) {
        withTempDir { dir =>
          import scala.jdk.CollectionConverters._

          // 1. create df0 and df1 and save under /data/f0 and /data/f1
          val df0 = spark.createDataFrame(data0.asJava, fileSchema)
          val f0 = new File(dir, "data/f0").getCanonicalPath
          df0.coalesce(1).write.format(testFileFormat).save(f0)

          val df1 = spark.createDataFrame(data1.asJava, fileSchema)
          val f1 = new File(dir, "data/f1").getCanonicalPath
          df1.coalesce(1).write.format(testFileFormat).save(f1)

          // 2. read both f0 and f1
          val df = spark.read.format(testFileFormat).schema(fileSchema)
            .load(new File(dir, "data").getCanonicalPath + "/*")

          val realF0 = new File(dir, "data/f0").listFiles()
            .filter(_.getName.endsWith(s".$testFileFormat")).head

          val realF1 = new File(dir, "data/f1").listFiles()
            .filter(_.getName.endsWith(s".$testFileFormat")).head

          f(df, getMetadataForFile(realF0), getMetadataForFile(realF1))
        }
      }
    }
  }

  metadataColumnsTest("read partial/all metadata struct fields", schema) { (df, f0, f1) =>
    // read all available metadata struct fields
    checkAnswer(
      df.select("name", "age", "info",
        METADATA_FILE_NAME, METADATA_FILE_PATH,
        METADATA_FILE_SIZE, METADATA_FILE_BLOCK_START, METADATA_FILE_BLOCK_LENGTH,
        METADATA_FILE_MODIFICATION_TIME),
      Seq(
        Row("jack", 24, Row(12345L, "uom"),
          f0(METADATA_FILE_NAME), f0(METADATA_FILE_PATH),
          f0(METADATA_FILE_SIZE), f0(METADATA_FILE_BLOCK_START), f0(METADATA_FILE_BLOCK_LENGTH),
          f0(METADATA_FILE_MODIFICATION_TIME)),
        Row("lily", 31, Row(54321L, "ucb"),
          f1(METADATA_FILE_NAME), f1(METADATA_FILE_PATH),
          f1(METADATA_FILE_SIZE), f1(METADATA_FILE_BLOCK_START), f1(METADATA_FILE_BLOCK_LENGTH),
          f1(METADATA_FILE_MODIFICATION_TIME))
      )
    )

    // read a part of metadata struct fields
    checkAnswer(
      df.select("name", "info.university", METADATA_FILE_NAME, METADATA_FILE_SIZE),
      Seq(
        Row("jack", "uom", f0(METADATA_FILE_NAME), f0(METADATA_FILE_SIZE)),
        Row("lily", "ucb", f1(METADATA_FILE_NAME), f1(METADATA_FILE_SIZE))
      )
    )
  }

  metadataColumnsTest("read metadata struct fields with random ordering", schema) { (df, f0, f1) =>
    // read a part of metadata struct fields with random ordering
    checkAnswer(
      df.select(METADATA_FILE_NAME, "name", METADATA_FILE_SIZE, "info.university"),
      Seq(
        Row(f0(METADATA_FILE_NAME), "jack", f0(METADATA_FILE_SIZE), "uom"),
        Row(f1(METADATA_FILE_NAME), "lily", f1(METADATA_FILE_SIZE), "ucb")
      )
    )
  }

  metadataColumnsTest("read metadata struct fields with expressions", schema) { (df, f0, f1) =>
    checkAnswer(
      df.select(
        // substring of file name
        substring(col(METADATA_FILE_NAME), 1, 3),
        // format timestamp
        date_format(col(METADATA_FILE_MODIFICATION_TIME), "yyyy-MM")
          .as("_file_modification_year_month"),
        // convert to kb
        col(METADATA_FILE_SIZE).divide(lit(1024)).as("_file_size_kb"),
        // get the file format
        substring_index(col(METADATA_FILE_PATH), ".", -1).as("_file_format")
      ),
      Seq(
        Row(
          f0(METADATA_FILE_NAME).toString.substring(0, 3), // sql substring vs scala substring
          new SimpleDateFormat("yyyy-MM").format(f0(METADATA_FILE_MODIFICATION_TIME)),
          f0(METADATA_FILE_SIZE).asInstanceOf[Long] / 1024.toDouble,
          f0(METADATA_FILE_PATH).toString.split("\\.").takeRight(1).head
        ),
        Row(
          f1(METADATA_FILE_NAME).toString.substring(0, 3), // sql substring vs scala substring
          new SimpleDateFormat("yyyy-MM").format(f1(METADATA_FILE_MODIFICATION_TIME)),
          f1(METADATA_FILE_SIZE).asInstanceOf[Long] / 1024.toDouble,
          f1(METADATA_FILE_PATH).toString.split("\\.").takeRight(1).head
        )
      )
    )
  }

  metadataColumnsTest("select all will not select metadata struct fields", schema) { (df, _, _) =>
    checkAnswer(
      df.select("*"),
      Seq(
        Row("jack", 24, Row(12345L, "uom")),
        Row("lily", 31, Row(54321L, "ucb"))
      )
    )
  }

  metadataColumnsTest("metadata will not overwrite user data",
    schemaWithNameConflicts) { (df, _, _) =>
    // the user data has the schema: name, age, _metadata.id, _metadata.university

    // select user data
    checkAnswer(
      df.select("name", "age", "_METADATA", "_metadata"),
      Seq(
        Row("jack", 24, Row(12345L, "uom"), Row(12345L, "uom")),
        Row("lily", 31, Row(54321L, "ucb"), Row(54321L, "ucb"))
      )
    )

    // select metadata will fail when analysis
    checkError(
      exception = intercept[AnalysisException] {
        df.select("name", METADATA_FILE_NAME).collect()
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`file_name`", "fields" -> "`id`, `university`"),
      context =
        ExpectedContext(fragment = "select", callSitePattern = getCurrentClassCallSitePattern))
  }

  metadataColumnsTest("SPARK-42683: df metadataColumn - schema conflict",
    schemaWithNameConflicts) { (df, f0, f1) =>
    // the user data has the schema: name, age, _metadata.id, _metadata.university

    // select user data + metadata
    checkAnswer(
      df.select("name", "age", "_METADATA", "_metadata")
        .withColumn("file_name", df.metadataColumn("_metadata").getField("file_name")),
      Seq(
        Row("jack", 24, Row(12345L, "uom"), Row(12345L, "uom"), f0(METADATA_FILE_NAME)),
        Row("lily", 31, Row(54321L, "ucb"), Row(54321L, "ucb"), f1(METADATA_FILE_NAME))
      )
    )
  }

  metadataColumnsTest("SPARK-42683: df metadataColumn - no schema conflict",
    schema) { (df, f0, f1) =>

    // select user data + metadata
    checkAnswer(
      df.select("name", "age")
        .withColumn("file_name", df.metadataColumn("_metadata").getField("file_name")),
      Seq(
        Row("jack", 24, f0(METADATA_FILE_NAME)),
        Row("lily", 31, f1(METADATA_FILE_NAME))
      )
    )
  }

  metadataColumnsTest("SPARK-42683: df metadataColumn - manually renamed",
    schema) { (baseDf, f0, f1) =>

    // select renamed metadata column
    var df = baseDf.select(col("_metadata").as("renamed_metadata"))
    checkAnswer(
      df.select(df.metadataColumn("_metadata").getField("file_name")),
      Seq(
        Row(f0(METADATA_FILE_NAME)),
        Row(f1(METADATA_FILE_NAME))
      )
    )

    df = baseDf.withColumnRenamed("_metadata", "renamed_metadata")
    checkAnswer(
      df.select(df.metadataColumn("_metadata").getField("file_name")),
      Seq(
        Row(f0(METADATA_FILE_NAME)),
        Row(f1(METADATA_FILE_NAME))
      )
    )
  }

  metadataColumnsTest("SPARK-42683: df metadataColumn - column not found",
    schema) { (df, f0, f1) =>

    // Not a column at all
    checkError(
      exception = intercept[AnalysisException] {
        df.metadataColumn("foo")
      },
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`foo`", "proposal" -> "`_metadata`"))

    // Name exists, but does not reference a metadata column
    checkError(
      exception = intercept[AnalysisException] {
        df.metadataColumn("name")
      },
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`name`", "proposal" -> "`_metadata`"))
  }

  metadataColumnsTest("SPARK-42683: metadata name conflict resolved by leading underscores - one",
    schemaWithNameConflicts) { (df, f0, f1) =>
    // the user data has the schema: name, age, _metadata.id, _metadata.university

    checkAnswer(
      df.select("name", "age", "_metadata", "__metadata.file_name"),
      Seq(
        Row("jack", 24, Row(12345L, "uom"), f0(METADATA_FILE_NAME)),
        Row("lily", 31, Row(54321L, "ucb"), f1(METADATA_FILE_NAME))
      )
    )
  }

  metadataColumnsTest("SPARK-42683: metadata name conflict resolved by leading underscores - many",
    new StructType()
      .add(schema("name").copy(name = "_metadata"))
      .add(schema("age").copy(name = "__metadata"))
      .add(schema("info").copy(name = "___metadata"))) { (df, f0, f1) =>
    // the user data has the schema: _metadata, __metadata, ___metadata.id, ___metadata.university

    checkAnswer(
      df.select("_metadata", "__metadata", "___metadata", "____metadata.file_name"),
      Seq(
        Row("jack", 24, Row(12345L, "uom"), f0(METADATA_FILE_NAME)),
        Row("lily", 31, Row(54321L, "ucb"), f1(METADATA_FILE_NAME))
      )
    )
  }

  metadataColumnsTest("select only metadata", schema) { (df, f0, f1) =>
    checkAnswer(
      df.select(METADATA_FILE_NAME, METADATA_FILE_PATH,
        METADATA_FILE_SIZE, METADATA_FILE_BLOCK_START, METADATA_FILE_BLOCK_LENGTH,
        METADATA_FILE_MODIFICATION_TIME),
      Seq(
        Row(f0(METADATA_FILE_NAME), f0(METADATA_FILE_PATH),
          f0(METADATA_FILE_SIZE), f0(METADATA_FILE_BLOCK_START), f0(METADATA_FILE_BLOCK_LENGTH),
          f0(METADATA_FILE_MODIFICATION_TIME)),
        Row(f1(METADATA_FILE_NAME), f1(METADATA_FILE_PATH),
          f1(METADATA_FILE_SIZE), f1(METADATA_FILE_BLOCK_START), f1(METADATA_FILE_BLOCK_LENGTH),
          f1(METADATA_FILE_MODIFICATION_TIME))
      )
    )
    checkAnswer(
      df.select("_metadata"),
      Seq(
        Row(getMetadataRow(f0)),
        Row(getMetadataRow(f1))
      )
    )
  }

  metadataColumnsTest("select and re-select", schema) { (df, f0, f1) =>
    checkAnswer(
      df.select("name", "age", "info",
        METADATA_FILE_NAME, METADATA_FILE_PATH,
        METADATA_FILE_SIZE, METADATA_FILE_BLOCK_START, METADATA_FILE_BLOCK_LENGTH,
        METADATA_FILE_MODIFICATION_TIME)
        .select("name", "file_path"), // cast _metadata.file_path as file_path
      Seq(
        Row("jack", f0(METADATA_FILE_PATH)),
        Row("lily", f1(METADATA_FILE_PATH))
      )
    )
  }

  metadataColumnsTest("alias", schema) { (df, f0, f1) =>

    val aliasDF = df.select(
      Column("name").as("myName"),
      Column("age").as("myAge"),
      Column(METADATA_FILE_NAME).as("myFileName"),
      Column(METADATA_FILE_SIZE).as("myFileSize")
    )

    // check schema
    val expectedSchema = new StructType()
      .add(StructField("myName", StringType))
      .add(StructField("myAge", IntegerType))
      .add(StructField("myFileName", StringType, nullable = false))
      .add(StructField("myFileSize", LongType, nullable = false))

    assert(aliasDF.schema.fields.toSet == expectedSchema.fields.toSet)

    // check data
    checkAnswer(
      aliasDF,
      Seq(
        Row("jack", 24, f0(METADATA_FILE_NAME), f0(METADATA_FILE_SIZE)),
        Row("lily", 31, f1(METADATA_FILE_NAME), f1(METADATA_FILE_SIZE))
      )
    )
  }

  metadataColumnsTest("filter", schema) { (df, f0, f1) =>
    val filteredDF = df.select("name", "age", METADATA_FILE_NAME)
      .where(Column(METADATA_FILE_NAME) === f0(METADATA_FILE_NAME))

    // Check the filtered file.
    val partitions = filteredDF.queryExecution.sparkPlan.collectFirst {
      case p: FileSourceScanExec => p.selectedPartitions.filePartitionIterator.toSeq
    }.get

    assert(partitions.length == 1) // 1 partition
    assert(partitions.head.numFiles == 1) // 1 file in that partition
    // The file is f0.
    assert(partitions.head.files.toSeq.head.getPath.toString == f0(METADATA_FILE_PATH))

    // check result
    checkAnswer(
      filteredDF,
      Seq(
        // _file_name == f0's name, so we will only have 1 row
        Row("jack", 24, f0(METADATA_FILE_NAME))
      )
    )

    checkAnswer(
      df.where(s"$METADATA_FILE_SIZE > 0").select(METADATA_FILE_SIZE),
      Seq(
        Row(f0(METADATA_FILE_SIZE)),
        Row(f1(METADATA_FILE_SIZE)))
    )
    checkAnswer(
      df.where(s"$METADATA_FILE_SIZE > 0").select(METADATA_FILE_PATH),
      Seq(
        Row(f0(METADATA_FILE_PATH)),
        Row(f1(METADATA_FILE_PATH)))
    )
  }

  metadataColumnsTest("filter on metadata and user data", schema) { (df, _, f1) =>

    val filteredDF = df.select("name", "age", "info",
      METADATA_FILE_NAME, METADATA_FILE_PATH,
      METADATA_FILE_SIZE, METADATA_FILE_BLOCK_START, METADATA_FILE_BLOCK_LENGTH,
      METADATA_FILE_MODIFICATION_TIME)
      // mix metadata column + user column
      .where(Column(METADATA_FILE_NAME) === f1(METADATA_FILE_NAME) and Column("name") === "lily")
      // only metadata columns
      .where(Column(METADATA_FILE_PATH) === f1(METADATA_FILE_PATH))
      // only user column
      .where("age == 31")

    // Check the filtered file.
    val partitions = filteredDF.queryExecution.sparkPlan.collectFirst {
      case p: FileSourceScanExec => p.selectedPartitions.filePartitionIterator.toSeq
    }.get

    assert(partitions.length == 1) // 1 partition
    assert(partitions.head.numFiles == 1) // 1 file in that partition
    // The file is f0.
    assert(partitions.head.files.toSeq.head.getPath.toString == f1(METADATA_FILE_PATH))

    // check result
    checkAnswer(
      filteredDF,
      Seq(Row("lily", 31, Row(54321L, "ucb"),
        f1(METADATA_FILE_NAME), f1(METADATA_FILE_PATH),
        f1(METADATA_FILE_SIZE), f1(METADATA_FILE_BLOCK_START), f1(METADATA_FILE_BLOCK_LENGTH),
        f1(METADATA_FILE_MODIFICATION_TIME)))
    )
  }

  Seq(true, false).foreach { caseSensitive =>
    metadataColumnsTest(s"upper/lower case when case " +
      s"sensitive is $caseSensitive", schemaWithNameConflicts) { (df, f0, f1) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        // file schema: name, age, _METADATA.id, _METADATA.university

        if (caseSensitive) {
          // for case sensitive mode:
          // _METADATA is user data
          // _metadata is metadata
          checkAnswer(
            df.select("name", "age", "_METADATA", "_metadata"),
            Seq(
              Row("jack", 24, Row(12345L, "uom"),
                getMetadataRow(f0)),
              Row("lily", 31, Row(54321L, "ucb"),
                getMetadataRow(f1))
            )
          )
        } else {
          // for case insensitive mode:
          // _METADATA and _metadata are both user data

          // select user data
          checkAnswer(
            df.select("name", "age",
              // user columns
              "_METADATA", "_metadata",
              "_metadata.ID", "_METADATA.UniVerSity"),
            Seq(
              Row("jack", 24, Row(12345L, "uom"), Row(12345L, "uom"), 12345L, "uom"),
              Row("lily", 31, Row(54321L, "ucb"), Row(54321L, "ucb"), 54321L, "ucb")
            )
          )

          // select metadata will fail when analysis - metadata cannot overwrite user data
          checkError(
            exception = intercept[AnalysisException] {
              df.select("name", "_metadata.file_name").collect()
            },
            condition = "FIELD_NOT_FOUND",
            parameters = Map("fieldName" -> "`file_name`", "fields" -> "`id`, `university`"),
            context = ExpectedContext(
              fragment = "select",
              callSitePattern = getCurrentClassCallSitePattern))

          checkError(
            exception = intercept[AnalysisException] {
              df.select("name", "_METADATA.file_NAME").collect()
            },
            condition = "FIELD_NOT_FOUND",
            parameters = Map("fieldName" -> "`file_NAME`", "fields" -> "`id`, `university`"),
            context = ExpectedContext(
              fragment = "select",
              callSitePattern = getCurrentClassCallSitePattern))
        }
      }
    }
  }

  Seq("true", "false").foreach { offHeapColumnVectorEnabled =>
    withSQLConf("spark.sql.columnVector.offheap.enabled" -> offHeapColumnVectorEnabled) {
      metadataColumnsTest(s"read metadata with " +
        s"offheap set to $offHeapColumnVectorEnabled", schema) { (df, f0, f1) =>
        // read all available metadata struct fields
        checkAnswer(
          df.select("name", "age", "info",
            METADATA_FILE_NAME, METADATA_FILE_PATH,
            METADATA_FILE_SIZE, METADATA_FILE_BLOCK_START, METADATA_FILE_BLOCK_LENGTH,
            METADATA_FILE_MODIFICATION_TIME),
          Seq(
            Row("jack", 24, Row(12345L, "uom"), f0(METADATA_FILE_NAME), f0(METADATA_FILE_PATH),
              f0(METADATA_FILE_SIZE), f0(METADATA_FILE_BLOCK_START), f0(METADATA_FILE_BLOCK_LENGTH),
              f0(METADATA_FILE_MODIFICATION_TIME)),
            Row("lily", 31, Row(54321L, "ucb"), f1(METADATA_FILE_NAME), f1(METADATA_FILE_PATH),
              f1(METADATA_FILE_SIZE), f1(METADATA_FILE_BLOCK_START), f1(METADATA_FILE_BLOCK_LENGTH),
              f1(METADATA_FILE_MODIFICATION_TIME))
          )
        )

        // read a part of metadata struct fields
        checkAnswer(
          df.select("name", "info.university", METADATA_FILE_NAME, METADATA_FILE_SIZE),
          Seq(
            Row("jack", "uom", f0(METADATA_FILE_NAME), f0(METADATA_FILE_SIZE)),
            Row("lily", "ucb", f1(METADATA_FILE_NAME), f1(METADATA_FILE_SIZE))
          )
        )
      }
    }
  }

  Seq("true", "false").foreach { enabled =>
    withSQLConf("spark.sql.optimizer.nestedSchemaPruning.enabled" -> enabled) {
      metadataColumnsTest(s"read metadata with" +
        s"nestedSchemaPruning set to $enabled", schema) { (df, f0, f1) =>
        // read a part of data: schema pruning
        checkAnswer(
          df.select("name", "info.university", METADATA_FILE_NAME, METADATA_FILE_SIZE),
          Seq(
            Row("jack", "uom", f0(METADATA_FILE_NAME), f0(METADATA_FILE_SIZE)),
            Row("lily", "ucb", f1(METADATA_FILE_NAME), f1(METADATA_FILE_SIZE))
          )
        )
      }
    }
  }

  metadataColumnsTest("prune metadata schema in projects", schema) { (df, f0, f1) =>
    val prunedDF = df.select("name", "age", "info.id", METADATA_FILE_NAME)
    val fileSourceScanMetaCols = prunedDF.queryExecution.sparkPlan.collectFirst {
      case p: FileSourceScanExec => p.fileConstantMetadataColumns
    }.get
    assert(fileSourceScanMetaCols.size == 1)
    assert(fileSourceScanMetaCols.head.name == "file_name")

    checkAnswer(
      prunedDF,
      Seq(Row("jack", 24, 12345L, f0(METADATA_FILE_NAME)),
        Row("lily", 31, 54321L, f1(METADATA_FILE_NAME)))
    )
  }

  metadataColumnsTest("prune metadata schema in filters", schema) { (df, f0, f1) =>
    val prunedDF = df.select("name", "age", "info.id")
      .where(col(METADATA_FILE_PATH).contains("data/f0"))

    val fileSourceScanMetaCols = prunedDF.queryExecution.sparkPlan.collectFirst {
      case p: FileSourceScanExec => p.fileConstantMetadataColumns
    }.get
    assert(fileSourceScanMetaCols.size == 1)
    assert(fileSourceScanMetaCols.head.name == "file_path")

    checkAnswer(
      prunedDF,
      Seq(Row("jack", 24, 12345L))
    )
  }

  metadataColumnsTest("prune metadata schema in projects and filters", schema) { (df, f0, f1) =>
    val prunedDF = df.select("name", "age", "info.id", METADATA_FILE_SIZE)
      .where(col(METADATA_FILE_PATH).contains("data/f0"))

    val fileSourceScanMetaCols = prunedDF.queryExecution.sparkPlan.collectFirst {
      case p: FileSourceScanExec => p.fileConstantMetadataColumns
    }.get
    assert(fileSourceScanMetaCols.size == 2)
    assert(fileSourceScanMetaCols.map(_.name).toSet == Set("file_size", "file_path"))

    checkAnswer(
      prunedDF,
      Seq(Row("jack", 24, 12345L, f0(METADATA_FILE_SIZE)))
    )
  }

  metadataColumnsTest("write _metadata in parquet and read back", schema) { (df, f0, f1) =>
    // SPARK-38314: Selecting and then writing df containing hidden file
    // metadata column `_metadata` into parquet files will still keep the internal `Attribute`
    // metadata information of the column. It will then fail when read again.
    withTempDir { dir =>
      df.select("*", "_metadata")
        .write.format("parquet").save(dir.getCanonicalPath + "/new-data")

      val newDF = spark.read.format("parquet").load(dir.getCanonicalPath + "/new-data")

      // SELECT * will have: name, age, info, _metadata of f0 and f1
      checkAnswer(
        newDF.select("*"),
        Seq(
          Row("jack", 24, Row(12345L, "uom"), getMetadataRow(f0)),
          Row("lily", 31, Row(54321L, "ucb"), getMetadataRow(f1))
        )
      )

      // SELECT _metadata won't override the existing user data (_metadata of f0 and f1)
      checkAnswer(
        newDF.select("_metadata"),
        Seq(
          Row(getMetadataRow(f0)),
          Row(getMetadataRow(f1))
        )
      )
    }
  }

  metadataColumnsTest("file metadata in streaming", schema) { (df, _, _) =>
    withTempDir { dir =>
      df.coalesce(1).write.format("json").save(dir.getCanonicalPath + "/source/new-streaming-data")

      val streamDf = spark.readStream.format("json")
        .schema(schema)
        .load(dir.getCanonicalPath + "/source/new-streaming-data")
        .select("*", "_metadata")

      val streamQuery0 = streamDf
        .writeStream.format("json")
        .option("checkpointLocation", dir.getCanonicalPath + "/target/checkpoint")
        .trigger(Trigger.AvailableNow())
        .start(dir.getCanonicalPath + "/target/new-streaming-data")

      streamQuery0.awaitTermination()
      assert(streamQuery0.lastProgress.numInputRows == 2L)

      val newDF = spark.read.format("json")
        .load(dir.getCanonicalPath + "/target/new-streaming-data")

      val sourceFile = new File(dir, "/source/new-streaming-data").listFiles()
        .filter(_.getName.endsWith(".json")).head
      val sourceFileMetadata = Map(
        METADATA_FILE_PATH -> sourceFile.toURI.toString,
        METADATA_FILE_NAME -> sourceFile.getName,
        METADATA_FILE_SIZE -> sourceFile.length(),
        METADATA_FILE_BLOCK_START -> 0,
        METADATA_FILE_BLOCK_LENGTH -> sourceFile.length(),
        METADATA_FILE_MODIFICATION_TIME -> new Timestamp(sourceFile.lastModified())
      )

      // SELECT * will have: name, age, info, _metadata of /source/new-streaming-data
      assert(newDF.select("*").columns.toSet == Set("name", "age", "info", "_metadata"))
      // Verify the data is expected
      checkAnswer(
        newDF.select(col("name"), col("age"), col("info"),
          col(METADATA_FILE_PATH), col(METADATA_FILE_NAME),
          col(METADATA_FILE_SIZE), col(METADATA_FILE_BLOCK_START), col(METADATA_FILE_BLOCK_LENGTH),
          // since we are writing _metadata to a json file,
          // we should explicitly cast the column to timestamp type
          to_timestamp(col(METADATA_FILE_MODIFICATION_TIME))),
        Seq(
          Row(
            "jack", 24, Row(12345L, "uom"),
            sourceFileMetadata(METADATA_FILE_PATH),
            sourceFileMetadata(METADATA_FILE_NAME),
            sourceFileMetadata(METADATA_FILE_SIZE),
            sourceFileMetadata(METADATA_FILE_BLOCK_START),
            sourceFileMetadata(METADATA_FILE_BLOCK_LENGTH),
            sourceFileMetadata(METADATA_FILE_MODIFICATION_TIME)),
          Row(
            "lily", 31, Row(54321L, "ucb"),
            sourceFileMetadata(METADATA_FILE_PATH),
            sourceFileMetadata(METADATA_FILE_NAME),
            sourceFileMetadata(METADATA_FILE_SIZE),
            sourceFileMetadata(METADATA_FILE_BLOCK_START),
            sourceFileMetadata(METADATA_FILE_BLOCK_LENGTH),
            sourceFileMetadata(METADATA_FILE_MODIFICATION_TIME))
        )
      )

      checkAnswer(
        newDF.where(s"$METADATA_FILE_SIZE > 0").select(METADATA_FILE_SIZE),
        Seq(
          Row(sourceFileMetadata(METADATA_FILE_SIZE)),
          Row(sourceFileMetadata(METADATA_FILE_SIZE)))
      )
      checkAnswer(
        newDF.where(s"$METADATA_FILE_SIZE > 0").select(METADATA_FILE_PATH),
        Seq(
          Row(sourceFileMetadata(METADATA_FILE_PATH)),
          Row(sourceFileMetadata(METADATA_FILE_PATH)))
      )

      // Verify self-union
      val streamQuery1 = streamDf.union(streamDf)
        .writeStream.format("json")
        .option("checkpointLocation", dir.getCanonicalPath + "/target/checkpoint_union")
        .trigger(Trigger.AvailableNow())
        .start(dir.getCanonicalPath + "/target/new-streaming-data-union")
      streamQuery1.awaitTermination()
      val df1 = spark.read.format("json")
        .load(dir.getCanonicalPath + "/target/new-streaming-data-union")
      // Verify self-union results
      assert(streamQuery1.lastProgress.numInputRows == 4L)
      assert(df1.count() == 4L)
      assert(df1.select("*").columns.toSet == Set("name", "age", "info", "_metadata"))

      // Verify self-join
      val streamQuery2 = streamDf.join(streamDf, Seq("name", "age", "info", "_metadata"))
        .writeStream.format("json")
        .option("checkpointLocation", dir.getCanonicalPath + "/target/checkpoint_join")
        .trigger(Trigger.AvailableNow())
        .start(dir.getCanonicalPath + "/target/new-streaming-data-join")
      streamQuery2.awaitTermination()
      val df2 = spark.read.format("json")
        .load(dir.getCanonicalPath + "/target/new-streaming-data-join")
      // Verify self-join results
      assert(streamQuery2.lastProgress.numInputRows == 2L)
      assert(df2.count() == 2L)
      assert(df2.select("*").columns.toSet == Set("name", "age", "info", "_metadata"))
    }
  }

  Seq(true, false).foreach { useVectorizedReader =>
    val label = if (useVectorizedReader) "reading batches" else "reading rows"
    test(s"SPARK-39806: metadata for a partitioned table ($label)") {
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> useVectorizedReader.toString) {
        withTempPath { dir =>
          // Store dynamically partitioned data.
          Seq(1 -> 1).toDF("a", "b").write.format("parquet").partitionBy("b")
            .save(dir.getAbsolutePath)

          // Identify the data file and its metadata.
          val file = TestUtils.recursiveList(dir)
            .filter(_.getName.endsWith(".parquet")).head
          val expectedDf = Seq(1 -> 1).toDF("a", "b")
            .withColumn(FileFormat.FILE_NAME, lit(file.getName))
            .withColumn(FileFormat.FILE_SIZE, lit(file.length()))

          checkAnswer(spark.read.parquet(dir.getAbsolutePath)
            .select("*", METADATA_FILE_NAME, METADATA_FILE_SIZE), expectedDf)
        }
      }
    }
  }

  Seq("parquet", "orc").foreach { format =>
    test(s"SPARK-40918: Output cols around WSCG.isTooManyFields limit in $format") {
      // The issue was that ParquetFileFormat would not count the _metadata columns towards
      // the WholeStageCodegenExec.isTooManyFields limit, while FileSourceScanExec would,
      // resulting in Parquet reader returning columnar output, while scan expected row.
      withTempPath { dir =>
        sql(s"SELECT ${(1 to 100).map(i => s"id+$i as c$i").mkString(", ")} FROM RANGE(100)")
          .write.format(format).save(dir.getAbsolutePath)
        (98 to 102).foreach { wscgCols =>
          withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> wscgCols.toString) {
            // Would fail with
            // java.lang.ClassCastException: org.apache.spark.sql.vectorized.ColumnarBatch
            // cannot be cast to org.apache.spark.sql.catalyst.InternalRow
            sql(
              s"""
                 |SELECT
                 |  ${(1 to 100).map(i => s"sum(c$i)").mkString(", ")},
                 |  max(_metadata.file_path)
                 |FROM $format.`$dir`""".stripMargin
            ).collect()
          }
        }
      }
    }
  }

  metadataColumnsTest("SPARK-41151: consistent _metadata nullability " +
    "between analyzed and executed", schema) { (df, _, _) =>
    val queryExecution = df.select("_metadata").queryExecution
    val analyzedSchema = queryExecution.analyzed.schema
    val executedSchema = queryExecution.executedPlan.schema
    // For stateful streaming, we store the schema in the state store
    // and check consistency across batches.
    // To avoid state schema compatibility mismatched,
    // we should keep nullability consistent for _metadata struct
    assert(analyzedSchema.fields.head.name == "_metadata")
    assert(executedSchema.fields.head.name == "_metadata")

    // Metadata struct is not nullable
    assert(!analyzedSchema.fields.head.nullable)
    assert(analyzedSchema.fields.head.nullable == executedSchema.fields.head.nullable)

    // All sub-fields all not nullable
    val analyzedStruct = analyzedSchema.fields.head.dataType.asInstanceOf[StructType]
    val executedStruct = executedSchema.fields.head.dataType.asInstanceOf[StructType]
    assert(analyzedStruct.fields.forall(!_.nullable), analyzedStruct.fields.mkString(", "))
    assert(executedStruct.fields.forall(!_.nullable), executedStruct.fields.mkString(", "))
  }

  test("SPARK-41896: Filter on row_index and a stored column at the same time") {
    withTempPath { dir =>
      val storedIdName = "stored_id"
      val storedIdUpperLimitExclusive = 520
      val rowIndexLowerLimitInclusive = 10

      spark.range(start = 500, end = 600)
        .toDF(storedIdName)
        .write
        .format("parquet")
        .save(dir.getAbsolutePath)

      // Select stored_id 510 to 519 via a stored_id and row_index filter.
      val collectedRows = spark.read.load(dir.getAbsolutePath)
        .select(storedIdName, METADATA_ROW_INDEX)
        .where(col(storedIdName).lt(lit(storedIdUpperLimitExclusive)))
        .where(col(METADATA_ROW_INDEX).geq(lit(rowIndexLowerLimitInclusive)))
        .collect()

      assert(collectedRows.length === 10)
      assert(collectedRows.forall(_.getLong(0) < storedIdUpperLimitExclusive))
      assert(collectedRows.forall(_.getLong(1) >= rowIndexLowerLimitInclusive))
    }
  }

  test("SPARK-41896: Filter on constant and generated metadata attributes at the same time") {
    withTempPath { dir =>
      val idColumnName = "id"
      val partitionColumnName = "partition"
      val numFiles = 4
      val totalNumRows = 40

      spark.range(end = totalNumRows)
        .toDF(idColumnName)
        .withColumn(partitionColumnName, col(idColumnName).mod(lit(numFiles)))
        .write
        .partitionBy(partitionColumnName)
        .format("parquet")
        .save(dir.getAbsolutePath)

      // Get one file path.
      val randomTableFilePath = spark.read.load(dir.getAbsolutePath)
        .select(METADATA_FILE_PATH).collect().head.getString(0)

      // Select half the rows from one file.
      val halfTheNumberOfRowsPerFile = totalNumRows / (numFiles * 2)
      val collectedRows = spark.read.load(dir.getAbsolutePath)
        .select(METADATA_FILE_PATH, METADATA_ROW_INDEX)
        .where(col(METADATA_FILE_PATH).equalTo(lit(randomTableFilePath)))
        .where(col(METADATA_ROW_INDEX).leq(lit(halfTheNumberOfRowsPerFile)))
        .collect()

      // Assert we only select rows from one file.
      assert(collectedRows.map(_.getString(0)).distinct.length === 1)
      // Assert we filtered by row index.
      assert(collectedRows.forall(row => row.getLong(1) < halfTheNumberOfRowsPerFile))
      assert(collectedRows.length === halfTheNumberOfRowsPerFile)
    }
  }

  test("SPARK-41896: Filter by a function that takes the metadata struct as argument") {
    withTempPath { dir =>
      val idColumnName = "id"
      val numFiles = 4
      spark.range(end = numFiles)
        .toDF(idColumnName)
        .withColumn("partition", col(idColumnName))
        .write
        .format("parquet")
        .partitionBy("partition")
        .save(dir.getAbsolutePath)

      // Select path and partition value for a random file.
      val testFileData = spark.read.load(dir.getAbsolutePath)
        .select(idColumnName, METADATA_FILE_PATH).collect().head
      val testFilePartition = testFileData.getLong(0)
      val testFilePath = testFileData.getString(1)

      val filterFunctionName = "isTestFile"
      withUserDefinedFunction(filterFunctionName -> true) {
        // Create and use a filter using the file path.
        spark.udf.register(filterFunctionName,
          (metadata: Row) => {
            metadata.getAs[String]("file_path") == testFilePath
          })
        val udfFilterResult = spark.read.load(dir.getAbsolutePath)
          .select(idColumnName, METADATA_FILE_PATH)
          .where(s"$filterFunctionName(_metadata)")
          .collect().head

        assert(testFilePartition === udfFilterResult.getLong(0))
        assert(testFilePath === udfFilterResult.getString(1))
      }
    }
  }

  test("SPARK-42423: Add metadata column file block start and length") {
    withSQLConf(
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1") {
      withTempPath { path =>
        spark.range(2).write.json(path.getCanonicalPath)
        assert(path.listFiles().count(_.getName.endsWith("json")) == 1)

        val df = spark.read.json(path.getCanonicalPath)
          .select("id", METADATA_FILE_BLOCK_START, METADATA_FILE_BLOCK_LENGTH)
        assert(df.rdd.partitions.length > 1)
        val res = df.collect()
        assert(res.length == 2)
        assert(res.head.getLong(0) == 0) // id
        assert(res.head.getLong(1) == 0) // file_block_start
        assert(res.head.getLong(2) > 0) // file_block_length
        assert(res(1).getLong(0) == 1L) // id
        assert(res(1).getLong(1) > 0) // file_block_start
        assert(res(1).getLong(2) > 0) // file_block_length

        // make sure `_metadata.file_block_start` and `_metadata.file_block_length` does not affect
        // pruning listed files
        val df2 = spark.read.json(path.getCanonicalPath)
          .where("_metadata.File_bLoCk_start > 0 and _metadata.file_block_length > 0 " +
            "and _metadata.file_SizE > 0")
          .select("id", METADATA_FILE_BLOCK_START, METADATA_FILE_BLOCK_LENGTH)
        val fileSourceScan2 = df2.queryExecution.sparkPlan.collectFirst {
          case p: FileSourceScanExec => p
        }.get
        // Assert that there's 1 selected partition with 1 file.
        assert(fileSourceScan2.selectedPartitions.partitionCount == 1)
        assert(fileSourceScan2.selectedPartitions.totalNumberOfFiles == 1)
        val res2 = df2.collect()
        assert(res2.length == 1)
        assert(res2.head.getLong(0) == 1L) // id
        assert(res2.head.getLong(1) > 0) // file_block_start
        assert(res2.head.getLong(2) > 0) // file_block_length

        // make sure `_metadata.file_size > 1000000` still work for pruning listed files
        val df3 = spark.read.json(path.getCanonicalPath)
          .where("_metadata.File_bLoCk_start > 0 and _metadata.file_SizE > 1000000")
          .select("id", METADATA_FILE_BLOCK_START, METADATA_FILE_BLOCK_LENGTH)
        val fileSourceScan3 = df3.queryExecution.sparkPlan.collectFirst {
          case p: FileSourceScanExec => p
        }.get
        // Assert that there's 1 selected partition with no files.
        assert(fileSourceScan3.selectedPartitions.partitionCount == 1)
        assert(fileSourceScan3.selectedPartitions.totalNumberOfFiles == 0)
        assert(df3.collect().isEmpty)
      }
    }
  }


  Seq("parquet", "json", "csv", "text", "orc").foreach { format =>
    test(s"metadata file path is url encoded for format: $format") {
      withTempPath { f =>
        val dirWithSpace = s"$f/with space"
        spark.range(10)
          .selectExpr("cast(id as string) as str")
          .repartition(1)
          .write
          .format(format)
          .mode("append")
          .save(dirWithSpace)

        val encodedPath = SparkPath.fromPathString(dirWithSpace).urlEncoded
        val df = spark.read.format(format).load(dirWithSpace)
        val metadataPath = df.select(METADATA_FILE_PATH).as[String].head()
        assert(metadataPath.contains(encodedPath))
      }
    }

    test(s"metadata file name is url encoded for format: $format") {
      val suffix = if (format == "text") ".txt" else s".$format"
      withTempPath { f =>
        val dirWithSpace = s"$f/with space"
        spark.range(10)
          .selectExpr("cast(id as string) as str")
          .repartition(1)
          .write
          .format(format)
          .mode("append")
          .save(dirWithSpace)

        val pathWithSpace = s"$dirWithSpace/file with space.$suffix"
        new File(dirWithSpace)
          .listFiles((_, f) => f.endsWith(suffix))
          .headOption
          .getOrElse(fail(s"no file with suffix $suffix in $dirWithSpace"))
          .renameTo(new File(pathWithSpace))

        val encodedPath = SparkPath.fromPathString(pathWithSpace).urlEncoded
        val encodedName = encodedPath.split("/").last
        val df = spark.read.format(format).load(dirWithSpace)
        val metadataName = df.select(METADATA_FILE_NAME).as[String].head()
        assert(metadataName == encodedName)
      }
    }
  }

  test("SPARK-43422: Keep tags during optimization when adding metadata columns") {
    withTempPath { path =>
      spark.range(end = 10).write.format("parquet").save(path.toString)

      // Add the tag to the base Dataframe before selecting a metadata column.
      val customTag = TreeNodeTag[Unit]("customTag")
      val baseDf = spark.read.format("parquet").load(path.toString)
      val tagsPut = baseDf.queryExecution.analyzed.collect {
        case rel: LogicalRelation => rel.setTagValue(customTag, ())
      }

      assert(tagsPut.nonEmpty)

      val dfWithMetadata = baseDf.select("_metadata.row_index")

      // Expect the tag in the analyzed and optimized plan after querying a metadata column.
      def isTaggedRelation(plan: LogicalPlan): Boolean = plan match {
        case rel: LogicalRelation => rel.getTagValue(customTag).isDefined
        case _ => false
      }

      assert(dfWithMetadata.queryExecution.analyzed.exists(isTaggedRelation))
      assert(dfWithMetadata.queryExecution.optimizedPlan.exists(isTaggedRelation))
    }
  }

  test("SPARK-43450: Filter on full _metadata column struct") {
    withTempPath { dir =>
      val numRows = 10
      spark.range(end = numRows)
        .toDF()
        .write
        .format("parquet")
        .save(dir.getAbsolutePath)

      // Get the metadata of a random row. The metadata is unique per row because of row_index.
      val metadataColumnRow = spark.read.load(dir.getAbsolutePath)
        .select("_metadata")
        .collect()
        .head
        .getStruct(0)

      // Transform the result into a literal that can be used in an expression.
      val metadataColumnFields = metadataColumnRow.schema.fields
        .map(field => lit(metadataColumnRow.getAs[Any](field.name)).as(field.name))
      import org.apache.spark.util.ArrayImplicits._
      val metadataColumnStruct = struct(metadataColumnFields.toImmutableArraySeq: _*)

      val selectSingleRowDf = spark.read.load(dir.getAbsolutePath)
        .where(col("_metadata").equalTo(lit(metadataColumnStruct)))

      assert(selectSingleRowDf.count() === 1)
    }
  }

  test("SPARK-43450: Is not null filter on _metadata column") {
    withTempPath { dir =>
      val numRows = 10
      spark.range(start = 0, end = numRows, step = 1, numPartitions = 1)
        .toDF()
        .write
        .format("parquet")
        .save(dir.getAbsolutePath)

      // There is only one file, so we will select all rows.
      val selectAllDf = spark.read.load(dir.getAbsolutePath)
        .where(not(isnull(col("_metadata"))))

      assert(selectAllDf.count() === numRows)
    }
  }

  test("SPARK-43450: Filter on aliased _metadata.row_index") {
    withTempPath { dir =>
      val numRows = 10
      spark.range(start = 0, end = numRows, step = 1, numPartitions = 1)
        .toDF()
        .write
        .format("parquet")
        .save(dir.getAbsolutePath)

      // There is only one file, so row_index is unique.
      val selectSingleRowDf = spark.read.load(dir.getAbsolutePath)
        .select(col("_metadata"), col("_metadata.row_index").as("renamed_row_index"))
        .where(col("renamed_row_index").equalTo(lit(0)))

      assert(selectSingleRowDf.count() === 1)
    }
  }
}
