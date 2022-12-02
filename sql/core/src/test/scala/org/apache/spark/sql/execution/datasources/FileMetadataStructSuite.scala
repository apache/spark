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
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, QueryTest, Row}
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

  private val METADATA_FILE_MODIFICATION_TIME = "_metadata.file_modification_time"

  private val METADATA_ROW_INDEX = "_metadata.row_index"

  private val FILE_FORMAT = "fileFormat"

  private def getMetadataRow(f: Map[String, Any]): Row = f(FILE_FORMAT) match {
    case "parquet" =>
      Row(f(METADATA_FILE_PATH), f(METADATA_FILE_NAME), f(METADATA_FILE_SIZE),
        f(METADATA_FILE_MODIFICATION_TIME), f(METADATA_ROW_INDEX))
    case _ =>
      Row(f(METADATA_FILE_PATH), f(METADATA_FILE_NAME), f(METADATA_FILE_SIZE),
        f(METADATA_FILE_MODIFICATION_TIME))
  }

  private def getMetadataForFile(f: File): Map[String, Any] = {
    Map(
      METADATA_FILE_PATH -> f.toURI.toString,
      METADATA_FILE_NAME -> f.getName,
      METADATA_FILE_SIZE -> f.length(),
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
          import scala.collection.JavaConverters._

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
        METADATA_FILE_SIZE, METADATA_FILE_MODIFICATION_TIME),
      Seq(
        Row("jack", 24, Row(12345L, "uom"),
          f0(METADATA_FILE_NAME), f0(METADATA_FILE_PATH),
          f0(METADATA_FILE_SIZE), f0(METADATA_FILE_MODIFICATION_TIME)),
        Row("lily", 31, Row(54321L, "ucb"),
          f1(METADATA_FILE_NAME), f1(METADATA_FILE_PATH),
          f1(METADATA_FILE_SIZE), f1(METADATA_FILE_MODIFICATION_TIME))
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
      errorClass = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`file_name`", "fields" -> "`id`, `university`"))
  }

  metadataColumnsTest("select only metadata", schema) { (df, f0, f1) =>
    checkAnswer(
      df.select(METADATA_FILE_NAME, METADATA_FILE_PATH,
        METADATA_FILE_SIZE, METADATA_FILE_MODIFICATION_TIME),
      Seq(
        Row(f0(METADATA_FILE_NAME), f0(METADATA_FILE_PATH),
          f0(METADATA_FILE_SIZE), f0(METADATA_FILE_MODIFICATION_TIME)),
        Row(f1(METADATA_FILE_NAME), f1(METADATA_FILE_PATH),
          f1(METADATA_FILE_SIZE), f1(METADATA_FILE_MODIFICATION_TIME))
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
        METADATA_FILE_SIZE, METADATA_FILE_MODIFICATION_TIME)
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
      .add(StructField("myFileName", StringType))
      .add(StructField("myFileSize", LongType))

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

  metadataColumnsTest("filter", schema) { (df, f0, _) =>
    val filteredDF = df.select("name", "age", METADATA_FILE_NAME)
      .where(Column(METADATA_FILE_NAME) === f0(METADATA_FILE_NAME))

    // check the filtered file
    val partitions = filteredDF.queryExecution.sparkPlan.collectFirst {
      case p: FileSourceScanExec => p.selectedPartitions
    }.get

    assert(partitions.length == 1) // 1 partition
    assert(partitions.head.files.length == 1) // 1 file in that partition
    assert(partitions.head.files.head.getPath.toString == f0(METADATA_FILE_PATH)) // the file is f0

    // check result
    checkAnswer(
      filteredDF,
      Seq(
        // _file_name == f0's name, so we will only have 1 row
        Row("jack", 24, f0(METADATA_FILE_NAME))
      )
    )
  }

  metadataColumnsTest("filter on metadata and user data", schema) { (df, _, f1) =>

    val filteredDF = df.select("name", "age", "info",
      METADATA_FILE_NAME, METADATA_FILE_PATH,
      METADATA_FILE_SIZE, METADATA_FILE_MODIFICATION_TIME)
      // mix metadata column + user column
      .where(Column(METADATA_FILE_NAME) === f1(METADATA_FILE_NAME) and Column("name") === "lily")
      // only metadata columns
      .where(Column(METADATA_FILE_PATH) === f1(METADATA_FILE_PATH))
      // only user column
      .where("age == 31")

    // check the filtered file
    val partitions = filteredDF.queryExecution.sparkPlan.collectFirst {
      case p: FileSourceScanExec => p.selectedPartitions
    }.get

    assert(partitions.length == 1) // 1 partition
    assert(partitions.head.files.length == 1) // 1 file in that partition
    assert(partitions.head.files.head.getPath.toString == f1(METADATA_FILE_PATH)) // the file is f1

    // check result
    checkAnswer(
      filteredDF,
      Seq(Row("lily", 31, Row(54321L, "ucb"),
        f1(METADATA_FILE_NAME), f1(METADATA_FILE_PATH),
        f1(METADATA_FILE_SIZE), f1(METADATA_FILE_MODIFICATION_TIME)))
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
            errorClass = "FIELD_NOT_FOUND",
            parameters = Map("fieldName" -> "`file_name`", "fields" -> "`id`, `university`"))

          checkError(
            exception = intercept[AnalysisException] {
              df.select("name", "_METADATA.file_NAME").collect()
            },
            errorClass = "FIELD_NOT_FOUND",
            parameters = Map("fieldName" -> "`file_NAME`", "fields" -> "`id`, `university`"))
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
            METADATA_FILE_SIZE, METADATA_FILE_MODIFICATION_TIME),
          Seq(
            Row("jack", 24, Row(12345L, "uom"), f0(METADATA_FILE_NAME), f0(METADATA_FILE_PATH),
              f0(METADATA_FILE_SIZE), f0(METADATA_FILE_MODIFICATION_TIME)),
            Row("lily", 31, Row(54321L, "ucb"), f1(METADATA_FILE_NAME), f1(METADATA_FILE_PATH),
              f1(METADATA_FILE_SIZE), f1(METADATA_FILE_MODIFICATION_TIME))
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
      case p: FileSourceScanExec => p.metadataColumns
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
      case p: FileSourceScanExec => p.metadataColumns
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
      case p: FileSourceScanExec => p.metadataColumns
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
        METADATA_FILE_MODIFICATION_TIME -> new Timestamp(sourceFile.lastModified())
      )

      // SELECT * will have: name, age, info, _metadata of /source/new-streaming-data
      assert(newDF.select("*").columns.toSet == Set("name", "age", "info", "_metadata"))
      // Verify the data is expected
      checkAnswer(
        newDF.select(col("name"), col("age"), col("info"),
          col(METADATA_FILE_PATH), col(METADATA_FILE_NAME),
          // since we are writing _metadata to a json file,
          // we should explicitly cast the column to timestamp type
          col(METADATA_FILE_SIZE), to_timestamp(col(METADATA_FILE_MODIFICATION_TIME))),
        Seq(
          Row(
            "jack", 24, Row(12345L, "uom"),
            sourceFileMetadata(METADATA_FILE_PATH),
            sourceFileMetadata(METADATA_FILE_NAME),
            sourceFileMetadata(METADATA_FILE_SIZE),
            sourceFileMetadata(METADATA_FILE_MODIFICATION_TIME)),
          Row(
            "lily", 31, Row(54321L, "ucb"),
            sourceFileMetadata(METADATA_FILE_PATH),
            sourceFileMetadata(METADATA_FILE_NAME),
            sourceFileMetadata(METADATA_FILE_SIZE),
            sourceFileMetadata(METADATA_FILE_MODIFICATION_TIME))
        )
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
    assert(analyzedSchema.fields.head.name == "_metadata")
    assert(executedSchema.fields.head.name == "_metadata")
    // For stateful streaming, we store the schema in the state store
    // and check consistency across batches.
    // To avoid state schema compatibility mismatched,
    // we should keep nullability consistent for _metadata struct
    assert(!analyzedSchema.fields.head.nullable)
    assert(analyzedSchema.fields.head.nullable == executedSchema.fields.head.nullable)
  }
}
