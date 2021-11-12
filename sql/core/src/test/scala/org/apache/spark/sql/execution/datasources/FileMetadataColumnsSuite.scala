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
import java.nio.file.Files
import java.text.SimpleDateFormat

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

class FileMetadataColumnsSuite extends QueryTest with SharedSparkSession {

  val data0: String =
    """
      |jack,24,12345,uom
      |""".stripMargin

  val data1: String =
    """
      |lily,31,,ucb
      |""".stripMargin

  val schema: StructType = new StructType()
    .add(StructField("name", StringType))
    .add(StructField("age", IntegerType))
    .add(StructField("id", LongType))
    .add(StructField("university", StringType))

  val schemaWithNameConflicts: StructType = new StructType()
    .add(StructField("name", StringType))
    .add(StructField("age", IntegerType))
    .add(StructField("_metadata.file_size", LongType))
    .add(StructField("_metadata.FILE_NAME", StringType))

  private val METADATA_FILE_PATH = "_metadata.file_path"

  private val METADATA_FILE_NAME = "_metadata.file_name"

  private val METADATA_FILE_SIZE = "_metadata.file_size"

  private val METADATA_FILE_MODIFICATION_TIME = "_metadata.file_modification_time"

  /**
   * Create a CSV file named `fileName` with `data` under `dir` directory.
   */
  private def createCSVFile(data: String, dir: File, fileName: String): String = {
    val dataFile = new File(dir, s"/$fileName")
    dataFile.getParentFile.mkdirs()
    val bytes = data.getBytes()
    Files.write(dataFile.toPath, bytes)
    dataFile.getCanonicalPath
  }

  /**
   * This test wrapper will test for both row-based and column-based file formats (csv and parquet)
   * 1. read data0 and data1 and write them as testFileFormat: f0 and f1
   * 2. read both f0 and f1, return the df to the downstream for further testing
   * 3. construct actual metadata map for both f0 and f1 to the downstream for further testing
   *
   * The final df will have data:
   * jack | 24 | 12345 | uom
   * lily | 31 | null | ucb
   *
   * The schema of the df will be the `fileSchema` provided to this method
   *
   * This test wrapper will provide a `df` and actual metadata map `f0`, `f1`
   */
  private def metadataColumnsTest(
      testName: String, fileSchema: StructType)
    (f: (DataFrame, Map[String, Any], Map[String, Any]) => Unit): Unit = {
    Seq("csv", "parquet").foreach { testFileFormat =>
      test(s"metadata columns ($testFileFormat): " + testName) {
        withTempDir { dir =>
          // read data0 as CSV and write f0 as testFileFormat
          val df0 = spark.read.schema(fileSchema).csv(
            createCSVFile(data0, dir, "temp/0")
          )
          val f0Path = new File(dir, "data/f0").getCanonicalPath
          df0.coalesce(1).write.format(testFileFormat).save(f0Path)

          // read data1 as CSV and write f1 as testFileFormat
          val df1 = spark.read.schema(fileSchema).csv(
            createCSVFile(data1, dir, "temp/1")
          )
          val f1Path = new File(dir, "data/f1").getCanonicalPath
          df1.coalesce(1).write.format(testFileFormat).save(f1Path)

          // read both f0 and f1
          val df = spark.read.format(testFileFormat).schema(fileSchema)
            .load(new File(dir, "data").getCanonicalPath + "/*")

          val realF0 = new File(dir, "data/f0").listFiles()
            .filter(_.getName.endsWith(s".$testFileFormat")).head

          val realF1 = new File(dir, "data/f1").listFiles()
            .filter(_.getName.endsWith(s".$testFileFormat")).head

          // construct f0 and f1 metadata data
          val f0Metadata = Map(
            METADATA_FILE_PATH -> realF0.toURI.toString,
            METADATA_FILE_NAME -> realF0.getName,
            METADATA_FILE_SIZE -> realF0.length(),
            METADATA_FILE_MODIFICATION_TIME -> realF0.lastModified()
          )
          val f1Metadata = Map(
            METADATA_FILE_PATH -> realF1.toURI.toString,
            METADATA_FILE_NAME -> realF1.getName,
            METADATA_FILE_SIZE -> realF1.length(),
            METADATA_FILE_MODIFICATION_TIME -> realF1.lastModified()
          )

          f(df, f0Metadata, f1Metadata)
        }
      }
    }
  }

  metadataColumnsTest("read partial/all metadata columns", schema) { (df, f0, f1) =>
    // read all available metadata columns
    checkAnswer(
      df.select("name", "age", "id", "university",
        METADATA_FILE_NAME, METADATA_FILE_PATH,
        METADATA_FILE_SIZE, METADATA_FILE_MODIFICATION_TIME),
      Seq(
        Row("jack", 24, 12345L, "uom", f0(METADATA_FILE_NAME), f0(METADATA_FILE_PATH),
          f0(METADATA_FILE_SIZE), f0(METADATA_FILE_MODIFICATION_TIME)),
        Row("lily", 31, null, "ucb", f1(METADATA_FILE_NAME), f1(METADATA_FILE_PATH),
          f1(METADATA_FILE_SIZE), f1(METADATA_FILE_MODIFICATION_TIME))
      )
    )

    // read a part of metadata columns
    checkAnswer(
      df.select("name", "university", METADATA_FILE_NAME, METADATA_FILE_SIZE),
      Seq(
        Row("jack", "uom", f0(METADATA_FILE_NAME), f0(METADATA_FILE_SIZE)),
        Row("lily", "ucb", f1(METADATA_FILE_NAME), f1(METADATA_FILE_SIZE))
      )
    )
  }

  metadataColumnsTest("read metadata columns with random ordering", schema) { (df, f0, f1) =>
    // read a part of metadata columns with random ordering
    checkAnswer(
      df.select(METADATA_FILE_NAME, "name", METADATA_FILE_SIZE, "university"),
      Seq(
        Row(f0(METADATA_FILE_NAME), "jack", f0(METADATA_FILE_SIZE), "uom"),
        Row(f1(METADATA_FILE_NAME), "lily", f1(METADATA_FILE_SIZE), "ucb")
      )
    )
  }

  metadataColumnsTest("read metadata columns with expressions", schema) { (df, f0, f1) =>
    checkAnswer(
      df.select(
        // substring of file name
        substring(col(METADATA_FILE_NAME), 1, 3),
        // convert timestamp in millis to unixtime and to date format
        from_unixtime(col(METADATA_FILE_MODIFICATION_TIME).divide(lit(1000)), "yyyy-MM")
          .as("_file_modification_date"),
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

  metadataColumnsTest("select all will not select metadata columns", schema) { (df, _, _) =>
    checkAnswer(
      df.select("*"),
      Seq(
        Row("jack", 24, 12345L, "uom"),
        Row("lily", 31, null, "ucb")
      )
    )
  }

  metadataColumnsTest("metadata columns will not " +
    "overwrite user data schema", schemaWithNameConflicts) { (df, f0, f1) =>
    // here: the data has the schema: name, age, _metadata.file_size, _metadata.file_name
    checkAnswer(
      df.select("name", "age", "`_metadata.file_size`", "`_metadata.FILE_NAME`",
        METADATA_FILE_NAME, METADATA_FILE_PATH,
        METADATA_FILE_SIZE, METADATA_FILE_MODIFICATION_TIME),
      Seq(
        Row("jack", 24, 12345L, "uom",
          // uom and 12345L will not be overwritten,
          // and we still can read metadata columns correctly
          f0(METADATA_FILE_NAME), f0(METADATA_FILE_PATH),
          f0(METADATA_FILE_SIZE), f0(METADATA_FILE_MODIFICATION_TIME)),
        Row("lily", 31, null, "ucb",
          // ucb and `null` will not be overwritten,
          // and we still can read metadata columns correctly
          f1(METADATA_FILE_NAME), f1(METADATA_FILE_PATH),
          f1(METADATA_FILE_SIZE), f1(METADATA_FILE_MODIFICATION_TIME))
      )
    )
  }

  metadataColumnsTest("select only metadata columns", schema) { (df, f0, f1) =>
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
  }

  metadataColumnsTest("select and re-select", schema) { (df, f0, f1) =>
    // test and make sure we are not accidentally making unsafe row
    // to the more general internal row, thus it will fail to re-select
    checkAnswer(
      df.select("name", "age", "id", "university",
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
    checkAnswer(
      df.select("name", "age", METADATA_FILE_NAME)
        .where(Column(METADATA_FILE_NAME) === f0(METADATA_FILE_NAME)),
      Seq(
        // _file_name == f0's name, so we will only have 1 row
        Row("jack", 24, f0(METADATA_FILE_NAME))
      )
    )
  }

  Seq(true, false).foreach { caseSensitive =>
    metadataColumnsTest(s"upper/lower case when case " +
      s"sensitive is $caseSensitive", schemaWithNameConflicts) { (df, f0, f1) =>
      withSQLConf("spark.sql.caseSensitive" -> caseSensitive.toString) {

        // file schema: name, age, _file_size, _FILE_NAME
        if (caseSensitive) {
          // for case sensitive mode:
          // _METADATA.FILE_SIZE is not a part of user schema or metadata columns
          val ex = intercept[Exception] {
            df.select("name", "age", "_METADATA.FILE_SIZE").show()
          }
          assert(ex.getMessage.contains("_METADATA.FILE_SIZE"))

          // for case sensitive mode:
          // `_metadata.file_size` and `_metadata.FILE_NAME` are in the user schema
          // _metadata.file_name and _metadata.file_modification_time are metadata columns
          checkAnswer(
            df.select("name", "age", "`_metadata.file_size`", "`_metadata.FILE_NAME`",
              "_metadata.file_name", "_metadata.file_modification_time"),
            Seq(
              Row("jack", 24, 12345L, "uom",
                f0(METADATA_FILE_NAME), f0(METADATA_FILE_MODIFICATION_TIME)),
              Row("lily", 31, null, "ucb",
                f1(METADATA_FILE_NAME), f1(METADATA_FILE_MODIFICATION_TIME))
            )
          )
        } else {
          // for case insensitive mode:
          // `_metadata.file_size`, `_metadata.FILE_SIZE`,
          // `_metadata.file_name`, `_metadata.FILE_NAME` are all from the user schema.
          // different casings of _metadata.file_path and
          // _metadata.file_modification_time are metadata columns
          checkAnswer(
            df.select("name", "age",
              // user columns
              "`_metadata.file_size`", "`_metadata.FILE_SIZE`",
              "`_metadata.file_name`", "`_metadata.FILE_NAME`",
              // metadata columns
              "_metadata.file_path", "_metadata.FILE_PATH",
              "_metadata.file_modification_time", "_metadata.FILE_modification_TiMe"),
            Seq(
              Row("jack", 24, 12345L, 12345L, "uom", "uom",
                f0(METADATA_FILE_PATH), f0(METADATA_FILE_PATH),
                f0(METADATA_FILE_MODIFICATION_TIME), f0(METADATA_FILE_MODIFICATION_TIME)),
              Row("lily", 31, null, null, "ucb", "ucb",
                f1(METADATA_FILE_PATH), f1(METADATA_FILE_PATH),
                f1(METADATA_FILE_MODIFICATION_TIME), f1(METADATA_FILE_MODIFICATION_TIME))
            )
          )
        }
      }
    }
  }

  Seq("true", "false").foreach { photonEnabled =>
    Seq("true", "false").foreach { offHeapColumnVectorEnabled =>
      withSQLConf("spark.sql.columnVector.offheap.enabled" -> offHeapColumnVectorEnabled,
        "spark.databricks.photon.enabled" -> photonEnabled) {
        metadataColumnsTest(s"read metadata with " +
          s"offheap set to $offHeapColumnVectorEnabled, " +
          s"photon set to $photonEnabled", schema) { (df, f0, f1) =>
          // read all available metadata columns
          checkAnswer(
            df.select("name", "age", "id", "university",
              METADATA_FILE_NAME, METADATA_FILE_PATH,
              METADATA_FILE_SIZE, METADATA_FILE_MODIFICATION_TIME),
            Seq(
              Row("jack", 24, 12345L, "uom", f0(METADATA_FILE_NAME), f0(METADATA_FILE_PATH),
                f0(METADATA_FILE_SIZE), f0(METADATA_FILE_MODIFICATION_TIME)),
              Row("lily", 31, null, "ucb", f1(METADATA_FILE_NAME), f1(METADATA_FILE_PATH),
                f1(METADATA_FILE_SIZE), f1(METADATA_FILE_MODIFICATION_TIME))
            )
          )

          // read a part of metadata columns
          checkAnswer(
            df.select("name", "university", METADATA_FILE_NAME, METADATA_FILE_SIZE),
            Seq(
              Row("jack", "uom", f0(METADATA_FILE_NAME), f0(METADATA_FILE_SIZE)),
              Row("lily", "ucb", f1(METADATA_FILE_NAME), f1(METADATA_FILE_SIZE))
            )
          )
        }
      }
    }
  }

  //////////////////////////
  // TEST METADATA STRUCT //
  //////////////////////////

  // has _metadata.file_name
  val jsonData0 =
    """
      |{
      | "name":"jack",
      | "_metadata":{
      |   "age":24,
      |   "file_name":"jack.json"
      | }
      |}
      |""".stripMargin
  val jsonSchema0: StructType = new StructType()
    .add(StructField("name", StringType))
    .add(StructField("_metadata", new StructType()
      .add(StructField("age", IntegerType))
      .add(StructField("file_name", StringType))))

  // no naming conflicts at all
  val jsonData1 =
    """
      |{
      | "name":"jack",
      | "metadata":{
      |   "age":24,
      |   "file_name":"jack.json"
      | }
      |}
      |""".stripMargin
  val jsonSchema1: StructType = new StructType()
    .add(StructField("name", StringType))
    .add(StructField("metadata", new StructType()
      .add(StructField("age", IntegerType))
      .add(StructField("file_name", StringType))))

  /**
   * Create a JSON file named `fileName` with `data` under `dir` directory.
   */
  private def createJSONFile(data: String, dir: File, fileName: String): String = {
    val dataFile = new File(dir, s"/$fileName")
    dataFile.getParentFile.mkdirs()
    val bytes = data.filter(_ >= ' ').getBytes
    Files.write(dataFile.toPath, bytes)
    dataFile.getCanonicalPath
  }

  test("test data schema has _metadata struct") {
    withTempDir { dir =>

      // 0 - select metadata will fail when analysis
      val df0 = spark.read.schema(jsonSchema0).json(
        createJSONFile(jsonData0, dir, "temp/0"))
      checkAnswer(
        df0.select("name", "_metadata.file_name"),
        Row("jack", "jack.json")
      )
      val ex0 = intercept[AnalysisException] {
        df0.select("name", "_metadata.file_path").show()
      }
      assert(ex0.getMessage.contains("No such struct field file_path in age, file_name"))

      // 1 - no conflict, everything is fine
      val df1 = spark.read.schema(jsonSchema1).json(
        createJSONFile(jsonData1, dir, "temp/1"))

      // get metadata
      val f1 = new File(dir, "temp/1")
      val metadata = Map(
        METADATA_FILE_PATH -> f1.toURI.toString,
        METADATA_FILE_NAME -> f1.getName,
        METADATA_FILE_SIZE -> f1.length(),
        METADATA_FILE_MODIFICATION_TIME -> f1.lastModified()
      )

      checkAnswer(
        df1.select("name", "metadata.file_name",
          "_metadata.file_path", "_metadata.file_name",
          "_metadata.file_size", "_metadata.file_modification_time",
          "_metadata"),
        Row("jack", "jack.json",
          metadata(METADATA_FILE_PATH), metadata(METADATA_FILE_NAME),
          metadata(METADATA_FILE_SIZE), metadata(METADATA_FILE_MODIFICATION_TIME),
          // struct of _metadata
          Row(
            metadata(METADATA_FILE_PATH), metadata(METADATA_FILE_NAME),
            metadata(METADATA_FILE_SIZE), metadata(METADATA_FILE_MODIFICATION_TIME))
        )
      )
    }
  }
}
