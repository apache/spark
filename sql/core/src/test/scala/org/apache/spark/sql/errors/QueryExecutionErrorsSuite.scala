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

package org.apache.spark.sql.errors

import java.io.{File, IOException}
import java.net.{URI, URL}
import java.sql.{Connection, DatabaseMetaData, Driver, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}
import java.util.{Locale, Properties, ServiceConfigurationError}

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.mockito.Mockito.{mock, spy, when}
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Encoder, KryoData, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{NamedParameter, UnresolvedGenerator}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Concat, CreateArray, EmptyRow, Expression, Flatten, Grouping, Literal, RowNumber, UnaryExpression, Years}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.objects.InitializeJavaBean
import org.apache.spark.sql.catalyst.rules.RuleIdCollection
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLExpr
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JDBCOptions}
import org.apache.spark.sql.execution.datasources.jdbc.connection.ConnectionProvider
import org.apache.spark.sql.execution.datasources.orc.OrcTest
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.functions.{lit, lower, struct, sum, udf}
import org.apache.spark.sql.internal.LegacyBehaviorPolicy.EXCEPTION
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DecimalType, IntegerType, LongType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarArray
import org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.Utils


class QueryExecutionErrorsSuite
  extends QueryTest
  with ParquetTest
  with OrcTest
  with SharedSparkSession
  with DataTypeErrorsBase {

  import testImplicits._

  test("CONVERSION_INVALID_INPUT: to_binary conversion function base64") {
    for (codegenMode <- Seq(CODEGEN_ONLY, NO_CODEGEN)) {
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode.toString) {
        val exception = intercept[SparkIllegalArgumentException] {
          Seq(("???")).toDF("a").selectExpr("to_binary(a, 'base64')").collect()
        }
        checkError(
          exception,
          condition = "CONVERSION_INVALID_INPUT",
          parameters = Map(
            "str" -> "'???'",
            "fmt" -> "'BASE64'",
            "targetType" -> "\"BINARY\"",
            "suggestion" -> "`try_to_binary`"))
      }
    }
  }

  test("CONVERSION_INVALID_INPUT: to_binary conversion function hex") {
    for (codegenMode <- Seq(CODEGEN_ONLY, NO_CODEGEN)) {
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode.toString) {
        val exception = intercept[SparkIllegalArgumentException] {
          Seq(("???")).toDF("a").selectExpr("to_binary(a, 'hex')").collect()
        }
        checkError(
          exception,
          condition = "CONVERSION_INVALID_INPUT",
          parameters = Map(
            "str" -> "'???'",
            "fmt" -> "'HEX'",
            "targetType" -> "\"BINARY\"",
            "suggestion" -> "`try_to_binary`"))
      }
    }
  }

  private def getAesInputs(): (DataFrame, DataFrame) = {
    val encryptedText16 = "4Hv0UKCx6nfUeAoPZo1z+w=="
    val encryptedText24 = "NeTYNgA+PCQBN50DA//O2w=="
    val encryptedText32 = "9J3iZbIxnmaG+OIA9Amd+A=="
    val encryptedEmptyText16 = "jmTOhz8XTbskI/zYFFgOFQ=="
    val encryptedEmptyText24 = "9RDK70sHNzqAFRcpfGM5gQ=="
    val encryptedEmptyText32 = "j9IDsCvlYXtcVJUf4FAjQQ=="

    val df1 = Seq("Spark", "").toDF()
    val df2 = Seq(
      (encryptedText16, encryptedText24, encryptedText32),
      (encryptedEmptyText16, encryptedEmptyText24, encryptedEmptyText32)
    ).toDF("value16", "value24", "value32")

    (df1, df2)
  }

  test("INVALID_PARAMETER_VALUE.AES_KEY_LENGTH: invalid key lengths in AES functions") {
    val (df1, df2) = getAesInputs()
    def checkInvalidKeyLength(df: => DataFrame, inputBytes: Int): Unit = {
      checkError(
        exception = intercept[SparkRuntimeException] {
          df.collect()
        },
        condition = "INVALID_PARAMETER_VALUE.AES_KEY_LENGTH",
        parameters = Map(
          "parameter" -> "`key`",
          "functionName" -> "`aes_encrypt`/`aes_decrypt`",
          "actualLength" -> inputBytes.toString),
        sqlState = "22023")
    }

    // Encryption failure - invalid key length
    checkInvalidKeyLength(df1.selectExpr("aes_encrypt(value, '12345678901234567')"), 17)
    checkInvalidKeyLength(df1.selectExpr("aes_encrypt(value, binary('123456789012345'))"),
      15)
    checkInvalidKeyLength(df1.selectExpr("aes_encrypt(value, binary(''))"), 0)

    // Decryption failure - invalid key length
    Seq("value16", "value24", "value32").foreach { colName =>
      checkInvalidKeyLength(df2.selectExpr(
        s"aes_decrypt(unbase64($colName), '12345678901234567')"), 17)
      checkInvalidKeyLength(df2.selectExpr(
        s"aes_decrypt(unbase64($colName), binary('123456789012345'))"), 15)
      checkInvalidKeyLength(df2.selectExpr(
        s"aes_decrypt(unbase64($colName), '')"), 0)
      checkInvalidKeyLength(df2.selectExpr(
        s"aes_decrypt(unbase64($colName), binary(''))"), 0)
    }
  }

  test("INVALID_PARAMETER_VALUE.AES_CRYPTO_ERROR: AES decrypt failure - key mismatch") {
    val (_, df2) = getAesInputs()
    Seq(
      ("value16", "1234567812345678"),
      ("value24", "123456781234567812345678"),
      ("value32", "12345678123456781234567812345678")).foreach { case (colName, key) =>
      checkError(
        exception = intercept[SparkRuntimeException] {
          df2.selectExpr(s"aes_decrypt(unbase64($colName), binary('$key'), 'ECB')").collect()
        },
        condition = "INVALID_PARAMETER_VALUE.AES_CRYPTO_ERROR",
        parameters = Map("parameter" -> "`expr`, `key`",
          "functionName" -> "`aes_encrypt`/`aes_decrypt`",
          "detailMessage" -> ("Given final block not properly padded. " +
            "Such issues can arise if a bad key is used during decryption.")),
        sqlState = "22023")
    }
  }

  test("UNSUPPORTED_FEATURE: unsupported combinations of AES modes and padding") {
    val key16 = "abcdefghijklmnop"
    val key32 = "abcdefghijklmnop12345678ABCDEFGH"
    val (df1, df2) = getAesInputs()
    def checkUnsupportedMode(df: => DataFrame, mode: String, padding: String): Unit = {
      checkError(
        exception = intercept[SparkRuntimeException] {
          df.collect()
        },
        condition = "UNSUPPORTED_FEATURE.AES_MODE",
        parameters = Map("mode" -> mode,
        "padding" -> padding,
        "functionName" -> "`aes_encrypt`/`aes_decrypt`"),
        sqlState = "0A000")
    }

    // Unsupported AES mode and padding in encrypt
    checkUnsupportedMode(df1.selectExpr(s"aes_encrypt(value, '$key16', 'CBC', 'None')"),
      "CBC", "None")
    checkUnsupportedMode(df1.selectExpr(s"aes_encrypt(value, '$key16', 'ECB', 'NoPadding')"),
      "ECB", "NoPadding")

    // Unsupported AES mode and padding in decrypt
    checkUnsupportedMode(df2.selectExpr(s"aes_decrypt(value16, '$key16', 'GSM')"),
      "GSM", "DEFAULT")
    checkUnsupportedMode(df2.selectExpr(s"aes_decrypt(value16, '$key16', 'GCM', 'PKCS')"),
      "GCM", "PKCS")
    checkUnsupportedMode(df2.selectExpr(s"aes_decrypt(value32, '$key32', 'ECB', 'None')"),
      "ECB", "None")
    checkUnsupportedMode(df2.selectExpr(s"aes_decrypt(value32, '$key32', 'CBC', 'NoPadding')"),
      "CBC", "NoPadding")
  }

  test("UNSUPPORTED_FEATURE: unsupported types (map and struct) in lit()") {
    def checkUnsupportedTypeInLiteral(v: Any, literal: String, dataType: String): Unit = {
      checkError(
        exception = intercept[SparkRuntimeException] { spark.expression(lit(v)) },
        condition = "UNSUPPORTED_FEATURE.LITERAL_TYPE",
        parameters = Map("value" -> literal, "type" -> dataType),
        sqlState = "0A000")
    }
    checkUnsupportedTypeInLiteral(Map("key1" -> 1, "key2" -> 2),
      "Map(key1 -> 1, key2 -> 2)",
      "class scala.collection.immutable.Map$Map2")
    checkUnsupportedTypeInLiteral(("mike", 29, 1.0), "(mike,29,1.0)", "class scala.Tuple3")

    val e2 = intercept[SparkRuntimeException] {
      trainingSales
        .groupBy($"sales.year")
        .pivot(struct(lower(trainingSales("sales.course")), trainingSales("training")))
        .agg(sum($"sales.earnings"))
        .collect()
    }
    checkError(
      exception = e2,
      condition = "UNSUPPORTED_FEATURE.PIVOT_TYPE",
      parameters = Map("value" -> "[dotnet,Dummies]",
      "type" -> "unknown"),
      sqlState = "0A000")
  }

  test("UNSUPPORTED_FEATURE: unsupported pivot operations") {
    val e1 = intercept[SparkUnsupportedOperationException] {
      trainingSales
        .groupBy($"sales.year")
        .pivot($"sales.course")
        .pivot($"training")
        .agg(sum($"sales.earnings"))
        .collect()
    }
    checkError(
      exception = e1,
      condition = "REPEATED_CLAUSE",
      parameters = Map("clause" -> "PIVOT", "operation" -> "SUBQUERY"),
      sqlState = "42614")

    val e2 = intercept[SparkUnsupportedOperationException] {
      trainingSales
        .rollup($"sales.year")
        .pivot($"training")
        .agg(sum($"sales.earnings"))
        .collect()
    }
    checkError(
      exception = e2,
      condition = "UNSUPPORTED_FEATURE.PIVOT_AFTER_GROUP_BY",
      parameters = Map[String, String](),
      sqlState = "0A000")
  }

  test("INCONSISTENT_BEHAVIOR_CROSS_VERSION: " +
    "compatibility with Spark 2.4/3.2 in reading/writing dates") {

    // Fail to read ancient datetime values.
    withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_READ.key -> EXCEPTION.toString) {
      val fileName = "before_1582_date_v2_4_5.snappy.parquet"
      val filePath = getResourceParquetFilePath("test-data/" + fileName)
      val e = intercept[SparkUpgradeException] {
        spark.read.parquet(filePath).collect()
      }

      val format = "Parquet"
      val config = "\"" + SQLConf.PARQUET_REBASE_MODE_IN_READ.key + "\""
      val option = "\"datetimeRebaseMode\""
      checkError(
        exception = e,
        condition = "INCONSISTENT_BEHAVIOR_CROSS_VERSION.READ_ANCIENT_DATETIME",
        parameters = Map("format" -> format, "config" -> config, "option" -> option))
    }

    // Fail to write ancient datetime values.
    withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> EXCEPTION.toString) {
      withTempPath { dir =>
        val df = Seq(java.sql.Date.valueOf("1001-01-01")).toDF("dt")
        val e = intercept[SparkException] {
          df.write.parquet(dir.getCanonicalPath)
        }
        assert(e.getCondition == "TASK_WRITE_FAILED")

        val format = "Parquet"
        val config = "\"" + SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key + "\""
        checkError(
          exception = e.getCause.asInstanceOf[SparkUpgradeException],
          condition = "INCONSISTENT_BEHAVIOR_CROSS_VERSION.WRITE_ANCIENT_DATETIME",
          parameters = Map("format" -> format, "config" -> config))
      }
    }
  }

  test("UNSUPPORTED_FEATURE - SPARK-36346: can't read Timestamp as TimestampNTZ") {
    withTempPath { file =>
      sql("select timestamp_ltz'2019-03-21 00:02:03'").write.orc(file.getCanonicalPath)
      withAllNativeOrcReaders {
        val ex = intercept[SparkException] {
          spark.read.schema("time timestamp_ntz").orc(file.getCanonicalPath).collect()
        }
        assert(ex.getCondition.startsWith("FAILED_READ_FILE"))
        checkError(
          exception = ex.getCause.asInstanceOf[SparkUnsupportedOperationException],
          condition = "UNSUPPORTED_FEATURE.ORC_TYPE_CAST",
          parameters = Map("orcType" -> "\"TIMESTAMP\"",
            "toType" -> "\"TIMESTAMP_NTZ\""),
          sqlState = "0A000")
      }
    }
  }

  test("SPARK-42290: NotEnoughMemory error can't be create") {
    QueryExecutionErrors.notEnoughMemoryToBuildAndBroadcastTableError(new OutOfMemoryError(), Seq())
  }

  test("UNSUPPORTED_FEATURE - SPARK-38504: can't read TimestampNTZ as TimestampLTZ") {
    withTempPath { file =>
      sql("select timestamp_ntz'2019-03-21 00:02:03'").write.orc(file.getCanonicalPath)
      withAllNativeOrcReaders {
        val ex = intercept[SparkException] {
          spark.read.schema("time timestamp_ltz").orc(file.getCanonicalPath).collect()
        }
        assert(ex.getCondition.startsWith("FAILED_READ_FILE"))
        checkError(
          exception = ex.getCause.asInstanceOf[SparkUnsupportedOperationException],
          condition = "UNSUPPORTED_FEATURE.ORC_TYPE_CAST",
          parameters = Map("orcType" -> "\"TIMESTAMP_NTZ\"",
            "toType" -> "\"TIMESTAMP\""),
          sqlState = "0A000")
      }
    }
  }

  test("DATETIME_OVERFLOW: timestampadd() overflows its input timestamp") {
    checkError(
      exception = intercept[SparkArithmeticException] {
        sql("select timestampadd(YEAR, 1000000, timestamp'2022-03-09 01:02:03')").collect()
      },
      condition = "DATETIME_OVERFLOW",
      parameters = Map("operation" -> "add 1000000L YEAR to TIMESTAMP '2022-03-09 01:02:03'"),
      sqlState = "22008")
  }

  test("CANNOT_PARSE_DECIMAL: unparseable decimal") {
    val e1 = intercept[SparkException] {
      withTempPath { path =>

        // original text
        val df1 = Seq(
          "money",
          "\"$92,807.99\""
        ).toDF()

        df1.coalesce(1).write.text(path.getAbsolutePath)

        val schema = new StructType().add("money", DecimalType.DoubleDecimal)
        spark
          .read
          .schema(schema)
          .format("csv")
          .option("header", "true")
          .option("locale", Locale.ROOT.toLanguageTag)
          .option("multiLine", "true")
          .option("inferSchema", "false")
          .option("mode", "FAILFAST")
          .load(path.getAbsolutePath).select($"money").collect()
      }
    }

    val e2 = e1.getCause.asInstanceOf[SparkException]
    assert(e2.getCondition == "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION")

    checkError(
      exception = e2.getCause.asInstanceOf[SparkRuntimeException],
      condition = "CANNOT_PARSE_DECIMAL",
      parameters = Map[String, String](),
      sqlState = "22018")
  }

  test("CANNOT_PARSE_JSON_ARRAYS_AS_STRUCTS: parse json arrays as structs") {
    val jsonStr = """[{"a":1, "b":0.8}]"""
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql(s"SELECT from_json('$jsonStr', 'a INT, b DOUBLE', map('mode','FAILFAST') )")
          .collect()
      },
      condition = "MALFORMED_RECORD_IN_PARSING.CANNOT_PARSE_JSON_ARRAYS_AS_STRUCTS",
      parameters = Map(
        "badRecord" -> jsonStr,
        "failFastMode" -> "FAILFAST"
      ),
      sqlState = "22023")
  }

  test("FAILED_EXECUTE_UDF: execute user defined function with registered UDF") {
    val luckyCharOfWord = udf { (word: String, index: Int) => {
      word.substring(index, index + 1)
    }}
    spark.udf.register("luckyCharOfWord", luckyCharOfWord)

    val functionNameRegex = if (Utils.isJavaVersionAtLeast21) {
      "`luckyCharOfWord \\(QueryExecutionErrorsSuite\\$\\$Lambda/\\w+\\)`"
    } else {
      "`luckyCharOfWord \\(QueryExecutionErrorsSuite\\$\\$Lambda\\$\\d+/\\w+\\)`"
    }
    val reason = if (Utils.isJavaVersionAtLeast21) {
      "java.lang.StringIndexOutOfBoundsException: Range \\[5, 6\\) out of bounds for length 5"
    } else {
      "java.lang.StringIndexOutOfBoundsException: begin 5, end 6, length 5"
    }

    checkError(
      exception = intercept[SparkException] {
        Seq(("Jacek", 5), ("Agata", 5), ("Sweet", 6))
          .toDF("word", "index")
          .createOrReplaceTempView("words")
        spark.sql("select luckyCharOfWord(word, index) from words").collect()
      },
      condition = "FAILED_EXECUTE_UDF",
      parameters = Map(
        "functionName" -> functionNameRegex,
        "signature" -> "string, int",
        "result" -> "string",
        "reason" -> reason),
      matchPVals = true)
  }

  test("FAILED_EXECUTE_UDF: execute user defined function") {
    val luckyCharOfWord = udf { (word: String, index: Int) => {
      word.substring(index, index + 1)
    }}
    val functionNameRegex = if (Utils.isJavaVersionAtLeast21) {
      "`QueryExecutionErrorsSuite\\$\\$Lambda/\\w+`"
    } else {
      "`QueryExecutionErrorsSuite\\$\\$Lambda\\$\\d+/\\w+`"
    }
    val reason = if (Utils.isJavaVersionAtLeast21) {
      "java.lang.StringIndexOutOfBoundsException: Range \\[5, 6\\) out of bounds for length 5"
    } else {
      "java.lang.StringIndexOutOfBoundsException: begin 5, end 6, length 5"
    }

    checkError(
      exception = intercept[SparkException] {
        val words = Seq(("Jacek", 5), ("Agata", 5), ("Sweet", 6)).toDF("word", "index")
        words.select(luckyCharOfWord($"word", $"index")).collect()
      },
      condition = "FAILED_EXECUTE_UDF",
      parameters = Map("functionName" -> functionNameRegex,
        "signature" -> "string, int",
        "result" -> "string",
        "reason" -> reason),
      matchPVals = true)
  }

  test("INCOMPARABLE_PIVOT_COLUMN: an incomparable column of the map type") {
    val e = intercept[AnalysisException] {
      trainingSales
      sql(
        """
          | select *
          | from (
          |   select *,map(sales.course, sales.year) as map
          |   from trainingSales
          | )
          | pivot (
          |   sum(sales.earnings) as sum
          |   for map in (
          |     map("dotNET", 2012), map("JAVA", 2012),
          |     map("dotNet", 2013), map("Java", 2013)
          |   )
          | )
          |""".stripMargin).collect()
    }
    checkError(
      exception = e,
      condition = "INCOMPARABLE_PIVOT_COLUMN",
      parameters = Map("columnName" -> "`map`"),
      sqlState = "42818")
  }

  test("UNSUPPORTED_SAVE_MODE: unsupported null saveMode whether the path exists or not") {
    withTempPath { path =>
      val e1 = intercept[SparkIllegalArgumentException] {
        val saveMode: SaveMode = null
        Seq(1, 2).toDS().write.mode(saveMode).parquet(path.getAbsolutePath)
      }
      checkError(
        exception = e1,
        condition = "UNSUPPORTED_SAVE_MODE.NON_EXISTENT_PATH",
        parameters = Map("saveMode" -> "NULL"))

      Utils.createDirectory(path)

      val e2 = intercept[SparkIllegalArgumentException] {
        val saveMode: SaveMode = null
        Seq(1, 2).toDS().write.mode(saveMode).parquet(path.getAbsolutePath)
      }
      checkError(
        exception = e2,
        condition = "UNSUPPORTED_SAVE_MODE.EXISTENT_PATH",
        parameters = Map("saveMode" -> "NULL"))
    }
  }

  test("SPARK-42330: rule id not found") {
    checkError(
      exception = intercept[SparkException] {
          RuleIdCollection.getRuleId("incorrect")
      },
      condition = "RULE_ID_NOT_FOUND",
      parameters = Map("ruleName" -> "incorrect")
    )
  }

  test("CANNOT_RESTORE_PERMISSIONS_FOR_PATH: can't set permission") {
      withTable("t") {
        withSQLConf(
          "fs.file.impl" -> classOf[FakeFileSystemSetPermission].getName,
          "fs.file.impl.disable.cache" -> "true") {
          sql("CREATE TABLE t(c String) USING parquet")

          val e = intercept[AnalysisException] {
            sql("TRUNCATE TABLE t")
          }
          assert(e.getCause.isInstanceOf[SparkSecurityException])

          checkError(
            exception = e.getCause.asInstanceOf[SparkSecurityException],
            condition = "CANNOT_RESTORE_PERMISSIONS_FOR_PATH",
            parameters = Map("permission" -> ".+",
              "path" -> ".+"),
            matchPVals = true)
      }
    }
  }

  test("INCOMPATIBLE_DATASOURCE_REGISTER: create table using an incompatible data source") {
    val newClassLoader = new ClassLoader() {

      override def getResources(name: String): java.util.Enumeration[URL] = {
        if (name.equals("META-INF/services/org.apache.spark.sql.sources.DataSourceRegister")) {
          // scalastyle:off throwerror
          throw new ServiceConfigurationError(s"Illegal configuration-file syntax: $name",
            new NoClassDefFoundError("org.apache.spark.sql.sources.HadoopFsRelationProvider"))
          // scalastyle:on throwerror
        } else {
          super.getResources(name)
        }
      }
    }

    Utils.withContextClassLoader(newClassLoader) {
      val e = intercept[SparkClassNotFoundException] {
        sql("CREATE TABLE student (id INT, name STRING, age INT) USING org.apache.spark.sql.fake")
      }
      checkError(
        exception = e,
        condition = "INCOMPATIBLE_DATASOURCE_REGISTER",
        parameters = Map("message" -> ("Illegal configuration-file syntax: " +
          "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister")))
    }
  }

  test("UNRECOGNIZED_SQL_TYPE: unrecognized SQL type DATALINK") {
    Utils.classForName("org.h2.Driver")

    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")

    val url = "jdbc:h2:mem:testdb0"
    val urlWithUserAndPass = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
    val tableName = "test.table1"
    val unrecognizedColumnType = java.sql.Types.DATALINK
    val unrecognizedColumnTypeName = "h2" + java.sql.Types.DATALINK.toString

    var conn: java.sql.Connection = null
    try {
      conn = DriverManager.getConnection(url, properties)
      conn.prepareStatement("create schema test").executeUpdate()
      conn.commit()

      conn.prepareStatement(s"create table $tableName (a INT)").executeUpdate()
      conn.prepareStatement(
        s"insert into $tableName values (1)").executeUpdate()
      conn.commit()
    } finally {
      if (null != conn) {
        conn.close()
      }
    }

    val testH2DialectUnrecognizedSQLType = new JdbcDialect {
      override def canHandle(url: String): Boolean = url.startsWith("jdbc:h2")

      override def getCatalystType(sqlType: Int, typeName: String, size: Int,
        md: MetadataBuilder): Option[DataType] = {
        sqlType match {
          case _ => None
        }
      }

      override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
        val driverClass: String = options.driverClass

        (_: Int) => {
          DriverRegistry.register(driverClass)

          val resultSetMetaData = mock(classOf[ResultSetMetaData])
          when(resultSetMetaData.getColumnCount).thenReturn(1)
          when(resultSetMetaData.getColumnType(1)).thenReturn(unrecognizedColumnType)
          when(resultSetMetaData.getColumnTypeName(1)).thenReturn(unrecognizedColumnTypeName)

          val resultSet = mock(classOf[ResultSet])
          when(resultSet.next()).thenReturn(true).thenReturn(false)
          when(resultSet.getMetaData).thenReturn(resultSetMetaData)

          val preparedStatement = mock(classOf[PreparedStatement])
          when(preparedStatement.executeQuery).thenReturn(resultSet)

          val connection = mock(classOf[Connection])
          when(connection.prepareStatement(s"SELECT * FROM $tableName WHERE 1=0")).
            thenReturn(preparedStatement)

          connection
        }
      }
    }

    val existH2Dialect = JdbcDialects.get(urlWithUserAndPass)
    JdbcDialects.unregisterDialect(existH2Dialect)

    JdbcDialects.registerDialect(testH2DialectUnrecognizedSQLType)

    checkError(
      exception = intercept[SparkSQLException] {
        spark.read.jdbc(urlWithUserAndPass, tableName, new Properties()).collect()
      },
      condition = "UNRECOGNIZED_SQL_TYPE",
      parameters = Map("typeName" -> unrecognizedColumnTypeName, "jdbcType" -> "DATALINK"),
      sqlState = "42704")

    JdbcDialects.unregisterDialect(testH2DialectUnrecognizedSQLType)
  }

  test("INVALID_BUCKET_FILE: error if there exists any malformed bucket files") {
    val df1 = (0 until 50).map(i => (i % 5, i % 13, i.toString)).
      toDF("i", "j", "k").as("df1")

    withTable("bucketed_table") {
      df1.write.format("parquet").bucketBy(8, "i").
        saveAsTable("bucketed_table")
      val warehouseFilePath = new URI(spark.sessionState.conf.warehousePath).getPath
      val tableDir = new File(warehouseFilePath, "bucketed_table")
      Utils.deleteRecursively(tableDir)
      df1.write.parquet(tableDir.getAbsolutePath)

      val aggregated = spark.table("bucketed_table").groupBy("i").count()

      checkError(
        exception = intercept[SparkException] {
          aggregated.count()
        },
        condition = "INVALID_BUCKET_FILE",
        parameters = Map("path" -> ".+"),
        matchPVals = true)
    }
  }

  test(
    "SCALAR_SUBQUERY_TOO_MANY_ROWS: " +
    "More than one row returned by a subquery used as an expression") {
    checkError(
      exception = intercept[SparkException] {
        sql("select (select a from (select 1 as a union all select 2 as a) t) as b").collect()
      },
      condition = "SCALAR_SUBQUERY_TOO_MANY_ROWS",
      queryContext = Array(
        ExpectedContext(
          fragment = "(select a from (select 1 as a union all select 2 as a) t)",
          start = 7,
          stop = 63
        )
      )
    )
  }

  test("ARITHMETIC_OVERFLOW: overflow on adding months") {
    checkError(
      exception = intercept[SparkArithmeticException](
        sql("select add_months('5500000-12-31', 10000000)").collect()
      ),
      condition = "ARITHMETIC_OVERFLOW",
      parameters = Map(
        "message" -> "integer overflow",
        "alternative" -> "",
        "config" -> s""""${SQLConf.ANSI_ENABLED.key}""""))
  }

  test("FAILED_PARSE_STRUCT_TYPE: parsing invalid struct type") {
    val raw = """{"type":"array","elementType":"integer","containsNull":false}"""
    checkError(
      exception = intercept[SparkRuntimeException] {
        StructType.fromString(raw)
      },
      condition = "FAILED_PARSE_STRUCT_TYPE",
      parameters = Map("raw" -> s"'$raw'"))
  }

  test("CAST_OVERFLOW: from long to ANSI intervals") {
    Seq(
      LongType -> "9223372036854775807L",
      DecimalType(19, 0) -> "9223372036854775807BD").foreach { case (sourceType, sourceValue) =>
      Seq("INTERVAL YEAR TO MONTH", "INTERVAL HOUR TO MINUTE").foreach { it =>
        checkError(
          exception = intercept[SparkArithmeticException] {
            sql(s"select CAST($sourceValue AS $it)").collect()
          },
          condition = "CAST_OVERFLOW",
          parameters = Map(
            "value" -> sourceValue,
            "sourceType" -> s""""${sourceType.sql}"""",
            "targetType" -> s""""$it"""",
            "ansiConfig" -> s""""${SQLConf.ANSI_ENABLED.key}""""),
          sqlState = "22003")
      }
    }
  }

  test("CANNOT_PARSE_STRING_AS_DATATYPE: parse string as float use from_json") {
    val jsonStr = """{"a": "str"}"""
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql(s"""SELECT from_json('$jsonStr', 'a FLOAT', map('mode','FAILFAST'))""").collect()
      },
      condition = "MALFORMED_RECORD_IN_PARSING.CANNOT_PARSE_STRING_AS_DATATYPE",
      parameters = Map(
        "badRecord" -> jsonStr,
        "failFastMode" -> "FAILFAST",
        "fieldName" -> "`a`",
        "fieldValue" -> "'str'",
        "inputType" -> "StringType",
        "targetType" -> "FloatType"),
      sqlState = "22023")
  }

  test("BINARY_ARITHMETIC_OVERFLOW: byte plus byte result overflow") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkError(
        exception = intercept[SparkArithmeticException] {
          sql(s"select 127Y + 5Y").collect()
        },
        condition = "BINARY_ARITHMETIC_OVERFLOW",
        parameters = Map(
          "value1" -> "127S",
          "symbol" -> "+",
          "value2" -> "5S",
          "functionName" -> "`try_add`"),
        sqlState = "22003")
    }
  }

  test("BINARY_ARITHMETIC_OVERFLOW: byte minus byte result overflow") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkError(
        exception = intercept[SparkArithmeticException] {
          sql(s"select -2Y - 127Y").collect()
        },
        condition = "BINARY_ARITHMETIC_OVERFLOW",
        parameters = Map(
          "value1" -> "-2S",
          "symbol" -> "-",
          "value2" -> "127S",
          "functionName" -> "`try_subtract`"),
        sqlState = "22003")
    }
  }

  test("BINARY_ARITHMETIC_OVERFLOW: byte multiply byte result overflow") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkError(
        exception = intercept[SparkArithmeticException] {
          sql(s"select 127Y * 5Y").collect()
        },
        condition = "BINARY_ARITHMETIC_OVERFLOW",
        parameters = Map(
          "value1" -> "127S",
          "symbol" -> "*",
          "value2" -> "5S",
          "functionName" -> "`try_multiply`"),
        sqlState = "22003")
    }
  }

  test("UNSUPPORTED_DATATYPE: invalid StructType raw format") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        val row = spark.sparkContext.parallelize(Seq(1, 2)).map(Row(_))
        spark.createDataFrame(row, StructType.fromString("StructType()"))
      },
      condition = "UNSUPPORTED_DATATYPE",
      parameters = Map(
        "typeName" ->
          "StructType()[1.1] failure: 'TimestampType' expected but 'S' found\n\nStructType()\n^"
      ),
      sqlState = "0A000")
  }

  test("RENAME_SRC_PATH_NOT_FOUND: rename the file which source path does not exist") {
    withTempPath { p =>
      withSQLConf(
        "spark.sql.streaming.checkpointFileManagerClass" ->
          classOf[FileSystemBasedCheckpointFileManager].getName,
        "fs.file.impl" -> classOf[FakeFileSystemNeverExists].getName,
        // FileSystem caching could cause a different implementation of fs.file to be used
        "fs.file.impl.disable.cache" -> "true"
      ) {
        val checkpointLocation = p.getAbsolutePath

        val ds = spark.readStream.format("rate").load()
        val e = intercept[SparkFileNotFoundException] {
          ds.writeStream
            .option("checkpointLocation", checkpointLocation)
            .queryName("_")
            .format("memory")
            .start()
        }

        val expectedPath = p.toURI
        checkError(
          exception = e,
          condition = "RENAME_SRC_PATH_NOT_FOUND",
          matchPVals = true,
          parameters = Map("sourcePath" -> s"$expectedPath.+")
        )
      }
    }
  }

  test("UNSUPPORTED_FEATURE.MULTI_ACTION_ALTER: The target JDBC server hosting table " +
    "does not support ALTER TABLE with multiple actions.") {
    withTempDir { tempDir =>
      val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
      Utils.classForName("org.h2.Driver")
      var conn: java.sql.Connection = null
      try {
        conn = DriverManager.getConnection(url, new Properties())
        conn.prepareStatement("""CREATE SCHEMA "test"""").executeUpdate()
        conn.prepareStatement(
          """CREATE TABLE "test"."people" (name TEXT(32) NOT NULL, id INTEGER NOT NULL)""")
          .executeUpdate()
        conn.commit()
      } finally {
        if (null != conn) {
          conn.close()
        }
      }

      val testH2DialectUnsupportedJdbcTransaction = new JdbcDialect {
        override def canHandle(url: String): Boolean = url.startsWith("jdbc:h2")

        override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
          val driverClass: String = options.driverClass

          (_: Int) => {
            DriverRegistry.register(driverClass)
            val driver: Driver = DriverRegistry.get(driverClass)
            val connection = ConnectionProvider.create(
              driver, options.parameters, options.connectionProviderName)
            val spyConnection = spy[Connection](connection)
            val spyMetaData = spy[DatabaseMetaData](connection.getMetaData)
            when(spyConnection.getMetaData).thenReturn(spyMetaData)
            when(spyMetaData.supportsTransactions()).thenReturn(false)

            spyConnection
          }
        }
      }

      withSQLConf(
        "spark.sql.catalog.h2" -> classOf[JDBCTableCatalog].getName,
        "spark.sql.catalog.h2.url" -> url,
        "spark.sql.catalog.h2.driver" -> "org.h2.Driver") {

        val existedH2Dialect = JdbcDialects.get(url)
        JdbcDialects.unregisterDialect(existedH2Dialect)
        JdbcDialects.registerDialect(testH2DialectUnsupportedJdbcTransaction)

        checkError(
          exception = intercept[SparkSQLFeatureNotSupportedException] {
            sql("alter TABLE h2.test.people SET TBLPROPERTIES (xx='xx', yy='yy')")
          },
          condition = "UNSUPPORTED_FEATURE.MULTI_ACTION_ALTER",
          parameters = Map("tableName" -> "\"test\".\"people\""))

        JdbcDialects.unregisterDialect(testH2DialectUnsupportedJdbcTransaction)
      }
    }
  }

  test("STREAM_FAILED: NPE in user code") {
    val ds = spark.readStream.format("rate").load()
    val query = ds.writeStream.foreachBatch { (_: Dataset[Row], _: Long) =>
      val s: String = null
      s.length: Unit
    }.start()
    val e = intercept[StreamingQueryException] {
      query.awaitTermination()
    }
    assert(e.getCondition === "STREAM_FAILED")
    assert(e.getCause.isInstanceOf[NullPointerException])
  }

  test("CONCURRENT_QUERY: streaming query is resumed from many sessions") {
    failAfter(90 seconds) {
      withSQLConf(SQLConf.STREAMING_STOP_ACTIVE_RUN_ON_RESTART.key -> "true") {
        withTempDir { dir =>
          val ds = spark.readStream.format("rate").load()

          // Queries have the same ID when they are resumed from the same checkpoint.
          val chkLocation = new File(dir, "_checkpoint").getCanonicalPath
          val dataLocation = new File(dir, "data").getCanonicalPath

          // Run an initial query to setup the checkpoint.
          val initialQuery = ds.writeStream.format("parquet")
            .option("checkpointLocation", chkLocation).start(dataLocation)

          // Error is thrown due to a race condition. Ensure it happens with high likelihood in the
          // test by spawning many threads.
          val exceptions = ThreadUtils.parmap(Seq.range(1, 50), "QueryExecutionErrorsSuite", 50)
            { _ =>
              var exception = None : Option[SparkConcurrentModificationException]
              try {
                val restartedQuery = ds.writeStream.format("parquet")
                  .option("checkpointLocation", chkLocation).start(dataLocation)
                restartedQuery.stop()
                restartedQuery.awaitTermination()
              } catch {
                case e: SparkConcurrentModificationException =>
                  exception = Some(e)
              }
              exception
            }
          // Only check if errors exist to deflake. We couldn't guarantee that
          // the above 50 runs must hit this error.
          exceptions.flatten.map { e =>
            checkError(
              e,
              condition = "CONCURRENT_QUERY",
              sqlState = Some("0A000"),
              parameters = e.getMessageParameters.asScala.toMap
            )
          }
          spark.streams.active.foreach(_.stop())
        }
      }
    }
  }

  test("UNSUPPORTED_EXPR_FOR_WINDOW: to_date is not supported with WINDOW") {
    withTable("t") {
      sql("CREATE TABLE t(c String) USING parquet")

      val e = intercept[AnalysisException] {
        sql("SELECT to_date('2009-07-30 04:17:52') OVER (PARTITION BY c ORDER BY c) FROM t;")
      }

      checkError(
        exception = e,
        condition = "UNSUPPORTED_EXPR_FOR_WINDOW",
        parameters = Map(
          "sqlExpr" -> "\"to_date(2009-07-30 04:17:52)\""
        ),
        queryContext = Array(
          ExpectedContext(
            fragment = "to_date('2009-07-30 04:17:52') OVER (PARTITION BY c ORDER BY c)",
            start = 7,
            stop = 69
          )
        )
      )
    }
  }

  test("INTERNAL_ERROR: Calling eval on Unevaluable expression") {
    val e = intercept[SparkException] {
      NamedParameter("foo").eval()
    }
    checkError(
      exception = e,
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "Cannot evaluate expression: namedparameter(foo)"),
      sqlState = "XX000")
  }

  test("PartitionTransformExpression error on eval") {
    val expr = Years(Literal("foo"))
    val e = intercept[SparkException] {
      expr.eval()
    }
    checkError(
      exception = e,
      condition = "PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY",
      parameters = Map("expression" -> toSQLExpr(expr)))
  }

  test("INTERNAL_ERROR: Calling doGenCode on unresolved") {
    val e = intercept[SparkException] {
      val ctx = new CodegenContext
      Grouping(NamedParameter("foo")).genCode(ctx)
    }
    checkError(
      exception = e,
      condition = "INTERNAL_ERROR",
      parameters = Map(
        "message" -> ("Cannot generate code for expression: " +
          "grouping(namedparameter(foo))")),
      sqlState = "XX000")
  }

  test("INTERNAL_ERROR: Calling terminate on UnresolvedGenerator") {
    val e = intercept[SparkException] {
      UnresolvedGenerator(FunctionIdentifier("foo"), Seq.empty).terminate()
    }
    checkError(
      exception = e,
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "Cannot terminate expression: 'foo()"),
      sqlState = "XX000")
  }

  test("INTERNAL_ERROR: Initializing JavaBean with non existing method") {
    val e = intercept[SparkException] {
      val initializeWithNonexistingMethod = InitializeJavaBean(
        Literal.fromObject(new java.util.LinkedList[Int]),
        Map("nonexistent" -> Literal(1)))
      initializeWithNonexistingMethod.eval()
    }
    checkError(
      exception = e,
      condition = "INTERNAL_ERROR",
      parameters = Map(
        "message" -> ("""A method named "nonexistent" is not declared in """ +
          "any enclosing class nor any supertype")),
      sqlState = "XX000")
  }

  test("INTERNAL_ERROR: Aggregate Window Functions do not support merging") {
    val e = intercept[SparkException] {
      RowNumber().mergeExpressions
    }
    checkError(
      exception = e,
      condition = "INTERNAL_ERROR",
      parameters = Map(
        "message" -> "The aggregate window function `row_number` does not support merging."),
      sqlState = "XX000")
  }

  test("INVALID_BITMAP_POSITION: position out of bounds") {
    checkError(
      exception = intercept[SparkArrayIndexOutOfBoundsException] {
        sql("select bitmap_construct_agg(col) from values (32768) as tab(col)").collect()
      },
      condition = "INVALID_BITMAP_POSITION",
      parameters = Map(
        "bitPosition" -> "32768",
        "bitmapNumBytes" -> "4096",
        "bitmapNumBits" -> "32768"),
      sqlState = "22003")
  }

  test("INVALID_BITMAP_POSITION: negative position") {
    checkError(
      exception = intercept[SparkArrayIndexOutOfBoundsException] {
        sql("select bitmap_construct_agg(col) from values (-1) as tab(col)").collect()
      },
      condition = "INVALID_BITMAP_POSITION",
      parameters = Map(
        "bitPosition" -> "-1",
        "bitmapNumBytes" -> "4096",
        "bitmapNumBits" -> "32768"),
      sqlState = "22003")
  }

  test("SPARK-43589: Use bytesToString instead of shift operation") {
    checkError(
      exception = intercept[SparkException] {
        throw QueryExecutionErrors.cannotBroadcastTableOverMaxTableBytesError(
          maxBroadcastTableBytes = 1024 * 1024 * 1024,
          dataSize = 2 * 1024 * 1024 * 1024 - 1)
      },
      condition = "_LEGACY_ERROR_TEMP_2249",
      parameters = Map("maxBroadcastTableBytes" -> "1024.0 MiB", "dataSize" -> "2048.0 MiB"))
  }

  test("V1 table don't support time travel") {
    withTable("t") {
      sql("CREATE TABLE t(c String) USING parquet")
      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM t TIMESTAMP AS OF '2021-01-29 00:00:00'").collect()
        },
        condition = "UNSUPPORTED_FEATURE.TIME_TRAVEL",
        parameters = Map("relationId" -> "`spark_catalog`.`default`.`t`")
      )
    }
  }

  test("Unexpected `start` for slice()") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("select slice(array(1,2,3), 0, 1)").collect()
      },
      condition = "INVALID_PARAMETER_VALUE.START",
      parameters = Map(
        "parameter" -> toSQLId("start"),
        "functionName" -> toSQLId("slice")
      )
    )
  }

  test("Unexpected `length` for slice()") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql("select slice(array(1,2,3), 1, -1)").collect()
      },
      condition = "INVALID_PARAMETER_VALUE.LENGTH",
      parameters = Map(
        "parameter" -> toSQLId("length"),
        "length" -> (-1).toString,
        "functionName" -> toSQLId("slice")
      )
    )
  }

  test("Elements exceed limit for concat()") {
    val array = new ColumnarArray(
      new ConstantColumnVector(Int.MaxValue, BooleanType), 0, Int.MaxValue)

    checkError(
      exception = intercept[SparkRuntimeException] {
        Concat(Seq(Literal.create(array, ArrayType(BooleanType)))).eval(EmptyRow)
      },
      condition = "COLLECTION_SIZE_LIMIT_EXCEEDED.FUNCTION",
      parameters = Map(
        "numberOfElements" -> Int.MaxValue.toString,
        "maxRoundedArrayLength" -> MAX_ROUNDED_ARRAY_LENGTH.toString,
        "functionName" -> toSQLId("concat")
      )
    )
  }

  test("Elements exceed limit for flatten()") {
    val array = new ColumnarArray(
      new ConstantColumnVector(Int.MaxValue, BooleanType), 0, Int.MaxValue)

    checkError(
      exception = intercept[SparkRuntimeException] {
        Flatten(CreateArray(Seq(Literal.create(array, ArrayType(BooleanType))))).eval(EmptyRow)
      },
      condition = "COLLECTION_SIZE_LIMIT_EXCEEDED.FUNCTION",
      parameters = Map(
        "numberOfElements" -> Int.MaxValue.toString,
        "maxRoundedArrayLength" -> MAX_ROUNDED_ARRAY_LENGTH.toString,
        "functionName" -> toSQLId("flatten")
      )
    )
  }

  test("Elements exceed limit for array_repeat()") {
    val count = 2147483647
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql(s"select array_repeat(1, $count)").collect()
      },
      condition = "COLLECTION_SIZE_LIMIT_EXCEEDED.PARAMETER",
      parameters = Map(
        "parameter" -> toSQLId("count"),
        "numberOfElements" -> count.toString,
        "functionName" -> toSQLId("array_repeat"),
        "maxRoundedArrayLength" -> MAX_ROUNDED_ARRAY_LENGTH.toString
      )
    )
  }

  test("SPARK-43259: Uses unsupported KryoData encoder") {
    implicit val kryoEncoder = new Encoder[KryoData] {
      override def schema: StructType = StructType(Array.empty[StructField])

      override def clsTag: ClassTag[KryoData] = ClassTag(classOf[KryoData])
    }
    checkError(
      exception = intercept[SparkRuntimeException] {
        Seq(KryoData(1), KryoData(2)).toDS()
      },
      condition = "INVALID_EXPRESSION_ENCODER",
      parameters = Map(
        "encoderType" -> kryoEncoder.getClass.getName,
        "docroot" -> SPARK_DOC_ROOT
      )
    )
  }

  test("ExpressionEncoder.objDeserializer should be a unsolved encoder ") {
    val rowEnc = RowEncoder.encoderFor(new StructType(Array(StructField("v", IntegerType))))
    val enc: ExpressionEncoder[Row] = ExpressionEncoder(rowEnc)
    val deserializer = AttributeReference.apply("v", IntegerType)()
    implicit val im: ExpressionEncoder[Row] = new ExpressionEncoder[Row](
      rowEnc, enc.objSerializer, deserializer)

    checkError(
      exception = intercept[SparkRuntimeException] {
        spark.createDataset(Seq(Row(1))).collect()
      },
      condition = "NOT_UNRESOLVED_ENCODER",
      parameters = Map(
        "attr" -> deserializer.toString
      )
    )
  }


  test("UnaryExpression should override eval or nullSafeEval") {
    case class MyUnaryExpression(child: Expression)
      extends UnaryExpression {
      override def dataType: DataType = IntegerType

      override protected def withNewChildInternal(newChild: Expression): UnaryExpression = this

      override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = null
    }

    val expr = MyUnaryExpression(Literal.create(1, IntegerType))
    checkError(
      exception = intercept[SparkRuntimeException] {
        expr.eval(EmptyRow)
      },
      condition = "CLASS_NOT_OVERRIDE_EXPECTED_METHOD",
      parameters = Map(
        "className" -> expr.getClass.getName,
        "method1" -> "eval",
        "method2" -> "nullSafeEval"
      )
    )
  }

  test("SPARK-50485: Unwrap SparkThrowable in UEE thrown by tableRelationCache") {
    withTable("t") {
      sql("CREATE TABLE t (a INT)")
      checkError(
        exception = intercept[SparkUnsupportedOperationException] {
          sql("ALTER TABLE t SET LOCATION 'https://mister/spark'")
        },
        condition = "FAILED_READ_FILE.UNSUPPORTED_FILE_SYSTEM",
        parameters = Map(
          "path" -> "https://mister/spark",
          "fileSystemClass" -> "org.apache.hadoop.fs.http.HttpsFileSystem",
          "method" -> "listStatus"))
      sql("ALTER TABLE t SET LOCATION '/mister/spark'")
    }
  }
}

class FakeFileSystemSetPermission extends LocalFileSystem {

  override def setPermission(src: Path, permission: FsPermission): Unit = {
    throw new IOException(s"fake fileSystem failed to set permission: $permission")
  }
}

class FakeFileSystemNeverExists extends DebugFilesystem {
  override def exists(f: Path): Boolean = false
}
