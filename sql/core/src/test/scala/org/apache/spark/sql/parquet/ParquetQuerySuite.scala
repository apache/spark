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

package org.apache.spark.sql.parquet

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}
import parquet.filter2.predicate.{FilterPredicate, Operators}
import parquet.hadoop.ParquetFileWriter
import parquet.hadoop.util.ContextUtil
import parquet.io.api.Binary

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.sql.catalyst.util.getTempFilePath
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.util.Utils

case class TestRDDEntry(key: Int, value: String)

case class NullReflectData(
    intField: java.lang.Integer,
    longField: java.lang.Long,
    floatField: java.lang.Float,
    doubleField: java.lang.Double,
    booleanField: java.lang.Boolean)

case class OptionalReflectData(
    intField: Option[Int],
    longField: Option[Long],
    floatField: Option[Float],
    doubleField: Option[Double],
    booleanField: Option[Boolean])

case class Nested(i: Int, s: String)

case class Data(array: Seq[Int], nested: Nested)

case class AllDataTypes(
    stringField: String,
    intField: Int,
    longField: Long,
    floatField: Float,
    doubleField: Double,
    shortField: Short,
    byteField: Byte,
    booleanField: Boolean)

case class AllDataTypesWithNonPrimitiveType(
    stringField: String,
    intField: Int,
    longField: Long,
    floatField: Float,
    doubleField: Double,
    shortField: Short,
    byteField: Byte,
    booleanField: Boolean,
    array: Seq[Int],
    arrayContainsNull: Seq[Option[Int]],
    map: Map[Int, Long],
    mapValueContainsNull: Map[Int, Option[Long]],
    data: Data)

case class BinaryData(binaryData: Array[Byte])

case class NumericData(i: Int, d: Double)

class ParquetQuerySuite extends QueryTest with FunSuiteLike with BeforeAndAfterAll {
  TestData // Load test data tables.

  private var testRDD: SchemaRDD = null
  private val originalParquetFilterPushdownEnabled = TestSQLContext.parquetFilterPushDown

  override def beforeAll() {
    ParquetTestData.writeFile()
    ParquetTestData.writeFilterFile()
    ParquetTestData.writeNestedFile1()
    ParquetTestData.writeNestedFile2()
    ParquetTestData.writeNestedFile3()
    ParquetTestData.writeNestedFile4()
    ParquetTestData.writeGlobFiles()
    testRDD = parquetFile(ParquetTestData.testDir.toString)
    testRDD.registerTempTable("testsource")
    parquetFile(ParquetTestData.testFilterDir.toString)
      .registerTempTable("testfiltersource")

    setConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED, "true")
  }

  override def afterAll() {
    Utils.deleteRecursively(ParquetTestData.testDir)
    Utils.deleteRecursively(ParquetTestData.testFilterDir)
    Utils.deleteRecursively(ParquetTestData.testNestedDir1)
    Utils.deleteRecursively(ParquetTestData.testNestedDir2)
    Utils.deleteRecursively(ParquetTestData.testNestedDir3)
    Utils.deleteRecursively(ParquetTestData.testNestedDir4)
    Utils.deleteRecursively(ParquetTestData.testGlobDir)
    // here we should also unregister the table??

    setConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED, originalParquetFilterPushdownEnabled.toString)
  }

  test("Read/Write All Types") {
    val tempDir = getTempFilePath("parquetTest").getCanonicalPath
    val range = (0 to 255)
    val data = sparkContext.parallelize(range).map { x =>
      parquet.AllDataTypes(
        s"$x", x, x.toLong, x.toFloat, x.toDouble, x.toShort, x.toByte, x % 2 == 0)
    }

    data.saveAsParquetFile(tempDir)

    checkAnswer(
      parquetFile(tempDir),
      data.toSchemaRDD.collect().toSeq)
  }

  test("read/write binary data") {
    // Since equality for Array[Byte] is broken we test this separately.
    val tempDir = getTempFilePath("parquetTest").getCanonicalPath
    sparkContext.parallelize(BinaryData("test".getBytes("utf8")) :: Nil).saveAsParquetFile(tempDir)
    parquetFile(tempDir)
      .map(r => new String(r(0).asInstanceOf[Array[Byte]], "utf8"))
      .collect().toSeq == Seq("test")
  }

  ignore("Treat binary as string") {
    val oldIsParquetBinaryAsString = TestSQLContext.isParquetBinaryAsString

    // Create the test file.
    val file = getTempFilePath("parquet")
    val path = file.toString
    val range = (0 to 255)
    val rowRDD = TestSQLContext.sparkContext.parallelize(range)
      .map(i => org.apache.spark.sql.Row(i, s"val_$i".getBytes))
    // We need to ask Parquet to store the String column as a Binary column.
    val schema = StructType(
      StructField("c1", IntegerType, false) ::
      StructField("c2", BinaryType, false) :: Nil)
    val schemaRDD1 = applySchema(rowRDD, schema)
    schemaRDD1.saveAsParquetFile(path)
    checkAnswer(
      parquetFile(path).select('c1, 'c2.cast(StringType)),
      schemaRDD1.select('c1, 'c2.cast(StringType)).collect().toSeq)

    setConf(SQLConf.PARQUET_BINARY_AS_STRING, "true")
    parquetFile(path).printSchema()
    checkAnswer(
      parquetFile(path),
      schemaRDD1.select('c1, 'c2.cast(StringType)).collect().toSeq)


    // Set it back.
    TestSQLContext.setConf(SQLConf.PARQUET_BINARY_AS_STRING, oldIsParquetBinaryAsString.toString)
  }

  test("Compression options for writing to a Parquetfile") {
    val defaultParquetCompressionCodec = TestSQLContext.parquetCompressionCodec
    import scala.collection.JavaConversions._

    val file = getTempFilePath("parquet")
    val path = file.toString
    val rdd = TestSQLContext.sparkContext.parallelize((1 to 100))
      .map(i => TestRDDEntry(i, s"val_$i"))

    // test default compression codec
    rdd.saveAsParquetFile(path)
    var actualCodec = ParquetTypesConverter.readMetaData(new Path(path), Some(TestSQLContext.sparkContext.hadoopConfiguration))
      .getBlocks.flatMap(block => block.getColumns).map(column => column.getCodec.name()).distinct
    assert(actualCodec === TestSQLContext.parquetCompressionCodec.toUpperCase :: Nil)

    parquetFile(path).registerTempTable("tmp")
    checkAnswer(
      sql("SELECT key, value FROM tmp WHERE value = 'val_5' OR value = 'val_7'"),
      (5, "val_5") ::
      (7, "val_7") :: Nil)

    Utils.deleteRecursively(file)

    // test uncompressed parquet file with property value "UNCOMPRESSED"
    TestSQLContext.setConf(SQLConf.PARQUET_COMPRESSION, "UNCOMPRESSED")

    rdd.saveAsParquetFile(path)
    actualCodec = ParquetTypesConverter.readMetaData(new Path(path), Some(TestSQLContext.sparkContext.hadoopConfiguration))
      .getBlocks.flatMap(block => block.getColumns).map(column => column.getCodec.name()).distinct
    assert(actualCodec === TestSQLContext.parquetCompressionCodec.toUpperCase :: Nil)

    parquetFile(path).registerTempTable("tmp")
    checkAnswer(
      sql("SELECT key, value FROM tmp WHERE value = 'val_5' OR value = 'val_7'"),
      (5, "val_5") ::
      (7, "val_7") :: Nil)

    Utils.deleteRecursively(file)

    // test uncompressed parquet file with property value "none"
    TestSQLContext.setConf(SQLConf.PARQUET_COMPRESSION, "none")

    rdd.saveAsParquetFile(path)
    actualCodec = ParquetTypesConverter.readMetaData(new Path(path), Some(TestSQLContext.sparkContext.hadoopConfiguration))
      .getBlocks.flatMap(block => block.getColumns).map(column => column.getCodec.name()).distinct
    assert(actualCodec === "UNCOMPRESSED" :: Nil)

    parquetFile(path).registerTempTable("tmp")
    checkAnswer(
      sql("SELECT key, value FROM tmp WHERE value = 'val_5' OR value = 'val_7'"),
      (5, "val_5") ::
      (7, "val_7") :: Nil)

    Utils.deleteRecursively(file)

    // test gzip compression codec
    TestSQLContext.setConf(SQLConf.PARQUET_COMPRESSION, "gzip")

    rdd.saveAsParquetFile(path)
    actualCodec = ParquetTypesConverter.readMetaData(new Path(path), Some(TestSQLContext.sparkContext.hadoopConfiguration))
      .getBlocks.flatMap(block => block.getColumns).map(column => column.getCodec.name()).distinct
    assert(actualCodec === TestSQLContext.parquetCompressionCodec.toUpperCase :: Nil)

    parquetFile(path).registerTempTable("tmp")
    checkAnswer(
      sql("SELECT key, value FROM tmp WHERE value = 'val_5' OR value = 'val_7'"),
      (5, "val_5") ::
      (7, "val_7") :: Nil)

    Utils.deleteRecursively(file)

    // test snappy compression codec
    TestSQLContext.setConf(SQLConf.PARQUET_COMPRESSION, "snappy")

    rdd.saveAsParquetFile(path)
    actualCodec = ParquetTypesConverter.readMetaData(new Path(path), Some(TestSQLContext.sparkContext.hadoopConfiguration))
      .getBlocks.flatMap(block => block.getColumns).map(column => column.getCodec.name()).distinct
    assert(actualCodec === TestSQLContext.parquetCompressionCodec.toUpperCase :: Nil)

    parquetFile(path).registerTempTable("tmp")
    checkAnswer(
      sql("SELECT key, value FROM tmp WHERE value = 'val_5' OR value = 'val_7'"),
      (5, "val_5") ::
      (7, "val_7") :: Nil)

    Utils.deleteRecursively(file)

    // TODO: Lzo requires additional external setup steps so leave it out for now
    // ref.: https://github.com/Parquet/parquet-mr/blob/parquet-1.5.0/parquet-hadoop/src/test/java/parquet/hadoop/example/TestInputOutputFormat.java#L169

    // Set it back.
    TestSQLContext.setConf(SQLConf.PARQUET_COMPRESSION, defaultParquetCompressionCodec)
  }

  test("Read/Write All Types with non-primitive type") {
    val tempDir = getTempFilePath("parquetTest").getCanonicalPath
    val range = (0 to 255)
    val data = sparkContext.parallelize(range).map { x =>
      parquet.AllDataTypesWithNonPrimitiveType(
        s"$x", x, x.toLong, x.toFloat, x.toDouble, x.toShort, x.toByte, x % 2 == 0,
        (0 until x),
        (0 until x).map(Option(_).filter(_ % 3 == 0)),
        (0 until x).map(i => i -> i.toLong).toMap,
        (0 until x).map(i => i -> Option(i.toLong)).toMap + (x -> None),
        parquet.Data((0 until x), parquet.Nested(x, s"$x")))
    }
    data.saveAsParquetFile(tempDir)

    checkAnswer(
      parquetFile(tempDir),
      data.toSchemaRDD.collect().toSeq)
  }

  test("self-join parquet files") {
    val x = ParquetTestData.testData.as('x)
    val y = ParquetTestData.testData.as('y)
    val query = x.join(y).where("x.myint".attr === "y.myint".attr)

    // Check to make sure that the attributes from either side of the join have unique expression
    // ids.
    query.queryExecution.analyzed.output.filter(_.name == "myint") match {
      case Seq(i1, i2) if(i1.exprId == i2.exprId) =>
        fail(s"Duplicate expression IDs found in query plan: $query")
      case Seq(_, _) => // All good
    }

    val result = query.collect()
    assert(result.size === 9, "self-join result has incorrect size")
    assert(result(0).size === 12, "result row has incorrect size")
    result.zipWithIndex.foreach {
      case (row, index) => row.zipWithIndex.foreach {
        case (field, column) => assert(field != null, s"self-join contains null value in row $index field $column")
      }
    }
  }

  test("Import of simple Parquet file") {
    val result = parquetFile(ParquetTestData.testDir.toString).collect()
    assert(result.size === 15)
    result.zipWithIndex.foreach {
      case (row, index) => {
        val checkBoolean =
          if (index % 3 == 0)
            row(0) == true
          else
            row(0) == false
        assert(checkBoolean === true, s"boolean field value in line $index did not match")
        if (index % 5 == 0) assert(row(1) === 5, s"int field value in line $index did not match")
        assert(row(2) === "abc", s"string field value in line $index did not match")
        assert(row(3) === (index.toLong << 33), s"long value in line $index did not match")
        assert(row(4) === 2.5F, s"float field value in line $index did not match")
        assert(row(5) === 4.5D, s"double field value in line $index did not match")
      }
    }
  }

  test("Projection of simple Parquet file") {
    val result = ParquetTestData.testData.select('myboolean, 'mylong).collect()
    result.zipWithIndex.foreach {
      case (row, index) => {
          if (index % 3 == 0)
            assert(row(0) === true, s"boolean field value in line $index did not match (every third row)")
          else
            assert(row(0) === false, s"boolean field value in line $index did not match")
        assert(row(1) === (index.toLong << 33), s"long field value in line $index did not match")
        assert(row.size === 2, s"number of columns in projection in line $index is incorrect")
      }
    }
  }

  test("Writing metadata from scratch for table CREATE") {
    val job = new Job()
    val path = new Path(getTempFilePath("testtable").getCanonicalFile.toURI.toString)
    val fs: FileSystem = FileSystem.getLocal(ContextUtil.getConfiguration(job))
    ParquetTypesConverter.writeMetaData(
      ParquetTestData.testData.output,
      path,
      TestSQLContext.sparkContext.hadoopConfiguration)
    assert(fs.exists(new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)))
    val metaData = ParquetTypesConverter.readMetaData(path, Some(ContextUtil.getConfiguration(job)))
    assert(metaData != null)
    ParquetTestData
      .testData
      .parquetSchema
      .checkContains(metaData.getFileMetaData.getSchema) // throws exception if incompatible
    metaData
      .getFileMetaData
      .getSchema
      .checkContains(ParquetTestData.testData.parquetSchema) // throws exception if incompatible
    fs.delete(path, true)
  }

  test("Creating case class RDD table") {
    TestSQLContext.sparkContext.parallelize((1 to 100))
      .map(i => TestRDDEntry(i, s"val_$i"))
      .registerTempTable("tmp")
    val rdd = sql("SELECT * FROM tmp").collect().sortBy(_.getInt(0))
    var counter = 1
    rdd.foreach {
      // '===' does not like string comparison?
      row: Row => {
        assert(row.getString(1).equals(s"val_$counter"), s"row $counter value ${row.getString(1)} does not match val_$counter")
        counter = counter + 1
      }
    }
  }

  test("Read a parquet file instead of a directory") {
    val file = getTempFilePath("parquet")
    val path = file.toString
    val fsPath = new Path(path)
    val fs: FileSystem = fsPath.getFileSystem(TestSQLContext.sparkContext.hadoopConfiguration)
    val rdd = TestSQLContext.sparkContext.parallelize((1 to 100))
      .map(i => TestRDDEntry(i, s"val_$i"))
    rdd.coalesce(1).saveAsParquetFile(path)

    val children = fs.listStatus(fsPath).filter(_.getPath.getName.endsWith(".parquet"))
    assert(children.length > 0)
    val readFile = parquetFile(path + "/" + children(0).getPath.getName)
    readFile.registerTempTable("tmpx")
    val rdd_copy = sql("SELECT * FROM tmpx").collect()
    val rdd_orig = rdd.collect()
    for(i <- 0 to 99) {
      assert(rdd_copy(i).apply(0) === rdd_orig(i).key,   s"key error in line $i")
      assert(rdd_copy(i).apply(1) === rdd_orig(i).value, s"value error in line $i")
    }
    Utils.deleteRecursively(file)
  }

  test("Insert (overwrite) via Scala API") {
    val dirname = Utils.createTempDir()
    val source_rdd = TestSQLContext.sparkContext.parallelize((1 to 100))
      .map(i => TestRDDEntry(i, s"val_$i"))
    source_rdd.registerTempTable("source")
    val dest_rdd = createParquetFile[TestRDDEntry](dirname.toString)
    dest_rdd.registerTempTable("dest")
    sql("INSERT OVERWRITE INTO dest SELECT * FROM source").collect()
    val rdd_copy1 = sql("SELECT * FROM dest").collect()
    assert(rdd_copy1.size === 100)

    sql("INSERT INTO dest SELECT * FROM source")
    val rdd_copy2 = sql("SELECT * FROM dest").collect().sortBy(_.getInt(0))
    assert(rdd_copy2.size === 200)
    Utils.deleteRecursively(dirname)
  }

  test("Insert (appending) to same table via Scala API") {
    sql("INSERT INTO testsource SELECT * FROM testsource")
    val double_rdd = sql("SELECT * FROM testsource").collect()
    assert(double_rdd != null)
    assert(double_rdd.size === 30)

    // let's restore the original test data
    Utils.deleteRecursively(ParquetTestData.testDir)
    ParquetTestData.writeFile()
  }

  test("save and load case class RDD with nulls as parquet") {
    val data = parquet.NullReflectData(null, null, null, null, null)
    val rdd = sparkContext.parallelize(data :: Nil)

    val file = getTempFilePath("parquet")
    val path = file.toString
    rdd.saveAsParquetFile(path)
    val readFile = parquetFile(path)

    val rdd_saved = readFile.collect()
    assert(rdd_saved(0) === Seq.fill(5)(null))
    Utils.deleteRecursively(file)
    assert(true)
  }

  test("save and load case class RDD with Nones as parquet") {
    val data = parquet.OptionalReflectData(None, None, None, None, None)
    val rdd = sparkContext.parallelize(data :: Nil)

    val file = getTempFilePath("parquet")
    val path = file.toString
    rdd.saveAsParquetFile(path)
    val readFile = parquetFile(path)

    val rdd_saved = readFile.collect()
    assert(rdd_saved(0) === Seq.fill(5)(null))
    Utils.deleteRecursively(file)
    assert(true)
  }

  test("make RecordFilter for simple predicates") {
    def checkFilter[T <: FilterPredicate](predicate: Expression, defined: Boolean = true): Unit = {
      val filter = ParquetFilters.createFilter(predicate)
      if (defined) {
        assert(filter.isDefined)
        assert(filter.get.isInstanceOf[T])
      } else {
        assert(filter.isEmpty)
      }
    }

    checkFilter[Operators.Eq[Integer]]('a.int === 1)
    checkFilter[Operators.Eq[Integer]](Literal(1) === 'a.int)

    checkFilter[Operators.Lt[Integer]]('a.int < 4)
    checkFilter[Operators.Lt[Integer]](Literal(4) > 'a.int)
    checkFilter[Operators.LtEq[Integer]]('a.int <= 4)
    checkFilter[Operators.LtEq[Integer]](Literal(4) >= 'a.int)

    checkFilter[Operators.Gt[Integer]]('a.int > 4)
    checkFilter[Operators.Gt[Integer]](Literal(4) < 'a.int)
    checkFilter[Operators.GtEq[Integer]]('a.int >= 4)
    checkFilter[Operators.GtEq[Integer]](Literal(4) <= 'a.int)

    checkFilter[Operators.And]('a.int === 1 && 'a.int < 4)
    checkFilter[Operators.Or]('a.int === 1 || 'a.int < 4)
    checkFilter[Operators.Not](!('a.int === 1))

    checkFilter('a.int > 'b.int, defined = false)
    checkFilter(('a.int > 'b.int) && ('a.int > 'b.int), defined = false)
  }

  test("test filter by predicate pushdown") {
    for(myval <- Seq("myint", "mylong", "mydouble", "myfloat")) {
      val query1 = sql(s"SELECT * FROM testfiltersource WHERE $myval < 150 AND $myval >= 100")
      assert(
        query1.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
        "Top operator should be ParquetTableScan after pushdown")
      val result1 = query1.collect()
      assert(result1.size === 50)
      assert(result1(0)(1) === 100)
      assert(result1(49)(1) === 149)
      val query2 = sql(s"SELECT * FROM testfiltersource WHERE $myval > 150 AND $myval <= 200")
      assert(
        query2.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
        "Top operator should be ParquetTableScan after pushdown")
      val result2 = query2.collect()
      assert(result2.size === 50)
      if (myval == "myint" || myval == "mylong") {
        assert(result2(0)(1) === 151)
        assert(result2(49)(1) === 200)
      } else {
        assert(result2(0)(1) === 150)
        assert(result2(49)(1) === 199)
      }
    }
    for(myval <- Seq("myint", "mylong")) {
      val query3 = sql(s"SELECT * FROM testfiltersource WHERE $myval > 190 OR $myval < 10")
      assert(
        query3.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
        "Top operator should be ParquetTableScan after pushdown")
      val result3 = query3.collect()
      assert(result3.size === 20)
      assert(result3(0)(1) === 0)
      assert(result3(9)(1) === 9)
      assert(result3(10)(1) === 191)
      assert(result3(19)(1) === 200)
    }
    for(myval <- Seq("mydouble", "myfloat")) {
      val result4 =
        if (myval == "mydouble") {
          val query4 = sql(s"SELECT * FROM testfiltersource WHERE $myval > 190.5 OR $myval < 10.0")
          assert(
            query4.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
            "Top operator should be ParquetTableScan after pushdown")
          query4.collect()
        } else {
          // CASTs are problematic. Here myfloat will be casted to a double and it seems there is
          // currently no way to specify float constants in SqlParser?
          sql(s"SELECT * FROM testfiltersource WHERE $myval > 190.5 OR $myval < 10").collect()
        }
      assert(result4.size === 20)
      assert(result4(0)(1) === 0)
      assert(result4(9)(1) === 9)
      assert(result4(10)(1) === 191)
      assert(result4(19)(1) === 200)
    }
    val query5 = sql(s"SELECT * FROM testfiltersource WHERE myboolean = true AND myint < 40")
    assert(
      query5.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
      "Top operator should be ParquetTableScan after pushdown")
    val booleanResult = query5.collect()
    assert(booleanResult.size === 10)
    for(i <- 0 until 10) {
      if (!booleanResult(i).getBoolean(0)) {
        fail(s"Boolean value in result row $i not true")
      }
      if (booleanResult(i).getInt(1) != i * 4) {
        fail(s"Int value in result row $i should be ${4*i}")
      }
    }
    val query6 = sql("SELECT * FROM testfiltersource WHERE mystring = \"100\"")
    assert(
      query6.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
      "Top operator should be ParquetTableScan after pushdown")
    val stringResult = query6.collect()
    assert(stringResult.size === 1)
    assert(stringResult(0).getString(2) == "100", "stringvalue incorrect")
    assert(stringResult(0).getInt(1) === 100)

    val query7 = sql(s"SELECT * FROM testfiltersource WHERE myoptint < 40")
    assert(
      query7.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
      "Top operator should be ParquetTableScan after pushdown")
    val optResult = query7.collect()
    assert(optResult.size === 20)
    for(i <- 0 until 20) {
      if (optResult(i)(7) != i * 2) {
        fail(s"optional Int value in result row $i should be ${2*4*i}")
      }
    }
    for(myval <- Seq("myoptint", "myoptlong", "myoptdouble", "myoptfloat")) {
      val query8 = sql(s"SELECT * FROM testfiltersource WHERE $myval < 150 AND $myval >= 100")
      assert(
        query8.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
        "Top operator should be ParquetTableScan after pushdown")
      val result8 = query8.collect()
      assert(result8.size === 25)
      assert(result8(0)(7) === 100)
      assert(result8(24)(7) === 148)
      val query9 = sql(s"SELECT * FROM testfiltersource WHERE $myval > 150 AND $myval <= 200")
      assert(
        query9.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
        "Top operator should be ParquetTableScan after pushdown")
      val result9 = query9.collect()
      assert(result9.size === 25)
      if (myval == "myoptint" || myval == "myoptlong") {
        assert(result9(0)(7) === 152)
        assert(result9(24)(7) === 200)
      } else {
        assert(result9(0)(7) === 150)
        assert(result9(24)(7) === 198)
      }
    }
    val query10 = sql("SELECT * FROM testfiltersource WHERE myoptstring = \"100\"")
    assert(
      query10.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
      "Top operator should be ParquetTableScan after pushdown")
    val result10 = query10.collect()
    assert(result10.size === 1)
    assert(result10(0).getString(8) == "100", "stringvalue incorrect")
    assert(result10(0).getInt(7) === 100)
    val query11 = sql(s"SELECT * FROM testfiltersource WHERE myoptboolean = true AND myoptint < 40")
    assert(
      query11.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
      "Top operator should be ParquetTableScan after pushdown")
    val result11 = query11.collect()
    assert(result11.size === 7)
    for(i <- 0 until 6) {
      if (!result11(i).getBoolean(6)) {
        fail(s"optional Boolean value in result row $i not true")
      }
      if (result11(i).getInt(7) != i * 6) {
        fail(s"optional Int value in result row $i should be ${6*i}")
      }
    }

    val query12 = sql("SELECT * FROM testfiltersource WHERE mystring >= \"50\"")
    assert(
      query12.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
      "Top operator should be ParquetTableScan after pushdown")
    val result12 = query12.collect()
    assert(result12.size === 54)
    assert(result12(0).getString(2) == "6")
    assert(result12(4).getString(2) == "50")
    assert(result12(53).getString(2) == "99")

    val query13 = sql("SELECT * FROM testfiltersource WHERE mystring > \"50\"")
    assert(
      query13.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
      "Top operator should be ParquetTableScan after pushdown")
    val result13 = query13.collect()
    assert(result13.size === 53)
    assert(result13(0).getString(2) == "6")
    assert(result13(4).getString(2) == "51")
    assert(result13(52).getString(2) == "99")

    val query14 = sql("SELECT * FROM testfiltersource WHERE mystring <= \"50\"")
    assert(
      query14.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
      "Top operator should be ParquetTableScan after pushdown")
    val result14 = query14.collect()
    assert(result14.size === 148)
    assert(result14(0).getString(2) == "0")
    assert(result14(46).getString(2) == "50")
    assert(result14(147).getString(2) == "200")

    val query15 = sql("SELECT * FROM testfiltersource WHERE mystring < \"50\"")
    assert(
      query15.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
      "Top operator should be ParquetTableScan after pushdown")
    val result15 = query15.collect()
    assert(result15.size === 147)
    assert(result15(0).getString(2) == "0")
    assert(result15(46).getString(2) == "100")
    assert(result15(146).getString(2) == "200")
  }

  test("SPARK-1913 regression: columns only referenced by pushed down filters should remain") {
    val query = sql(s"SELECT mystring FROM testfiltersource WHERE myint < 10")
    assert(query.collect().size === 10)
  }

  test("Importing nested Parquet file (Addressbook)") {
    val result = TestSQLContext
      .parquetFile(ParquetTestData.testNestedDir1.toString)
      .toSchemaRDD
      .collect()
    assert(result != null)
    assert(result.size === 2)
    val first_record = result(0)
    val second_record = result(1)
    assert(first_record != null)
    assert(second_record != null)
    assert(first_record.size === 3)
    assert(second_record(1) === null)
    assert(second_record(2) === null)
    assert(second_record(0) === "A. Nonymous")
    assert(first_record(0) === "Julien Le Dem")
    val first_owner_numbers = first_record(1)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]]
    val first_contacts = first_record(2)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]]
    assert(first_owner_numbers != null)
    assert(first_owner_numbers(0) === "555 123 4567")
    assert(first_owner_numbers(2) === "XXX XXX XXXX")
    assert(first_contacts(0)
      .asInstanceOf[CatalystConverter.StructScalaType[_]].size === 2)
    val first_contacts_entry_one = first_contacts(0)
      .asInstanceOf[CatalystConverter.StructScalaType[_]]
    assert(first_contacts_entry_one(0) === "Dmitriy Ryaboy")
    assert(first_contacts_entry_one(1) === "555 987 6543")
    val first_contacts_entry_two = first_contacts(1)
      .asInstanceOf[CatalystConverter.StructScalaType[_]]
    assert(first_contacts_entry_two(0) === "Chris Aniszczyk")
  }

  test("Importing nested Parquet file (nested numbers)") {
    val result = TestSQLContext
      .parquetFile(ParquetTestData.testNestedDir2.toString)
      .toSchemaRDD
      .collect()
    assert(result.size === 1, "number of top-level rows incorrect")
    assert(result(0).size === 5, "number of fields in row incorrect")
    assert(result(0)(0) === 1)
    assert(result(0)(1) === 7)
    val subresult1 = result(0)(2).asInstanceOf[CatalystConverter.ArrayScalaType[_]]
    assert(subresult1.size === 3)
    assert(subresult1(0) === (1.toLong << 32))
    assert(subresult1(1) === (1.toLong << 33))
    assert(subresult1(2) === (1.toLong << 34))
    val subresult2 = result(0)(3)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]](0)
      .asInstanceOf[CatalystConverter.StructScalaType[_]]
    assert(subresult2.size === 2)
    assert(subresult2(0) === 2.5)
    assert(subresult2(1) === false)
    val subresult3 = result(0)(4)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]]
    assert(subresult3.size === 2)
    assert(subresult3(0).asInstanceOf[CatalystConverter.ArrayScalaType[_]].size === 2)
    val subresult4 = subresult3(0).asInstanceOf[CatalystConverter.ArrayScalaType[_]]
    assert(subresult4(0).asInstanceOf[CatalystConverter.ArrayScalaType[_]](0) === 7)
    assert(subresult4(1).asInstanceOf[CatalystConverter.ArrayScalaType[_]](0) === 8)
    assert(subresult3(1).asInstanceOf[CatalystConverter.ArrayScalaType[_]].size === 1)
    assert(subresult3(1).asInstanceOf[CatalystConverter.ArrayScalaType[_]](0)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]](0) === 9)
  }

  test("Simple query on addressbook") {
    val data = TestSQLContext
      .parquetFile(ParquetTestData.testNestedDir1.toString)
      .toSchemaRDD
    val tmp = data.where('owner === "Julien Le Dem").select('owner as 'a, 'contacts as 'c).collect()
    assert(tmp.size === 1)
    assert(tmp(0)(0) === "Julien Le Dem")
  }

  test("Projection in addressbook") {
    val data = parquetFile(ParquetTestData.testNestedDir1.toString).toSchemaRDD
    data.registerTempTable("data")
    val query = sql("SELECT owner, contacts[1].name FROM data")
    val tmp = query.collect()
    assert(tmp.size === 2)
    assert(tmp(0).size === 2)
    assert(tmp(0)(0) === "Julien Le Dem")
    assert(tmp(0)(1) === "Chris Aniszczyk")
    assert(tmp(1)(0) === "A. Nonymous")
    assert(tmp(1)(1) === null)
  }

  test("Simple query on nested int data") {
    val data = parquetFile(ParquetTestData.testNestedDir2.toString).toSchemaRDD
    data.registerTempTable("data")
    val result1 = sql("SELECT entries[0].value FROM data").collect()
    assert(result1.size === 1)
    assert(result1(0).size === 1)
    assert(result1(0)(0) === 2.5)
    val result2 = sql("SELECT entries[0] FROM data").collect()
    assert(result2.size === 1)
    val subresult1 = result2(0)(0).asInstanceOf[CatalystConverter.StructScalaType[_]]
    assert(subresult1.size === 2)
    assert(subresult1(0) === 2.5)
    assert(subresult1(1) === false)
    val result3 = sql("SELECT outerouter FROM data").collect()
    val subresult2 = result3(0)(0)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]](0)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]]
    assert(subresult2(0).asInstanceOf[CatalystConverter.ArrayScalaType[_]](0) === 7)
    assert(subresult2(1).asInstanceOf[CatalystConverter.ArrayScalaType[_]](0) === 8)
    assert(result3(0)(0)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]](1)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]](0)
      .asInstanceOf[CatalystConverter.ArrayScalaType[_]](0) === 9)
  }

  test("nested structs") {
    val data = parquetFile(ParquetTestData.testNestedDir3.toString)
      .toSchemaRDD
    data.registerTempTable("data")
    val result1 = sql("SELECT booleanNumberPairs[0].value[0].truth FROM data").collect()
    assert(result1.size === 1)
    assert(result1(0).size === 1)
    assert(result1(0)(0) === false)
    val result2 = sql("SELECT booleanNumberPairs[0].value[1].truth FROM data").collect()
    assert(result2.size === 1)
    assert(result2(0).size === 1)
    assert(result2(0)(0) === true)
    val result3 = sql("SELECT booleanNumberPairs[1].value[0].truth FROM data").collect()
    assert(result3.size === 1)
    assert(result3(0).size === 1)
    assert(result3(0)(0) === false)
  }

  test("simple map") {
    val data = TestSQLContext
      .parquetFile(ParquetTestData.testNestedDir4.toString)
      .toSchemaRDD
    data.registerTempTable("mapTable")
    val result1 = sql("SELECT data1 FROM mapTable").collect()
    assert(result1.size === 1)
    assert(result1(0)(0)
      .asInstanceOf[CatalystConverter.MapScalaType[String, _]]
      .getOrElse("key1", 0) === 1)
    assert(result1(0)(0)
      .asInstanceOf[CatalystConverter.MapScalaType[String, _]]
      .getOrElse("key2", 0) === 2)
    val result2 = sql("""SELECT data1["key1"] FROM mapTable""").collect()
    assert(result2(0)(0) === 1)
  }

  test("map with struct values") {
    val data = parquetFile(ParquetTestData.testNestedDir4.toString).toSchemaRDD
    data.registerTempTable("mapTable")
    val result1 = sql("SELECT data2 FROM mapTable").collect()
    assert(result1.size === 1)
    val entry1 = result1(0)(0)
      .asInstanceOf[CatalystConverter.MapScalaType[String, CatalystConverter.StructScalaType[_]]]
      .getOrElse("seven", null)
    assert(entry1 != null)
    assert(entry1(0) === 42)
    assert(entry1(1) === "the answer")
    val entry2 = result1(0)(0)
      .asInstanceOf[CatalystConverter.MapScalaType[String, CatalystConverter.StructScalaType[_]]]
      .getOrElse("eight", null)
    assert(entry2 != null)
    assert(entry2(0) === 49)
    assert(entry2(1) === null)
    val result2 = sql("""SELECT data2["seven"].payload1, data2["seven"].payload2 FROM mapTable""").collect()
    assert(result2.size === 1)
    assert(result2(0)(0) === 42.toLong)
    assert(result2(0)(1) === "the answer")
  }

  test("Writing out Addressbook and reading it back in") {
    // TODO: find out why CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME
    // has no effect in this test case
    val tmpdir = Utils.createTempDir()
    Utils.deleteRecursively(tmpdir)
    val result = parquetFile(ParquetTestData.testNestedDir1.toString).toSchemaRDD
    result.saveAsParquetFile(tmpdir.toString)
    parquetFile(tmpdir.toString)
      .toSchemaRDD
      .registerTempTable("tmpcopy")
    val tmpdata = sql("SELECT owner, contacts[1].name FROM tmpcopy").collect()
    assert(tmpdata.size === 2)
    assert(tmpdata(0).size === 2)
    assert(tmpdata(0)(0) === "Julien Le Dem")
    assert(tmpdata(0)(1) === "Chris Aniszczyk")
    assert(tmpdata(1)(0) === "A. Nonymous")
    assert(tmpdata(1)(1) === null)
    Utils.deleteRecursively(tmpdir)
  }

  test("Writing out Map and reading it back in") {
    val data = parquetFile(ParquetTestData.testNestedDir4.toString).toSchemaRDD
    val tmpdir = Utils.createTempDir()
    Utils.deleteRecursively(tmpdir)
    data.saveAsParquetFile(tmpdir.toString)
    parquetFile(tmpdir.toString)
      .toSchemaRDD
      .registerTempTable("tmpmapcopy")
    val result1 = sql("""SELECT data1["key2"] FROM tmpmapcopy""").collect()
    assert(result1.size === 1)
    assert(result1(0)(0) === 2)
    val result2 = sql("SELECT data2 FROM tmpmapcopy").collect()
    assert(result2.size === 1)
    val entry1 = result2(0)(0)
      .asInstanceOf[CatalystConverter.MapScalaType[String, CatalystConverter.StructScalaType[_]]]
      .getOrElse("seven", null)
    assert(entry1 != null)
    assert(entry1(0) === 42)
    assert(entry1(1) === "the answer")
    val entry2 = result2(0)(0)
      .asInstanceOf[CatalystConverter.MapScalaType[String, CatalystConverter.StructScalaType[_]]]
      .getOrElse("eight", null)
    assert(entry2 != null)
    assert(entry2(0) === 49)
    assert(entry2(1) === null)
    val result3 = sql("""SELECT data2["seven"].payload1, data2["seven"].payload2 FROM tmpmapcopy""").collect()
    assert(result3.size === 1)
    assert(result3(0)(0) === 42.toLong)
    assert(result3(0)(1) === "the answer")
    Utils.deleteRecursively(tmpdir)
  }

  test("Querying on empty parquet throws exception (SPARK-3536)") {
    val tmpdir = Utils.createTempDir()
    Utils.deleteRecursively(tmpdir)
    createParquetFile[TestRDDEntry](tmpdir.toString()).registerTempTable("tmpemptytable")
    val result1 = sql("SELECT * FROM tmpemptytable").collect()
    assert(result1.size === 0)
    Utils.deleteRecursively(tmpdir)
  }

  test("DataType string parser compatibility") {
    val schema = StructType(List(
      StructField("c1", IntegerType, false),
      StructField("c2", BinaryType, false)))

    val fromCaseClassString = ParquetTypesConverter.convertFromString(schema.toString)
    val fromJson = ParquetTypesConverter.convertFromString(schema.json)

    (fromCaseClassString, fromJson).zipped.foreach { (a, b) =>
      assert(a.name == b.name)
      assert(a.dataType === b.dataType)
    }
  }

  test("read/write fixed-length decimals") {
    for ((precision, scale) <- Seq((5, 2), (1, 0), (1, 1), (18, 10), (18, 17))) {
      val tempDir = getTempFilePath("parquetTest").getCanonicalPath
      val data = sparkContext.parallelize(0 to 1000)
        .map(i => NumericData(i, i / 100.0))
        .select('i, 'd cast DecimalType(precision, scale))
      data.saveAsParquetFile(tempDir)
      checkAnswer(parquetFile(tempDir), data.toSchemaRDD.collect().toSeq)
    }

    // Decimals with precision above 18 are not yet supported
    intercept[RuntimeException] {
      val tempDir = getTempFilePath("parquetTest").getCanonicalPath
      val data = sparkContext.parallelize(0 to 1000)
        .map(i => NumericData(i, i / 100.0))
        .select('i, 'd cast DecimalType(19, 10))
      data.saveAsParquetFile(tempDir)
      checkAnswer(parquetFile(tempDir), data.toSchemaRDD.collect().toSeq)
    }

    // Unlimited-length decimals are not yet supported
    intercept[RuntimeException] {
      val tempDir = getTempFilePath("parquetTest").getCanonicalPath
      val data = sparkContext.parallelize(0 to 1000)
        .map(i => NumericData(i, i / 100.0))
        .select('i, 'd cast DecimalType.Unlimited)
      data.saveAsParquetFile(tempDir)
      checkAnswer(parquetFile(tempDir), data.toSchemaRDD.collect().toSeq)
    }
  }

  def checkFilter(predicate: Predicate, filterClass: Class[_ <: FilterPredicate]): Unit = {
    val filter = ParquetFilters.createFilter(predicate)
    assert(filter.isDefined)
    assert(filter.get.getClass == filterClass)
  }

  test("Pushdown IsNull predicate") {
    checkFilter('a.int.isNull,    classOf[Operators.Eq[Integer]])
    checkFilter('a.long.isNull,   classOf[Operators.Eq[java.lang.Long]])
    checkFilter('a.float.isNull,  classOf[Operators.Eq[java.lang.Float]])
    checkFilter('a.double.isNull, classOf[Operators.Eq[java.lang.Double]])
    checkFilter('a.string.isNull, classOf[Operators.Eq[Binary]])
    checkFilter('a.binary.isNull, classOf[Operators.Eq[Binary]])
  }

  test("Pushdown IsNotNull predicate") {
    checkFilter('a.int.isNotNull,    classOf[Operators.NotEq[Integer]])
    checkFilter('a.long.isNotNull,   classOf[Operators.NotEq[java.lang.Long]])
    checkFilter('a.float.isNotNull,  classOf[Operators.NotEq[java.lang.Float]])
    checkFilter('a.double.isNotNull, classOf[Operators.NotEq[java.lang.Double]])
    checkFilter('a.string.isNotNull, classOf[Operators.NotEq[Binary]])
    checkFilter('a.binary.isNotNull, classOf[Operators.NotEq[Binary]])
  }

  test("Pushdown EqualTo predicate") {
    checkFilter('a.int === 0,                 classOf[Operators.Eq[Integer]])
    checkFilter('a.long === 0.toLong,         classOf[Operators.Eq[java.lang.Long]])
    checkFilter('a.float === 0.toFloat,       classOf[Operators.Eq[java.lang.Float]])
    checkFilter('a.double === 0.toDouble,     classOf[Operators.Eq[java.lang.Double]])
    checkFilter('a.string === "foo",          classOf[Operators.Eq[Binary]])
    checkFilter('a.binary === "foo".getBytes, classOf[Operators.Eq[Binary]])
  }

  test("Pushdown Not(EqualTo) predicate") {
    checkFilter(!('a.int === 0),                 classOf[Operators.NotEq[Integer]])
    checkFilter(!('a.long === 0.toLong),         classOf[Operators.NotEq[java.lang.Long]])
    checkFilter(!('a.float === 0.toFloat),       classOf[Operators.NotEq[java.lang.Float]])
    checkFilter(!('a.double === 0.toDouble),     classOf[Operators.NotEq[java.lang.Double]])
    checkFilter(!('a.string === "foo"),          classOf[Operators.NotEq[Binary]])
    checkFilter(!('a.binary === "foo".getBytes), classOf[Operators.NotEq[Binary]])
  }

  test("Pushdown LessThan predicate") {
    checkFilter('a.int < 0,                 classOf[Operators.Lt[Integer]])
    checkFilter('a.long < 0.toLong,         classOf[Operators.Lt[java.lang.Long]])
    checkFilter('a.float < 0.toFloat,       classOf[Operators.Lt[java.lang.Float]])
    checkFilter('a.double < 0.toDouble,     classOf[Operators.Lt[java.lang.Double]])
    checkFilter('a.string < "foo",          classOf[Operators.Lt[Binary]])
    checkFilter('a.binary < "foo".getBytes, classOf[Operators.Lt[Binary]])
  }

  test("Pushdown LessThanOrEqual predicate") {
    checkFilter('a.int <= 0,                 classOf[Operators.LtEq[Integer]])
    checkFilter('a.long <= 0.toLong,         classOf[Operators.LtEq[java.lang.Long]])
    checkFilter('a.float <= 0.toFloat,       classOf[Operators.LtEq[java.lang.Float]])
    checkFilter('a.double <= 0.toDouble,     classOf[Operators.LtEq[java.lang.Double]])
    checkFilter('a.string <= "foo",          classOf[Operators.LtEq[Binary]])
    checkFilter('a.binary <= "foo".getBytes, classOf[Operators.LtEq[Binary]])
  }

  test("Pushdown GreaterThan predicate") {
    checkFilter('a.int > 0,                 classOf[Operators.Gt[Integer]])
    checkFilter('a.long > 0.toLong,         classOf[Operators.Gt[java.lang.Long]])
    checkFilter('a.float > 0.toFloat,       classOf[Operators.Gt[java.lang.Float]])
    checkFilter('a.double > 0.toDouble,     classOf[Operators.Gt[java.lang.Double]])
    checkFilter('a.string > "foo",          classOf[Operators.Gt[Binary]])
    checkFilter('a.binary > "foo".getBytes, classOf[Operators.Gt[Binary]])
  }

  test("Pushdown GreaterThanOrEqual predicate") {
    checkFilter('a.int >= 0,                 classOf[Operators.GtEq[Integer]])
    checkFilter('a.long >= 0.toLong,         classOf[Operators.GtEq[java.lang.Long]])
    checkFilter('a.float >= 0.toFloat,       classOf[Operators.GtEq[java.lang.Float]])
    checkFilter('a.double >= 0.toDouble,     classOf[Operators.GtEq[java.lang.Double]])
    checkFilter('a.string >= "foo",          classOf[Operators.GtEq[Binary]])
    checkFilter('a.binary >= "foo".getBytes, classOf[Operators.GtEq[Binary]])
  }

  test("Comparison with null should not be pushed down") {
    val predicates = Seq(
      'a.int === null,
      !('a.int === null),

      Literal(null) === 'a.int,
      !(Literal(null) === 'a.int),

      'a.int < null,
      'a.int <= null,
      'a.int > null,
      'a.int >= null,

      Literal(null) < 'a.int,
      Literal(null) <= 'a.int,
      Literal(null) > 'a.int,
      Literal(null) >= 'a.int
    )

    predicates.foreach { p =>
      assert(
        ParquetFilters.createFilter(p).isEmpty,
        "Comparison predicate with null shouldn't be pushed down")
    }
  }

  test("Import of simple Parquet files using glob wildcard pattern") {
    val testGlobDir = ParquetTestData.testGlobDir.toString
    val globPatterns = Array(testGlobDir + "/*/*", testGlobDir + "/spark-*/*", testGlobDir + "/?pa?k-*/*")
    globPatterns.foreach { path =>
      val result = parquetFile(path).collect()
      assert(result.size === 45)
      result.zipWithIndex.foreach {
        case (row, index) => {
          val checkBoolean =
            if ((index % 15) % 3 == 0)
              row(0) == true
            else
              row(0) == false
          assert(checkBoolean === true, s"boolean field value in line $index did not match")
          if ((index % 15) % 5 == 0) assert(row(1) === 5, s"int field value in line $index did not match")
          assert(row(2) === "abc", s"string field value in line $index did not match")
          assert(row(3) === ((index.toLong % 15) << 33), s"long value in line $index did not match")
          assert(row(4) === 2.5F, s"float field value in line $index did not match")
          assert(row(5) === 4.5D, s"double field value in line $index did not match")
        }
      }
    }
  }
}
