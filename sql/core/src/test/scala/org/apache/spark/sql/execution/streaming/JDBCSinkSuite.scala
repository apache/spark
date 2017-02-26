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

package org.apache.spark.sql.execution.streaming


import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.ScalaReflection

import scala.language.implicitConversions
import collection.JavaConversions._
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.Utils
import org.h2.jdbc.JdbcBatchUpdateException

class JDBCSinkSuite extends StreamTest with BeforeAndAfter {


  import testImplicits._

  val url = "jdbc:h2:mem:testdb2"
  var connection: Option[java.sql.Connection] = None

  val properties = new Properties()
  properties.setProperty("user", "testUser")
  properties.setProperty("password", "testPass")
  properties.setProperty("rowId", "false")

  before {
    Utils.classForName("org.h2.Driver")


    connection = Some(DriverManager.getConnection(url, properties))
    withConnection { conn =>
      conn.prepareStatement("create schema test").executeUpdate()
      conn.prepareStatement("drop table if exists data").executeUpdate()
//      conn.prepareStatement(
//        "CREATE TABLE data (\"id\" INTEGER NOT NULL, \"name\" TEXT not null, \"gender\" TEXT not null) ")
//        .executeUpdate()
      conn.prepareStatement("drop table if exists aggdata").executeUpdate()
//      conn.prepareStatement(
//        "create table aggdata (gender varchar(50), count integer, batch_id BIGINT null )")
//        .executeUpdate()
      conn.commit()
    }

  }

  after {
    sqlContext.streams.active.foreach(_.stop())
    withConnection {_.close()}
  }



  test("directly add data in Append output mode") {
    implicit val schema = StructType(List(StructField("id", IntegerType, false)
                                          , StructField("name", StringType, false)
                                          , StructField("gender", StringType, false)))

    var parameters = Map("url" -> url, "dbtable" -> "data", "queryname" -> "jdbcstream") ++
      properties.map(elem => (elem._1.toString, elem._2.toString)).toMap

    val sink = new JdbcSink(sqlContext
      , parameters
      , Seq.empty
      , OutputMode.Append)


    // Add batch 0 and check outputs
    sink.addBatch(0, List(Superhero(1, "Iron Man", "male"), Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male")))
    checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
      , Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male"))

    // Add batch 1 and check outputs
    sink.addBatch(1, List(Superhero(4, "Captain America", "male"), Superhero(5, "Supergirl", "female")))
    checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
      , Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male")
      , Superhero(4, "Captain America", "male")
      , Superhero(5, "Supergirl", "female"))

    // Add batch 1 and check outputs
    sink.addBatch(1, List(Superhero(4, "Captain America", "male"), Superhero(5, "Supergirl", "female")))
    checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
      , Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male")
      , Superhero(4, "Captain America", "male")
      , Superhero(5, "Supergirl", "female"))

    // Re-add batch 1 with different data, should not be added and outputs should not be changed
    sink.addBatch(1, List(Superhero(6, "Wonder Woman", "female"), Superhero(5, "Hulk", "male")))
    checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
      , Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male")
      , Superhero(4, "Captain America", "male")
      , Superhero(5, "Supergirl", "female"))

    // Add batch 2 and check outputs
    sink.addBatch(2, List(Superhero(6, "Wonder Woman", "female"), Superhero(7, "Hulk", "male")))
    checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
      , Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male")
      , Superhero(4, "Captain America", "male")
      , Superhero(5, "Supergirl", "female")
      , Superhero(6, "Wonder Woman", "female")
      , Superhero(7, "Hulk", "male"))
  }

  test("directly add data in Complete output mode") {
    implicit val schema = StructType(List(StructField("id", IntegerType, false)
      , StructField("name", StringType, false)
      , StructField("gender", StringType, false)))

    var parameters = Map("url" -> url, "dbtable" -> "data", "queryname" -> "jdbcstream") ++
      properties.map(elem => (elem._1.toString, elem._2.toString)).toMap

    val sink = new JdbcSink(sqlContext
      , parameters
      , Seq.empty
      , OutputMode.Complete)


    // Add batch 0 and check outputs
    sink.addBatch(0, List(Superhero(1, "Iron Man", "male"), Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male")))
    checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
      , Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male"))

    // Add batch 1 and check outputs
    sink.addBatch(1, List(Superhero(4, "Captain America", "male"), Superhero(5, "Supergirl", "female")))
    checkSuperheroesInDatabase(Superhero(4, "Captain America", "male")
      , Superhero(5, "Supergirl", "female"))

    // Add batch 1 and check outputs
    sink.addBatch(1, List(Superhero(4, "Captain America", "male"), Superhero(5, "Supergirl", "female")))
    checkSuperheroesInDatabase(Superhero(4, "Captain America", "male")
      , Superhero(5, "Supergirl", "female"))

    // Re-add batch 1 with different data, should not be added and outputs should not be changed
    sink.addBatch(1, List(Superhero(6, "Wonder Woman", "female"), Superhero(5, "Hulk", "male")))
    checkSuperheroesInDatabase(Superhero(4, "Captain America", "male")
      , Superhero(5, "Supergirl", "female"))

    // Add batch 2 and check outputs
    sink.addBatch(2, List(Superhero(6, "Wonder Woman", "female"), Superhero(7, "Hulk", "male")))
    checkSuperheroesInDatabase(Superhero(6, "Wonder Woman", "female")
      , Superhero(7, "Hulk", "male"))
  }

  test("directly add data with failure in Append output mode - atleast once") {
    implicit val schema = StructType(List(StructField("id", IntegerType, false)
      , StructField("name", StringType, false)
      , StructField("gender", StringType, false)))

    var parameters = Map("url" -> url, "dbtable" -> "data", "queryname" -> "jdbcstream") ++
      properties.map(elem => (elem._1.toString, elem._2.toString)).toMap

    val sink = new JdbcSink(sqlContext
      , parameters
      , Seq.empty
      , OutputMode.Append)


    // Add batch 0 and check outputs
    sink.addBatch(0, List(Superhero(1, "Iron Man", "male"), Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male")))
    checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
      , Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male"))

    // Add a bad batch 1 and check outputs
    try {
       val badSchema = StructType(List(StructField("id", IntegerType, false)
        , StructField("name", StringType, false)
        , StructField("gender", StringType, true)))
      sink.addBatch(1, rowsToDF(List(Row(4, "Captain America", "male"), Row(5, "Supergirl", null)), badSchema))
    } catch {
      case e: SparkException =>
      // do nothing.. it should throw this exception
      assert(true)
      case e: Throwable => throw e
    }
    checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
      , Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male")
      , Superhero(4, "Captain America", "male"))

    // Add batch 1 and check outputs
    sink.addBatch(1, List(Superhero(4, "Captain America", "male"), Superhero(5, "Supergirl", "female")))
    checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
      , Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male")
      , Superhero(4, "Captain America", "male")
      , Superhero(4, "Captain America", "male")
      , Superhero(5, "Supergirl", "female"))
  }

  test("directly add data with failure in Append output mode - exactly once") {
    implicit val schema = StructType(List(StructField("id", IntegerType, false)
      , StructField("name", StringType, false)
      , StructField("gender", StringType, false)))

    var parameters = Map("url" -> url, "dbtable" -> "data", "queryname" -> "jdbcstream"
                        , "batchIdCol" -> "batch_id") ++
      properties.map(elem => (elem._1.toString, elem._2.toString)).toMap

    val sink = new JdbcSink(sqlContext
      , parameters
      , Seq.empty
      , OutputMode.Append)

    // Add batch 0 and check outputs
    sink.addBatch(0, List(Superhero(1, "Iron Man", "male"), Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male")))
    checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
      , Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male"))

    // Add a bad batch 1 and check outputs
    try {
      val badSchema = StructType(List(StructField("id", IntegerType, false)
        , StructField("name", StringType, false)
        , StructField("gender", StringType, true)))
      sink.addBatch(1, rowsToDF(List(Row(4, "Captain America", "male"), Row(5, "Supergirl", null)), badSchema))
    } catch {
      case e: SparkException =>
        // do nothing.. it should throw this exception
        assert(true)
      case e: Throwable => throw e
    }

    // Add batch 1 and check outputs
    sink.addBatch(1, List(Superhero(4, "Captain America", "male"), Superhero(5, "Supergirl", "female")))
    checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
      , Superhero(2, "Black Widow", "female")
      , Superhero(3, "Batman", "male")
      , Superhero(4, "Captain America", "male")
      , Superhero(5, "Supergirl", "female"))
  }


  test("registering as a table in Append output mode") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Superhero]
      val query = input.toDF().writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .outputMode("append")
        .queryName("jdbcStream")
        .jdbc(url, "data", properties)
      input.addData(Superhero(1, "Iron Man", "male"), Superhero(2, "Black Widow", "female")
        , Superhero(3, "Batman", "male"))
      query.processAllAvailable()

      checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
        , Superhero(2, "Black Widow", "female")
        , Superhero(3, "Batman", "male"))

      input.addData(Superhero(4, "Captain America", "male"), Superhero(5, "Supergirl", "female"))
      query.processAllAvailable()

      checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
        , Superhero(2, "Black Widow", "female")
        , Superhero(3, "Batman", "male")
        , Superhero(4, "Captain America", "male")
        , Superhero(5, "Supergirl", "female"))

      query.stop()
    }
  }


  test("registering as a table in Append output mode - exactly once") {
    withTempDir { checkpointDir =>
      val exactlyOnceProperties = new Properties()
      exactlyOnceProperties.putAll(properties)
      exactlyOnceProperties.setProperty("batchIdCol", "batch_id")
      val input = MemoryStream[Superhero]
      val query = input.toDF().writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .outputMode("append")
        .queryName("jdbcStream")
        .jdbc(url, "data", exactlyOnceProperties)
      input.addData(Superhero(1, "Iron Man", "male"), Superhero(2, "Black Widow", "female")
        , Superhero(3, "Batman", "male"))
      query.processAllAvailable()

      checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
        , Superhero(2, "Black Widow", "female")
        , Superhero(3, "Batman", "male"))

      input.addData(Superhero(4, "Captain America", "male"), Superhero(5, "Supergirl", "female"))
      query.processAllAvailable()

      checkSuperheroesInDatabase(Superhero(1, "Iron Man", "male")
        , Superhero(2, "Black Widow", "female")
        , Superhero(3, "Batman", "male")
        , Superhero(4, "Captain America", "male")
        , Superhero(5, "Supergirl", "female"))

      query.stop()
    }
  }


  test("registering as a table in Overwrite output mode") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Superhero]
      val query = input.toDF()
        .groupBy("gender")
        .count()
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .outputMode("complete")
        .queryName("jdbcStream")
        .jdbc(url, "aggdata", properties)
      input.addData(Superhero(1, "Iron Man", "male"), Superhero(2, "Black Widow", "female")
        , Superhero(3, "Batman", "male"))
      query.processAllAvailable()


      checkGenderCountsInDatabase(Superhero(1, "Iron Man", "male")
        , Superhero(2, "Black Widow", "female")
        , Superhero(3, "Batman", "male"))

      input.addData(Superhero(4, "Captain America", "male"), Superhero(5, "Supergirl", "female"))
      query.processAllAvailable()


      checkGenderCountsInDatabase(Superhero(1, "Iron Man", "male")
        , Superhero(2, "Black Widow", "female")
        , Superhero(3, "Batman", "male")
        , Superhero(4, "Captain America", "male")
        , Superhero(5, "Supergirl", "female"))

      query.stop()
    }
  }


  test("registering as a table in Overwrite output mode - exactly once") {
    withTempDir { checkpointDir =>
      val exactlyOnceProperties = new Properties()
      exactlyOnceProperties.putAll(properties)
      exactlyOnceProperties.setProperty("batchIdCol", "batch_id")
      val input = MemoryStream[Superhero]
      val query = input.toDF()
        .groupBy("gender")
        .count()
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .outputMode("complete")
        .queryName("jdbcStream")
        .jdbc(url, "aggdata", exactlyOnceProperties)
      input.addData(Superhero(1, "Iron Man", "male"), Superhero(2, "Black Widow", "female")
        , Superhero(3, "Batman", "male"))
      query.processAllAvailable()


      checkGenderCountsInDatabase(Superhero(1, "Iron Man", "male")
        , Superhero(2, "Black Widow", "female")
        , Superhero(3, "Batman", "male"))

      input.addData(Superhero(4, "Captain America", "male"), Superhero(5, "Supergirl", "female"))
      query.processAllAvailable()


      checkGenderCountsInDatabase(Superhero(1, "Iron Man", "male")
        , Superhero(2, "Black Widow", "female")
        , Superhero(3, "Batman", "male")
        , Superhero(4, "Captain America", "male")
        , Superhero(5, "Supergirl", "female"))

      query.stop()
    }
  }

  def checkSuperheroesInDatabase(expected: Superhero*) : Unit = {
    val expectedTuples = expected.map(d => List(("id", d.id.toString), ("name", d.name)))
    foreachResult("Select \"id\", \"name\" from data")({row =>
      assert(expectedTuples.contains(row))
    }, {numRows =>
      assert(numRows==expected.size)
    })
  }

  def checkGenderCountsInDatabase(expected: Superhero*) : Unit = {
    val genderCounts = expected.map(_.gender)
                      .foldLeft(Map.empty[String, Int]){
                        (count, gender) =>
                          count + (gender -> (count.getOrElse(gender, 0) + 1))
                      }
    foreachResult("Select * from aggdata")({row =>
      val gender: String = row.filter(_._1.startsWith("gender")).map(_._2).head.toString
      val count: Int = row.filter(_._1.startsWith("count")).map(_._2).head.toString.toInt
      assert(genderCounts.getOrElse(gender, 0)==count
        , s"Checking counts for ${gender}")
    }, {numRows =>
      assert(numRows==genderCounts.size)
    })
  }

  def withConnection(f: (java.sql.Connection => Unit)) : Unit = {
    if (connection.isEmpty) {
      fail("Connection to empty database is closed")
    }
    connection.foreach(f)

  }
  def sql(sqlText: String, f: (java.sql.ResultSet => Unit)) : Unit = {
    withConnection {conn =>
      val stmt = conn.prepareStatement(sqlText)
      try {
        val rs: java.sql.ResultSet = stmt.executeQuery()
        try {
          f(rs)
        } finally {
          rs.close()
        }
      } finally {
        stmt.close()
      }
    }
  }
  def foreachResult(sqlText: String)(handleRow: (List[(String, Any)] => Unit), handleCount: (Integer => Unit)) : Unit = {
    sql(sqlText, {rs =>
      val cols = colNames(rs.getMetaData)
      var i = 0
      while(rs.next()) {
        val row = cols.map(col => (col, rs.getString(col)))
        handleRow(row)
        i = i + 1
      }
      handleCount(i)
    })
  }

  def colNames(metadata: java.sql.ResultSetMetaData, startCol: Int = 1) : List[String] = {
    if (startCol==metadata.getColumnCount) {
      List(metadata.getColumnName(startCol))
    } else {
      List(metadata.getColumnName(startCol)) ++ colNames(metadata, startCol + 1)
    }
  }



  private implicit def superheroesintsToDF(seq: List[Superhero])(implicit schema: StructType): DataFrame = {

    val rows : List[Row] = seq.map(s=>Row(s.id, s.name, s.gender))
    sqlContext.createDataFrame(rows, schema)
  }

  private  def rowsToDF(rows: List[Row], schema: StructType): DataFrame = {

    sqlContext.createDataFrame(rows, schema)
  }

}

case class Superhero(id: Int, name: String, gender: String)