package org.apache.spark.sql.execution.streaming

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import spark.{Route, Spark, Request, Response}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.BeforeAndAfter

class HttpStreamSinkSuite extends StreamTest with SharedSQLContext with BeforeAndAfter{
  import testImplicits._
  after {
    sqlContext.streams.active.foreach(_.stop())
  }
  test("http sink"){
    var output: String = ""
    Spark.port(3775)
    Spark.get("/welcome/:vistor", new Route{
      override def handle(req: Request, resp: Response) : Object = {
        val name: String = req.params(":vistor")
        output = name
        return s"welcome $name"
      }
    })
    val input = MemoryStream[String]
    val query = input.toDF().writeStream
      .outputMode("complete")
      .format("http")
      .option("url", "http://localhost:3775/welcome")
      .start()
    input.addData("Jerry")
    CheckAnswer(Row(output))
    query.awaitTermination()
  }
}
