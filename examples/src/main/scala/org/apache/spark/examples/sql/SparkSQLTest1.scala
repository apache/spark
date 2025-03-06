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

package org.apache.spark.examples.sql

import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.types.{DataType, UserDefinedType}

// scalastyle:off println
object SparkSQLTest1 {

  def show(rows: Array[Row]): Unit = {
    var printHeader = false
    for (row <- rows) {
      if (!printHeader) {
        val headerBuilder = new StringBuilder("|")
        val lineBuilder = new StringBuilder("|")
        for (field <- row.schema.fields) {
          headerBuilder.append(f"${field.name}%10s").append("|")
          lineBuilder.append("----------").append("|")
        }
        println(headerBuilder.toString())
        println(lineBuilder.toString())
        printHeader = true
      }
      val contextBuilder = new StringBuilder("|")
      for (ele <- row.toSeq) {
        contextBuilder.append(f"$ele%10s").append("|")
      }
      println(contextBuilder.toString)
    }
  }

  def example(spark: SparkSession, sqlCommand: String,
              sortByShuffle: Boolean, showResult: Boolean = true): Array[Row] = {
    spark.sqlContext.setConf(SORTED_SHUFFLE_ENABLED.key, sortByShuffle.toString)
    val res = spark.sql(sqlCommand).collect()
    if (showResult) {
      show(res)
    }
    res
  }

  def testSortByShuffle(spark: SparkSession, sqlCommand: String,
                        considerSort: Boolean = false, show: Boolean = true): Unit = {
    var success = true
    val expect = example(spark, sqlCommand, false, show)
    val actual = example(spark, sqlCommand, true, show)
    if (expect.length != actual.length) {
      println(s"Got wrong result size," +
        s"expected size is ${expect.size},actual size is ${actual.size}")
      success = false
    } else if (considerSort) {
      for ((e, a) <- expect.zip(actual)) {
        if (!e.equals(a)) {
          println(s"Got wrong matched result," +
            s"expected result is ${e},actual result is ${a}")
          success = false
        }
      }
    } else {
      val expectSet = expect.toSet
      for (a <- actual) {
        if (!expectSet.contains(a)) {
          println(s"Got actual result ${a} which is not expect.")
          success = false
        }
      }
    }
    if (success) {
      println(s"Executed ${sqlCommand}\nSUCCESS!!!")
    } else {
      println(s"Executed ${sqlCommand}\nFAIL!!!")
      sys.exit()
    }
  }

  case class UDFDataType(value: Int) extends UserDefinedType[UDFDataType] {
    override def sqlType: DataType = this
    override def serialize(obj: UDFDataType): Int = value
    def deserialize(datum: Any): UDFDataType = datum match {case v: Int => UDFDataType(v)}
    override def userClass: Class[UDFDataType] = classOf[UDFDataType]
    private[spark] override def asNullable: UDFDataType = this
  }

  object SumUDFDataType extends Aggregator[Integer, Integer, UDFDataType] {
    override def zero: Integer = 0
    override def reduce(b: Integer, a: Integer): Integer = a + b
    override def merge(b1: Integer, b2: Integer): Integer = b1 + b2
    override def finish(reduction: Integer): UDFDataType = UDFDataType(reduction)
    override def bufferEncoder: Encoder[Integer] = Encoders.INT
    override def outputEncoder: Encoder[UDFDataType] = ExpressionEncoder()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSQLTest1").
      config("spark.memory.fraction", "0.1").
      config(CODEGEN_COMMENTS.key, "true").master("local").getOrCreate()
    spark.sqlContext.setConf(ADAPTIVE_EXECUTION_ENABLED.key, "false")
    spark.sqlContext.setConf(SORTED_SHUFFLE_ENABLED.key, "true")
    spark.sqlContext.setConf(CODEGEN_FALLBACK.key, "false")
    // disable codegen
//    spark.sqlContext.setConf(WHOLESTAGE_HUGE_METHOD_LIMIT.key, "0")
//    spark.sqlContext.setConf(CODEGEN_FACTORY_MODE.key, "NO_CODEGEN")
    // Used to simulate that memory is not enough
//    spark.sqlContext.setConf("spark.sql.TungstenAggregate.testFallbackStartsAt", "10")
//    spark.sqlContext.setConf(HASH_AGG_MAX_RECORD_IN_MEMORY.key, "10")
//    // Only for ObjectHashAggregateExec and case "2.5 groupBy gender with udf"
//    spark.sqlContext.setConf(OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, "1")

    spark.read.json("examples/src/main/resources/student.json").createOrReplaceTempView("student")
    var sqlCommand = ""

    // 1 Select with filter
    // 1.1 Select with filter
    sqlCommand = "select id, name, age from student where age > 16"
    testSortByShuffle(spark, sqlCommand)
//    show(spark.sql(sqlCommand).collect())

    // 1.2 Select with filter and sort
    sqlCommand = "select * from student where age > 16 order by age"
    testSortByShuffle(spark, sqlCommand, true)
//    show(spark.sql(sqlCommand).collect())

    // 1.3 Select with filter and sort
    sqlCommand = "select * from student where age > 16 order by gender, score desc"
    testSortByShuffle(spark, sqlCommand, true)
//    show(spark.sql(sqlCommand).collect())

    // 1.4 select with filter and repartition
    sqlCommand = "select /*+ REPARTITION(4) */ * from student where age > 16"
    testSortByShuffle(spark, sqlCommand)
//    show(spark.sql(sqlCommand).collect())

    // 1.5 select with filter and sort and repartition
    sqlCommand = "select /*+ REPARTITION(4) */ * from student" +
      " where age > 16 order by gender, score desc"
    testSortByShuffle(spark, sqlCommand)
//    show(spark.sql(sqlCommand).collect())

    // 2 GroupBy
    // 2.1 GroupBy gender
    sqlCommand = "select gender, avg(score) from student group by gender"
    testSortByShuffle(spark, sqlCommand)
//    show(spark.sql(sqlCommand).collect())

    // 2.2 GroupBy gender and age
    sqlCommand = "select gender, age, avg(score) from student group by gender, age"
    testSortByShuffle(spark, sqlCommand)
//    show(spark.sql(sqlCommand).collect())

    // 2.3 GroupBy gender and age, then sort by gender and age
    sqlCommand = "select gender, age, avg(score) from student " +
      "group by gender, age order by gender, age"
    testSortByShuffle(spark, sqlCommand, true)
//    show(spark.sql(sqlCommand).collect())

    // 2.4 GroupBy gender, and distinct by age, avg by score
    sqlCommand = "select gender, avg(score), count(distinct age) " +
      "from student group by gender"
    testSortByShuffle(spark, sqlCommand)
//    show(spark.sql(sqlCommand).collect())

    // 2.5 groupBy gender with udf
    // Here we use user defined type so that we can use ObjectHashAggregateExec
    // but not HashAggregateExec
    import org.apache.spark.sql.functions
    sqlCommand = "select gender, mysum(score) as mysum_score from student group by gender"
    spark.udf.register("mysum", functions.udaf(SumUDFDataType))
    testSortByShuffle(spark, sqlCommand)
//    show(spark.sql(sqlCommand).collect())

    // 2.6 groupBy id, will generate many keys
    sqlCommand = "select id, avg(score) from student group by id"
    testSortByShuffle(spark, sqlCommand, false)
//    show(spark.sql(sqlCommand).collect())

    // 2.7 groupBy id and name without any aggregation functions
    sqlCommand = "select id, name from student group by id, name"
    testSortByShuffle(spark, sqlCommand)
//    show(spark.sql(sqlCommand).collect())

    // 2.8 groupBy id with distinct and sum
    // In this example, will generate two shuffle.
    // First shuffle have two grouping expression: gender, score.
    //   And the map of first Shuffle have one aggregate expressions: partial_sum(score)
    //   And the reduce of first shuffle have one aggregate expressions: merge_sum(score).
    // Second shuffle have one grouping expression: gender.
    //   And the map of second shuffle have two aggregate expressions:
    //     merge_sum(score), partial_count(distinct id)
    //   And the reduce of second shuffle have two aggregate expressions:
    //     sum(score), count(distinct id)
    sqlCommand = "select sum(score), count(distinct id) from student group by gender"
    testSortByShuffle(spark, sqlCommand)
//    show(spark.sql(sqlCommand).collect())

    // 3 window
    // 3.1 rank partitioned by gender, order by core.
    sqlCommand = "select id, name, gender, score, " +
      "rank() over(partition by gender order by score desc) as rank from student"
    testSortByShuffle(spark, sqlCommand, true)
//    show(spark.sql(sqlCommand).collect())

    // 4 Join
    spark.read.json("examples/src/main/resources/student_info1.json")
      .createOrReplaceTempView("student_info1")
    spark.read.json("examples/src/main/resources/student_info2.json")
      .createOrReplaceTempView("student_info2")
//    // disable broadcast join and prefer sort merge join
    spark.sqlContext.setConf(AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    spark.sqlContext.setConf(PREFER_SORTMERGEJOIN.key, "true")
    // 4.1 two table join
    sqlCommand = "select student.id,student.name,student.gender," +
      "student_info1.address from student " +
      "join student_info1 on student.name = student_info1.name " +
      "and student.score = student_info1.score"
    testSortByShuffle(spark, sqlCommand)
//    show(spark.sql(sqlCommand).collect())

    // 4.2 three table join
    sqlCommand = "select student.id,student.name,student.gender," +
      "student_info1.address, student_info2.hobby from student " +
      "join student_info1 on student.name = student_info1.name " +
      "join student_info2 on student.name = student_info2.name"
    testSortByShuffle(spark, sqlCommand)
//    show(spark.sql(sqlCommand).collect())

    spark.stop()
  }
}
