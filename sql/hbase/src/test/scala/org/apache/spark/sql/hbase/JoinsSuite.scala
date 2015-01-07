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

package org.apache.spark.sql.hbase

import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.log4j.Logger

case class JoinTable(intcol: Int)
case class JoinTable2Cols(intcol: Int, strcol: String)

class JoinsSuite extends JoinsSuiteBase  {

  private val logger = Logger.getLogger(getClass.getName)

  var testnm: String = _

  ignore ("Smoke test for SchemaRdd registerTempTable") {
    testnm = "Smoke test for SchemaRdd registerTempTable"
    import org.apache.spark.sql._

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val schema =
      new StructType(
        StructField("name", StringType, nullable = false) ::
          StructField("age", IntegerType, nullable = true) :: Nil)

    val people =
      sc.textFile("examples/src/main/resources/people.txt").map(
        _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
    val peopleSchemaRDD = sqlContext.applySchema(people, schema)
    peopleSchemaRDD.printSchema()
    // root
    // |-- name: string (nullable = false)
    // |-- age: integer (nullable = true)

    peopleSchemaRDD.registerTempTable("people")
    sqlContext.sql("select name from people").collect().foreach(println)
  }

//  testnm = "Smoke test for SchemaRdd registerTempTable"
//  test("Smoke test for SchemaRdd registerTempTable") {
//    case class Record(key: Int, value: String)
//
//    val sqlContext = new SQLContext(sc)
//
//    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
//    import sqlContext._
//
//    val rdd = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
//    // Any RDD containing case classes can be registered as a table.  The schema of the table is
//    // automatically inferred using scala reflection.
//    rdd.registerTempTable("records")
//
//    val results: SchemaRDD = sql("SELECT * FROM records")
//  }

  ignore("Basic Join on vanilla SparkSql: Simple Two Way") {
    testnm = "Basic Join on vanilla SparkSql: Simple Two Way"
    import org.apache.spark.sql._
//    val hbclocal = hbc.asInstanceOf[SQLContext]
//    import hbclocal._
    val ssc = new SQLContext(hbc.sparkContext)
    import ssc._
    val rdd1 = sc.parallelize((1 to 2).map{ix => JoinTable(ix)})
    val rdd2 = sc.parallelize((1 to 4).map{ix => JoinTable(ix/2)})
    /* val table1 = */ rdd1.registerTempTable("SparkJoinTable1")
    val table2 = rdd2.registerTempTable("SparkJoinTable2")
    val query = s"""select t1.intcol t1intcol, t2.intcol t2intcol from SparkJoinTable1 t1 JOIN
                    SparkJoinTable2 t2 on t1.intcol = t2.intcol""".stripMargin

    println(query)
    val res = ssc.sql(query)
//    res.collect.foreach(println)
    val exparr = Seq[Seq[Any]](
      Seq(1,1),
      Seq(1,1),
      Seq(2,2))
    run(ssc, testnm, query, exparr)
  }

  ignore("Basic Join on vanilla SparkSql: Simple Two Way using where") {
    testnm = "Basic Join on vanilla SparkSql: Simple Two Way using where"
    import org.apache.spark.sql._
//    val hbclocal = hbc.asInstanceOf[SQLContext]
//    import hbclocal._
    val ssc = new SQLContext(hbc.sparkContext)
    import ssc._
    val rdd1 = sc.parallelize((1 to 2).map{ix => JoinTable(ix)})
    val rdd2 = sc.parallelize((1 to 4).map{ix => JoinTable(ix/2)})
    /* val table1 = */ rdd1.registerTempTable("SparkJoinTable1")
    val table2 = rdd2.registerTempTable("SparkJoinTable2")
    val query = s"""select t1.intcol t1intcol, t2.intcol t2intcol from SparkJoinTable1 t1,
                    SparkJoinTable2 t2 where t1.intcol = t2.intcol""".stripMargin

    println(query)
    val res = ssc.sql(query)
//    res.collect.foreach(println)
    val exparr = Seq[Seq[Any]](
      Seq(1,1),
      Seq(1,1),
      Seq(2,2))
    run(ssc, testnm, query, exparr)
  }

  ignore("Basic Join on vanilla SparkSql: Simple Two Way  2 cols") {
    testnm = "Basic Join on vanilla SparkSql: Simple Two Way 2 cols"
    import org.apache.spark.sql._
//    val hbclocal = hbc.asInstanceOf[SQLContext]
//    import hbclocal._
    val ssc = new SQLContext(hbc.sparkContext)
//    val ssc = new SQLContext(sc)
    import ssc._
    val rdd1 = sc.parallelize((1 to 2).map{ix => JoinTable2Cols(ix, s"valA$ix")})
    rdd1.registerTempTable("SparkJoinTable1")
    val q1 = ssc.sql("select * from SparkJoinTable1").collect().foreach(println)
    println("hi")
    val ids = Seq((1,1),(1,2),(2,3),(2,4))
    val rdd2 = sc.parallelize(ids.map{ case (ix,is) => JoinTable2Cols(ix, s"valB$is")})
    val table2 = rdd2.registerTempTable("SparkJoinTable2")
    val q2 = ssc.sql("select * from SparkJoinTable2").collect().foreach(println)
    println("hi2")
    val query = s"""select t1.intcol t1intcol, t2.intcol t2intcol, t1.strcol t1strcol,
                t2.strcol t2strcol from SparkJoinTable1 t1 JOIN
                    SparkJoinTable2 t2 on t1.intcol = t2.intcol""".stripMargin

    println(query)
    val res = ssc.sql(query).sortBy( r =>
      s"${r.getInt(0)} ${r.getInt(1)} ${r.getString(2)} ${r.getString(3)}")
//    res.collect.foreach(println)
    val exparr = Seq[Seq[Any]](
      Seq(1,1, "valA1", "valB1"),
      Seq(1,1, "valA1", "valB2"),
      Seq(2,2, "valA2", "valB3"),
      Seq(2,2, "valA2", "valB4"))
    run(ssc, testnm, query, exparr)
  }

  test("Basic Join: Simple Two Way 2 cols Hbase on single col table") {
    testnm = "Basic Join: Simple Two Way 2 cols Hbase on single col table"
    val hdesc1 = new HTableDescriptor(TableName.valueOf("HbJoinTableOneCol1"))
    hdesc1.addFamily(new HColumnDescriptor("cf1"))
    hbaseAdmin.createTable(hdesc1)
    val hdesc2 = new HTableDescriptor(TableName.valueOf("HbJoinTableOneCol2"))
    hdesc2.addFamily(new HColumnDescriptor("cf2"))
    hbaseAdmin.createTable(hdesc2)
    val sql1 = "CREATE TABLE JoinTableOneCol1 (intcol INTEGER, PRIMARY KEY(intcol)) MAPPED BY" +
      " (HbJoinTableOneCol1, COLS=[])"
    runQuery(sql1)
    val sql2 = "CREATE TABLE JoinTableOneCol2 (intcol  INTEGER, primary key(intcol)) MAPPED BY" +
      " (HbJoinTableOneCol2, COLS=[])"
    runQuery(sql2)
    val query = s"""select t1.intcol t1intcol, t2.intcol t2intcol from JoinTableOneCol1 t1 JOIN
                    JoinTableOneCol2 t2 on t1.intcol = t2.intcol""".stripMargin

    val loads1 = s"load data local inpath '$CsvPath/onecoljoin1.txt' overwrite into table" +
      s" JoinTableOneCol1"
    runQuery(loads1)
    val loads2 = s"load data local inpath '$CsvPath/onecoljoin2.txt' into table" +
      s" JoinTableOneCol2"
    runQuery(loads2)
    val exparr = Seq(
      Seq(1, 1),
      Seq(1, 1))
    run(hbc, testnm, query, exparr)
  }

  ignore ("Basic Join: Simple Two Way 2 cols Hbase") {
    testnm = "Basic Join: Simple Two Way 2 cols Hbase"
    val query = s"""select t1.intcol t1intcol, t2.intcol t2intcol from JoinTable1 t1 JOIN
                    JoinTable2 t2 on t1.intcol = t2.intcol""".stripMargin

    val exparr = Seq(
      Seq(1, 1),
      Seq(1, 1))
    run(hbc, testnm, query, exparr)
  }

  ignore("Basic Join: Simple Two Way 2 cols Hbase using where") {
    testnm = "Basic Join: Simple Two Way 2 cols Hbase"
    val query = s"""select t1.intcol t1intcol, t2.intcol t2intcol from JoinTable1 t1,
                    JoinTable2 t2 where t1.intcol = t2.intcol""".stripMargin

    val exparr = Seq(
      Seq(1, 1),
      Seq(1, 1))
    run(hbc, testnm, query, exparr)
  }

  ignore("Basic Join: Simple Two Way") {
    testnm = "Basic Join: Simple Two Way"
    val query = s"""select t1.intcol t1intcol, t2.intcol t2intcol from JoinTable1 t1 JOIN
                    JoinTable2 t2 on t1.intcol = t2.intcol""".stripMargin

    val exparr = Seq(
      Seq(1, 23456783, 45657.83F, "Row3", 'c', 12343, 45657.83F, 5678912.345683, 3456789012343L),
      Seq(1, 23456782, 45657.82F, "Row2", 'b', 12342, 45657.82F, 5678912.345682, 3456789012342L))
    run(hbc, testnm, query, exparr)
  }

//  testnm = "Basic Join: Two Way"
//  test("Basic Join: Two Way") {
//    val query = makeJoin(2)
//    val exparr = Array(
//      Array(1, 23456783, 45657.83F, "Row3", 'c', 12343, 45657.83F, 5678912.345683, 3456789012343L),
//      Array(1, 23456782, 45657.82F, "Row2", 'b', 12342, 45657.82F, 5678912.345682, 3456789012342L))
//    run(testnm, query, exparr)
//  }

//  test("Join with GroupBy: Two Way") {
//    makeGroupByJoin(2)
//  }

    private val joinCols = Array(
      Array(("t1.bytecol", "t2.bytecol"), ("t1.intcol", "t2.intcol"), ("t1.longcol", "t2.longcol"), ("t1.floatcol", "t2.floatcol"), ("t1.doublecol", "t2.doublecol")),
      Array(("t2.bytecol", "t3.bytecol"), ("t2.intcol", "t3.intcol"), ("t2.longcol", "t3.longcol"), ("t2.floatcol", "t3.floatcol"), ("t2.doublecol", "t3.doublecol")),
      Array(("t3.bytecol", "t4.bytecol"), ("t3.intcol", "t4.intcol"), ("t3.longcol", "t4.longcol"), ("t3.floatcol", "t4.floatcol"), ("t3.doublecol", "t4.doublecol")))

  def makeJoin(ntabs: Int = 2) = {
    val TNameBase = "JoinTable"

    val (allcols, alljoins) = (1 to ntabs).foldLeft("", "") { case ((cols, joins), px) =>
      val tn = s"t$px"
      val newcols = s"""$tn.intcol ${tn}intcol, $tn.floatcol ${tn}floatcol, $tn.strcol ${tn}strcol, $tn.bytecol  ${tn}bytecol,
         | $tn.shortcol  ${tn}shortcol,
         | $tn.floatcol  ${tn}floatcol2, $tn.doublecol  ${tn}doublecol,
         | $tn.longcol  ${tn}longcol"""

      val newjoins = {
        val tname = TNameBase + px
        if (px == 1) {
          s"$tname t$px"
        } else {
          val newjoinCols = joinCols(px-1).foldLeft("") { case (cumjoins, (cola, colb)) =>
            cumjoins + (if (cumjoins.length > 0) " AND " else "") + s"$cola=$colb"
          }
          s"$joins JOIN $tname t$px ON $newjoinCols"
        }
      }

      val newgroups =
        s"""${tn}strcol,${tn}bytecol,${tn}shortcol,${tn}intcol,${tn}floatcol,
           |${tn}doublecol""".stripMargin
      ((if (cols.length > 0) s"$cols,\n" else "") + newcols,
        newjoins)
    }
    val query1 =
      s"""select
         |$allcols
         | from $alljoins
         |  where t1.strcol like '%Row%' and t2.shortcol < 12345 and t3.doublecol > 5678912.345681
         |  and t3.doublecol < 5678912.345684
                 | /* order by t1.strcol */"""
        //         | order by t1strcol desc"""  // Potential bug with DESC ??
        .stripMargin
    query1
  }

  def makeGroupByJoin(ntabs: Int = 3) = {
    val TNameBase = "JoinTable"

    val (allcols, alljoins, groupcols) = (1 to ntabs).foldLeft("", "", "") { case ((cols, joins, groups), px) =>
      val tn = s"t$px"
      val newcols = s"""$tn.intcol ${tn}intcol, $tn.floatcol ${tn}floatcol, $tn.strcol ${tn}strcol, max($tn.bytecol)  ${tn}bytecol,
         | max($tn.shortcol)  ${tn}shortcol,
         | max($tn.floatcol)  ${tn}floatcolmax, max($tn.doublecol)  ${tn}doublecol,
         | max($tn.longcol)  ${tn}longcol"""

      val newjoins = {
        val tname = TNameBase + px
        if (px == 1) {
          tname
        } else if (px < ntabs) {
          val newjoinCols = joinCols(px - 1).foldLeft("") { case (cumjoins, (cola, colb)) =>
            cumjoins + (if (cumjoins.length > 0) " AND " else "") + s"$cola=$colb"
          }
          s"$joins JOIN $tname ON $newjoinCols"
        } else {
          joins
        }
      }

      val newgroups =
        s"""${tn}strcol,${tn}bytecol,${tn}shortcol,${tn}intcol,${tn}floatcol,
           |${tn}doublecol""".stripMargin
      ((if (cols.length > 0) s"$cols,\n" else "") + newcols,
        newjoins,
        (if (groups.length > 0) s"$groups,\n" else "") + newgroups)

    }
    val query1 =
      s"""select count(1) as cnt,
         |$allcols
         | from $alljoins
         |  where t1.strcol like '%Row%' and t2.shortcol < 12345 and t3.doublecol > 5678912.345681
         |  and t3.doublecol < 5678912.345684
         | group by $groupcols"""
        //         | order by t1strcol"""
        //         | order by t1strcol desc"""  // Potential bug with DESC ??
        .stripMargin
      query1
    }


}

