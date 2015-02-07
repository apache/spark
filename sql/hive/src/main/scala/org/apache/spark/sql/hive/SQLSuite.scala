package org.apache.spark.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.{SparkConf, SparkContext}

object SQLSuite {

  val master = "ec2-52-0-150-221.compute-1.amazonaws.com"
  val spark = s"spark://$master:7077"
  val hdfs = s"hdfs://$master:9010"

  val sparkConf = new SparkConf()
    .set("spark.sql.test", "")
    .set("spark.executor.memory", "48g")
  val sparkContext = new SparkContext(spark, "SQLSuite", sparkConf)

  val hiveContext = new HiveContext(sparkContext)

  val hiveConf = Seq(
    SQLConf.SHUFFLE_PARTITIONS -> "40",
    SQLConf.CODEGEN_ENABLED -> "false"
  )
  hiveConf.foreach { case (key, value) =>
    hiveContext.setConf(key, value)
  }

  var debug = false

  def main(args: Array[String]): Unit = {
    args(0) match {
      case "create" => create(args(1))
      case "drop" => drop()
      case "show" => show()
      case "test" => test()
      case queryName =>
        hiveRun(s"Q$queryName")
    }
  }

  def hiveRun(queryName: String): Unit = {
    val query = hiveContext.sql(queries(queryName))
    printPlan(query)

    printResult(query)
  }

  def printResult(query: DataFrame): Unit = {
    query.queryExecution.executedPlan // force to initialize the query plan

    var rows: Array[Row] = null
    benchmark {
      rows = query.collect()
    }

    println(s"==============Result===============")
    rows.foreach(row => println(row.mkString(", ")))
    println("====================================")
  }

  def printPlan(query: DataFrame): Unit = {
    if (debug) {
      println("=============Plan=================")
      println(query.queryExecution.executedPlan)
      println("==================================")
    }
  }

  def create(tpch: String): Unit = {
    val ddls = Seq(
      hiveContext.sql(
        s"""
          |create external table lineitem (
          | L_ORDERKEY INT,
          | L_PARTKEY INT,
          | L_SUPPKEY INT,
          | L_LINENUMBER INT,
          | L_QUANTITY DOUBLE,
          | L_EXTENDEDPRICE DOUBLE,
          | L_DISCOUNT DOUBLE,
          | L_TAX DOUBLE,
          | L_RETURNFLAG STRING,
          | L_LINESTATUS STRING,
          | L_SHIPDATE STRING,
          | L_COMMITDATE STRING,
          | L_RECEIPTDATE STRING,
          | L_SHIPINSTRUCT STRING,
          | L_SHIPMODE STRING,
          | L_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/lineitem'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table orders (
          | O_ORDERKEY INT,
          | O_CUSTKEY INT,
          | O_ORDERSTATUS STRING,
          | O_TOTALPRICE DOUBLE,
          | O_ORDERDATE STRING,
          | O_ORDERPRIORITY STRING,
          | O_CLERK STRING,
          | O_SHIPPRIORITY INT,
          | O_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/orders'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table customer (
          | C_CUSTKEY INT,
          | C_NAME STRING,
          | C_ADDRESS STRING,
          | C_NATIONKEY INT,
          | C_PHONE STRING,
          | C_ACCTBAL DOUBLE,
          | C_MKTSEGMENT STRING,
          | C_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/customer'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table supplier (
          | S_SUPPKEY INT,
          | S_NAME STRING,
          | S_ADDRESS STRING,
          | S_NATIONKEY INT,
          | S_PHONE STRING,
          | S_ACCTBAL DOUBLE,
          | S_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/supplier'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table partsupp (
          | PS_PARTKEY INT,
          | PS_SUPPKEY INT,
          | PS_AVAILQTY INT,
          | PS_SUPPLYCOST DOUBLE,
          | PS_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/partsupp'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table part (
          | P_PARTKEY INT,
          | P_NAME STRING,
          | P_MFGR STRING,
          | P_BRAND STRING,
          | P_TYPE STRING,
          | P_SIZE INT,
          | P_CONTAINER STRING,
          | P_RETAILPRICE DOUBLE,
          | P_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/part'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table nation (
          | N_NATIONKEY INT,
          | N_NAME STRING,
          | N_REGIONKEY INT,
          | N_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/nation'
        """.stripMargin),
      hiveContext.sql(
        s"""
          |create external table region (
          | R_REGIONKEY INT,
          | R_NAME STRING,
          | R_COMMENT STRING
          |)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED
          |AS TEXTFILE LOCATION '$hdfs/$tpch/region'
        """.stripMargin)
    )

    ddls.foreach(_.collect())
  }

  def drop(): Unit = {
    val tables = Seq("lineitem", "orders", "customer", "supplier", "partsupp", "part", "nation", "region")
    val queries = tables.map(table => hiveContext.sql(s"drop table $table"))
    queries.map(_.collect())
  }

  def show(): Unit = {
    val tables = hiveContext.sql("show tables").collect()
    println("=====================")
    tables.foreach(println(_))
    println("=====================")
  }

  def test(): Unit = {
    val tables = Seq("lineitem", "orders", "customer", "supplier", "partsupp", "part", "nation", "region")
    val queries = tables.map(table => hiveContext.sql(s"select count(*) from $table"))
    val results = queries.map(_.collect())

    println("====================")
    tables.zip(results).foreach {
      case (table, result) => println(s"# of $table = ${result.mkString}")
    }
    println("====================")
  }

  val arithmeticTemplate =
    s"""
       |SELECT l_returnflag
       |	,l_linestatus
       |	,sum(l_quantity + cnt1)
       |	,sum(l_quantity + cnt2)
       |	,sum(l_quantity + cnt3)
       |	,sum(l_quantity + cnt4)
       |	,sum(l_quantity + cnt5)
       |	,sum(l_quantity + cnt6)
       |	,sum(l_quantity + cnt7)
       |	,sum(l_quantity + cnt8)
       |	,sum(l_quantity + cnt9)
       |	,sum(l_quantity + cnt10)
       |	,sum(l_quantity + cnt11)
       |	,sum(l_quantity + cnt12)
       |	,sum(l_quantity + cnt13)
       |	,sum(l_quantity + cnt14)
       |	,sum(l_quantity + cnt15)
       |	,sum(l_quantity + cnt16)
       |	,sum(l_quantity + cnt17)
       |	,sum(l_quantity + cnt18)
       |	,sum(l_quantity + cnt19)
       |	,sum(l_quantity + cnt20)
       |	,sum(l_quantity + cnt21)
       |	,sum(l_quantity + cnt22)
       |	,sum(l_quantity + cnt23)
       |	,sum(l_quantity + cnt24)
       |	,sum(l_quantity + cnt25)
       |	,sum(l_quantity + cnt26)
       |	,sum(l_quantity + cnt27)
       |	,sum(l_quantity + cnt28)
       |	,sum(l_quantity + cnt29)
       |	,sum(l_quantity + cnt30)
       |	,sum(l_quantity + cnt31)
       |	,sum(l_quantity + cnt32)
       |	,sum(l_quantity + cnt33)
       |	,sum(l_quantity + cnt34)
       |	,sum(l_quantity + cnt35)
       |	,sum(l_quantity + cnt36)
       |	,sum(l_quantity + cnt37)
       |	,sum(l_quantity + cnt38)
       |	,sum(l_quantity + cnt39)
       |	,sum(l_quantity + cnt40)
       |	,sum(l_quantity + cnt41)
       |	,sum(l_quantity + cnt42)
       |	,sum(l_quantity + cnt43)
       |	,sum(l_quantity + cnt44)
       |	,sum(l_quantity + cnt45)
       |	,sum(l_quantity + cnt46)
       |	,sum(l_quantity + cnt47)
       |	,sum(l_quantity + cnt48)
       |	,sum(l_quantity + cnt49)
       |	,sum(l_quantity + cnt50)
       |	,sum(l_quantity + cnt51)
       |	,sum(l_quantity + cnt52)
       |	,sum(l_quantity + cnt53)
       |	,sum(l_quantity + cnt54)
       |	,sum(l_quantity + cnt55)
       |	,sum(l_quantity + cnt56)
       |	,sum(l_quantity + cnt57)
       |	,sum(l_quantity + cnt58)
       |	,sum(l_quantity + cnt59)
       |	,sum(l_quantity + cnt60)
       |	,sum(l_quantity + cnt61)
       |	,sum(l_quantity + cnt62)
       |	,sum(l_quantity + cnt63)
       |	,sum(l_quantity + cnt64)
       |	,sum(l_quantity + cnt65)
       |	,sum(l_quantity + cnt66)
       |	,sum(l_quantity + cnt67)
       |	,sum(l_quantity + cnt68)
       |	,sum(l_quantity + cnt69)
       |	,sum(l_quantity + cnt70)
       |	,sum(l_quantity + cnt71)
       |	,sum(l_quantity + cnt72)
       |	,sum(l_quantity + cnt73)
       |	,sum(l_quantity + cnt74)
       |	,sum(l_quantity + cnt75)
       |	,sum(l_quantity + cnt76)
       |	,sum(l_quantity + cnt77)
       |	,sum(l_quantity + cnt78)
       |	,sum(l_quantity + cnt79)
       |	,sum(l_quantity + cnt80)
       |	,sum(l_quantity + cnt81)
       |	,sum(l_quantity + cnt82)
       |	,sum(l_quantity + cnt83)
       |	,sum(l_quantity + cnt84)
       |	,sum(l_quantity + cnt85)
       |	,sum(l_quantity + cnt86)
       |	,sum(l_quantity + cnt87)
       |	,sum(l_quantity + cnt88)
       |	,sum(l_quantity + cnt89)
       |	,sum(l_quantity + cnt90)
       |	,sum(l_quantity + cnt91)
       |	,sum(l_quantity + cnt92)
       |	,sum(l_quantity + cnt93)
       |	,sum(l_quantity + cnt94)
       |	,sum(l_quantity + cnt95)
       |	,sum(l_quantity + cnt96)
       |	,sum(l_quantity + cnt97)
       |	,sum(l_quantity + cnt98)
       |	,sum(l_quantity + cnt99)
       |	,sum(l_quantity + cnt100)
       |	,sum(l_quantity + cnt101)
       |	,sum(l_quantity + cnt102)
       |	,sum(l_quantity + cnt103)
       |	,sum(l_quantity + cnt104)
       |	,sum(l_quantity + cnt105)
       |	,sum(l_quantity + cnt106)
       |	,sum(l_quantity + cnt107)
       |	,sum(l_quantity + cnt108)
       |	,sum(l_quantity + cnt109)
       |	,sum(l_quantity + cnt110)
       |	,sum(l_quantity + cnt111)
       |	,sum(l_quantity + cnt112)
       |	,sum(l_quantity + cnt113)
       |	,sum(l_quantity + cnt114)
       |	,sum(l_quantity + cnt115)
       |	,sum(l_quantity + cnt116)
       |	,sum(l_quantity + cnt117)
       |	,sum(l_quantity + cnt118)
       |	,sum(l_quantity + cnt119)
       |	,sum(l_quantity + cnt120)
       |	,sum(l_quantity + cnt121)
       |	,sum(l_quantity + cnt122)
       |	,sum(l_quantity + cnt123)
       |	,sum(l_quantity + cnt124)
       |	,sum(l_quantity + cnt125)
       |	,sum(l_quantity + cnt126)
       |	,sum(l_quantity + cnt127)
       |	,sum(l_quantity + cnt128)
       |	,sum(l_quantity + cnt129)
       |	,sum(l_quantity + cnt130)
       |	,sum(l_quantity + cnt131)
       |	,sum(l_quantity + cnt132)
       |	,sum(l_quantity + cnt133)
       |	,sum(l_quantity + cnt134)
       |	,sum(l_quantity + cnt135)
       |	,sum(l_quantity + cnt136)
       |	,sum(l_quantity + cnt137)
       |	,sum(l_quantity + cnt138)
       |	,sum(l_quantity + cnt139)
       |	,sum(l_quantity + cnt140)
       |	,sum(l_quantity + cnt141)
       |	,sum(l_quantity + cnt142)
       |	,sum(l_quantity + cnt143)
       |	,sum(l_quantity + cnt144)
       |	,sum(l_quantity + cnt145)
       |	,sum(l_quantity + cnt146)
       |	,sum(l_quantity + cnt147)
       |	,sum(l_quantity + cnt148)
       |	,sum(l_quantity + cnt149)
       |	,sum(l_quantity + cnt150)
       |	,sum(l_quantity + cnt151)
       |	,sum(l_quantity + cnt152)
       |	,sum(l_quantity + cnt153)
       |	,sum(l_quantity + cnt154)
       |	,sum(l_quantity + cnt155)
       |	,sum(l_quantity + cnt156)
       |	,sum(l_quantity + cnt157)
       |	,sum(l_quantity + cnt158)
       |	,sum(l_quantity + cnt159)
       |	,sum(l_quantity + cnt160)
       |	,sum(l_quantity + cnt161)
       |	,sum(l_quantity + cnt162)
       |	,sum(l_quantity + cnt163)
       |	,sum(l_quantity + cnt164)
       |	,sum(l_quantity + cnt165)
       |	,sum(l_quantity + cnt166)
       |	,sum(l_quantity + cnt167)
       |	,sum(l_quantity + cnt168)
       |	,sum(l_quantity + cnt169)
       |	,sum(l_quantity + cnt170)
       |	,sum(l_quantity + cnt171)
       |	,sum(l_quantity + cnt172)
       |	,sum(l_quantity + cnt173)
       |	,sum(l_quantity + cnt174)
       |	,sum(l_quantity + cnt175)
       |	,sum(l_quantity + cnt176)
       |	,sum(l_quantity + cnt177)
       |	,sum(l_quantity + cnt178)
       |	,sum(l_quantity + cnt179)
       |	,sum(l_quantity + cnt180)
       |	,sum(l_quantity + cnt181)
       |	,sum(l_quantity + cnt182)
       |	,sum(l_quantity + cnt183)
       |	,sum(l_quantity + cnt184)
       |	,sum(l_quantity + cnt185)
       |	,sum(l_quantity + cnt186)
       |	,sum(l_quantity + cnt187)
       |	,sum(l_quantity + cnt188)
       |	,sum(l_quantity + cnt189)
       |	,sum(l_quantity + cnt190)
       |	,sum(l_quantity + cnt191)
       |	,sum(l_quantity + cnt192)
       |	,sum(l_quantity + cnt193)
       |	,sum(l_quantity + cnt194)
       |	,sum(l_quantity + cnt195)
       |	,sum(l_quantity + cnt196)
       |	,sum(l_quantity + cnt197)
       |	,sum(l_quantity + cnt198)
       |	,sum(l_quantity + cnt199)
       |	,sum(l_quantity + cnt200)
       |	,sum(l_quantity + cnt201)
       |	,sum(l_quantity + cnt202)
       |	,sum(l_quantity + cnt203)
       |	,sum(l_quantity + cnt204)
       |	,sum(l_quantity + cnt205)
       |	,sum(l_quantity + cnt206)
       |	,sum(l_quantity + cnt207)
       |	,sum(l_quantity + cnt208)
       |	,sum(l_quantity + cnt209)
       |	,sum(l_quantity + cnt210)
       |	,sum(l_quantity + cnt211)
       |	,sum(l_quantity + cnt212)
       |	,sum(l_quantity + cnt213)
       |	,sum(l_quantity + cnt214)
       |	,sum(l_quantity + cnt215)
       |	,sum(l_quantity + cnt216)
       |	,sum(l_quantity + cnt217)
       |	,sum(l_quantity + cnt218)
       |	,sum(l_quantity + cnt219)
       |	,sum(l_quantity + cnt220)
       |	,sum(l_quantity + cnt221)
       |	,sum(l_quantity + cnt222)
       |	,sum(l_quantity + cnt223)
       |	,sum(l_quantity + cnt224)
       |	,sum(l_quantity + cnt225)
       |	,sum(l_quantity + cnt226)
       |	,sum(l_quantity + cnt227)
       |	,sum(l_quantity + cnt228)
       |	,sum(l_quantity + cnt229)
       |	,sum(l_quantity + cnt230)
       |	,sum(l_quantity + cnt231)
       |	,sum(l_quantity + cnt232)
       |	,sum(l_quantity + cnt233)
       |	,sum(l_quantity + cnt234)
       |	,sum(l_quantity + cnt235)
       |	,sum(l_quantity + cnt236)
       |	,sum(l_quantity + cnt237)
       |	,sum(l_quantity + cnt238)
       |	,sum(l_quantity + cnt239)
       |	,sum(l_quantity + cnt240)
       |	,sum(l_quantity + cnt241)
       |	,sum(l_quantity + cnt242)
       |	,sum(l_quantity + cnt243)
       |	,sum(l_quantity + cnt244)
       |	,sum(l_quantity + cnt245)
       |	,sum(l_quantity + cnt246)
       |	,sum(l_quantity + cnt247)
       |	,sum(l_quantity + cnt248)
       |	,sum(l_quantity + cnt249)
       |	,sum(l_quantity + cnt250)
       |	,sum(l_quantity + cnt251)
       |	,sum(l_quantity + cnt252)
       |	,sum(l_quantity + cnt253)
       |	,sum(l_quantity + cnt254)
       |	,sum(l_quantity + cnt255)
       |	,sum(l_quantity + cnt256)
       |	,sum(l_quantity + cnt257)
       |	,sum(l_quantity + cnt258)
       |	,sum(l_quantity + cnt259)
       |	,sum(l_quantity + cnt260)
       |	,sum(l_quantity + cnt261)
       |	,sum(l_quantity + cnt262)
       |	,sum(l_quantity + cnt263)
       |	,sum(l_quantity + cnt264)
       |	,sum(l_quantity + cnt265)
       |	,sum(l_quantity + cnt266)
       |	,sum(l_quantity + cnt267)
       |	,sum(l_quantity + cnt268)
       |	,sum(l_quantity + cnt269)
       |	,sum(l_quantity + cnt270)
       |	,sum(l_quantity + cnt271)
       |	,sum(l_quantity + cnt272)
       |	,sum(l_quantity + cnt273)
       |	,sum(l_quantity + cnt274)
       |	,sum(l_quantity + cnt275)
       |	,sum(l_quantity + cnt276)
       |	,sum(l_quantity + cnt277)
       |	,sum(l_quantity + cnt278)
       |	,sum(l_quantity + cnt279)
       |	,sum(l_quantity + cnt280)
       |	,sum(l_quantity + cnt281)
       |	,sum(l_quantity + cnt282)
       |	,sum(l_quantity + cnt283)
       |	,sum(l_quantity + cnt284)
       |	,sum(l_quantity + cnt285)
       |	,sum(l_quantity + cnt286)
       |	,sum(l_quantity + cnt287)
       |	,sum(l_quantity + cnt288)
       |	,sum(l_quantity + cnt289)
       |	,sum(l_quantity + cnt290)
       |	,sum(l_quantity + cnt291)
       |	,sum(l_quantity + cnt292)
       |	,sum(l_quantity + cnt293)
       |	,sum(l_quantity + cnt294)
       |	,sum(l_quantity + cnt295)
       |	,sum(l_quantity + cnt296)
       |	,sum(l_quantity + cnt297)
       |	,sum(l_quantity + cnt298)
       |	,sum(l_quantity + cnt299)
       |	,sum(l_quantity + cnt300)
       |	,sum(l_quantity + cnt301)
       |	,sum(l_quantity + cnt302)
       |	,sum(l_quantity + cnt303)
       |	,sum(l_quantity + cnt304)
       |	,sum(l_quantity + cnt305)
       |	,sum(l_quantity + cnt306)
       |	,sum(l_quantity + cnt307)
       |	,sum(l_quantity + cnt308)
       |	,sum(l_quantity + cnt309)
       |	,sum(l_quantity + cnt310)
       |	,sum(l_quantity + cnt311)
       |	,sum(l_quantity + cnt312)
       |	,sum(l_quantity + cnt313)
       |	,sum(l_quantity + cnt314)
       |	,sum(l_quantity + cnt315)
       |	,sum(l_quantity + cnt316)
       |	,sum(l_quantity + cnt317)
       |	,sum(l_quantity + cnt318)
       |	,sum(l_quantity + cnt319)
       |	,sum(l_quantity + cnt320)
       |	,sum(l_quantity + cnt321)
       |	,sum(l_quantity + cnt322)
       |	,sum(l_quantity + cnt323)
       |	,sum(l_quantity + cnt324)
       |	,sum(l_quantity + cnt325)
       |	,sum(l_quantity + cnt326)
       |	,sum(l_quantity + cnt327)
       |	,sum(l_quantity + cnt328)
       |	,sum(l_quantity + cnt329)
       |	,sum(l_quantity + cnt330)
       |	,sum(l_quantity + cnt331)
       |	,sum(l_quantity + cnt332)
       |	,sum(l_quantity + cnt333)
       |	,sum(l_quantity + cnt334)
       |	,sum(l_quantity + cnt335)
       |	,sum(l_quantity + cnt336)
       |	,sum(l_quantity + cnt337)
       |	,sum(l_quantity + cnt338)
       |	,sum(l_quantity + cnt339)
       |	,sum(l_quantity + cnt340)
       |	,sum(l_quantity + cnt341)
       |	,sum(l_quantity + cnt342)
       |	,sum(l_quantity + cnt343)
       |	,sum(l_quantity + cnt344)
       |	,sum(l_quantity + cnt345)
       |	,sum(l_quantity + cnt346)
       |	,sum(l_quantity + cnt347)
       |	,sum(l_quantity + cnt348)
       |	,sum(l_quantity + cnt349)
       |	,sum(l_quantity + cnt350)
       |	,sum(l_quantity + cnt351)
       |	,sum(l_quantity + cnt352)
       |	,sum(l_quantity + cnt353)
       |	,sum(l_quantity + cnt354)
       |	,sum(l_quantity + cnt355)
       |	,sum(l_quantity + cnt356)
       |	,sum(l_quantity + cnt357)
       |	,sum(l_quantity + cnt358)
       |	,sum(l_quantity + cnt359)
       |	,sum(l_quantity + cnt360)
       |	,sum(l_quantity + cnt361)
       |	,sum(l_quantity + cnt362)
       |	,sum(l_quantity + cnt363)
       |	,sum(l_quantity + cnt364)
       |	,sum(l_quantity + cnt365)
       |	,sum(l_quantity + cnt366)
       |	,sum(l_quantity + cnt367)
       |	,sum(l_quantity + cnt368)
       |	,sum(l_quantity + cnt369)
       |	,sum(l_quantity + cnt370)
       |	,sum(l_quantity + cnt371)
       |	,sum(l_quantity + cnt372)
       |	,sum(l_quantity + cnt373)
       |	,sum(l_quantity + cnt374)
       |	,sum(l_quantity + cnt375)
       |	,sum(l_quantity + cnt376)
       |	,sum(l_quantity + cnt377)
       |	,sum(l_quantity + cnt378)
       |	,sum(l_quantity + cnt379)
       |	,sum(l_quantity + cnt380)
       |	,sum(l_quantity + cnt381)
       |	,sum(l_quantity + cnt382)
       |	,sum(l_quantity + cnt383)
       |	,sum(l_quantity + cnt384)
       |	,sum(l_quantity + cnt385)
       |	,sum(l_quantity + cnt386)
       |	,sum(l_quantity + cnt387)
       |	,sum(l_quantity + cnt388)
       |	,sum(l_quantity + cnt389)
       |	,sum(l_quantity + cnt390)
       |	,sum(l_quantity + cnt391)
       |	,sum(l_quantity + cnt392)
       |	,sum(l_quantity + cnt393)
       |	,sum(l_quantity + cnt394)
       |	,sum(l_quantity + cnt395)
       |	,sum(l_quantity + cnt396)
       |	,sum(l_quantity + cnt397)
       |	,sum(l_quantity + cnt398)
       |	,sum(l_quantity + cnt399)
       |	,sum(l_quantity + cnt400)
       |	,sum(l_quantity + cnt401)
       |	,sum(l_quantity + cnt402)
       |	,sum(l_quantity + cnt403)
       |	,sum(l_quantity + cnt404)
       |	,sum(l_quantity + cnt405)
       |	,sum(l_quantity + cnt406)
       |	,sum(l_quantity + cnt407)
       |	,sum(l_quantity + cnt408)
       |	,sum(l_quantity + cnt409)
       |	,sum(l_quantity + cnt410)
       |	,sum(l_quantity + cnt411)
       |	,sum(l_quantity + cnt412)
       |	,sum(l_quantity + cnt413)
       |	,sum(l_quantity + cnt414)
       |	,sum(l_quantity + cnt415)
       |	,sum(l_quantity + cnt416)
       |	,sum(l_quantity + cnt417)
       |	,sum(l_quantity + cnt418)
       |	,sum(l_quantity + cnt419)
       |	,sum(l_quantity + cnt420)
       |	,sum(l_quantity + cnt421)
       |	,sum(l_quantity + cnt422)
       |	,sum(l_quantity + cnt423)
       |	,sum(l_quantity + cnt424)
       |	,sum(l_quantity + cnt425)
       |	,sum(l_quantity + cnt426)
       |	,sum(l_quantity + cnt427)
       |	,sum(l_quantity + cnt428)
       |	,sum(l_quantity + cnt429)
       |	,sum(l_quantity + cnt430)
       |	,sum(l_quantity + cnt431)
       |	,sum(l_quantity + cnt432)
       |	,sum(l_quantity + cnt433)
       |	,sum(l_quantity + cnt434)
       |	,sum(l_quantity + cnt435)
       |	,sum(l_quantity + cnt436)
       |	,sum(l_quantity + cnt437)
       |	,sum(l_quantity + cnt438)
       |	,sum(l_quantity + cnt439)
       |	,sum(l_quantity + cnt440)
       |	,sum(l_quantity + cnt441)
       |	,sum(l_quantity + cnt442)
       |	,sum(l_quantity + cnt443)
       |	,sum(l_quantity + cnt444)
       |	,sum(l_quantity + cnt445)
       |	,sum(l_quantity + cnt446)
       |	,sum(l_quantity + cnt447)
       |	,sum(l_quantity + cnt448)
       |	,sum(l_quantity + cnt449)
       |	,sum(l_quantity + cnt450)
       |	,sum(l_quantity + cnt451)
       |	,sum(l_quantity + cnt452)
       |	,sum(l_quantity + cnt453)
       |	,sum(l_quantity + cnt454)
       |	,sum(l_quantity + cnt455)
       |	,sum(l_quantity + cnt456)
       |	,sum(l_quantity + cnt457)
       |	,sum(l_quantity + cnt458)
       |	,sum(l_quantity + cnt459)
       |	,sum(l_quantity + cnt460)
       |	,sum(l_quantity + cnt461)
       |	,sum(l_quantity + cnt462)
       |	,sum(l_quantity + cnt463)
       |	,sum(l_quantity + cnt464)
       |	,sum(l_quantity + cnt465)
       |	,sum(l_quantity + cnt466)
       |	,sum(l_quantity + cnt467)
       |	,sum(l_quantity + cnt468)
       |	,sum(l_quantity + cnt469)
       |	,sum(l_quantity + cnt470)
       |	,sum(l_quantity + cnt471)
       |	,sum(l_quantity + cnt472)
       |	,sum(l_quantity + cnt473)
       |	,sum(l_quantity + cnt474)
       |	,sum(l_quantity + cnt475)
       |	,sum(l_quantity + cnt476)
       |	,sum(l_quantity + cnt477)
       |	,sum(l_quantity + cnt478)
       |	,sum(l_quantity + cnt479)
       |	,sum(l_quantity + cnt480)
       |	,sum(l_quantity + cnt481)
       |	,sum(l_quantity + cnt482)
       |	,sum(l_quantity + cnt483)
       |	,sum(l_quantity + cnt484)
       |	,sum(l_quantity + cnt485)
       |	,sum(l_quantity + cnt486)
       |	,sum(l_quantity + cnt487)
       |	,sum(l_quantity + cnt488)
       |	,sum(l_quantity + cnt489)
       |	,sum(l_quantity + cnt490)
       |	,sum(l_quantity + cnt491)
       |	,sum(l_quantity + cnt492)
       |	,sum(l_quantity + cnt493)
       |	,sum(l_quantity + cnt494)
       |	,sum(l_quantity + cnt495)
       |	,sum(l_quantity + cnt496)
       |	,sum(l_quantity + cnt497)
       |	,sum(l_quantity + cnt498)
       |	,sum(l_quantity + cnt499)
       |	,sum(l_quantity + cnt500)
       |	,sum(l_quantity + cnt501)
       |	,sum(l_quantity + cnt502)
       |	,sum(l_quantity + cnt503)
       |	,sum(l_quantity + cnt504)
       |	,sum(l_quantity + cnt505)
       |	,sum(l_quantity + cnt506)
       |	,sum(l_quantity + cnt507)
       |	,sum(l_quantity + cnt508)
       |	,sum(l_quantity + cnt509)
       |	,sum(l_quantity + cnt510)
       |	,sum(l_quantity + cnt511)
       |	,sum(l_quantity + cnt512)
       |	,sum(l_quantity + cnt513)
       |	,sum(l_quantity + cnt514)
       |	,sum(l_quantity + cnt515)
       |	,sum(l_quantity + cnt516)
       |	,sum(l_quantity + cnt517)
       |	,sum(l_quantity + cnt518)
       |	,sum(l_quantity + cnt519)
       |	,sum(l_quantity + cnt520)
       |	,sum(l_quantity + cnt521)
       |	,sum(l_quantity + cnt522)
       |	,sum(l_quantity + cnt523)
       |	,sum(l_quantity + cnt524)
       |	,sum(l_quantity + cnt525)
       |	,sum(l_quantity + cnt526)
       |	,sum(l_quantity + cnt527)
       |	,sum(l_quantity + cnt528)
       |	,sum(l_quantity + cnt529)
       |	,sum(l_quantity + cnt530)
       |	,sum(l_quantity + cnt531)
       |	,sum(l_quantity + cnt532)
       |	,sum(l_quantity + cnt533)
       |	,sum(l_quantity + cnt534)
       |	,sum(l_quantity + cnt535)
       |	,sum(l_quantity + cnt536)
       |	,sum(l_quantity + cnt537)
       |	,sum(l_quantity + cnt538)
       |	,sum(l_quantity + cnt539)
       |	,sum(l_quantity + cnt540)
       |	,sum(l_quantity + cnt541)
       |	,sum(l_quantity + cnt542)
       |	,sum(l_quantity + cnt543)
       |	,sum(l_quantity + cnt544)
       |	,sum(l_quantity + cnt545)
       |	,sum(l_quantity + cnt546)
       |	,sum(l_quantity + cnt547)
       |	,sum(l_quantity + cnt548)
       |	,sum(l_quantity + cnt549)
       |	,sum(l_quantity + cnt550)
       |	,sum(l_quantity + cnt551)
       |	,sum(l_quantity + cnt552)
       |	,sum(l_quantity + cnt553)
       |	,sum(l_quantity + cnt554)
       |	,sum(l_quantity + cnt555)
       |	,sum(l_quantity + cnt556)
       |	,sum(l_quantity + cnt557)
       |	,sum(l_quantity + cnt558)
       |	,sum(l_quantity + cnt559)
       |	,sum(l_quantity + cnt560)
       |	,sum(l_quantity + cnt561)
       |	,sum(l_quantity + cnt562)
       |	,sum(l_quantity + cnt563)
       |	,sum(l_quantity + cnt564)
       |	,sum(l_quantity + cnt565)
       |	,sum(l_quantity + cnt566)
       |	,sum(l_quantity + cnt567)
       |	,sum(l_quantity + cnt568)
       |	,sum(l_quantity + cnt569)
       |	,sum(l_quantity + cnt570)
       |	,sum(l_quantity + cnt571)
       |	,sum(l_quantity + cnt572)
       |	,sum(l_quantity + cnt573)
       |	,sum(l_quantity + cnt574)
       |	,sum(l_quantity + cnt575)
       |	,sum(l_quantity + cnt576)
       |	,sum(l_quantity + cnt577)
       |	,sum(l_quantity + cnt578)
       |	,sum(l_quantity + cnt579)
       |	,sum(l_quantity + cnt580)
       |	,sum(l_quantity + cnt581)
       |	,sum(l_quantity + cnt582)
       |	,sum(l_quantity + cnt583)
       |	,sum(l_quantity + cnt584)
       |	,sum(l_quantity + cnt585)
       |	,sum(l_quantity + cnt586)
       |	,sum(l_quantity + cnt587)
       |	,sum(l_quantity + cnt588)
       |	,sum(l_quantity + cnt589)
       |	,sum(l_quantity + cnt590)
       |	,sum(l_quantity + cnt591)
       |	,sum(l_quantity + cnt592)
       |	,sum(l_quantity + cnt593)
       |	,sum(l_quantity + cnt594)
       |	,sum(l_quantity + cnt595)
       |	,sum(l_quantity + cnt596)
       |	,sum(l_quantity + cnt597)
       |	,sum(l_quantity + cnt598)
       |	,sum(l_quantity + cnt599)
       |	,sum(l_quantity + cnt600)
       |	,sum(l_quantity + cnt601)
       |	,sum(l_quantity + cnt602)
       |	,sum(l_quantity + cnt603)
       |	,sum(l_quantity + cnt604)
       |	,sum(l_quantity + cnt605)
       |	,sum(l_quantity + cnt606)
       |	,sum(l_quantity + cnt607)
       |	,sum(l_quantity + cnt608)
       |	,sum(l_quantity + cnt609)
       |	,sum(l_quantity + cnt610)
       |	,sum(l_quantity + cnt611)
       |	,sum(l_quantity + cnt612)
       |	,sum(l_quantity + cnt613)
       |	,sum(l_quantity + cnt614)
       |	,sum(l_quantity + cnt615)
       |	,sum(l_quantity + cnt616)
       |	,sum(l_quantity + cnt617)
       |	,sum(l_quantity + cnt618)
       |	,sum(l_quantity + cnt619)
       |	,sum(l_quantity + cnt620)
       |	,sum(l_quantity + cnt621)
       |	,sum(l_quantity + cnt622)
       |	,sum(l_quantity + cnt623)
       |	,sum(l_quantity + cnt624)
       |	,sum(l_quantity + cnt625)
       |	,sum(l_quantity + cnt626)
       |	,sum(l_quantity + cnt627)
       |	,sum(l_quantity + cnt628)
       |	,sum(l_quantity + cnt629)
       |	,sum(l_quantity + cnt630)
       |	,sum(l_quantity + cnt631)
       |	,sum(l_quantity + cnt632)
       |	,sum(l_quantity + cnt633)
       |	,sum(l_quantity + cnt634)
       |	,sum(l_quantity + cnt635)
       |	,sum(l_quantity + cnt636)
       |	,sum(l_quantity + cnt637)
       |	,sum(l_quantity + cnt638)
       |	,sum(l_quantity + cnt639)
       |	,sum(l_quantity + cnt640)
       |	,sum(l_quantity + cnt641)
       |	,sum(l_quantity + cnt642)
       |	,sum(l_quantity + cnt643)
       |	,sum(l_quantity + cnt644)
       |	,sum(l_quantity + cnt645)
       |	,sum(l_quantity + cnt646)
       |	,sum(l_quantity + cnt647)
       |	,sum(l_quantity + cnt648)
       |	,sum(l_quantity + cnt649)
       |	,sum(l_quantity + cnt650)
       |	,sum(l_quantity + cnt651)
       |	,sum(l_quantity + cnt652)
       |	,sum(l_quantity + cnt653)
       |	,sum(l_quantity + cnt654)
       |	,sum(l_quantity + cnt655)
       |	,sum(l_quantity + cnt656)
       |	,sum(l_quantity + cnt657)
       |	,sum(l_quantity + cnt658)
       |	,sum(l_quantity + cnt659)
       |	,sum(l_quantity + cnt660)
       |	,sum(l_quantity + cnt661)
       |	,sum(l_quantity + cnt662)
       |	,sum(l_quantity + cnt663)
       |	,sum(l_quantity + cnt664)
       |	,sum(l_quantity + cnt665)
       |	,sum(l_quantity + cnt666)
       |	,sum(l_quantity + cnt667)
       |	,sum(l_quantity + cnt668)
       |	,sum(l_quantity + cnt669)
       |	,sum(l_quantity + cnt670)
       |	,sum(l_quantity + cnt671)
       |	,sum(l_quantity + cnt672)
       |	,sum(l_quantity + cnt673)
       |	,sum(l_quantity + cnt674)
       |	,sum(l_quantity + cnt675)
       |	,sum(l_quantity + cnt676)
       |	,sum(l_quantity + cnt677)
       |	,sum(l_quantity + cnt678)
       |	,sum(l_quantity + cnt679)
       |	,sum(l_quantity + cnt680)
       |	,sum(l_quantity + cnt681)
       |	,sum(l_quantity + cnt682)
       |	,sum(l_quantity + cnt683)
       |	,sum(l_quantity + cnt684)
       |	,sum(l_quantity + cnt685)
       |	,sum(l_quantity + cnt686)
       |	,sum(l_quantity + cnt687)
       |	,sum(l_quantity + cnt688)
       |	,sum(l_quantity + cnt689)
       |	,sum(l_quantity + cnt690)
       |	,sum(l_quantity + cnt691)
       |	,sum(l_quantity + cnt692)
       |	,sum(l_quantity + cnt693)
       |	,sum(l_quantity + cnt694)
       |	,sum(l_quantity + cnt695)
       |	,sum(l_quantity + cnt696)
       |	,sum(l_quantity + cnt697)
       |	,sum(l_quantity + cnt698)
       |	,sum(l_quantity + cnt699)
       |	,sum(l_quantity + cnt700)
       |FROM (
       |	SELECT l_returnflag
       |		,l_linestatus
       |		,l_quantity
       |		,1 AS cnt1
       |		,2 AS cnt2
       |		,3 AS cnt3
       |		,4 AS cnt4
       |		,5 AS cnt5
       |		,6 AS cnt6
       |		,7 AS cnt7
       |		,8 AS cnt8
       |		,9 AS cnt9
       |		,10 AS cnt10
       |		,11 AS cnt11
       |		,12 AS cnt12
       |		,13 AS cnt13
       |		,14 AS cnt14
       |		,15 AS cnt15
       |		,16 AS cnt16
       |		,17 AS cnt17
       |		,18 AS cnt18
       |		,19 AS cnt19
       |		,20 AS cnt20
       |		,21 AS cnt21
       |		,22 AS cnt22
       |		,23 AS cnt23
       |		,24 AS cnt24
       |		,25 AS cnt25
       |		,26 AS cnt26
       |		,27 AS cnt27
       |		,28 AS cnt28
       |		,29 AS cnt29
       |		,30 AS cnt30
       |		,31 AS cnt31
       |		,32 AS cnt32
       |		,33 AS cnt33
       |		,34 AS cnt34
       |		,35 AS cnt35
       |		,36 AS cnt36
       |		,37 AS cnt37
       |		,38 AS cnt38
       |		,39 AS cnt39
       |		,40 AS cnt40
       |		,41 AS cnt41
       |		,42 AS cnt42
       |		,43 AS cnt43
       |		,44 AS cnt44
       |		,45 AS cnt45
       |		,46 AS cnt46
       |		,47 AS cnt47
       |		,48 AS cnt48
       |		,49 AS cnt49
       |		,50 AS cnt50
       |		,51 AS cnt51
       |		,52 AS cnt52
       |		,53 AS cnt53
       |		,54 AS cnt54
       |		,55 AS cnt55
       |		,56 AS cnt56
       |		,57 AS cnt57
       |		,58 AS cnt58
       |		,59 AS cnt59
       |		,60 AS cnt60
       |		,61 AS cnt61
       |		,62 AS cnt62
       |		,63 AS cnt63
       |		,64 AS cnt64
       |		,65 AS cnt65
       |		,66 AS cnt66
       |		,67 AS cnt67
       |		,68 AS cnt68
       |		,69 AS cnt69
       |		,70 AS cnt70
       |		,71 AS cnt71
       |		,72 AS cnt72
       |		,73 AS cnt73
       |		,74 AS cnt74
       |		,75 AS cnt75
       |		,76 AS cnt76
       |		,77 AS cnt77
       |		,78 AS cnt78
       |		,79 AS cnt79
       |		,80 AS cnt80
       |		,81 AS cnt81
       |		,82 AS cnt82
       |		,83 AS cnt83
       |		,84 AS cnt84
       |		,85 AS cnt85
       |		,86 AS cnt86
       |		,87 AS cnt87
       |		,88 AS cnt88
       |		,89 AS cnt89
       |		,90 AS cnt90
       |		,91 AS cnt91
       |		,92 AS cnt92
       |		,93 AS cnt93
       |		,94 AS cnt94
       |		,95 AS cnt95
       |		,96 AS cnt96
       |		,97 AS cnt97
       |		,98 AS cnt98
       |		,99 AS cnt99
       |		,100 AS cnt100
       |		,101 AS cnt101
       |		,102 AS cnt102
       |		,103 AS cnt103
       |		,104 AS cnt104
       |		,105 AS cnt105
       |		,106 AS cnt106
       |		,107 AS cnt107
       |		,108 AS cnt108
       |		,109 AS cnt109
       |		,110 AS cnt110
       |		,111 AS cnt111
       |		,112 AS cnt112
       |		,113 AS cnt113
       |		,114 AS cnt114
       |		,115 AS cnt115
       |		,116 AS cnt116
       |		,117 AS cnt117
       |		,118 AS cnt118
       |		,119 AS cnt119
       |		,120 AS cnt120
       |		,121 AS cnt121
       |		,122 AS cnt122
       |		,123 AS cnt123
       |		,124 AS cnt124
       |		,125 AS cnt125
       |		,126 AS cnt126
       |		,127 AS cnt127
       |		,128 AS cnt128
       |		,129 AS cnt129
       |		,130 AS cnt130
       |		,131 AS cnt131
       |		,132 AS cnt132
       |		,133 AS cnt133
       |		,134 AS cnt134
       |		,135 AS cnt135
       |		,136 AS cnt136
       |		,137 AS cnt137
       |		,138 AS cnt138
       |		,139 AS cnt139
       |		,140 AS cnt140
       |		,141 AS cnt141
       |		,142 AS cnt142
       |		,143 AS cnt143
       |		,144 AS cnt144
       |		,145 AS cnt145
       |		,146 AS cnt146
       |		,147 AS cnt147
       |		,148 AS cnt148
       |		,149 AS cnt149
       |		,150 AS cnt150
       |		,151 AS cnt151
       |		,152 AS cnt152
       |		,153 AS cnt153
       |		,154 AS cnt154
       |		,155 AS cnt155
       |		,156 AS cnt156
       |		,157 AS cnt157
       |		,158 AS cnt158
       |		,159 AS cnt159
       |		,160 AS cnt160
       |		,161 AS cnt161
       |		,162 AS cnt162
       |		,163 AS cnt163
       |		,164 AS cnt164
       |		,165 AS cnt165
       |		,166 AS cnt166
       |		,167 AS cnt167
       |		,168 AS cnt168
       |		,169 AS cnt169
       |		,170 AS cnt170
       |		,171 AS cnt171
       |		,172 AS cnt172
       |		,173 AS cnt173
       |		,174 AS cnt174
       |		,175 AS cnt175
       |		,176 AS cnt176
       |		,177 AS cnt177
       |		,178 AS cnt178
       |		,179 AS cnt179
       |		,180 AS cnt180
       |		,181 AS cnt181
       |		,182 AS cnt182
       |		,183 AS cnt183
       |		,184 AS cnt184
       |		,185 AS cnt185
       |		,186 AS cnt186
       |		,187 AS cnt187
       |		,188 AS cnt188
       |		,189 AS cnt189
       |		,190 AS cnt190
       |		,191 AS cnt191
       |		,192 AS cnt192
       |		,193 AS cnt193
       |		,194 AS cnt194
       |		,195 AS cnt195
       |		,196 AS cnt196
       |		,197 AS cnt197
       |		,198 AS cnt198
       |		,199 AS cnt199
       |		,200 AS cnt200
       |		,201 AS cnt201
       |		,202 AS cnt202
       |		,203 AS cnt203
       |		,204 AS cnt204
       |		,205 AS cnt205
       |		,206 AS cnt206
       |		,207 AS cnt207
       |		,208 AS cnt208
       |		,209 AS cnt209
       |		,210 AS cnt210
       |		,211 AS cnt211
       |		,212 AS cnt212
       |		,213 AS cnt213
       |		,214 AS cnt214
       |		,215 AS cnt215
       |		,216 AS cnt216
       |		,217 AS cnt217
       |		,218 AS cnt218
       |		,219 AS cnt219
       |		,220 AS cnt220
       |		,221 AS cnt221
       |		,222 AS cnt222
       |		,223 AS cnt223
       |		,224 AS cnt224
       |		,225 AS cnt225
       |		,226 AS cnt226
       |		,227 AS cnt227
       |		,228 AS cnt228
       |		,229 AS cnt229
       |		,230 AS cnt230
       |		,231 AS cnt231
       |		,232 AS cnt232
       |		,233 AS cnt233
       |		,234 AS cnt234
       |		,235 AS cnt235
       |		,236 AS cnt236
       |		,237 AS cnt237
       |		,238 AS cnt238
       |		,239 AS cnt239
       |		,240 AS cnt240
       |		,241 AS cnt241
       |		,242 AS cnt242
       |		,243 AS cnt243
       |		,244 AS cnt244
       |		,245 AS cnt245
       |		,246 AS cnt246
       |		,247 AS cnt247
       |		,248 AS cnt248
       |		,249 AS cnt249
       |		,250 AS cnt250
       |		,251 AS cnt251
       |		,252 AS cnt252
       |		,253 AS cnt253
       |		,254 AS cnt254
       |		,255 AS cnt255
       |		,256 AS cnt256
       |		,257 AS cnt257
       |		,258 AS cnt258
       |		,259 AS cnt259
       |		,260 AS cnt260
       |		,261 AS cnt261
       |		,262 AS cnt262
       |		,263 AS cnt263
       |		,264 AS cnt264
       |		,265 AS cnt265
       |		,266 AS cnt266
       |		,267 AS cnt267
       |		,268 AS cnt268
       |		,269 AS cnt269
       |		,270 AS cnt270
       |		,271 AS cnt271
       |		,272 AS cnt272
       |		,273 AS cnt273
       |		,274 AS cnt274
       |		,275 AS cnt275
       |		,276 AS cnt276
       |		,277 AS cnt277
       |		,278 AS cnt278
       |		,279 AS cnt279
       |		,280 AS cnt280
       |		,281 AS cnt281
       |		,282 AS cnt282
       |		,283 AS cnt283
       |		,284 AS cnt284
       |		,285 AS cnt285
       |		,286 AS cnt286
       |		,287 AS cnt287
       |		,288 AS cnt288
       |		,289 AS cnt289
       |		,290 AS cnt290
       |		,291 AS cnt291
       |		,292 AS cnt292
       |		,293 AS cnt293
       |		,294 AS cnt294
       |		,295 AS cnt295
       |		,296 AS cnt296
       |		,297 AS cnt297
       |		,298 AS cnt298
       |		,299 AS cnt299
       |		,300 AS cnt300
       |		,301 AS cnt301
       |		,302 AS cnt302
       |		,303 AS cnt303
       |		,304 AS cnt304
       |		,305 AS cnt305
       |		,306 AS cnt306
       |		,307 AS cnt307
       |		,308 AS cnt308
       |		,309 AS cnt309
       |		,310 AS cnt310
       |		,311 AS cnt311
       |		,312 AS cnt312
       |		,313 AS cnt313
       |		,314 AS cnt314
       |		,315 AS cnt315
       |		,316 AS cnt316
       |		,317 AS cnt317
       |		,318 AS cnt318
       |		,319 AS cnt319
       |		,320 AS cnt320
       |		,321 AS cnt321
       |		,322 AS cnt322
       |		,323 AS cnt323
       |		,324 AS cnt324
       |		,325 AS cnt325
       |		,326 AS cnt326
       |		,327 AS cnt327
       |		,328 AS cnt328
       |		,329 AS cnt329
       |		,330 AS cnt330
       |		,331 AS cnt331
       |		,332 AS cnt332
       |		,333 AS cnt333
       |		,334 AS cnt334
       |		,335 AS cnt335
       |		,336 AS cnt336
       |		,337 AS cnt337
       |		,338 AS cnt338
       |		,339 AS cnt339
       |		,340 AS cnt340
       |		,341 AS cnt341
       |		,342 AS cnt342
       |		,343 AS cnt343
       |		,344 AS cnt344
       |		,345 AS cnt345
       |		,346 AS cnt346
       |		,347 AS cnt347
       |		,348 AS cnt348
       |		,349 AS cnt349
       |		,350 AS cnt350
       |		,351 AS cnt351
       |		,352 AS cnt352
       |		,353 AS cnt353
       |		,354 AS cnt354
       |		,355 AS cnt355
       |		,356 AS cnt356
       |		,357 AS cnt357
       |		,358 AS cnt358
       |		,359 AS cnt359
       |		,360 AS cnt360
       |		,361 AS cnt361
       |		,362 AS cnt362
       |		,363 AS cnt363
       |		,364 AS cnt364
       |		,365 AS cnt365
       |		,366 AS cnt366
       |		,367 AS cnt367
       |		,368 AS cnt368
       |		,369 AS cnt369
       |		,370 AS cnt370
       |		,371 AS cnt371
       |		,372 AS cnt372
       |		,373 AS cnt373
       |		,374 AS cnt374
       |		,375 AS cnt375
       |		,376 AS cnt376
       |		,377 AS cnt377
       |		,378 AS cnt378
       |		,379 AS cnt379
       |		,380 AS cnt380
       |		,381 AS cnt381
       |		,382 AS cnt382
       |		,383 AS cnt383
       |		,384 AS cnt384
       |		,385 AS cnt385
       |		,386 AS cnt386
       |		,387 AS cnt387
       |		,388 AS cnt388
       |		,389 AS cnt389
       |		,390 AS cnt390
       |		,391 AS cnt391
       |		,392 AS cnt392
       |		,393 AS cnt393
       |		,394 AS cnt394
       |		,395 AS cnt395
       |		,396 AS cnt396
       |		,397 AS cnt397
       |		,398 AS cnt398
       |		,399 AS cnt399
       |		,400 AS cnt400
       |		,401 AS cnt401
       |		,402 AS cnt402
       |		,403 AS cnt403
       |		,404 AS cnt404
       |		,405 AS cnt405
       |		,406 AS cnt406
       |		,407 AS cnt407
       |		,408 AS cnt408
       |		,409 AS cnt409
       |		,410 AS cnt410
       |		,411 AS cnt411
       |		,412 AS cnt412
       |		,413 AS cnt413
       |		,414 AS cnt414
       |		,415 AS cnt415
       |		,416 AS cnt416
       |		,417 AS cnt417
       |		,418 AS cnt418
       |		,419 AS cnt419
       |		,420 AS cnt420
       |		,421 AS cnt421
       |		,422 AS cnt422
       |		,423 AS cnt423
       |		,424 AS cnt424
       |		,425 AS cnt425
       |		,426 AS cnt426
       |		,427 AS cnt427
       |		,428 AS cnt428
       |		,429 AS cnt429
       |		,430 AS cnt430
       |		,431 AS cnt431
       |		,432 AS cnt432
       |		,433 AS cnt433
       |		,434 AS cnt434
       |		,435 AS cnt435
       |		,436 AS cnt436
       |		,437 AS cnt437
       |		,438 AS cnt438
       |		,439 AS cnt439
       |		,440 AS cnt440
       |		,441 AS cnt441
       |		,442 AS cnt442
       |		,443 AS cnt443
       |		,444 AS cnt444
       |		,445 AS cnt445
       |		,446 AS cnt446
       |		,447 AS cnt447
       |		,448 AS cnt448
       |		,449 AS cnt449
       |		,450 AS cnt450
       |		,451 AS cnt451
       |		,452 AS cnt452
       |		,453 AS cnt453
       |		,454 AS cnt454
       |		,455 AS cnt455
       |		,456 AS cnt456
       |		,457 AS cnt457
       |		,458 AS cnt458
       |		,459 AS cnt459
       |		,460 AS cnt460
       |		,461 AS cnt461
       |		,462 AS cnt462
       |		,463 AS cnt463
       |		,464 AS cnt464
       |		,465 AS cnt465
       |		,466 AS cnt466
       |		,467 AS cnt467
       |		,468 AS cnt468
       |		,469 AS cnt469
       |		,470 AS cnt470
       |		,471 AS cnt471
       |		,472 AS cnt472
       |		,473 AS cnt473
       |		,474 AS cnt474
       |		,475 AS cnt475
       |		,476 AS cnt476
       |		,477 AS cnt477
       |		,478 AS cnt478
       |		,479 AS cnt479
       |		,480 AS cnt480
       |		,481 AS cnt481
       |		,482 AS cnt482
       |		,483 AS cnt483
       |		,484 AS cnt484
       |		,485 AS cnt485
       |		,486 AS cnt486
       |		,487 AS cnt487
       |		,488 AS cnt488
       |		,489 AS cnt489
       |		,490 AS cnt490
       |		,491 AS cnt491
       |		,492 AS cnt492
       |		,493 AS cnt493
       |		,494 AS cnt494
       |		,495 AS cnt495
       |		,496 AS cnt496
       |		,497 AS cnt497
       |		,498 AS cnt498
       |		,499 AS cnt499
       |		,500 AS cnt500
       |		,501 AS cnt501
       |		,502 AS cnt502
       |		,503 AS cnt503
       |		,504 AS cnt504
       |		,505 AS cnt505
       |		,506 AS cnt506
       |		,507 AS cnt507
       |		,508 AS cnt508
       |		,509 AS cnt509
       |		,510 AS cnt510
       |		,511 AS cnt511
       |		,512 AS cnt512
       |		,513 AS cnt513
       |		,514 AS cnt514
       |		,515 AS cnt515
       |		,516 AS cnt516
       |		,517 AS cnt517
       |		,518 AS cnt518
       |		,519 AS cnt519
       |		,520 AS cnt520
       |		,521 AS cnt521
       |		,522 AS cnt522
       |		,523 AS cnt523
       |		,524 AS cnt524
       |		,525 AS cnt525
       |		,526 AS cnt526
       |		,527 AS cnt527
       |		,528 AS cnt528
       |		,529 AS cnt529
       |		,530 AS cnt530
       |		,531 AS cnt531
       |		,532 AS cnt532
       |		,533 AS cnt533
       |		,534 AS cnt534
       |		,535 AS cnt535
       |		,536 AS cnt536
       |		,537 AS cnt537
       |		,538 AS cnt538
       |		,539 AS cnt539
       |		,540 AS cnt540
       |		,541 AS cnt541
       |		,542 AS cnt542
       |		,543 AS cnt543
       |		,544 AS cnt544
       |		,545 AS cnt545
       |		,546 AS cnt546
       |		,547 AS cnt547
       |		,548 AS cnt548
       |		,549 AS cnt549
       |		,550 AS cnt550
       |		,551 AS cnt551
       |		,552 AS cnt552
       |		,553 AS cnt553
       |		,554 AS cnt554
       |		,555 AS cnt555
       |		,556 AS cnt556
       |		,557 AS cnt557
       |		,558 AS cnt558
       |		,559 AS cnt559
       |		,560 AS cnt560
       |		,561 AS cnt561
       |		,562 AS cnt562
       |		,563 AS cnt563
       |		,564 AS cnt564
       |		,565 AS cnt565
       |		,566 AS cnt566
       |		,567 AS cnt567
       |		,568 AS cnt568
       |		,569 AS cnt569
       |		,570 AS cnt570
       |		,571 AS cnt571
       |		,572 AS cnt572
       |		,573 AS cnt573
       |		,574 AS cnt574
       |		,575 AS cnt575
       |		,576 AS cnt576
       |		,577 AS cnt577
       |		,578 AS cnt578
       |		,579 AS cnt579
       |		,580 AS cnt580
       |		,581 AS cnt581
       |		,582 AS cnt582
       |		,583 AS cnt583
       |		,584 AS cnt584
       |		,585 AS cnt585
       |		,586 AS cnt586
       |		,587 AS cnt587
       |		,588 AS cnt588
       |		,589 AS cnt589
       |		,590 AS cnt590
       |		,591 AS cnt591
       |		,592 AS cnt592
       |		,593 AS cnt593
       |		,594 AS cnt594
       |		,595 AS cnt595
       |		,596 AS cnt596
       |		,597 AS cnt597
       |		,598 AS cnt598
       |		,599 AS cnt599
       |		,600 AS cnt600
       |		,601 AS cnt601
       |		,602 AS cnt602
       |		,603 AS cnt603
       |		,604 AS cnt604
       |		,605 AS cnt605
       |		,606 AS cnt606
       |		,607 AS cnt607
       |		,608 AS cnt608
       |		,609 AS cnt609
       |		,610 AS cnt610
       |		,611 AS cnt611
       |		,612 AS cnt612
       |		,613 AS cnt613
       |		,614 AS cnt614
       |		,615 AS cnt615
       |		,616 AS cnt616
       |		,617 AS cnt617
       |		,618 AS cnt618
       |		,619 AS cnt619
       |		,620 AS cnt620
       |		,621 AS cnt621
       |		,622 AS cnt622
       |		,623 AS cnt623
       |		,624 AS cnt624
       |		,625 AS cnt625
       |		,626 AS cnt626
       |		,627 AS cnt627
       |		,628 AS cnt628
       |		,629 AS cnt629
       |		,630 AS cnt630
       |		,631 AS cnt631
       |		,632 AS cnt632
       |		,633 AS cnt633
       |		,634 AS cnt634
       |		,635 AS cnt635
       |		,636 AS cnt636
       |		,637 AS cnt637
       |		,638 AS cnt638
       |		,639 AS cnt639
       |		,640 AS cnt640
       |		,641 AS cnt641
       |		,642 AS cnt642
       |		,643 AS cnt643
       |		,644 AS cnt644
       |		,645 AS cnt645
       |		,646 AS cnt646
       |		,647 AS cnt647
       |		,648 AS cnt648
       |		,649 AS cnt649
       |		,650 AS cnt650
       |		,651 AS cnt651
       |		,652 AS cnt652
       |		,653 AS cnt653
       |		,654 AS cnt654
       |		,655 AS cnt655
       |		,656 AS cnt656
       |		,657 AS cnt657
       |		,658 AS cnt658
       |		,659 AS cnt659
       |		,660 AS cnt660
       |		,661 AS cnt661
       |		,662 AS cnt662
       |		,663 AS cnt663
       |		,664 AS cnt664
       |		,665 AS cnt665
       |		,666 AS cnt666
       |		,667 AS cnt667
       |		,668 AS cnt668
       |		,669 AS cnt669
       |		,670 AS cnt670
       |		,671 AS cnt671
       |		,672 AS cnt672
       |		,673 AS cnt673
       |		,674 AS cnt674
       |		,675 AS cnt675
       |		,676 AS cnt676
       |		,677 AS cnt677
       |		,678 AS cnt678
       |		,679 AS cnt679
       |		,680 AS cnt680
       |		,681 AS cnt681
       |		,682 AS cnt682
       |		,683 AS cnt683
       |		,684 AS cnt684
       |		,685 AS cnt685
       |		,686 AS cnt686
       |		,687 AS cnt687
       |		,688 AS cnt688
       |		,689 AS cnt689
       |		,690 AS cnt690
       |		,691 AS cnt691
       |		,692 AS cnt692
       |		,693 AS cnt693
       |		,694 AS cnt694
       |		,695 AS cnt695
       |		,696 AS cnt696
       |		,697 AS cnt697
       |		,698 AS cnt698
       |		,699 AS cnt699
       |		,700 AS cnt700
       |	FROM lineitem
       |	WHERE l_shipdate <= '1998-09-01'
       |	) AS A
       |GROUP BY l_returnflag
       |	,l_linestatus
     """.stripMargin

  val queries = Map(
    "Q1" -> """
              |SELECT l_returnflag
              |	,l_linestatus
              |	,sum(l_quantity) AS sum_qty
              |	,sum(l_extendedprice) AS sum_base_price
              |	,sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price
              |	,sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge
              |	,avg(l_quantity) AS avg_qty
              |	,avg(l_extendedprice) AS avg_price
              |	,avg(l_discount) AS avg_disc
              |	,count(*) AS count_order
              |FROM lineitem
              |WHERE l_shipdate <= '1998-09-01'
              |GROUP BY l_returnflag
              |	,l_linestatus
            """.stripMargin,
    "Q3" -> """
              |SELECT o_orderdate
              |	,o_shippriority
              |	,sum(l_extendedprice * (1 - l_discount)) AS revenue
              |FROM customer
              |	,orders
              |	,lineitem
              |WHERE c_mktsegment = 'BUILDING'
              |	AND c_custkey = o_custkey
              |	AND l_orderkey = o_orderkey
              |	AND o_orderdate < '1995-07-01'
              |	AND o_orderdate > '1994-01-01'
              |	AND l_shipdate > '1994-01-01'
              |GROUP BY o_orderdate
              |	,o_shippriority
            """.stripMargin,
    "Q5" -> """
              |SELECT n_name
              |	,sum(l_extendedprice * (1 - l_discount)) AS revenue
              |FROM customer
              |	,orders
              |	,lineitem
              |	,supplier
              |	,nation
              |	,region
              |WHERE c_custkey = o_custkey
              |	AND l_orderkey = o_orderkey
              |	AND l_suppkey = s_suppkey
              |	AND c_nationkey = s_nationkey
              |	AND s_nationkey = n_nationkey
              |	AND n_regionkey = r_regionkey
              |	AND r_name = 'AMERICA'
              |	AND o_orderdate >= '1995-01-01'
              |	AND o_orderdate < '1996-01-01'
              |GROUP BY n_name
            """.stripMargin,
    "Q6" -> """
              |SELECT sum(l_extendedprice * l_discount) AS revenues
              |FROM lineitem
              |WHERE l_shipdate >= '1996-01-01'
              |	AND l_shipdate < '1997-01-01'
              |	AND l_discount BETWEEN 0.06 AND 0.08
              |	AND l_quantity < 24
            """.stripMargin,
    "Q7" -> """
              |SELECT supp_nation
              |	,cust_nation
              |	,l_year
              |	,sum(volume) AS revenue
              |FROM (
              |	SELECT n1.n_name AS supp_nation
              |		,n2.n_name AS cust_nation
              |		,substring(l_shipdate, 1, 4) AS l_year
              |		,l_extendedprice * (1 - l_discount) AS volume
              |	FROM supplier
              |		,lineitem
              |		,orders
              |		,customer
              |		,nation n1
              |		,nation n2
              |	WHERE s_suppkey = l_suppkey
              |		AND o_orderkey = l_orderkey
              |		AND c_custkey = o_custkey
              |		AND s_nationkey = n1.n_nationkey
              |		AND c_nationkey = n2.n_nationkey
              |		AND (
              |			(
              |				n1.n_name = 'VIETNAM'
              |				AND n2.n_name = 'KENYA'
              |				)
              |			OR (
              |				n1.n_name = 'KENYA'
              |				AND n2.n_name = 'VIETNAM'
              |				)
              |			)
              |		AND l_shipdate BETWEEN '1995-01-01'
              |			AND '1996-12-31'
              |	) AS shipping
              |GROUP BY supp_nation
              |	,cust_nation
              |	,l_year
            """.stripMargin,
    "Q8" -> """
              |SELECT o_year
              |	,sum(CASE
              |			WHEN nation = 'JAPAN'
              |				THEN volume
              |			ELSE 0
              |			END) / sum(volume) AS mkt_share
              |FROM (
              |	SELECT substring(o_orderdate, 1, 4) AS o_year
              |		,l_extendedprice * (1 - l_discount) AS volume
              |		,n2.n_name AS nation
              |	FROM lineitem
              |   ,part
              |		,supplier
              |		,orders
              |		,customer
              |		,nation n1
              |		,nation n2
              |		,region
              |	WHERE p_partkey = l_partkey
              |		AND s_suppkey = l_suppkey
              |		AND l_orderkey = o_orderkey
              |		AND o_custkey = c_custkey
              |		AND c_nationkey = n1.n_nationkey
              |		AND n1.n_regionkey = r_regionkey
              |		AND r_name = 'ASIA'
              |		AND s_nationkey = n2.n_nationkey
              |		AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
              |		AND p_type = 'LARGE POLISHED BRASS'
              |	) AS all_nations
              |GROUP BY o_year
            """.stripMargin,
    "Q9" -> """
              |SELECT nation
              |	,o_year
              |	,sum(amount) AS sum_profit
              |FROM (
              |	SELECT n_name AS nation
              |		,substring(o_orderdate, 1, 4) AS o_year
              |		,l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
              |	FROM lineitem
              |   ,part
              |		,supplier
              |		,partsupp
              |		,orders
              |		,nation
              |	WHERE s_suppkey = l_suppkey
              |		AND ps_suppkey = l_suppkey
              |		AND ps_partkey = l_partkey
              |		AND p_partkey = l_partkey
              |		AND o_orderkey = l_orderkey
              |		AND s_nationkey = n_nationkey
              |		AND p_name LIKE '%ghost%'
              |	) AS profit
              |GROUP BY nation
              |	,o_year
            """.stripMargin,
    "Q10" -> """
               |SELECT n_name
               |	,sum(l_extendedprice * (1 - l_discount)) AS revenue
               |FROM customer
               |	,orders
               |	,lineitem
               |	,nation
               |WHERE c_custkey = o_custkey
               |	AND l_orderkey = o_orderkey
               |	AND o_orderdate >= '1994-10-01'
               |	AND o_orderdate < '1995-01-01'
               |	AND l_returnflag = 'R'
               |	AND c_nationkey = n_nationkey
               |GROUP BY n_name
             """.stripMargin,
    "Q11" -> """
               |SELECT n_nationkey
               |	,value
               |FROM (
               |	SELECT 0 AS KEY
               |		,n_nationkey
               |		,sum(ps_supplycost * ps_availqty) AS value
               |	FROM partsupp
               |		,supplier
               |		,nation
               |	WHERE ps_suppkey = s_suppkey
               |		AND s_nationkey = n_nationkey
               |	GROUP BY n_nationkey
               |	) AS A
               |	,(
               |		SELECT 0 AS KEY
               |			,sum(ps_supplycost * ps_availqty) * 0.00002 AS threshold
               |		FROM partsupp
               |			,supplier
               |			,nation
               |		WHERE ps_suppkey = s_suppkey
               |			AND s_nationkey = n_nationkey
               |		) AS B
               |WHERE A.KEY = B.KEY
               |	AND value > threshold
             """.stripMargin,
    "Q12" -> """
               |SELECT l_shipmode
               |	,sum(CASE
               |			WHEN o_orderpriority = '1-URGENT'
               |				OR o_orderpriority = '2-HIGH'
               |				THEN 1
               |			ELSE 0
               |			END) AS high_line_count
               |	,sum(CASE
               |			WHEN o_orderpriority <> '1-URGENT'
               |				AND o_orderpriority <> '2-HIGH'
               |				THEN 1
               |			ELSE 0
               |			END) AS low_line_count
               |FROM orders
               |	,lineitem
               |WHERE o_orderkey = l_orderkey
               |	AND l_shipmode IN (
               |		'RAIL'
               |		,'MAIL'
               |		)
               |	AND l_commitdate < l_receiptdate
               |	AND l_shipdate < l_commitdate
               |	AND l_receiptdate >= '1993-01-01'
               |	AND l_receiptdate < '1994-01-01'
               |GROUP BY l_shipmode
             """.stripMargin,
    "Q14" -> """
               |SELECT 100.00 * sum(CASE
               |			WHEN p_type LIKE 'PROMO%'
               |				THEN l_extendedprice * (1 - l_discount)
               |			ELSE 0
               |			END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
               |FROM lineitem
               |	,part
               |WHERE l_partkey = p_partkey
               |	AND l_shipdate >= '1996-12-01'
               |	AND l_shipdate < '1997-01-01'
             """.stripMargin,
    "Q16" -> """
               |SELECT p_type
               |	,p_size
               |	,count(ps_suppkey) AS supplier_cnt
               |FROM (
               |	SELECT p_brand
               |		,p_type
               |		,p_size
               |		,ps_suppkey
               |	FROM partsupp
               |		,part
               |	WHERE p_partkey = ps_partkey
               |		AND p_brand <> 'Brand#43'
               |		AND p_type NOT LIKE 'STANDARD BURNISHED%'
               |		AND p_size IN (
               |			22
               |			,7
               |			,8
               |			,35
               |			,33
               |			,11
               |			,31
               |			,39
               |			)
               |	) A
               |JOIN (
               |	SELECT s_suppkey
               |	FROM supplier
               |	WHERE s_comment NOT LIKE '%Customer%Complaints%'
               | GROUP BY s_suppkey
               |	) B ON ps_suppkey = s_suppkey
               |GROUP BY p_brand
               |	,p_type
               |	,p_size
             """.stripMargin,
    "Q17" -> """
               |SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
               |FROM (
               |	SELECT p_mfgr
               |		,l_quantity
               |		,l_extendedprice
               |	FROM lineitem
               |		,part
               |	WHERE p_partkey = l_partkey
               |		AND p_brand = 'Brand#42'
               |		AND p_container = 'JUMBO BOX'
               |	) AS A
               |	,(
               |		SELECT p_mfgr
               |			,0.2 * avg(l_quantity) AS threshold
               |		FROM lineitem
               |			,part
               |		WHERE l_partkey = p_partkey
               |		GROUP BY p_mfgr
               |		) AS B
               |WHERE A.p_mfgr = B.p_mfgr
               |	AND l_quantity < threshold
             """.stripMargin,
    "Q18" -> """
               |SELECT c_nationkey
               |	,sum(l_quantity)
               |FROM (
               |	SELECT c_nationkey
               |		,l_quantity
               |		,o_orderpriority
               |	FROM customer
               |		,orders
               |		,lineitem
               |	WHERE c_custkey = o_custkey
               |		AND o_orderkey = l_orderkey
               |	) A
               |JOIN (
               |SELECT o_orderpriority
               |FROM (
               |	SELECT o_orderpriority, sum(l_quantity) AS tot_qty
               |	FROM orders
               |		,lineitem
               |	WHERE o_orderkey = l_orderkey
               |	GROUP BY o_orderpriority
               | ) B
               |WHERE tot_qty > 3050250000
               |	) C ON (A.o_orderpriority = C.o_orderpriority)
               |GROUP BY c_nationkey
             """.stripMargin,
    "Q19" -> """
               |SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
               |FROM lineitem
               |	,part
               |WHERE p_partkey = l_partkey
               |	AND l_shipmode IN (
               |		'AIR'
               |		,'AIR REG'
               |		)
               |	AND l_shipinstruct = 'DELIVER IN PERSON'
               |	AND (
               |		(
               |			p_brand = 'Brand#45'
               |			AND p_container IN (
               |				'SM CASE'
               |				,'SM BOX'
               |				,'SM PACK'
               |				,'SM PKG'
               |				)
               |			AND l_quantity >= 7
               |			AND l_quantity <= 7 + 10
               |			AND p_size BETWEEN 1 AND 50
               |			)
               |		OR (
               |			p_brand = 'Brand#51'
               |			AND p_container IN (
               |				'MED BAG'
               |				,'MED BOX'
               |				,'MED PKG'
               |				,'MED PACK'
               |				)
               |			AND l_quantity >= 20
               |			AND l_quantity <= 20 + 10
               |			AND p_size BETWEEN 1 AND 10
               |			)
               |		OR (
               |			p_brand = 'Brand#51'
               |			AND p_container IN (
               |				'LG CASE'
               |				,'LG BOX'
               |				,'LG PACK'
               |				,'LG PKG'
               |				)
               |			AND l_quantity >= 28
               |			AND l_quantity <= 28 + 10
               |			AND p_size BETWEEN 1 AND 15
               |			)
               |		)
             """.stripMargin,
    "Q20" -> """
               |SELECT s_name
               |	,s_address
               |FROM (
               |	SELECT s_name
               |		,s_address
               |		,s_suppkey
               |	FROM supplier
               |		,nation
               |	WHERE s_nationkey = n_nationkey
               |		AND n_name = 'ETHIOPIA'
               |	) A
               |JOIN (
               |	SELECT ps_suppkey
               |	FROM partsupp
               |	JOIN (
               |		SELECT p_partkey
               |		FROM part
               |		WHERE p_name LIKE 'cornsilk%'
               |   GROUP BY p_partkey
               |		) C ON (ps_partkey = p_partkey)
               |	JOIN (
               |		SELECT s_suppkey
               |			,qty
               |		FROM (
               |			SELECT s_nationkey AS nationkey
               |				,sum(l_quantity) * 0.0001 AS qty
               |			FROM lineitem
               |				,supplier
               |			WHERE l_suppkey = s_suppkey
               |				AND l_shipdate >= '1996-01-01'
               |				AND l_shipdate < '1997-01-01'
               |			GROUP BY s_nationkey
               |			) lqty
               |			,supplier
               |		WHERE s_nationkey = nationkey
               |		) D ON (ps_suppkey = s_suppkey)
               |	WHERE ps_availqty > qty
               | GROUP BY ps_suppkey
               |	) B ON (s_suppkey = ps_suppkey)
             """.stripMargin,
    "Q22" -> """
               |SELECT cntrycode
               |	,count(*) AS numcust
               |	,sum(c_acctbal) AS totacctbal
               |FROM (
               |	SELECT 0 AS KEY
               |		,substring(c_phone, 1, 2) AS cntrycode
               |		,c_acctbal
               |	FROM customer
               |	WHERE substring(c_phone, 1, 2) IN (
               |			'26'
               |			,'29'
               |			,'31'
               |			,'28'
               |			,'30'
               |			,'25'
               |			,'40'
               |			)
               |	) AS A
               |	,(
               |		SELECT 0 AS KEY
               |			,avg(c_acctbal) AS threshold
               |		FROM customer
               |		WHERE c_acctbal > 0.00
               |			AND substring(c_phone, 1, 2) IN (
               |				'26'
               |				,'29'
               |				,'31'
               |				,'28'
               |				,'30'
               |				,'25'
               |				,'40'
               |				)
               |		) AS B
               |WHERE A.KEY = B.KEY
               |	AND c_acctbal > threshold
               |GROUP BY cntrycode
             """.stripMargin,
    "Qplus" -> arithmeticTemplate,
    "Qminus" -> arithmeticTemplate.replace('+', '-'),
    "Qtimes" -> arithmeticTemplate.replace('+', '*'),
    "Qdiv" -> arithmeticTemplate.replace('+', '/'),
    "Qrem" -> arithmeticTemplate.replace('+', '%')
  )
}
