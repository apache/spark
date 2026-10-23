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

import org.scalactic.Equality

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.SchemaPruningTest
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.tags.ExtendedSQLTest

/**
 * End-to-end query-shape coverage for nested schema pruning through generators (SPARK-47230):
 * each test runs a SQL query typical of analytics over arrays of structs - explode chains,
 * views, CTEs, windows, grouping sets, subqueries, higher-order functions - and checks both
 * the scan schema and that the results match the ones produced with
 * [[SQLConf.NESTED_SCHEMA_PRUNING_THROUGH_GENERATE_ENABLED]] disabled.
 *
 * [[SchemaPruningSuite]] covers the mechanics of the pruning rules; this suite covers the
 * surrounding query shapes.
 */
abstract class SchemaPruningThroughGenerateSuite
  extends QueryTest
  with SchemaPruningTest
  with SharedSparkSession {

  // The first field of each struct is its smallest, which is the one the `size(...) > 0`
  // filters inferred for a non-outer explode read
  case class Info(source: String, score: Double, hits: Array[Int])
  case class Part(
    inStock: Boolean, visible: Boolean, pid: Int, label: String, weight: Double, price: Double,
    tiers: Array[Int])
  case class Item(active: Boolean, qty: Int, sku: String, note: String, parts: Array[Part])
  case class Order(
    id: Int, country: String, memo: String, items: Array[Item], tags: Map[String, Info])
  case class Dim(id: Int, name: String)

  val ordersData: Seq[Order] = Seq(
    Order(1, "US", "m1", Array(
      Item(true, 1, "chrome", "n1", Array(
        Part(true, true, 10, "c10", 1.5, 1.2, Array(1, 2)),
        Part(false, true, 11, "c11", 2.0, 0.4, Array.empty))),
      Item(false, 2, "firefox", "n2", Array(
        Part(true, false, 12, "f12", 0.5, 2.5, Array(3))))),
      Map("a" -> Info("s1", 0.5, Array(7, 8)), "b" -> Info("s2", 1.5, Array.empty))),
    Order(2, "UK", "m2", Array(
      Item(true, 3, "chrome", "n3", Array.empty)),
      Map.empty),
    Order(3, "US", "m3", Array.empty, Map("c" -> Info(null, 2.0, null))),
    Order(4, "DE", null, null, null))

  val dimData: Seq[Dim] = Seq(Dim(1, "one"), Dim(2, "two"))

  private val fullPartsType =
    "array<struct<inStock:boolean,visible:boolean,pid:int,label:string," +
      "weight:double,price:double,tiers:array<int>>>"
  private val fullItemsType =
    s"array<struct<active:boolean,qty:int,sku:string,note:string,parts:$fullPartsType>>"
  private val fullTagsType = "map<string,struct<source:string,score:double,hits:array<int>>>"

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.ANSI_ENABLED.key, "false")
      // one deterministic plan per query, so the scans are collected in a stable order
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")

  /**
   * Runs `testThunk` against an `orders` parquet table of [[ordersData]] and a `base` temp view
   * exploding it twice, the common "flattening" view pattern this optimization targets.
   */
  protected def testPruning(testName: String)(testThunk: => Unit): Unit = {
    test(testName) {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.createDataFrame(ordersData).write.format("parquet").save(path + "/orders")
        spark.createDataFrame(dimData).write.format("parquet").save(path + "/dim")
        withTempView("orders", "dim", "base") {
          spark.read.format("parquet").load(path + "/orders").createOrReplaceTempView("orders")
          spark.read.format("parquet").load(path + "/dim").createOrReplaceTempView("dim")
          sql(
            """CREATE OR REPLACE TEMP VIEW base AS
              |SELECT * FROM orders
              |LATERAL VIEW EXPLODE(items) t1 AS item
              |LATERAL VIEW EXPLODE(item.parts) t2 AS part""".stripMargin)
          testThunk
        }
      }
    }
  }

  /**
   * Checks that `sqlText` reads exactly `expectedSchemata` from its scans, and returns the same
   * rows as with pruning through generators disabled.
   */
  protected def checkPrunedQuery(sqlText: String, expectedSchemata: String*): Unit = {
    val key = SQLConf.NESTED_SCHEMA_PRUNING_THROUGH_GENERATE_ENABLED.key
    val expectedRows = withSQLConf(key -> "false") {
      sql(sqlText).collect().toSeq
    }
    withSQLConf(key -> "true") {
      val query = sql(sqlText)
      checkScanSchemata(query, expectedSchemata: _*)
      checkAnswer(query, expectedRows)
    }
  }

  protected val schemaEquality: Equality[StructType] = new Equality[StructType] {
    override def areEqual(a: StructType, b: Any): Boolean =
      b match {
        case otherType: StructType => DataTypeUtils.sameType(a, otherType)
        case _ => false
      }
  }

  protected def checkScanSchemata(df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
    val fileSourceScanSchemata = df.queryExecution.executedPlan.collect {
      case scan: FileSourceScanExec => scan.requiredSchema
    }
    assert(fileSourceScanSchemata.size === expectedSchemaCatalogStrings.size,
      s"Found ${fileSourceScanSchemata.size} file sources in dataframe, " +
        s"but expected $expectedSchemaCatalogStrings")
    fileSourceScanSchemata.zip(expectedSchemaCatalogStrings).foreach {
      case (scanSchema, expectedScanSchemaCatalogString) =>
        val expectedScanSchema = CatalystSqlParser.parseDataType(expectedScanSchemaCatalogString)
        implicit val equality = schemaEquality
        assert(scanSchema === expectedScanSchema)
    }
  }

  testPruning("SPARK-47230: top-level fields only") {
    checkPrunedQuery(
      "select id, country from orders",
      "struct<id:int,country:string>")
  }

  testPruning("SPARK-47230: double explode, fields from every level") {
    checkPrunedQuery(
      """select id, item.sku, part.inStock, part.pid
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part""".stripMargin,
      "struct<id:int,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean,pid:int>>>>>")
  }

  testPruning("SPARK-47230: through a SELECT * view") {
    // only `pid` is used from `part`, so GeneratorNestedColumnAliasing rewrites the inner
    // explode to explode(item.parts.pid), whose size() filter needs no extra smallest field
    checkPrunedQuery(
      "select id, item.sku, part.pid from base",
      "struct<id:int,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<pid:int>>>>>")
  }

  testPruning("SPARK-47230: filters on exploded fields") {
    checkPrunedQuery(
      """select id, item.sku, part.pid, part.price from base
        |where item.active = true and part.inStock = true and part.price > 0""".stripMargin,
      "struct<id:int,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean,pid:int,price:double>>>>>")
  }

  testPruning("SPARK-47230: aggregation with order and limit") {
    checkPrunedQuery(
      """select country, item.sku, part.pid, count(*) as cnt,
        |  sum(case when part.inStock then 1 else 0 end) as stocked,
        |  avg(part.price) as avg_price
        |from base
        |where items is not null
        |group by country, item.sku, part.pid
        |order by cnt desc, part.pid
        |limit 10""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean,pid:int,price:double>>>>>")
  }

  testPruning("SPARK-47230: only inner-element fields") {
    checkPrunedQuery(
      "select part.pid, part.label, part.price from base",
      "struct<items:array<struct<active:boolean," +
        "parts:array<struct<inStock:boolean,pid:int,label:string,price:double>>>>>")
  }

  testPruning("SPARK-47230: CTE over the explode chain") {
    // `id` and `pid` are selected by the CTE but unused above it, so they are pruned away
    checkPrunedQuery(
      """with exploded as (
        |  select id, country, item.sku, part.pid, part.inStock
        |  from orders
        |  lateral view explode(items) t1 as item
        |  lateral view explode(item.parts) t2 as part)
        |select country, sku, count(*) as total,
        |  sum(case when inStock then 1 else 0 end) as stocked
        |from exploded
        |group by country, sku""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean>>>>>")
  }

  testPruning("SPARK-47230: outer explode chain") {
    // no `size(...) > 0` filters are inferred for outer generators, so nothing beyond the
    // selected fields is read
    checkPrunedQuery(
      """select id, country, item.sku, part.pid
        |from orders
        |lateral view outer explode(items) t1 as item
        |lateral view outer explode(item.parts) t2 as part""".stripMargin,
      "struct<id:int,country:string,items:array<struct<sku:string," +
        "parts:array<struct<pid:int>>>>>")
  }

  testPruning("SPARK-47230: posexplode chain with positions selected") {
    checkPrunedQuery(
      """select id, ipos, item.sku, ppos, part.pid, part.inStock
        |from orders
        |lateral view posexplode(items) t1 as ipos, item
        |lateral view posexplode(item.parts) t2 as ppos, part""".stripMargin,
      "struct<id:int,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean,pid:int>>>>>")
  }

  testPruning("SPARK-47230: consecutive outer posexplodes under an aggregating query") {
    // the same aggregation over a posexploded CTE, with and without the position columns
    // selected: both prune the same way, and to the same results
    def aggregateOverExploded(positions: String): String =
      s"""with exploded as (
         |  select country, id, $positions item.sku, item.active,
         |    part.pid, part.inStock, part.price, part.visible
         |  from orders
         |  lateral view outer posexplode(items) t1 as ipos, item
         |  lateral view outer posexplode(item.parts) t2 as ppos, part)
         |select country, sku, pid, count(*) as cnt,
         |  sum(case when inStock then 1 else 0 end) as stocked,
         |  sum(case when visible then 1 else 0 end) as vis,
         |  avg(price) as avg_price
         |from exploded
         |where active = true
         |group by country, sku, pid
         |order by cnt desc, pid
         |limit 20""".stripMargin

    val expectedSchema =
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean,visible:boolean,pid:int,price:double>>>>>"
    checkPrunedQuery(aggregateOverExploded(positions = ""), expectedSchema)
    checkPrunedQuery(aggregateOverExploded(positions = "ipos, ppos,"), expectedSchema)
  }

  testPruning("SPARK-47230: window functions over exploded fields") {
    checkPrunedQuery(
      """select country, item.sku, part.pid,
        |  row_number() over (partition by country order by part.pid) as rn,
        |  sum(part.price) over (partition by country) as total_price
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean,pid:int,price:double>>>>>")
  }

  testPruning("SPARK-47230: lag and lead over exploded fields") {
    checkPrunedQuery(
      """select country, part.pid,
        |  lag(part.price, 1) over (partition by country order by part.pid) as prev_price,
        |  lead(part.price, 1) over (partition by country order by part.pid) as next_price
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean," +
        "parts:array<struct<inStock:boolean,pid:int,price:double>>>>>")
  }

  testPruning("SPARK-47230: transform lambda narrows the array without an explode") {
    checkPrunedQuery(
      "select id, transform(items, i -> i.sku) as skus from orders",
      "struct<id:int,items:array<struct<sku:string>>>")
  }

  testPruning("SPARK-47230: filter lambda keeps the elements whole") {
    // `filter` returns the elements themselves, so reading the whole struct is required,
    // not a missed pruning
    checkPrunedQuery(
      "select id, filter(items, i -> i.active) as active_items from orders",
      s"struct<id:int,items:$fullItemsType>")
  }

  testPruning("SPARK-47230: aggregate lambda is not narrowed") {
    // TODO: narrow the input of a higher-order `aggregate`; today it is read whole even
    // though only `parts` sizes are needed
    checkPrunedQuery(
      "select id, aggregate(items, 0L, (acc, i) -> acc + size(i.parts)) as total_parts " +
        "from orders",
      s"struct<id:int,items:$fullItemsType>")
  }

  testPruning("SPARK-47230: union of two explode chains prunes both scans") {
    val branch =
      """select country, item.sku, part.pid
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part""".stripMargin
    val branchSchema =
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<pid:int>>>>>"
    checkPrunedQuery(
      s"$branch\nwhere country = 'US'\nunion all\n$branch\nwhere country = 'UK'",
      branchSchema, branchSchema)
  }

  testPruning("SPARK-47230: self-join on exploded data prunes each side separately") {
    checkPrunedQuery(
      """with b as (
        |  select id, item.sku, part.pid, part.price from base)
        |select b1.id, b1.sku, b1.pid, b2.price
        |from b b1
        |inner join b b2 on b1.pid = b2.pid""".stripMargin,
      "struct<id:int,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<pid:int>>>>>",
      "struct<items:array<struct<active:boolean," +
        "parts:array<struct<inStock:boolean,pid:int,price:double>>>>>")
  }

  testPruning("SPARK-47230: exists subquery over an explode is not pruned") {
    // TODO: prune through the semi join an exists subquery is rewritten into; today it
    // blocks pruning on both sides
    checkPrunedQuery(
      """select id, item.sku
        |from orders
        |lateral view explode(items) t1 as item
        |where exists (
        |  select 1 from orders o2
        |  lateral view explode(o2.items) t2 as i2
        |  where i2.sku = item.sku)""".stripMargin,
      s"struct<id:int,items:$fullItemsType>",
      s"struct<items:$fullItemsType>")
  }

  testPruning("SPARK-47230: distinct over an exploded field") {
    checkPrunedQuery(
      "select distinct part.pid from base",
      "struct<items:array<struct<active:boolean,parts:array<struct<pid:int>>>>>")
  }

  testPruning("SPARK-47230: an unused inner element reads a single field") {
    checkPrunedQuery(
      """select country, item.sku, count(*) as cnt
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part
        |group by country, item.sku
        |having count(*) > 1""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean>>>>>")
  }

  testPruning("SPARK-47230: rollup over exploded fields prunes only the outer element") {
    // TODO: narrow the inner element under a rollup. Unlike cube below, grouping by
    // rollup on a nested field narrows the outer element but reads the inner one whole
    checkPrunedQuery(
      """select country, item.sku, sum(part.price) as total_price
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part
        |group by rollup(country, item.sku)""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        s"parts:$fullPartsType>>>")
  }

  testPruning("SPARK-47230: cube over exploded fields") {
    checkPrunedQuery(
      """select country, part.pid, count(*) as cnt
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part
        |group by cube(country, part.pid)""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean," +
        "parts:array<struct<pid:int>>>>>")
  }

  testPruning("SPARK-47230: case when over exploded fields") {
    checkPrunedQuery(
      """select country,
        |  case when part.price > 1.0 then 'high'
        |       when part.price > 0.5 then 'medium'
        |       else 'low' end as tier,
        |  part.pid
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean," +
        "parts:array<struct<inStock:boolean,pid:int,price:double>>>>>")
  }

  testPruning("SPARK-47230: elements passed whole between CTEs") {
    // `item` and `part` cross the CTE boundaries whole; only the fields the final query
    // uses are read
    checkPrunedQuery(
      """with items_exploded as (
        |  select id, country, item from orders
        |  lateral view explode(items) t1 as item),
        |parts_exploded as (
        |  select id, country, item.sku, part from items_exploded
        |  lateral view explode(item.parts) t2 as part),
        |aggregated as (
        |  select country, sku, avg(part.price) as avg_price
        |  from parts_exploded
        |  group by country, sku)
        |select * from aggregated where avg_price > 0""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<price:double>>>>>")
  }

  testPruning("SPARK-47230: coalesce over an outer explode chain") {
    checkPrunedQuery(
      """select id, coalesce(item.sku, 'unknown') as sku, coalesce(part.price, 0.0) as price
        |from orders
        |lateral view outer explode(items) t1 as item
        |lateral view outer explode(item.parts) t2 as part""".stripMargin,
      "struct<id:int,items:array<struct<sku:string,parts:array<struct<price:double>>>>>")
  }

  testPruning("SPARK-47230: string functions over an exploded field") {
    checkPrunedQuery(
      """select id, upper(item.sku) as sku_upper, length(item.sku) as sku_len,
        |  concat(country, '-', item.sku) as combined
        |from orders
        |lateral view explode(items) t1 as item""".stripMargin,
      "struct<id:int,country:string,items:array<struct<sku:string>>>")
  }

  testPruning("SPARK-47230: collect_list and collect_set of exploded fields") {
    checkPrunedQuery(
      """select country, collect_list(item.sku) as skus
        |from orders
        |lateral view explode(items) t1 as item
        |group by country""".stripMargin,
      "struct<country:string,items:array<struct<sku:string>>>")

    checkPrunedQuery(
      "select country, collect_set(part.pid) as pids from base group by country",
      "struct<country:string,items:array<struct<active:boolean," +
        "parts:array<struct<pid:int>>>>>")
  }

  testPruning("SPARK-47230: order by exploded fields with limit") {
    checkPrunedQuery(
      """select country, item.sku, part.pid, part.price
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part
        |order by country desc, part.price desc, part.pid
        |limit 100""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean,pid:int,price:double>>>>>")
  }

  testPruning("SPARK-47230: in subquery over an explode prunes both scans") {
    checkPrunedQuery(
      """select id, item.sku
        |from orders
        |lateral view explode(items) t1 as item
        |where item.sku in (
        |  select i2.sku from orders o2
        |  lateral view explode(o2.items) t2 as i2
        |  where i2.active = true)""".stripMargin,
      "struct<id:int,items:array<struct<active:boolean,sku:string>>>",
      "struct<items:array<struct<active:boolean,sku:string>>>")
  }

  testPruning("SPARK-47230: SELECT * CTE with an outer posexplode chain") {
    checkPrunedQuery(
      """with exploded as (
        |  select * from orders
        |  lateral view outer posexplode(items) t1 as ipos, item
        |  lateral view outer posexplode(item.parts) t2 as ppos, part)
        |select country, item.sku, part.pid, count(*) as cnt,
        |  sum(case when part.inStock then 1 else 0 end) as stocked,
        |  avg(part.price) as avg_price
        |from exploded
        |where item.active = true
        |group by country, item.sku, part.pid""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean,pid:int,price:double>>>>>")
  }

  testPruning("SPARK-47230: an explode of a map in the chain reads the map whole") {
    // TODO: prune the map through its explode; today maps are read whole. The array
    // explode in the same chain still prunes, and `item.sku` being the single field used
    // lets GeneratorNestedColumnAliasing narrow `explode(items)` itself, so no size()
    // field is needed either
    checkPrunedQuery(
      """select item.sku, k, v.score
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(tags) t2 as k, v""".stripMargin,
      s"struct<items:array<struct<sku:string>>,tags:$fullTagsType>")
  }

  testPruning("SPARK-47230: map_keys and map_values read the map whole") {
    // TODO: narrow the map's value struct to `score`; today it is read whole
    checkPrunedQuery(
      "select id, map_keys(tags)[0] as k0, map_values(tags)[0].score as s0 from orders",
      s"struct<id:int,tags:$fullTagsType>")
  }

  testPruning("SPARK-47230: left join of two exploded branches prunes each side") {
    checkPrunedQuery(
      """with a as (
        |  select id, item.sku, part.pid from base),
        |b as (
        |  select id, part.pid, part.price from base)
        |select a.id, a.sku, a.pid, b.price
        |from a left join b on a.pid = b.pid""".stripMargin,
      "struct<id:int,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<pid:int>>>>>",
      "struct<items:array<struct<active:boolean," +
        "parts:array<struct<inStock:boolean,pid:int,price:double>>>>>")
  }

  testPruning("SPARK-47230: a scala UDF over an exploded field") {
    spark.udf.register("skuLen", (s: String) => if (s == null) -1 else s.length)
    checkPrunedQuery(
      """select id, skuLen(item.sku) as sku_len
        |from orders
        |lateral view explode(items) t1 as item""".stripMargin,
      "struct<id:int,items:array<struct<sku:string>>>")
  }

  testPruning("SPARK-47230: aliases shadowing real column names") {
    // `country` and `memo` are real columns; using their names as aliases of exploded
    // fields must neither confuse the pruning nor read the shadowed columns
    checkPrunedQuery(
      "select part.pid as country, item.sku as memo from base",
      "struct<items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<pid:int>>>>>")
  }

  testPruning("SPARK-47230: an explode over an aliased element field across CTEs") {
    // the inner generator's input is an alias (`re_parts`) of a field of the outer element,
    // defined in a different CTE; `sku_alias` is selected there but unused in the end
    checkPrunedQuery(
      """with item_level as (
        |  select id as order_id, item.sku as sku_alias, item.parts as re_parts
        |  from orders
        |  lateral view outer explode(items) t1 as item
        |  where item.parts is not null),
        |part_level as (
        |  select order_id, sku_alias, p.pid as part_id, p.price as boost
        |  from item_level
        |  lateral view outer explode(re_parts) t2 as p
        |  where p.price is not null)
        |select order_id, part_id,
        |  cast(sum(coalesce(boost, 0)) as decimal(18,4)) as sum_boost,
        |  count(case when boost is not null then 1 end) as num_boost
        |from part_level
        |group by order_id, part_id""".stripMargin,
      "struct<id:int,items:array<struct<parts:array<struct<pid:int,price:double>>>>>")
  }

  testPruning("SPARK-47230: query shapes using the element whole are not pruned") {
    // ordering by, grouping by, or aggregating the element itself needs every field of it
    checkPrunedQuery(
      """select item.sku
        |from orders
        |lateral view explode(items) t1 as item
        |order by item""".stripMargin,
      s"struct<items:$fullItemsType>")

    checkPrunedQuery(
      """select max(item.sku) as msku, count(*) as cnt
        |from orders
        |lateral view explode(items) t1 as item
        |group by item""".stripMargin,
      s"struct<items:$fullItemsType>")

    checkPrunedQuery(
      """select item.qty, first(item) as it
        |from orders
        |lateral view explode(items) t1 as item
        |group by item.qty""".stripMargin,
      s"struct<items:$fullItemsType>")
  }

  testPruning("SPARK-47230: count distinct with case when over exploded fields") {
    checkPrunedQuery(
      """select country,
        |  count(distinct case when part.inStock then part.pid end) as d1,
        |  count(distinct case when part.visible then part.label end) as d2
        |from base
        |group by country""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean," +
        "parts:array<struct<inStock:boolean,visible:boolean,pid:int,label:string>>>>>")
  }

  testPruning("SPARK-47230: grouping sets over exploded fields") {
    // unlike rollup above, grouping sets prune like a plain group by
    checkPrunedQuery(
      """select country, item.sku, count(*) as cnt
        |from base
        |group by grouping sets ((country), (country, item.sku))""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean>>>>>")
  }

  testPruning("SPARK-47230: explode of an indexed sub-array") {
    checkPrunedQuery(
      "select p.pid from orders lateral view explode(items[0].parts) t as p",
      "struct<items:array<struct<parts:array<struct<pid:int>>>>>")
  }

  testPruning("SPARK-47230: named window with a frame whose columns are not all selected") {
    // first_value/last_value with an explicit frame; the window's partition and order
    // columns (id, item.sku, part.pid) are read even where not selected
    checkPrunedQuery(
      """select id, part.pid as pid,
        |  first_value(part.price) over w as first_price,
        |  last_value(part.label) over w as last_label
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part
        |where country = 'US' and part.inStock = true
        |window w as (partition by id, item.sku order by part.pid asc
        |             rows between current row and unbounded following)""".stripMargin,
      "struct<id:int,country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean,pid:int,label:string,price:double>>>>>")
  }

  testPruning("SPARK-47230: window aggregate over the whole array") {
    // first(items) over a window uses the array whole, so nothing can be pruned
    checkPrunedQuery(
      """with b as (
        |  select country, first(items) over w as arr
        |  from orders
        |  window w as (partition by country order by id desc))
        |select country, first(arr) as my_arr from b group by country""".stripMargin,
      s"struct<id:int,country:string,items:$fullItemsType>")
  }

  testPruning("SPARK-47230: size of a filtered sub-array inside aggregates") {
    // the filter lambda inside size() is narrowed to the fields it uses (inStock, label);
    // also covers greatest/if/coalesce, group by ordinal and having
    checkPrunedQuery(
      """with data as (
        |  select coalesce(part.pid, -1) as part_id,
        |    greatest(0, size(filter(item.parts,
        |      p -> p.inStock and p.label in ('c10', 'c11')))) as conv,
        |    coalesce(part.price, 0) * if(coalesce(part.visible, false), 1, 0) as spent,
        |    if(coalesce(part.visible, false), 1, 0) as clicks
        |  from orders
        |  lateral view explode(items) t1 as item
        |  lateral view explode(item.parts) t2 as part
        |  where item.active = true and part.pid is not null)
        |select part_id, sum(conv) as conv_sum, sum(conv * conv) as conv_sq,
        |  sum(spent) as spent_sum, sum(clicks) as clicks_sum,
        |  if(sum(conv) > 0, sum(spent) / sum(conv), sum(spent)) as cpa,
        |  count(*) as impressions
        |from data
        |group by 1
        |having spent_sum >= 0""".stripMargin,
      "struct<items:array<struct<active:boolean," +
        "parts:array<struct<inStock:boolean,visible:boolean,pid:int,label:string," +
        "price:double>>>>>")
  }

  testPruning("SPARK-47230: three-level explode chain") {
    checkPrunedQuery(
      """select id, item.sku, part.pid, tier
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part
        |lateral view explode(part.tiers) t3 as tier""".stripMargin,
      "struct<id:int,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<inStock:boolean,pid:int,tiers:array<int>>>>>>")
  }

  testPruning("SPARK-47230: a map explode feeding an explode of the value's array") {
    // the exploded map is read whole; the array explode above it still works
    checkPrunedQuery(
      """select k, hit
        |from orders
        |lateral view explode(tags) t1 as k, v
        |lateral view explode(v.hits) t2 as hit""".stripMargin,
      s"struct<tags:$fullTagsType>")
  }

  testPruning("SPARK-47230: explode of a map value lookup") {
    // unlike exploding the map itself, looking up one key narrows the value struct
    checkPrunedQuery(
      "select id, hit from orders lateral view explode(tags['a'].hits) t as hit",
      "struct<id:int,tags:map<string,struct<hits:array<int>>>>")
  }

  testPruning("SPARK-47230: position filters on an outer posexplode chain") {
    // the production view pattern: keep only the first element of each level by position
    checkPrunedQuery(
      """select id, item.sku
        |from orders
        |lateral view outer posexplode(items) t1 as ipos, item
        |lateral view outer posexplode(item.parts) t2 as ppos, part
        |where (ppos is null or ppos = 0) and ipos = 0""".stripMargin,
      "struct<id:int,items:array<struct<sku:string," +
        "parts:array<struct<inStock:boolean>>>>>")
  }

  testPruning("SPARK-47230: from_json over an exploded field") {
    checkPrunedQuery(
      """select id, from_json(item.note, 'a INT').a as a
        |from orders
        |lateral view explode(items) t1 as item""".stripMargin,
      "struct<id:int,items:array<struct<note:string>>>")
  }

  testPruning("SPARK-47230: an explode above a join is not pruned through") {
    // TODO: prune through generators that sit above a Join instead of directly over the
    // scan. Today the whole `items` column is read; the join itself still prunes
    // top-level columns. This shape (join a dimension table, then LATERAL VIEW) is
    // common in production ETL.
    checkPrunedQuery(
      """select /*+ BROADCAST(d) */ d.name, item.sku, part.pid
        |from orders o
        |inner join dim d on o.id = d.id
        |lateral view explode(o.items) t1 as item
        |lateral view explode(item.parts) t2 as part""".stripMargin,
      s"struct<id:int,items:$fullItemsType>",
      "struct<id:int,name:string>")
  }

  testPruning("SPARK-47230: a UDF consuming a whole sub-array keeps it whole") {
    // the UDF is opaque, so `parts` must be read whole; the element is still narrowed
    spark.udf.register("partsCount",
      (parts: Seq[Row]) => if (parts == null) -1 else parts.size)
    checkPrunedQuery(
      """select id, partsCount(item.parts) as pc
        |from orders
        |lateral view explode(items) t1 as item""".stripMargin,
      s"struct<id:int,items:array<struct<active:boolean,parts:$fullPartsType>>>")
  }

  testPruning("SPARK-47230: distribute by and sort by after the explode chain") {
    checkPrunedQuery(
      """select country, item.sku as sku, part.pid as pid
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part
        |distribute by country sort by pid""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<pid:int>>>>>")
  }

  testPruning("SPARK-47230: explode of arrays_zip over sub-array field projections") {
    checkPrunedQuery(
      """select z.pid, z.price
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(arrays_zip(item.parts.pid, item.parts.price)) t2 as z""".stripMargin,
      "struct<items:array<struct<active:boolean," +
        "parts:array<struct<pid:int,price:double>>>>>")
  }

  testPruning("SPARK-47230: posexplode_outer in the select list") {
    checkPrunedQuery(
      """select id, pos, item.sku
        |from (select id, posexplode_outer(items) as (pos, item) from orders)""".stripMargin,
      "struct<id:int,items:array<struct<sku:string>>>")
  }

  testPruning("SPARK-47230: explode of a filtered array of named_structs") {
    // the generator input is built with named_struct from fields of the exploded element
    checkPrunedQuery(
      """select c.pid2
        |from orders
        |lateral view explode(items) t1 as item
        |lateral view explode(item.parts) t2 as part
        |lateral view explode(filter(array(named_struct('pid2', part.pid, 'pr', part.price)),
        |  c -> c.pid2 is not null)) t3 as c""".stripMargin,
      "struct<items:array<struct<active:boolean," +
        "parts:array<struct<inStock:boolean,pid:int,price:double>>>>>")
  }

  testPruning("SPARK-47230: tuple count distinct and approx_count_distinct") {
    checkPrunedQuery(
      """select country,
        |  count(distinct (item.sku, part.pid)) as tuple_cnt,
        |  approx_count_distinct(part.pid, 0.05) as approx_pids
        |from base
        |group by country""".stripMargin,
      "struct<country:string,items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<pid:int>>>>>")
  }

  testPruning("SPARK-47230: nested transform rebuilding slim structs") {
    // TODO: narrow the inner transform lambda; today only the outer one is narrowed
    checkPrunedQuery(
      """select id, transform(items,
        |  i -> struct(i.sku as sku,
        |              transform(i.parts, p -> struct(p.pid as pid)) as parts)) as slim
        |from orders""".stripMargin,
      s"struct<id:int,items:array<struct<sku:string,parts:$fullPartsType>>>")
  }

  testPruning("SPARK-47230: window over an aggregate of exploded fields with cluster by") {
    checkPrunedQuery(
      """with g as (
        |  select item.sku as sku, part.pid as pid, count(1) as cnt
        |  from base
        |  group by item.sku, part.pid)
        |select sku, pid, cnt, count(1) over (partition by sku) as per_sku
        |from g
        |cluster by sku""".stripMargin,
      "struct<items:array<struct<active:boolean,sku:string," +
        "parts:array<struct<pid:int>>>>>")
  }

  testPruning("SPARK-47230: a null filter on the inner array reads that array whole") {
    // TODO: `item.parts is not null` needs only the array's nullability, but today it
    // keeps every field of `parts`; the element itself is narrowed
    checkPrunedQuery(
      """select item.sku
        |from orders
        |lateral view explode(items) t1 as item
        |where item.parts is not null""".stripMargin,
      "struct<items:array<struct<active:boolean,sku:string," +
        s"parts:$fullPartsType>>>")
  }
}

@ExtendedSQLTest
class ParquetV1SchemaPruningThroughGenerateSuite extends SchemaPruningThroughGenerateSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "parquet")
}

@ExtendedSQLTest
class ParquetV2SchemaPruningThroughGenerateSuite extends SchemaPruningThroughGenerateSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "")

  override protected def checkScanSchemata(
      df: DataFrame,
      expectedSchemaCatalogStrings: String*): Unit = {
    val fileSourceScanSchemata = df.queryExecution.executedPlan.collect {
      case scan: BatchScanExec => scan.scan.asInstanceOf[ParquetScan].readDataSchema
    }
    assert(fileSourceScanSchemata.size === expectedSchemaCatalogStrings.size,
      s"Found ${fileSourceScanSchemata.size} file sources in dataframe, " +
        s"but expected $expectedSchemaCatalogStrings")
    fileSourceScanSchemata.zip(expectedSchemaCatalogStrings).foreach {
      case (scanSchema, expectedScanSchemaCatalogString) =>
        val expectedScanSchema = CatalystSqlParser.parseDataType(expectedScanSchemaCatalogString)
        implicit val equality = schemaEquality
        assert(scanSchema === expectedScanSchema)
    }
  }
}
