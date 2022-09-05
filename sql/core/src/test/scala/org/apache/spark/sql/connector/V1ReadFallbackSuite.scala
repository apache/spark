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

package org.apache.spark.sql.connector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, QueryTest, Row, SparkSession, SQLContext}
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, Identifier, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, V1Scan}
import org.apache.spark.sql.execution.RowDataSourceScanExec
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.{BaseRelation, Filter, GreaterThan, TableScan}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

abstract class V1ReadFallbackSuite extends QueryTest with SharedSparkSession {
  protected def baseTableScan(): DataFrame

  test("full scan") {
    val df = baseTableScan()
    val v1Scan = df.queryExecution.executedPlan.collect {
      case s: RowDataSourceScanExec => s
    }
    assert(v1Scan.length == 1)
    checkAnswer(df, Seq(Row(1, 10), Row(2, 20), Row(3, 30)))
  }

  test("column pruning") {
    val df = baseTableScan().select("i")
    val v1Scan = df.queryExecution.executedPlan.collect {
      case s: RowDataSourceScanExec => s
    }
    assert(v1Scan.length == 1)
    assert(v1Scan.head.output.map(_.name) == Seq("i"))
    checkAnswer(df, Seq(Row(1), Row(2), Row(3)))
  }

  test("filter push down") {
    val df = baseTableScan().filter("i > 1 and j < 30")
    val v1Scan = df.queryExecution.executedPlan.collect {
      case s: RowDataSourceScanExec => s
    }
    assert(v1Scan.length == 1)
    // `j < 30` can't be pushed.
    assert(v1Scan.head.handledFilters.size == 1)
    checkAnswer(df, Seq(Row(2, 20)))
  }

  test("filter push down + column pruning") {
    val df = baseTableScan().filter("i > 1").select("i")
    val v1Scan = df.queryExecution.executedPlan.collect {
      case s: RowDataSourceScanExec => s
    }
    assert(v1Scan.length == 1)
    assert(v1Scan.head.output.map(_.name) == Seq("i"))
    assert(v1Scan.head.handledFilters.size == 1)
    checkAnswer(df, Seq(Row(2), Row(3)))
  }
}

class V1ReadFallbackWithDataFrameReaderSuite extends V1ReadFallbackSuite {
  override protected def baseTableScan(): DataFrame = {
    spark.read.format(classOf[V1ReadFallbackTableProvider].getName).load()
  }
}

class V1ReadFallbackWithCatalogSuite extends V1ReadFallbackSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.catalog.read_fallback", classOf[V1ReadFallbackCatalog].getName)
    sql("CREATE TABLE read_fallback.tbl(i int, j int) USING foo")
  }

  override def afterAll(): Unit = {
    spark.conf.unset("spark.sql.catalog.read_fallback")
    super.afterAll()
  }

  override protected def baseTableScan(): DataFrame = {
    spark.table("read_fallback.tbl")
  }
}

class V1ReadFallbackCatalog extends BasicInMemoryTableCatalog {
  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    // To simplify the test implementation, only support fixed schema.
    if (schema != V1ReadFallbackCatalog.schema || partitions.nonEmpty) {
      throw new UnsupportedOperationException
    }
    val table = new TableWithV1ReadFallback(ident.toString)
    tables.put(ident, table)
    table
  }
}

object V1ReadFallbackCatalog {
  val schema = new StructType().add("i", "int").add("j", "int")
}

class V1ReadFallbackTableProvider extends SimpleTableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new TableWithV1ReadFallback("v1-read-fallback")
  }
}

class TableWithV1ReadFallback(override val name: String) extends Table with SupportsRead {

  override def schema(): StructType = V1ReadFallbackCatalog.schema

  override def capabilities(): java.util.Set[TableCapability] = {
    java.util.EnumSet.of(TableCapability.BATCH_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new V1ReadFallbackScanBuilder
  }

  private class V1ReadFallbackScanBuilder extends ScanBuilder
    with SupportsPushDownRequiredColumns with SupportsPushDownFilters {

    private var requiredSchema: StructType = schema()
    override def pruneColumns(requiredSchema: StructType): Unit = {
      this.requiredSchema = requiredSchema
    }

    private var filters: Array[Filter] = Array.empty
    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
      val (supported, unsupported) = filters.partition {
        case GreaterThan("i", _: Int) => true
        case _ => false
      }
      this.filters = supported
      unsupported
    }
    override def pushedFilters(): Array[Filter] = filters

    override def build(): Scan = new V1ReadFallbackScan(requiredSchema, filters)
  }

  private class V1ReadFallbackScan(
      requiredSchema: StructType,
      filters: Array[Filter]) extends V1Scan {
    override def readSchema(): StructType = requiredSchema

    override def toV1TableScan[T <: BaseRelation with TableScan](context: SQLContext): T = {
      new V1TableScan(context, requiredSchema, filters).asInstanceOf[T]
    }
  }
}

class V1TableScan(
    context: SQLContext,
    requiredSchema: StructType,
    filters: Array[Filter]) extends BaseRelation with TableScan {
  override def sqlContext: SQLContext = context
  override def schema: StructType = requiredSchema
  override def buildScan(): RDD[Row] = {
    val lowerBound = if (filters.isEmpty) {
      0
    } else {
      filters.collect { case GreaterThan("i", v: Int) => v }.max
    }
    val data = Seq(Row(1, 10), Row(2, 20), Row(3, 30)).filter(_.getInt(0) > lowerBound)
    val result = if (requiredSchema.length == 2) {
      data
    } else if (requiredSchema.map(_.name) == Seq("i")) {
      data.map(row => Row(row.getInt(0)))
    } else if (requiredSchema.map(_.name) == Seq("j")) {
      data.map(row => Row(row.getInt(1)))
    } else {
      throw new UnsupportedOperationException
    }

    SparkSession.active.sparkContext.makeRDD(result)
  }
}
