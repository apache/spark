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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.scalatest.BeforeAndAfter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.QueryTest.withQueryExecutionsCaptured
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTable, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomSumMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, V1Scan}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, LogicalWriteInfoImpl, SupportsOverwrite, SupportsTruncate, V1Write, WriteBuilder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, OverwriteByExpressionExecV1}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf.{OPTIMIZER_MAX_ITERATIONS, V2_SESSION_CATALOG_IMPLEMENTATION}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

class V1WriteFallbackSuite extends QueryTest with SharedSparkSession with BeforeAndAfter {

  import testImplicits._

  private val v2Format = classOf[InMemoryV1Provider].getName

  override def beforeAll(): Unit = {
    super.beforeAll()
    InMemoryV1Provider.clear()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    InMemoryV1Provider.clear()
  }

  test("append fallback") {
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")
    df.write.mode("append").option("name", "t1").format(v2Format).save()

    checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df)
    assert(InMemoryV1Provider.tables("t1").schema === df.schema.asNullable)
    assert(InMemoryV1Provider.tables("t1").partitioning.isEmpty)

    df.write.mode("append").option("name", "t1").format(v2Format).save()
    checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df.union(df))
  }

  test("overwrite by truncate fallback") {
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")
    df.write.mode("append").option("name", "t1").format(v2Format).save()

    val df2 = Seq((10, "k"), (20, "l"), (30, "m")).toDF("a", "b")
    df2.write.mode("overwrite").option("name", "t1").format(v2Format).save()
    checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df2)
  }

  SaveMode.values().foreach { mode =>
    test(s"save: new table creations with partitioning for table - mode: $mode") {
      val format = classOf[InMemoryV1Provider].getName
      val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")
      df.write.mode(mode).option("name", "t1").format(format).partitionBy("a").save()

      checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df)
      assert(InMemoryV1Provider.tables("t1").schema === df.schema.asNullable)
      assert(InMemoryV1Provider.tables("t1").partitioning.sameElements(
        Array(IdentityTransform(FieldReference(Seq("a"))))))
    }
  }

  test("save: default mode is ErrorIfExists") {
    val format = classOf[InMemoryV1Provider].getName
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")

    df.write.option("name", "t1").format(format).partitionBy("a").save()
    // default is ErrorIfExists, and since a table already exists we throw an exception
    val e = intercept[AnalysisException] {
      df.write.option("name", "t1").format(format).partitionBy("a").save()
    }
    checkErrorTableAlreadyExists(e, "`t1`")
  }

  test("save: Ignore mode") {
    val format = classOf[InMemoryV1Provider].getName
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")

    df.write.option("name", "t1").format(format).partitionBy("a").save()
    // no-op
    df.write.option("name", "t1").format(format).mode("ignore").partitionBy("a").save()

    checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df)
  }

  test("save: tables can perform schema and partitioning checks if they already exist") {
    val format = classOf[InMemoryV1Provider].getName
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")

    df.write.option("name", "t1").format(format).partitionBy("a").save()
    val e2 = intercept[IllegalArgumentException] {
      df.write.mode("append").option("name", "t1").format(format).partitionBy("b").save()
    }
    assert(e2.getMessage.contains("partitioning"))

    val e3 = intercept[IllegalArgumentException] {
      Seq((1, "x")).toDF("c", "d").write.mode("append").option("name", "t1").format(format)
        .save()
    }
    assert(e3.getMessage.contains("schema"))
  }

  test("SPARK-41437: fallback writes should only analyze/optimize plan once") {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    try {
      val session = SparkSession.builder()
        .master("local[1]")
        .withExtensions(_.injectPostHocResolutionRule(_ => OnlyOnceRule))
        .withExtensions(_.injectOptimizerRule(_ => OnlyOnceOptimizerRule))
        .config(OPTIMIZER_MAX_ITERATIONS.key, "1")
        .config(V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[V1FallbackTableCatalog].getName)
        .getOrCreate()
      val df = session.createDataFrame(Seq((1, "x"), (2, "y"), (3, "z")))
      df.write.mode("append").option("name", "t1").format(v2Format).saveAsTable("test")
      val df2 = session.createDataFrame(Seq((4, "a"), (5, "b"), (6, "c")))
      df2.writeTo("test").append()
    } finally {
      SparkSession.setActiveSession(spark)
      SparkSession.setDefaultSession(spark)
    }
  }

  test("SPARK-33492: append fallback should refresh cache") {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    try {
      val session = SparkSession.builder()
        .master("local[1]")
        .config(V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[V1FallbackTableCatalog].getName)
        .getOrCreate()
      val df = session.createDataFrame(Seq((1, "x")))
      df.write.mode("append").option("name", "t1").format(v2Format).saveAsTable("test")
      session.catalog.cacheTable("test")
      checkAnswer(session.read.table("test"), Row(1, "x") :: Nil)

      val df2 = session.createDataFrame(Seq((2, "y")))
      df2.writeTo("test").append()
      checkAnswer(session.read.table("test"), Row(1, "x") :: Row(2, "y") :: Nil)

    } finally {
      SparkSession.setActiveSession(spark)
      SparkSession.setDefaultSession(spark)
    }
  }

  test("SPARK-33492: overwrite fallback should refresh cache") {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    try {
      val session = SparkSession.builder()
        .master("local[1]")
        .config(V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[V1FallbackTableCatalog].getName)
        .getOrCreate()
      val df = session.createDataFrame(Seq((1, "x")))
      df.write.mode("append").option("name", "t1").format(v2Format).saveAsTable("test")
      session.catalog.cacheTable("test")
      checkAnswer(session.read.table("test"), Row(1, "x") :: Nil)

      val df2 = session.createDataFrame(Seq((2, "y")))
      df2.writeTo("test").overwrite(lit(true))
      checkAnswer(session.read.table("test"), Row(2, "y") :: Nil)

    } finally {
      SparkSession.setActiveSession(spark)
      SparkSession.setDefaultSession(spark)
    }
  }

  test("SPARK-50315: metrics for V1 fallback writers") {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    try {
      val session = SparkSession.builder()
        .master("local[1]")
        .config(V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[V1FallbackTableCatalog].getName)
        .getOrCreate()

      def captureWrite(sparkSession: SparkSession)(thunk: => Unit): SparkPlan = {
        val queryExecutions = withQueryExecutionsCaptured(sparkSession)(thunk)
        val v1FallbackWritePlans = queryExecutions.map(_.executedPlan).filter {
          case _: AppendDataExecV1 | _: OverwriteByExpressionExecV1 => true
          case _ => false
        }

        assert(v1FallbackWritePlans.size === 1)
        v1FallbackWritePlans.head
      }

      val appendPlan = captureWrite(session) {
        val df = session.createDataFrame(Seq((1, "x")))
        df.write.mode("append").option("name", "t1").format(v2Format).saveAsTable("test")
      }
      assert(appendPlan.metrics("numOutputRows").value === 1)

      val overwritePlan = captureWrite(session) {
        val df2 = session.createDataFrame(Seq((2, "y")))
        df2.writeTo("test").overwrite(lit(true))
      }
      assert(overwritePlan.metrics("numOutputRows").value === 1)
    } finally {
      SparkSession.setActiveSession(spark)
      SparkSession.setDefaultSession(spark)
    }
  }
}

class V1WriteFallbackSessionCatalogSuite
  extends InsertIntoTests(supportsDynamicOverwrite = false, includeSQLOnlyTests = true)
  with SessionCatalogTest[InMemoryTableWithV1Fallback, V1FallbackTableCatalog] {

  override protected val v2Format = classOf[InMemoryV1Provider].getName
  override protected val catalogClassName: String = classOf[V1FallbackTableCatalog].getName
  override protected val catalogAndNamespace: String = ""

  override protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(InMemoryV1Provider.getTableData(spark, s"default.$tableName"), expected)
  }

  protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      sql(s"INSERT $overwrite TABLE $tableName SELECT * FROM $tmpView")
    }
  }
}

class V1FallbackTableCatalog extends TestV2SessionCatalogBase[InMemoryTableWithV1Fallback] {
  override def newTable(
      name: String,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): InMemoryTableWithV1Fallback = {
    val t = new InMemoryTableWithV1Fallback(name, schema, partitions, properties)
    InMemoryV1Provider.tables.put(name, t)
    tables.put(Identifier.of(Array("default"), name), t)
    t
  }
}

private object InMemoryV1Provider {
  val tables: mutable.Map[String, InMemoryTableWithV1Fallback] = mutable.Map.empty

  def getTableData(spark: SparkSession, name: String): DataFrame = {
    val t = tables.getOrElse(name, throw new IllegalArgumentException(s"Table $name doesn't exist"))
    spark.createDataFrame(t.getData.asJava, t.schema)
  }

  def clear(): Unit = {
    tables.clear()
  }
}

class InMemoryV1Provider
  extends FakeV2ProviderWithCustomSchema
  with DataSourceRegister
  with CreatableRelationProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {

    InMemoryV1Provider.tables.getOrElse(options.get("name"), {
      new InMemoryTableWithV1Fallback(
        "InMemoryTableWithV1Fallback",
        new StructType(),
        Array.empty,
        options.asCaseSensitiveMap()
      )
    })
  }

  override def shortName(): String = "in-memory"

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val _sqlContext = sqlContext

    val partitioning = parameters.get(DataSourceUtils.PARTITIONING_COLUMNS_KEY).map { value =>
      DataSourceUtils.decodePartitioningColumns(value).map { partitioningColumn =>
        IdentityTransform(FieldReference(partitioningColumn))
      }
    }.getOrElse(Nil)

    val tableName = parameters("name")
    val tableOpt = InMemoryV1Provider.tables.get(tableName)
    val table = tableOpt.getOrElse(new InMemoryTableWithV1Fallback(
      "InMemoryTableWithV1Fallback",
      data.schema.asNullable,
      partitioning.toArray,
      Map.empty[String, String].asJava
    ))
    if (tableOpt.isEmpty) {
      InMemoryV1Provider.tables.put(tableName, table)
    } else {
      if (data.schema.asNullable != table.schema) {
        throw new IllegalArgumentException("Wrong schema provided")
      }
      if (!partitioning.sameElements(table.partitioning)) {
        throw new IllegalArgumentException("Wrong partitioning provided")
      }
    }

    def getRelation: BaseRelation = new BaseRelation {
      override def sqlContext: SQLContext = _sqlContext
      override def schema: StructType = table.schema
    }

    if (mode == SaveMode.ErrorIfExists && tableOpt.isDefined) {
      throw new TableAlreadyExistsException(quoteIdentifier(tableName))
    } else if (mode == SaveMode.Ignore && tableOpt.isDefined) {
      // do nothing
      return getRelation
    }
    val writer = table.newWriteBuilder(
      LogicalWriteInfoImpl(
        "", StructType(Seq.empty), new CaseInsensitiveStringMap(parameters.asJava)))
    if (mode == SaveMode.Overwrite) {
      writer.asInstanceOf[SupportsTruncate].truncate()
    }
    val write = writer.build()
    write.asInstanceOf[V1Write].toInsertableRelation.insert(data, overwrite = false)
    getRelation
  }
}

class InMemoryTableWithV1Fallback(
    override val name: String,
    override val schema: StructType,
    override val partitioning: Array[Transform],
    override val properties: java.util.Map[String, String])
  extends Table
  with SupportsWrite with SupportsRead {

  partitioning.foreach { t =>
    if (!t.isInstanceOf[IdentityTransform]) {
      throw new IllegalArgumentException(s"Transform $t must be IdentityTransform")
    }
  }

  override def capabilities: java.util.Set[TableCapability] = java.util.EnumSet.of(
    TableCapability.BATCH_READ,
    TableCapability.V1_BATCH_WRITE,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.TRUNCATE)

  @volatile private var dataMap: mutable.Map[Seq[Any], Seq[Row]] = mutable.Map.empty
  private val partFieldNames = partitioning.flatMap(_.references).toSeq.flatMap(_.fieldNames)
  private val partIndexes = partFieldNames.map(schema.fieldIndex(_))

  def getData: Seq[Row] = dataMap.values.flatten.toSeq

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new FallbackWriteBuilder(info.options)
  }

  private class FallbackWriteBuilder(options: CaseInsensitiveStringMap)
    extends WriteBuilder
    with SupportsTruncate
    with SupportsOverwrite {

    private var mode = "append"

    override def truncate(): WriteBuilder = {
      dataMap.clear()
      mode = "truncate"
      this
    }

    override def overwrite(filters: Array[Filter]): WriteBuilder = {
      val keys = InMemoryTable.filtersToKeys(dataMap.keys, partFieldNames, filters)
      dataMap --= keys
      mode = "overwrite"
      this
    }

    private def getPartitionValues(row: Row): Seq[Any] = {
      partIndexes.map(row.get)
    }

    override def build(): V1Write = new V1Write {
      case class SupportedV1WriteMetric(name: String, description: String) extends CustomSumMetric

      override def supportedCustomMetrics(): Array[CustomMetric] =
        Array(SupportedV1WriteMetric("numOutputRows", "Number of output rows"))

      private var writeMetrics = Array.empty[CustomTaskMetric]

      override def reportDriverMetrics(): Array[CustomTaskMetric] = writeMetrics

      override def toInsertableRelation: InsertableRelation = {
        (data: DataFrame, overwrite: Boolean) => {
          assert(!overwrite, "V1 write fallbacks cannot be called with overwrite=true")
          val rows = data.collect()

          case class V1WriteTaskMetric(name: String, value: Long) extends CustomTaskMetric
          writeMetrics = Array(V1WriteTaskMetric("numOutputRows", rows.length))

          rows.groupBy(getPartitionValues).foreach { case (partition, elements) =>
            if (dataMap.contains(partition) && mode == "append") {
              dataMap.put(partition, dataMap(partition) ++ elements)
            } else if (dataMap.contains(partition)) {
              throw new IllegalStateException("Partition was not removed properly")
            } else {
              dataMap.put(partition, elements.toImmutableArraySeq)
            }
          }
        }
      }
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new V1ReadFallbackScanBuilder(schema)

  private class V1ReadFallbackScanBuilder(schema: StructType) extends ScanBuilder {
    override def build(): Scan = new V1ReadFallbackScan(schema)
  }

  private class V1ReadFallbackScan(schema: StructType) extends V1Scan {
    override def readSchema(): StructType = schema
    override def toV1TableScan[T <: BaseRelation with TableScan](context: SQLContext): T =
      new V1TableScan(context, schema).asInstanceOf[T]
  }

  private class V1TableScan(
      context: SQLContext,
      requiredSchema: StructType) extends BaseRelation with TableScan {
    override def sqlContext: SQLContext = context
    override def schema: StructType = requiredSchema
    override def buildScan(): RDD[Row] = {
      val data = InMemoryV1Provider.getTableData(context.sparkSession, name).collect()
      context.sparkContext.makeRDD(data.toImmutableArraySeq)
    }
  }
}

/** A rule that fails if a query plan is analyzed twice. */
object OnlyOnceRule extends Rule[LogicalPlan] {
  private val tag = TreeNodeTag[String]("test")
  private val counts = new mutable.HashMap[LogicalPlan, Int]()

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.getTagValue(tag).isEmpty) {
      plan.setTagValue(tag, "abc")
      plan
    } else {
      val cnt = counts.getOrElseUpdate(plan, 0) + 1
      // This rule will be run as injectPostHocResolutionRule, and is supposed to be run only twice.
      // Once during planning and once during checkBatchIdempotence
      assert(cnt <= 1, "This rule shouldn't have been called again")
      counts.put(plan, cnt)
      plan
    }

  }
}

// A rule that fails if the input query of a V2WriteCommand is optimized twice
object OnlyOnceOptimizerRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case l: LocalRelation =>
        // The test inserts 3 rows with local data and sets OPTIMIZER_MAX_ITERATIONS to 1. This rule
        // is supposed to be run only once.
        assert(l.data.length >= 2, "Input query shouldn't be optimized again")
        l.copy(data = l.data.drop(1))
    }
  }
}
