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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.QueryTest.withQueryExecutionsCaptured
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, GetStructField, PlanExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join}
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryBaseTable, InMemoryCatalog, InMemoryTable}
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, BucketFunction, ScalarFunction, UnboundBucketFunction, UnboundFunction}
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.{NamedReference, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.Expressions.{bucket, identity}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, ScanBuilder, StorageFilterRequest, SupportsStorageFilters}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.internal.connector.SchemaAlignmentConfig
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PerPartitionRuntimeFilterSuite extends KeyGroupedPartitioningSuiteBase with PredicateHelper {

  // The in-memory scans are small, so the size threshold of the application side is lifted.
  override def sparkConf: SparkConf = super.sparkConf
    .set(V2_BUCKETING_ENABLED, true)
    .set(AUTO_BROADCASTJOIN_THRESHOLD, -1L)
    .set(RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD, 0L)
    .set(RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED, true)

  before {
    catalog.createFunction(Identifier.of(Array.empty, UnboundBucketFunction.name()),
      UnboundBucketFunction)
  }

  after {
    catalog.clearTables()
    catalog.clearFunctions()
  }

  private def createTables(
      idType: DataType = LongType,
      factPartitioning: Array[Transform] = Array(bucket(4, "id")),
      dimPartitioning: Array[Transform] = Array(bucket(4, "id")),
      numFactRows: Int = 16): Unit = {
    createTable("fact", Array(Column.create("id", idType), Column.create("v", IntegerType)),
      factPartitioning)
    sql("INSERT INTO testcat.ns.fact VALUES " +
      (0 until numFactRows).map(i => s"($i, ${i * 10})").mkString(", "))
    createTable("dim", Array(Column.create("id", idType), Column.create("flag", IntegerType)),
      dimPartitioning)
    sql("INSERT INTO testcat.ns.dim VALUES (1, 1), (2, 0), (3, 1), (5, 1), (6, 0), (9, 1)")
  }

  // Runs `f` with `fact` and `dim` in a `PartitionSourceFilteringCatalog`, and the join of the two.
  private def withPartitionSourceFilteringTables(f: (() => DataFrame) => Unit): Unit = {
    spark.conf.set("spark.sql.catalog.rfcat", classOf[PartitionSourceFilteringCatalog].getName)
    val rfcat = spark.sessionState.catalogManager.catalog("rfcat")
      .asInstanceOf[PartitionSourceFilteringCatalog]
    try {
      rfcat.createFunction(Identifier.of(Array.empty, UnboundBucketFunction.name()),
        UnboundBucketFunction)
      createTable("fact", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
        Array(bucket(4, "id")), catalog = rfcat)
      sql("INSERT INTO rfcat.ns.fact VALUES " +
        (0 until 16).map(i => s"($i, ${i * 10})").mkString(", "))
      createTable("dim", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
        Array(bucket(4, "id")), catalog = rfcat)
      sql("INSERT INTO rfcat.ns.dim VALUES (1, 1), (2, 0), (3, 1), (5, 1), (6, 0), (9, 1)")
      f(() => sql("SELECT f.id, f.v FROM rfcat.ns.fact f " +
        "JOIN rfcat.ns.dim d ON f.id = d.id WHERE d.flag = 1"))
    } finally {
      rfcat.clearTables()
      rfcat.clearFunctions()
      spark.conf.unset("spark.sql.catalog.rfcat")
    }
  }

  private def join(): DataFrame = sql(
    """
      |SELECT f.id, f.v
      |FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id
      |WHERE d.flag = 1
      |""".stripMargin)

  private val expectedRows = Seq(Row(1, 10), Row(3, 30), Row(5, 50), Row(9, 90))

  // The size of the creation side that a join on `id` gets from `table` with `flag = 1`.
  private def creationSideSize(table: String): BigInt =
    spark.table(s"testcat.ns.$table").where("flag = 1").select("id")
      .queryExecution.optimizedPlan.stats.sizeInBytes

  // A join on two keys, which gets two bloom filters of the same creation side. `MergeSubplans`
  // merges them into one subquery. Only `id` has a per-partition one.
  private def twoKeyJoin(): DataFrame = {
    createTable("fact", Array(Column.create("id", LongType), Column.create("x", IntegerType),
      Column.create("v", IntegerType)), Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.fact VALUES " +
      (0 until 16).map(i => s"($i, ${i % 3}, ${i * 10})").mkString(", "))
    createTable("dim", Array(Column.create("id", LongType), Column.create("x", IntegerType),
      Column.create("flag", IntegerType)), Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.dim VALUES (1, 1, 1), (2, 2, 0), (3, 0, 1), (5, 2, 1), (9, 0, 1)")
    sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d " +
      "ON f.id = d.id AND f.x = d.x WHERE d.flag = 1")
  }

  // The global bloom filters `plan` checks.
  private def executedGlobalBloomFilters(plan: SparkPlan): Seq[BloomFilterMightContain] =
    executedGlobalChecks(plan).flatMap(f => splitConjunctivePredicates(f.condition)).collect {
      case b: BloomFilterMightContain if isGlobalCheck(b) => b
    }

  // The columns the bloom filters check.
  private def columnsOf(blooms: Seq[BloomFilterMightContain]): Set[String] =
    blooms.flatMap(_.valueExpression.references.map(_.name)).toSet

  private def perPartitionSemiJoins(df: DataFrame): Seq[Join] =
    df.queryExecution.optimizedPlan.collect {
      case j @ Join(_, _, LeftSemi, Some(condition), _)
          if condition.exists(_.isInstanceOf[BloomFilterMightContain]) => j
    }

  // A bloom filter of the whole creation side, which a subquery computes, alone or merged with
  // others into one.
  private def isGlobalCheck(e: Expression): Boolean = e match {
    case BloomFilterMightContain(
        _: PlanExpression[_] | GetStructField(_: PlanExpression[_], _, _), _) => true
    case _ => false
  }

  private def hasGlobalBloomFilter(df: DataFrame): Boolean =
    df.queryExecution.optimizedPlan.exists {
      case Filter(condition, _) => condition.exists(isGlobalCheck)
      case _ => false
    }

  // `InjectRuntimeFilter` added only the bloom filter of the whole creation side.
  private def checkOnlyGlobalBloomFilter(df: DataFrame): Unit = {
    val plan = df.queryExecution.optimizedPlan
    assert(perPartitionSemiJoins(df).isEmpty, plan)
    assert(hasGlobalBloomFilter(df), plan)
    checkAnswer(df, expectedRows)
  }

  private def executedSemiJoins(plan: SparkPlan): Seq[ShuffledHashJoinExec] =
    collect(plan) { case j: ShuffledHashJoinExec if j.joinType == LeftSemi => j }

  private def executedGlobalChecks(plan: SparkPlan): Seq[FilterExec] =
    collect(plan) { case f: FilterExec if f.condition.exists(isGlobalCheck) => f }

  private def executedShuffles(plan: SparkPlan): Seq[ShuffleExchangeExec] =
    collect(plan) { case s: ShuffleExchangeExec => s }

  test("a per-partition bloom filter joins the application side next to the global bloom filter") {
    createTables()
    val df = join()
    assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
    assert(hasGlobalBloomFilter(df), df.queryExecution.optimizedPlan)
    checkAnswer(df, expectedRows)
  }

  Seq(true, false).foreach { adaptive =>
    test(s"the semi join stays and the bloom filter goes when it pairs (AQE $adaptive)") {
      createTables()
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = join()
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        assert(executedSemiJoins(plan).size == 1, plan)
        assert(executedGlobalChecks(plan).isEmpty, plan)
        assert(executedShuffles(plan).isEmpty,
          "the semi join pairs the partitions as the join does")
      }
    }

    test(s"the semi join goes and the bloom filter stays when it would shuffle (AQE $adaptive)") {
      // Without pushing the partition values, the two sides' different keys cannot pair.
      createTables()
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "false") {
        val df = join()
        assert(perPartitionSemiJoins(df).size == 1, "test setup: the rule added it")
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        assert(executedSemiJoins(plan).isEmpty, plan)
        assert(executedGlobalChecks(plan).size == 1, plan)
        assert(collect(plan) { case p @ ProjectExec(_, _: ProjectExec) => p }.isEmpty,
          s"the semi join's projection must go with it:\n$plan")
      }
    }

    test(s"the semi join replaces a merged subquery's bloom filter of its key (AQE $adaptive)") {
      // The join runs on the partition key alone, so it groups the splits the same way.
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.V2_BUCKETING_REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false") {
        val df = twoKeyJoin()
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        val blooms = executedGlobalBloomFilters(plan)
        assert(blooms.exists(_.bloomFilterExpression.isInstanceOf[GetStructField]),
          s"test setup: the bloom filters must come from one merged subquery:\n$plan")
        assert(columnsOf(blooms) == Set("x"), s"only the bloom filter of `x` may stay:\n$plan")
        assert(executedSemiJoins(plan).size == 1, plan)
        assert(executedShuffles(plan).isEmpty, s"test setup: the join must not shuffle:\n$plan")
      }
    }

    test(s"the semi join goes when the join shuffles the application side anyway (AQE $adaptive)") {
      // The join's keys are more than the partition key, so it shuffles both sides. The semi join
      // would group the application side's splits ahead of that shuffle.
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = twoKeyJoin()
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        assert(executedShuffles(plan).nonEmpty, s"test setup: the join must shuffle:\n$plan")
        assert(executedSemiJoins(plan).isEmpty, plan)
        assert(columnsOf(executedGlobalBloomFilters(plan)) == Set("id", "x"),
          s"both bloom filters must stay:\n$plan")
      }
    }

    test(s"with partially clustered distribution, the semi join keeps the application side's " +
        s"splits (AQE $adaptive)") {
      // The creation side's aggregate is the smaller side, so it is the one that repeats.
      createTables(numFactRows = 400)
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        val df = join()
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        val semiJoins = executedSemiJoins(plan)
        assert(semiJoins.size == 1, plan)
        assert(semiJoins.head.left.outputPartitioning.numPartitions > 4,
          s"the application side must keep its splits:\n$plan")
        assert(executedGlobalChecks(plan).isEmpty, plan)
      }
    }

    test(s"with partially clustered distribution, the semi join goes when it would group the " +
        s"splits the join keeps (AQE $adaptive)") {
      // The application side is the smaller side here, which a left semi join cannot repeat. So the
      // semi join would group the splits the join itself keeps apart. A limit, a top-N or a write
      // above the join reports one partition or none, however many tasks read below it.
      createTables()
      createTable("target", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
        Array.empty)
      withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "true") {
        Seq(join(), join().limit(100), join().orderBy("id").limit(100)).foreach { df =>
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(executedSemiJoins(plan).isEmpty, plan)
          assert(executedGlobalChecks(plan).size == 1, plan)
        }
        val write = withQueryExecutionsCaptured(spark) {
          sql("INSERT INTO testcat.ns.target SELECT f.id, f.v FROM testcat.ns.fact f " +
            "JOIN testcat.ns.dim d ON f.id = d.id WHERE d.flag = 1")
        }.map(_.executedPlan)
        checkAnswer(spark.table("testcat.ns.target"), expectedRows)
        assert(write.flatMap(executedSemiJoins).isEmpty, write)
        assert(write.flatMap(executedGlobalChecks).size == 1, write)
      }
    }

    test(s"the semi join follows the join key through a join below it (AQE $adaptive)") {
      // The application side is the storage-partitioned join of `fact` and `other`.
      createTables()
      createTable("other", Array(Column.create("id", LongType)), Array(bucket(4, "id")))
      sql("INSERT INTO testcat.ns.other VALUES " + (0 until 16).map(i => s"($i)").mkString(", "))
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql(
          """
            |SELECT f.id, f.v
            |FROM (SELECT fact.id, fact.v FROM testcat.ns.fact JOIN testcat.ns.other
            |      ON fact.id = other.id) f
            |JOIN testcat.ns.dim d ON f.id = d.id
            |WHERE d.flag = 1
            |""".stripMargin)
        assert(perPartitionSemiJoins(df).size == 1, df.queryExecution.optimizedPlan)
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        assert(executedSemiJoins(plan).size == 1, plan)
        assert(executedShuffles(plan).isEmpty, plan)
      }
    }

    test(s"the semi join also drops the bloom filter's copy among the runtime filters " +
        s"(AQE $adaptive)") {
      // A scan that offers the column of its bucket transform for runtime filtering, as Iceberg's
      // does, gets the global bloom filter as a runtime filter too. Dynamic partition pruning on
      // that column would take the bloom filter's place.
      withPartitionSourceFilteringTables { query =>
        // Whether one of the runtime filters of the scan of `fact` is a bloom filter.
        def bloomRuntimeFilter(plan: SparkPlan): Boolean = collectScans(plan).exists { scan =>
          scan.table.name().endsWith("fact") &&
            scan.runtimeFilters.exists(_.exists(_.isInstanceOf[BloomFilterMightContain]))
        }
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
          withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
            val df = query()
            checkAnswer(df, expectedRows)
            val plan = df.queryExecution.executedPlan
            assert(bloomRuntimeFilter(plan),
              s"test setup: the bloom filter must be a runtime filter:\n$plan")
          }
          val df = query()
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(executedSemiJoins(plan).size == 1, plan)
          assert(executedGlobalChecks(plan).isEmpty, plan)
          assert(!bloomRuntimeFilter(plan), s"the runtime filter copy must go too:\n$plan")
        }
      }
    }

    test(s"the semi join goes when the scan applies the global bloom filter (AQE $adaptive)") {
      // Without whole-stage codegen the scan gets no checked column. So the check above the scan
      // stays as it is, and the scan's readers may skip the filter.
      withPartitionSourceFilteringTables { query =>
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
            SQLConf.V2_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          val df = query()
          checkAnswer(df, expectedRows)
          val plan = df.queryExecution.executedPlan
          assert(collectScans(plan).exists(_.storageFilters.exists(isGlobalCheck)),
            s"test setup: the scan must take the global bloom filter:\n$plan")
          assert(executedSemiJoins(plan).isEmpty, plan)
          assert(executedGlobalChecks(plan).size == 1, plan)
        }
      }
    }

    test(s"the semi join replaces the bloom filter of a renamed join key (AQE $adaptive)") {
      // The bloom filter is pushed below the rename, so it checks `id` rather than `fid`.
      createTables()
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val df = sql("SELECT f.fid, f.v FROM (SELECT id AS fid, v FROM testcat.ns.fact) f " +
          "JOIN testcat.ns.dim d ON f.fid = d.id WHERE d.flag = 1")
        checkAnswer(df, expectedRows)
        val plan = df.queryExecution.executedPlan
        assert(executedSemiJoins(plan).size == 1, plan)
        assert(executedGlobalChecks(plan).isEmpty, plan)
      }
    }
  }

  test("a later join gets its bloom filter on a key whose semi join has no global one") {
    // The creation side of `dim` is too big for one bloom filter of all its keys, so its join gets
    // only the semi join. The join with `dim2` on the same key still gets its bloom filter, but no
    // second semi join.
    createTables()
    createTable("dim2", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.dim2 VALUES (1, 1), (3, 1)")
    val size = creationSideSize("dim")
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> (size / 2).toString) {
      val df = sql("SELECT f.id, f.v FROM testcat.ns.fact f JOIN testcat.ns.dim d ON f.id = d.id " +
        "JOIN testcat.ns.dim2 d2 ON f.id = d2.id WHERE d.flag = 1 AND d2.flag = 1")
      val plan = df.queryExecution.optimizedPlan
      assert(perPartitionSemiJoins(df).size == 1, plan)
      assert(hasGlobalBloomFilter(df), plan)
      checkAnswer(df, Seq(Row(1, 10), Row(3, 30)))
    }
  }

  test("the right side still gets its bloom filter when the left side gets only a semi join") {
    // The creation side of `l` is too big for one bloom filter of all its keys, so `l` gets only
    // the semi join. `rt` still gets the bloom filter of `l`.
    createTable("l", Array(Column.create("id", LongType), Column.create("v", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.l VALUES " +
      (0 until 8).map(i => s"($i, ${i * 10})").mkString(", "))
    createTable("rt", Array(Column.create("id", LongType), Column.create("flag", IntegerType)),
      Array(bucket(4, "id")))
    sql("INSERT INTO testcat.ns.rt VALUES " +
      (0 until 64).map(i => s"($i, ${i % 2})").mkString(", "))
    val size = creationSideSize("rt")
    withSQLConf(RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> (size / 2).toString) {
      val df = sql("SELECT l.id, l.v FROM testcat.ns.l l JOIN testcat.ns.rt rt ON l.id = rt.id " +
        "WHERE l.v < 30 AND rt.flag = 1")
      val plan = df.queryExecution.optimizedPlan
      assert(perPartitionSemiJoins(df).nonEmpty, plan)
      assert(hasGlobalBloomFilter(df), plan)
      checkAnswer(df, Seq(Row(1, 10)))
    }
  }

  test("no per-partition bloom filter when it is turned off") {
    createTables()
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_PER_PARTITION_ENABLED.key -> "false") {
      checkOnlyGlobalBloomFilter(join())
    }
  }

  test("no per-partition bloom filter for an identity partitioning") {
    // Each partition would hold a single key, so its bloom filter would check nothing. Dynamic
    // partition pruning on `id` would keep the rule from adding any bloom filter.
    createTables(factPartitioning = Array(identity("id")), dimPartitioning = Array(identity("id")))
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
      checkOnlyGlobalBloomFilter(join())
    }
  }

  test("no per-partition bloom filter for different transforms") {
    createTables(dimPartitioning = Array(bucket(8, "id")))
    checkOnlyGlobalBloomFilter(join())
  }

  test("no per-partition bloom filter for a non-deterministic transform") {
    // Both sides of the semi join evaluate the transform, so they could disagree on a row's key.
    catalog.clearFunctions()
    catalog.createFunction(Identifier.of(Array.empty, "bucket"), new UnboundFunction {
      override def bind(inputType: StructType): BoundFunction = new ScalarFunction[Int] {
        override def inputTypes(): Array[DataType] = BucketFunction.inputTypes()
        override def resultType(): DataType = BucketFunction.resultType()
        override def name(): String = BucketFunction.name()
        override def canonicalName(): String = BucketFunction.canonicalName()
        override def isDeterministic(): Boolean = false
        override def produceResult(input: InternalRow): Int = BucketFunction.produceResult(input)
      }
      override def description(): String = name()
      override def name(): String = "bucket"
    })
    createTables()
    checkOnlyGlobalBloomFilter(join())
  }

  test("no per-partition bloom filter when the transform's input types differ from the column") {
    // The test catalog's `bucket` takes a BIGINT value, so over an INT column its call fails.
    createTables(idType = IntegerType)
    checkOnlyGlobalBloomFilter(join())
  }
}

/**
 * A catalog whose scans offer the source column of every partition transform for runtime
 * filtering, as Iceberg's do, rather than only the identity ones. They also take a storage filter
 * on those columns, which their readers skip.
 */
class PartitionSourceFilteringCatalog extends InMemoryCatalog {
  // scalastyle:off argcount
  override protected def newInMemoryTable(
      name: String,
      columns: Array[Column],
      transforms: Array[Transform],
      properties: java.util.Map[String, String],
      constraints: Array[Constraint],
      distribution: Distribution,
      ordering: Array[SortOrder],
      requiredNumPartitions: Option[Int],
      advisoryPartitionSize: Option[Long],
      distributionStrictlyRequired: Boolean,
      numRowsPerSplit: Int,
      id: String,
      schemaAlignmentConfig: SchemaAlignmentConfig): InMemoryBaseTable = {
    // scalastyle:on argcount
    new InMemoryTable(name, columns, transforms, properties, constraints, distribution,
      ordering, requiredNumPartitions, advisoryPartitionSize, distributionStrictlyRequired,
      numRowsPerSplit, id, schemaAlignmentConfig) {
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
        new InMemoryScanBuilder(schema, options) {
          override protected def createScan(
              partitions: Seq[InputPartition],
              scanSchema: StructType,
              fullSchema: StructType,
              scanOptions: CaseInsensitiveStringMap): BatchScanBaseClass =
            new InMemoryBatchScan(partitions, scanSchema, fullSchema, scanOptions)
                with SupportsStorageFilters {
              override def filterAttributes(): Array[NamedReference] =
                transforms.flatMap(_.references).filter(ref => scanSchema.fieldNames.contains(
                  ref.fieldNames.head))
              override def storageFilterAttributes(): Array[NamedReference] = filterAttributes()
              override def createReaderFactory(): PartitionReaderFactory =
                StorageFilterSkippingReaderFactory(super.createReaderFactory())
            }
        }
    }
  }
}

/**
 * Reads as `factory` does, without applying a storage filter. A reader may skip it when Spark asks
 * for no checked column.
 */
case class StorageFilterSkippingReaderFactory(factory: PartitionReaderFactory)
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    factory.createReader(partition)

  override def createReader(
      partition: InputPartition,
      request: StorageFilterRequest): PartitionReader[InternalRow] = {
    assert(!request.appendsCheckedColumn())
    factory.createReader(partition)
  }
}
