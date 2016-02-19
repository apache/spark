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

package org.apache.spark.sql.sources

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, PredicateHelper}
import org.apache.spark.sql.execution.{LogicalRDD, PhysicalRDD}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, execution}
import org.apache.spark.util.Utils

class SimpleTextHadoopFsRelationSuite extends HadoopFsRelationTest with PredicateHelper {
  import testImplicits._

  override val dataSourceName: String = classOf[SimpleTextSource].getCanonicalName

  // We have a very limited number of supported types at here since it is just for a
  // test relation and we do very basic testing at here.
  override protected def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: BinaryType => false
    // We are using random data generator and the generated strings are not really valid string.
    case _: StringType => false
    case _: BooleanType => false // see https://issues.apache.org/jira/browse/SPARK-10442
    case _: CalendarIntervalType => false
    case _: DateType => false
    case _: TimestampType => false
    case _: ArrayType => false
    case _: MapType => false
    case _: StructType => false
    case _: UserDefinedType[_] => false
    case _ => true
  }

  test("save()/load() - partitioned table - simple queries - partition columns in data") {
    withTempDir { file =>
      val basePath = new Path(file.getCanonicalPath)
      val fs = basePath.getFileSystem(SparkHadoopUtil.get.conf)
      val qualifiedBasePath = fs.makeQualified(basePath)

      for (p1 <- 1 to 2; p2 <- Seq("foo", "bar")) {
        val partitionDir = new Path(qualifiedBasePath, s"p1=$p1/p2=$p2")
        sparkContext
          .parallelize(for (i <- 1 to 3) yield s"$i,val_$i,$p1")
          .saveAsTextFile(partitionDir.toString)
      }

      val dataSchemaWithPartition =
        StructType(dataSchema.fields :+ StructField("p1", IntegerType, nullable = true))

      checkQueries(
        hiveContext.read.format(dataSourceName)
          .option("dataSchema", dataSchemaWithPartition.json)
          .load(file.getCanonicalPath))
    }
  }

  private var tempPath: File = _

  private var partitionedDF: DataFrame = _

  private val partitionedDataSchema: StructType =
    new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType)
      .add("c", StringType)

  protected override def beforeAll(): Unit = {
    this.tempPath = Utils.createTempDir()

    val df = sqlContext.range(10).select(
      'id cast IntegerType as 'a,
      ('id cast IntegerType) * 2 as 'b,
      concat(lit("val_"), 'id) as 'c
    )

    partitionedWriter(df).save(s"${tempPath.getCanonicalPath}/p=0")
    partitionedWriter(df).save(s"${tempPath.getCanonicalPath}/p=1")

    partitionedDF = partitionedReader.load(tempPath.getCanonicalPath)
  }

  override protected def afterAll(): Unit = {
    Utils.deleteRecursively(tempPath)
  }

  private def partitionedWriter(df: DataFrame) =
    df.write.option("dataSchema", partitionedDataSchema.json).format(dataSourceName)

  private def partitionedReader =
    sqlContext.read.option("dataSchema", partitionedDataSchema.json).format(dataSourceName)

  /**
   * Constructs test cases that test column pruning and filter push-down.
   *
   * For filter push-down, the following filters are not pushed-down.
   *
   * 1. Partitioning filters don't participate filter push-down, they are handled separately in
   *    `DataSourceStrategy`
   *
   * 2. Catalyst filter `Expression`s that cannot be converted to data source `Filter`s are not
   *    pushed down (e.g. UDF and filters referencing multiple columns).
   *
   * 3. Catalyst filter `Expression`s that can be converted to data source `Filter`s but cannot be
   *    handled by the underlying data source are not pushed down (e.g. returned from
   *    `BaseRelation.unhandledFilters()`).
   *
   *    Note that for [[SimpleTextRelation]], all data source [[Filter]]s other than [[GreaterThan]]
   *    are unhandled.  We made this assumption in [[SimpleTextRelation.unhandledFilters()]] only
   *    for testing purposes.
   *
   * @param projections Projection list of the query
   * @param filter Filter condition of the query
   * @param requiredColumns Expected names of required columns
   * @param pushedFilters Expected data source [[Filter]]s that are pushed down
   * @param inconvertibleFilters Expected Catalyst filter [[Expression]]s that cannot be converted
   *        to data source [[Filter]]s
   * @param unhandledFilters Expected Catalyst flter [[Expression]]s that can be converted to data
   *        source [[Filter]]s but cannot be handled by the data source relation
   * @param partitioningFilters Expected Catalyst filter [[Expression]]s that reference partition
   *        columns
   * @param expectedRawScanAnswer Expected query result of the raw table scan returned by the data
   *        source relation
   * @param expectedAnswer Expected query result of the full query
   */
  def testPruningAndFiltering(
      projections: Seq[Column],
      filter: Column,
      requiredColumns: Seq[String],
      pushedFilters: Seq[Filter],
      inconvertibleFilters: Seq[Column],
      unhandledFilters: Seq[Column],
      partitioningFilters: Seq[Column])(
      expectedRawScanAnswer: => Seq[Row])(
      expectedAnswer: => Seq[Row]): Unit = {
    test(s"pruning and filtering: df.select(${projections.mkString(", ")}).where($filter)") {
      val df = partitionedDF.where(filter).select(projections: _*)
      val queryExecution = df.queryExecution
      val executedPlan = queryExecution.executedPlan

      val rawScan = executedPlan.collect {
        case p: PhysicalRDD => p
      } match {
        case Seq(scan) => scan
        case _ => fail(s"More than one PhysicalRDD found\n$queryExecution")
      }

      markup("Checking raw scan answer")
      checkAnswer(
        DataFrame(sqlContext, LogicalRDD(rawScan.output, rawScan.rdd)(sqlContext)),
        expectedRawScanAnswer)

      markup("Checking full query answer")
      checkAnswer(df, expectedAnswer)

      markup("Checking required columns")
      assert(requiredColumns === SimpleTextRelation.requiredColumns)

      val nonPushedFilters = {
        val boundFilters = executedPlan.collect {
          case f: execution.Filter => f
        } match {
          case Nil => Nil
          case Seq(f) => splitConjunctivePredicates(f.condition)
          case _ => fail(s"More than one PhysicalRDD found\n$queryExecution")
        }

        // Unbound these bound filters so that we can easily compare them with expected results.
        boundFilters.map {
          _.transform { case a: AttributeReference => UnresolvedAttribute(a.name) }
        }.toSet
      }

      markup("Checking pushed filters")
      assert(SimpleTextRelation.pushedFilters === pushedFilters.toSet)

      val expectedInconvertibleFilters = inconvertibleFilters.map(_.expr).toSet
      val expectedUnhandledFilters = unhandledFilters.map(_.expr).toSet
      val expectedPartitioningFilters = partitioningFilters.map(_.expr).toSet

      markup("Checking unhandled and inconvertible filters")
      assert(expectedInconvertibleFilters ++ expectedUnhandledFilters === nonPushedFilters)

      markup("Checking partitioning filters")
      val actualPartitioningFilters = splitConjunctivePredicates(filter.expr).filter {
        _.references.contains(UnresolvedAttribute("p"))
      }.toSet

      // Partitioning filters are handled separately and don't participate filter push-down. So they
      // shouldn't be part of non-pushed filters.
      assert(expectedPartitioningFilters.intersect(nonPushedFilters).isEmpty)
      assert(expectedPartitioningFilters === actualPartitioningFilters)
    }
  }

  testPruningAndFiltering(
    projections = Seq('*),
    filter = 'p > 0,
    requiredColumns = Seq("a", "b", "c"),
    pushedFilters = Nil,
    inconvertibleFilters = Nil,
    unhandledFilters = Nil,
    partitioningFilters = Seq('p > 0)
  ) {
    Seq(
      Row(0, 0, "val_0", 1),
      Row(1, 2, "val_1", 1),
      Row(2, 4, "val_2", 1),
      Row(3, 6, "val_3", 1),
      Row(4, 8, "val_4", 1),
      Row(5, 10, "val_5", 1),
      Row(6, 12, "val_6", 1),
      Row(7, 14, "val_7", 1),
      Row(8, 16, "val_8", 1),
      Row(9, 18, "val_9", 1))
  } {
    Seq(
      Row(0, 0, "val_0", 1),
      Row(1, 2, "val_1", 1),
      Row(2, 4, "val_2", 1),
      Row(3, 6, "val_3", 1),
      Row(4, 8, "val_4", 1),
      Row(5, 10, "val_5", 1),
      Row(6, 12, "val_6", 1),
      Row(7, 14, "val_7", 1),
      Row(8, 16, "val_8", 1),
      Row(9, 18, "val_9", 1))
  }

  testPruningAndFiltering(
    projections = Seq('c, 'p),
    filter = 'a < 3 && 'p > 0,
    requiredColumns = Seq("c", "a"),
    pushedFilters = Seq(LessThan("a", 3)),
    inconvertibleFilters = Nil,
    unhandledFilters = Seq('a < 3),
    partitioningFilters = Seq('p > 0)
  ) {
    Seq(
      Row("val_0", 1, 0),
      Row("val_1", 1, 1),
      Row("val_2", 1, 2),
      Row("val_3", 1, 3),
      Row("val_4", 1, 4),
      Row("val_5", 1, 5),
      Row("val_6", 1, 6),
      Row("val_7", 1, 7),
      Row("val_8", 1, 8),
      Row("val_9", 1, 9))
  } {
    Seq(
      Row("val_0", 1),
      Row("val_1", 1),
      Row("val_2", 1))
  }

  testPruningAndFiltering(
    projections = Seq('*),
    filter = 'a > 8,
    requiredColumns = Seq("a", "b", "c"),
    pushedFilters = Seq(GreaterThan("a", 8)),
    inconvertibleFilters = Nil,
    unhandledFilters = Nil,
    partitioningFilters = Nil
  ) {
    Seq(
      Row(9, 18, "val_9", 0),
      Row(9, 18, "val_9", 1))
  } {
    Seq(
      Row(9, 18, "val_9", 0),
      Row(9, 18, "val_9", 1))
  }

  testPruningAndFiltering(
    projections = Seq('b, 'p),
    filter = 'a > 8,
    requiredColumns = Seq("b"),
    pushedFilters = Seq(GreaterThan("a", 8)),
    inconvertibleFilters = Nil,
    unhandledFilters = Nil,
    partitioningFilters = Nil
  ) {
    Seq(
      Row(18, 0),
      Row(18, 1))
  } {
    Seq(
      Row(18, 0),
      Row(18, 1))
  }

  testPruningAndFiltering(
    projections = Seq('b, 'p),
    filter = 'a > 8 && 'p > 0,
    requiredColumns = Seq("b"),
    pushedFilters = Seq(GreaterThan("a", 8)),
    inconvertibleFilters = Nil,
    unhandledFilters = Nil,
    partitioningFilters = Seq('p > 0)
  ) {
    Seq(
      Row(18, 1))
  } {
    Seq(
      Row(18, 1))
  }

  testPruningAndFiltering(
    projections = Seq('b, 'p),
    filter = 'c > "val_7" && 'b < 18 && 'p > 0,
    requiredColumns = Seq("b"),
    pushedFilters = Seq(GreaterThan("c", "val_7"), LessThan("b", 18)),
    inconvertibleFilters = Nil,
    unhandledFilters = Seq('b < 18),
    partitioningFilters = Seq('p > 0)
  ) {
    Seq(
      Row(16, 1),
      Row(18, 1))
  } {
    Seq(
      Row(16, 1))
  }

  testPruningAndFiltering(
    projections = Seq('b, 'p),
    filter = 'a % 2 === 0 && 'c > "val_7" && 'b < 18 && 'p > 0,
    requiredColumns = Seq("b", "a"),
    pushedFilters = Seq(GreaterThan("c", "val_7"), LessThan("b", 18)),
    inconvertibleFilters = Seq('a % 2 === 0),
    unhandledFilters = Seq('b < 18),
    partitioningFilters = Seq('p > 0)
  ) {
    Seq(
      Row(16, 1, 8),
      Row(18, 1, 9))
  } {
    Seq(
      Row(16, 1))
  }

  testPruningAndFiltering(
    projections = Seq('b, 'p),
    filter = 'a > 7 && 'a < 9,
    requiredColumns = Seq("b", "a"),
    pushedFilters = Seq(GreaterThan("a", 7), LessThan("a", 9)),
    inconvertibleFilters = Nil,
    unhandledFilters = Seq('a < 9),
    partitioningFilters = Nil
  ) {
    Seq(
      Row(16, 0, 8),
      Row(16, 1, 8),
      Row(18, 0, 9),
      Row(18, 1, 9))
  } {
    Seq(
      Row(16, 0),
      Row(16, 1))
  }
}
