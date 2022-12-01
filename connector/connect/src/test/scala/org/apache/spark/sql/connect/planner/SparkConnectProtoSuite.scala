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
package org.apache.spark.sql.connect.planner

import java.nio.file.{Files, Paths}

import com.google.protobuf.ByteString

import org.apache.spark.SparkClassNotFoundException
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Join.JoinType
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, PlanTest, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.dsl.MockRemoteSession
import org.apache.spark.sql.connect.dsl.commands._
import org.apache.spark.sql.connect.dsl.expressions._
import org.apache.spark.sql.connect.dsl.plans._
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, MapType, Metadata, StringType, StructField, StructType}

/**
 * This suite is based on connect DSL and test that given same dataframe operations, whether
 * connect could construct a proto plan that can be translated back, and after analyzed, be the
 * same as Spark dataframe's generated plan.
 */
class SparkConnectProtoSuite extends PlanTest with SparkConnectPlanTest {
  lazy val connect = new MockRemoteSession()

  lazy val connectTestRelation =
    createLocalRelationProto(
      Seq(AttributeReference("id", IntegerType)(), AttributeReference("name", StringType)()),
      Seq.empty)

  lazy val connectTestRelation2 =
    createLocalRelationProto(
      Seq(AttributeReference("id", IntegerType)(), AttributeReference("name", StringType)()),
      Seq.empty)

  lazy val connectTestRelationMap =
    createLocalRelationProto(
      Seq(AttributeReference("id", MapType(StringType, StringType))()),
      Seq.empty)

  lazy val sparkTestRelation: DataFrame =
    spark.createDataFrame(
      new java.util.ArrayList[Row](),
      StructType(Seq(StructField("id", IntegerType), StructField("name", StringType))))

  lazy val sparkTestRelation2: DataFrame =
    spark.createDataFrame(
      new java.util.ArrayList[Row](),
      StructType(Seq(StructField("id", IntegerType), StructField("name", StringType))))

  lazy val sparkTestRelationMap: DataFrame =
    spark.createDataFrame(
      new java.util.ArrayList[Row](),
      StructType(Seq(StructField("id", MapType(StringType, StringType)))))

  lazy val localRelation =
    createLocalRelationProto(Seq(AttributeReference("id", IntegerType)()), Seq.empty)

  test("Basic select") {
    val connectPlan = connectTestRelation.select("id".protoAttr)
    val sparkPlan = sparkTestRelation.select("id")
    comparePlans(connectPlan, sparkPlan)
  }

  test("Test select expression in strings") {
    val connectPlan = connectTestRelation.selectExpr("abs(id)", "name")
    val sparkPlan = sparkTestRelation.selectExpr("abs(id)", "name")
    comparePlans(connectPlan, sparkPlan)
  }

  test("UnresolvedFunction resolution.") {
    val connectPlan =
      connectTestRelation.select(callFunction(Seq("default", "hex"), Seq("id".protoAttr)))

    assertThrows[UnsupportedOperationException] {
      analyzePlan(transform(connectPlan))
    }

    val validPlan = connectTestRelation.select(callFunction(Seq("hex"), Seq("id".protoAttr)))
    assert(analyzePlan(transform(validPlan)) != null)
  }

  test("Basic filter") {
    val connectPlan = connectTestRelation.where("id".protoAttr < 0)
    val sparkPlan = sparkTestRelation.where(Column("id") < 0)
    comparePlans(connectPlan, sparkPlan)
  }

  test("Basic joins with different join types") {
    val connectPlan = connectTestRelation.join(connectTestRelation2)
    val sparkPlan = sparkTestRelation.join(sparkTestRelation2)
    comparePlans(connectPlan, sparkPlan)

    val connectPlan2 = connectTestRelation.join(connectTestRelation2)
    val sparkPlan2 = sparkTestRelation.join(sparkTestRelation2)
    comparePlans(connectPlan2, sparkPlan2)

    for ((t, y) <- Seq(
        (JoinType.JOIN_TYPE_LEFT_OUTER, LeftOuter),
        (JoinType.JOIN_TYPE_RIGHT_OUTER, RightOuter),
        (JoinType.JOIN_TYPE_FULL_OUTER, FullOuter),
        (JoinType.JOIN_TYPE_LEFT_ANTI, LeftAnti),
        (JoinType.JOIN_TYPE_LEFT_SEMI, LeftSemi),
        (JoinType.JOIN_TYPE_INNER, Inner))) {

      val connectPlan3 = connectTestRelation.join(connectTestRelation2, t, Seq("id"))
      val sparkPlan3 = sparkTestRelation.join(sparkTestRelation2, Seq("id"), y.toString)
      comparePlans(connectPlan3, sparkPlan3)
    }

    val connectPlan4 =
      connectTestRelation.join(connectTestRelation2, JoinType.JOIN_TYPE_INNER, Seq("name"))
    val sparkPlan4 = sparkTestRelation.join(sparkTestRelation2, Seq("name"), Inner.toString)
    comparePlans(connectPlan4, sparkPlan4)
  }

  test("Test sample") {
    val connectPlan = connectTestRelation.sample(0, 0.2, false, 1)
    val sparkPlan = sparkTestRelation.sample(false, 0.2 - 0, 1)
    comparePlans(connectPlan, sparkPlan)
  }

  test("Test sort") {
    val connectPlan = connectTestRelation.sort("id", "name")
    val sparkPlan = sparkTestRelation.sort("id", "name")
    comparePlans(connectPlan, sparkPlan)

    val connectPlan2 = connectTestRelation.sortWithinPartitions("id", "name")
    val sparkPlan2 = sparkTestRelation.sortWithinPartitions("id", "name")
    comparePlans(connectPlan2, sparkPlan2)
  }

  test("SPARK-41169: Test drop") {
    // single column
    val connectPlan = connectTestRelation.drop("id")
    val sparkPlan = sparkTestRelation.drop("id")
    comparePlans(connectPlan, sparkPlan)

    // all columns
    val connectPlan2 = connectTestRelation.drop("id", "name")
    val sparkPlan2 = sparkTestRelation.drop("id", "name")
    comparePlans(connectPlan2, sparkPlan2)

    // non-existing column
    val connectPlan3 = connectTestRelation.drop("id2", "name")
    val sparkPlan3 = sparkTestRelation.drop("id2", "name")
    comparePlans(connectPlan3, sparkPlan3)
  }

  test("SPARK-40809: column alias") {
    // Simple Test.
    val connectPlan = connectTestRelation.select("id".protoAttr.as("id2"))
    val sparkPlan = sparkTestRelation.select(Column("id").alias("id2"))
    comparePlans(connectPlan, sparkPlan)

    // Scalar columns with metadata
    val mdJson = "{\"max\": 99}"
    comparePlans(
      connectTestRelation.select("id".protoAttr.as("id2", mdJson)),
      sparkTestRelation.select(Column("id").as("id2", Metadata.fromJson(mdJson))))

    comparePlans(
      connectTestRelationMap.select(proto_explode("id".protoAttr).as(Seq("a", "b"))),
      sparkTestRelationMap.select(explode(Column("id")).as(Seq("a", "b"))))

    // Metadata must only be specified for regular Aliases.
    assertThrows[InvalidPlanInput] {
      val attr = proto_explode("id".protoAttr)
      val alias = proto.Expression.Alias
        .newBuilder()
        .setExpr(attr)
        .addName("a")
        .addName("b")
        .setMetadata(mdJson)
        .build()
      transform(
        connectTestRelationMap.select(proto.Expression.newBuilder().setAlias(alias).build()))
    }
  }

  test("Aggregate with more than 1 grouping expressions") {
    val connectPlan =
      connectTestRelation.groupBy("id".protoAttr, "name".protoAttr)()
    val sparkPlan =
      sparkTestRelation.groupBy(Column("id"), Column("name")).agg(Map.empty[String, String])
    comparePlans(connectPlan, sparkPlan)
  }

  test("Aggregate expressions") {
    val connectPlan =
      connectTestRelation.groupBy("id".protoAttr)(proto_min("name".protoAttr))
    val sparkPlan =
      sparkTestRelation.groupBy(Column("id")).agg(min(Column("name")))
    comparePlans(connectPlan, sparkPlan)

    val connectPlan2 =
      connectTestRelation.groupBy("id".protoAttr)(proto_min("name".protoAttr).as("agg1"))
    val sparkPlan2 =
      sparkTestRelation.groupBy(Column("id")).agg(min(Column("name")).as("agg1"))
    comparePlans(connectPlan2, sparkPlan2)
  }

  test("Test as(alias: String)") {
    val connectPlan = connectTestRelation.as("target_table")
    val sparkPlan = sparkTestRelation.as("target_table")
    comparePlans(connectPlan, sparkPlan)
  }

  test("Test StructType in LocalRelation") {
    val connectPlan = createLocalRelationProtoByAttributeReferences(
      Seq(AttributeReference("a", StructType(Seq(StructField("id", IntegerType))))()))
    val sparkPlan =
      LocalRelation(AttributeReference("a", StructType(Seq(StructField("id", IntegerType))))())
    comparePlans(connectPlan, sparkPlan)
  }

  test("Test limit offset") {
    val connectPlan = connectTestRelation.limit(10)
    val sparkPlan = sparkTestRelation.limit(10)
    comparePlans(connectPlan, sparkPlan)

    val connectPlan2 = connectTestRelation.offset(2)
    val sparkPlan2 = sparkTestRelation.offset(2)
    comparePlans(connectPlan2, sparkPlan2)

    val connectPlan3 = connectTestRelation.limit(10).offset(2)
    val sparkPlan3 = sparkTestRelation.limit(10).offset(2)
    comparePlans(connectPlan3, sparkPlan3)

    val connectPlan4 = connectTestRelation.offset(2).limit(10)
    val sparkPlan4 = sparkTestRelation.offset(2).limit(10)
    comparePlans(connectPlan4, sparkPlan4)
  }

  test("Test basic deduplicate") {
    val connectPlan = connectTestRelation.distinct()
    val sparkPlan = sparkTestRelation.distinct()
    comparePlans(connectPlan, sparkPlan)

    val connectPlan2 = connectTestRelation.deduplicate(Seq("id", "name"))
    val sparkPlan2 = sparkTestRelation.dropDuplicates(Seq("id", "name"))
    comparePlans(connectPlan2, sparkPlan2)
  }

  test("Test union, except, intersect") {
    val connectPlan1 = connectTestRelation.except(connectTestRelation, isAll = false)
    val sparkPlan1 = sparkTestRelation.except(sparkTestRelation)
    comparePlans(connectPlan1, sparkPlan1)

    val connectPlan2 = connectTestRelation.except(connectTestRelation, isAll = true)
    val sparkPlan2 = sparkTestRelation.exceptAll(sparkTestRelation)
    comparePlans(connectPlan2, sparkPlan2)

    val connectPlan3 = connectTestRelation.intersect(connectTestRelation, isAll = false)
    val sparkPlan3 = sparkTestRelation.intersect(sparkTestRelation)
    comparePlans(connectPlan3, sparkPlan3)

    val connectPlan4 = connectTestRelation.intersect(connectTestRelation, isAll = true)
    val sparkPlan4 = sparkTestRelation.intersectAll(sparkTestRelation)
    comparePlans(connectPlan4, sparkPlan4)

    val connectPlan5 = connectTestRelation.union(connectTestRelation, isAll = true)
    val sparkPlan5 = sparkTestRelation.union(sparkTestRelation)
    comparePlans(connectPlan5, sparkPlan5)

    val connectPlan6 = connectTestRelation.union(connectTestRelation, isAll = false)
    val sparkPlan6 = sparkTestRelation.union(sparkTestRelation).distinct()
    comparePlans(connectPlan6, sparkPlan6)

    val connectPlan7 =
      connectTestRelation.union(connectTestRelation2, isAll = true, byName = true)
    val sparkPlan7 = sparkTestRelation.unionByName(sparkTestRelation2)
    comparePlans(connectPlan7, sparkPlan7)

    val connectPlan8 =
      connectTestRelation.union(connectTestRelation2, isAll = false, byName = true)
    val sparkPlan8 = sparkTestRelation.unionByName(sparkTestRelation2).distinct()
    comparePlans(connectPlan8, sparkPlan8)
  }

  test("Test Range") {
    comparePlans(connect.range(None, 10, None, None), spark.range(10).toDF())
    comparePlans(connect.range(Some(2), 10, None, None), spark.range(2, 10).toDF())
    comparePlans(connect.range(Some(2), 10, Some(10), None), spark.range(2, 10, 10).toDF())
    comparePlans(
      connect.range(Some(2), 10, Some(10), Some(100)),
      spark.range(2, 10, 10, 100).toDF())
  }

  test("Test Session.sql") {
    comparePlans(connect.sql("SELECT 1"), spark.sql("SELECT 1"))
  }

  test("Test Repartition") {
    val connectPlan1 = connectTestRelation.repartition(12)
    val sparkPlan1 = sparkTestRelation.repartition(12)
    comparePlans(connectPlan1, sparkPlan1)

    val connectPlan2 = connectTestRelation.coalesce(2)
    val sparkPlan2 = sparkTestRelation.coalesce(2)
    comparePlans(connectPlan2, sparkPlan2)
  }

  test("SPARK-41128: Test fill na") {
    comparePlans(connectTestRelation.na.fillValue(1L), sparkTestRelation.na.fill(1L))
    comparePlans(connectTestRelation.na.fillValue(1.5), sparkTestRelation.na.fill(1.5))
    comparePlans(connectTestRelation.na.fillValue("str"), sparkTestRelation.na.fill("str"))
    comparePlans(
      connectTestRelation.na.fillColumns(1L, Seq("id")),
      sparkTestRelation.na.fill(1L, Seq("id")))
    comparePlans(
      connectTestRelation.na.fillValueMap(Map("id" -> 1L)),
      sparkTestRelation.na.fill(Map("id" -> 1L)))
    comparePlans(
      connectTestRelation.na.fillValueMap(Map("id" -> 1L, "name" -> "xyz")),
      sparkTestRelation.na.fill(Map("id" -> 1L, "name" -> "xyz")))
  }

  test("SPARK-41148: Test drop na") {
    comparePlans(connectTestRelation.na.drop(), sparkTestRelation.na.drop())
    comparePlans(
      connectTestRelation.na.drop(cols = Seq("id")),
      sparkTestRelation.na.drop(cols = Seq("id")))
    comparePlans(
      connectTestRelation.na.drop(how = Some("all")),
      sparkTestRelation.na.drop(how = "all"))
    comparePlans(
      connectTestRelation.na.drop(how = Some("all"), cols = Seq("id", "name")),
      sparkTestRelation.na.drop(how = "all", cols = Seq("id", "name")))
    comparePlans(
      connectTestRelation.na.drop(minNonNulls = Some(1)),
      sparkTestRelation.na.drop(minNonNulls = 1))
    comparePlans(
      connectTestRelation.na.drop(minNonNulls = Some(1), cols = Seq("id", "name")),
      sparkTestRelation.na.drop(minNonNulls = 1, cols = Seq("id", "name")))
  }

  test("SPARK-41315: Test replace") {
    comparePlans(
      connectTestRelation.na.replace(cols = Seq("id"), replacement = Map(1.0 -> 2.0)),
      sparkTestRelation.na.replace(cols = Seq("id"), replacement = Map(1.0 -> 2.0)))
    comparePlans(
      connectTestRelation.na.replace(cols = Seq("name"), replacement = Map("a" -> "b")),
      sparkTestRelation.na.replace(cols = Seq("name"), replacement = Map("a" -> "b")))
    comparePlans(
      connectTestRelation.na.replace(cols = Seq("*"), replacement = Map("a" -> "b")),
      sparkTestRelation.na.replace(col = "*", replacement = Map("a" -> "b")))
  }

  test("Test summary") {
    comparePlans(
      connectTestRelation.summary("count", "mean", "stddev"),
      sparkTestRelation.summary("count", "mean", "stddev"))
  }

  test("Test crosstab") {
    comparePlans(
      connectTestRelation.stat.crosstab("id", "name"),
      sparkTestRelation.stat.crosstab("id", "name"))
  }

  test("Test toDF") {
    comparePlans(connectTestRelation.toDF("col1", "col2"), sparkTestRelation.toDF("col1", "col2"))
  }

  test("Test withColumnsRenamed") {
    comparePlans(
      connectTestRelation.withColumnsRenamed(Map("id" -> "id1")),
      sparkTestRelation.withColumnsRenamed(Map("id" -> "id1")))
    comparePlans(
      connectTestRelation.withColumnsRenamed(Map("id" -> "id1", "name" -> "name1")),
      sparkTestRelation.withColumnsRenamed(Map("id" -> "id1", "name" -> "name1")))
    comparePlans(
      connectTestRelation.withColumnsRenamed(Map("id" -> "id1", "col1" -> "col2")),
      sparkTestRelation.withColumnsRenamed(Map("id" -> "id1", "col1" -> "col2")))
    comparePlans(
      connectTestRelation.withColumnsRenamed(Map("id" -> "id1", "id" -> "id2")),
      sparkTestRelation.withColumnsRenamed(Map("id" -> "id1", "id" -> "id2")))

    checkError(
      exception = intercept[AnalysisException] {
        transform(
          connectTestRelation.withColumnsRenamed(
            Map("id" -> "duplicatedCol", "name" -> "duplicatedCol")))
      },
      errorClass = "COLUMN_ALREADY_EXISTS",
      parameters = Map("columnName" -> "`duplicatedcol`"))
  }

  test("Writes fails without path or table") {
    assertThrows[UnsupportedOperationException] {
      transform(localRelation.write())
    }
  }

  test("Write fails with unknown table - AnalysisException") {
    val cmd = readRel.write(tableName = Some("dest"))
    assertThrows[AnalysisException] {
      transform(cmd)
    }
  }

  test("Write with partitions") {
    val cmd = localRelation.write(
      tableName = Some("testtable"),
      format = Some("parquet"),
      partitionByCols = Seq("noid"))
    assertThrows[AnalysisException] {
      transform(cmd)
    }
  }

  test("Write with invalid bucketBy configuration") {
    val cmd = localRelation.write(bucketByCols = Seq("id"), numBuckets = Some(0))
    assertThrows[InvalidCommandInput] {
      transform(cmd)
    }
  }

  test("Write to Path") {
    withTempDir { f =>
      val cmd = localRelation.write(
        format = Some("parquet"),
        path = Some(f.getPath),
        mode = Some("Overwrite"))
      transform(cmd)
      assert(Files.exists(Paths.get(f.getPath)), s"Output file must exist: ${f.getPath}")
    }
  }

  test("Write to Path with invalid input") {
    // Wrong data source.
    assertThrows[SparkClassNotFoundException](
      transform(
        localRelation.write(path = Some("/tmp/tmppath"), format = Some("ThisAintNoFormat"))))

    // Default data source not found.
    assertThrows[SparkClassNotFoundException](
      transform(localRelation.write(path = Some("/tmp/tmppath"))))
  }

  test("Write with sortBy") {
    // Sort by existing column.
    withTable("testtable") {
      transform(
        localRelation.write(
          tableName = Some("testtable"),
          format = Some("parquet"),
          sortByColumns = Seq("id"),
          bucketByCols = Seq("id"),
          numBuckets = Some(10)))
    }

    // Sort by non-existing column
    assertThrows[AnalysisException](
      transform(
        localRelation
          .write(
            tableName = Some("testtable"),
            format = Some("parquet"),
            sortByColumns = Seq("noid"),
            bucketByCols = Seq("id"),
            numBuckets = Some(10))))
  }

  test("Write to Table") {
    withTable("testtable") {
      val cmd = localRelation.write(format = Some("parquet"), tableName = Some("testtable"))
      transform(cmd)
      // Check that we can find and drop the table.
      spark.sql(s"select count(*) from testtable").collect()
    }
  }

  test("SaveMode conversion tests") {
    assertThrows[IllegalArgumentException](
      DataTypeProtoConverter.toSaveMode(proto.WriteOperation.SaveMode.SAVE_MODE_UNSPECIFIED))

    val combinations = Seq(
      (SaveMode.Append, proto.WriteOperation.SaveMode.SAVE_MODE_APPEND),
      (SaveMode.Ignore, proto.WriteOperation.SaveMode.SAVE_MODE_IGNORE),
      (SaveMode.Overwrite, proto.WriteOperation.SaveMode.SAVE_MODE_OVERWRITE),
      (SaveMode.ErrorIfExists, proto.WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS))
    combinations.foreach { a =>
      assert(DataTypeProtoConverter.toSaveModeProto(a._1) == a._2)
      assert(DataTypeProtoConverter.toSaveMode(a._2) == a._1)
    }
  }

  test("Test CreateView") {
    withView("view1", "view2", "view3", "view4") {
      transform(localRelation.createView("view1", global = true, replace = true))
      assert(spark.catalog.tableExists("global_temp.view1"))

      transform(localRelation.createView("view2", global = false, replace = true))
      assert(spark.catalog.tableExists("view2"))

      transform(localRelation.createView("view3", global = true, replace = false))
      assertThrows[AnalysisException] {
        transform(localRelation.createView("view3", global = true, replace = false))
      }

      transform(localRelation.createView("view4", global = false, replace = false))
      assertThrows[AnalysisException] {
        transform(localRelation.createView("view4", global = false, replace = false))
      }
    }
  }

  test("Project does not require an input") {
    comparePlans(select(1), spark.sql("SELECT 1"))
  }

  test("Test withColumns") {
    comparePlans(
      connectTestRelation.withColumns(Map("id" -> 1024, "col_not_exist" -> 2048)),
      sparkTestRelation.withColumns(Map("id" -> lit(1024), "col_not_exist" -> lit(2048))))
  }

  private def createLocalRelationProtoByAttributeReferences(
      attrs: Seq[AttributeReference]): proto.Relation = {
    val localRelationBuilder = proto.LocalRelation.newBuilder()

    val attributes = attrs.map(exp => AttributeReference(exp.name, exp.dataType)())
    val buffer = ArrowConverters
      .toBatchWithSchemaIterator(
        Iterator.empty,
        StructType.fromAttributes(attributes),
        Long.MaxValue,
        Long.MaxValue,
        null)
      .next()
    proto.Relation
      .newBuilder()
      .setLocalRelation(localRelationBuilder.setData(ByteString.copyFrom(buffer)).build())
      .build()
  }

  // This is a function for testing only. This is used when the plan is ready and it only waits
  // analyzer to analyze attribute references within the plan.
  private def analyzePlan(plan: LogicalPlan): LogicalPlan = {
    val connectAnalyzed = analysis.SimpleAnalyzer.execute(plan)
    analysis.SimpleAnalyzer.checkAnalysis(connectAnalyzed)
    connectAnalyzed
  }

  // Compares proto plan with DataFrame.
  private def comparePlans(connectPlan: proto.Relation, sparkPlan: DataFrame): Unit = {
    val connectAnalyzed = analyzePlan(transform(connectPlan))
    comparePlans(connectAnalyzed, sparkPlan.queryExecution.analyzed, false)
  }
}
