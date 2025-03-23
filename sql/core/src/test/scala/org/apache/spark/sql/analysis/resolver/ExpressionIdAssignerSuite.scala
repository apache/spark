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

package org.apache.spark.sql.analysis.resolver

import java.util.IdentityHashMap

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.resolver.{ExpressionIdAssigner, Resolver}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class ExpressionIdAssignerSuite extends QueryTest with SharedSparkSession {
  private val col1Integer = AttributeReference(name = "col1", dataType = IntegerType)()
  private val col1IntegerAlias = Alias(col1Integer, "a")()
  private val col2Integer = AttributeReference(name = "col2", dataType = IntegerType)()
  private val col2IntegerAlias = Alias(col2Integer, "b")()
  private val col3Integer = AttributeReference(name = "col3", dataType = IntegerType)()

  private val CONSTRAINTS_VALIDATED = TreeNodeTag[Boolean]("constraints_validated")

  test("Mapping is not created") {
    val assigner = new ExpressionIdAssigner

    intercept[SparkException] {
      assigner.mapExpression(col1Integer)
    }

    assigner.withNewMapping() {
      assigner.withNewMapping() {
        intercept[SparkException] {
          assigner.mapExpression(col1Integer)
        }
      }
    }
  }

  test("Mapping is created twice") {
    val assigner = new ExpressionIdAssigner

    intercept[SparkException] {
      assigner.createMapping()
      assigner.createMapping()
    }

    assigner.withNewMapping() {
      assigner.createMapping()

      assigner.withNewMapping() {
        assigner.createMapping()

        intercept[SparkException] {
          assigner.createMapping()
        }
      }

      intercept[SparkException] {
        assigner.createMapping()
      }
    }
  }

  test("Create mapping with new output and old output with different length") {
    val assigner = new ExpressionIdAssigner

    intercept[SparkException] {
      assigner.createMapping(
        newOutput = Seq(col1Integer.newInstance()),
        oldOutput = Some(Seq(col1Integer, col2Integer))
      )
    }
  }

  test("Left branch: Single AttributeReference") {
    val assigner = new ExpressionIdAssigner

    assigner.createMapping()

    val col1IntegerMapped = assigner.mapExpression(col1Integer)
    assert(col1IntegerMapped.isInstanceOf[AttributeReference])
    assert(col1IntegerMapped.exprId != col1Integer.exprId)

    val col1IntegerReferenced = assigner.mapExpression(col1Integer)
    assert(col1IntegerReferenced.isInstanceOf[AttributeReference])
    assert(col1IntegerReferenced.exprId == col1IntegerMapped.exprId)

    val col1IntegerMappedReferenced = assigner.mapExpression(col1IntegerMapped)
    assert(col1IntegerMappedReferenced.isInstanceOf[AttributeReference])
    assert(col1IntegerMappedReferenced.exprId == col1IntegerMapped.exprId)
  }

  test("Right branch: Single AttributeReference") {
    val assigner = new ExpressionIdAssigner
    assigner.withNewMapping() {
      assigner.createMapping()

      val col1IntegerMapped = assigner.mapExpression(col1Integer)
      assert(col1IntegerMapped.isInstanceOf[AttributeReference])
      assert(col1IntegerMapped.exprId != col1Integer.exprId)

      val col1IntegerReferenced = assigner.mapExpression(col1Integer)
      assert(col1IntegerReferenced.isInstanceOf[AttributeReference])
      assert(col1IntegerReferenced.exprId == col1IntegerMapped.exprId)

      val col1IntegerMappedReferenced = assigner.mapExpression(col1IntegerMapped)
      assert(col1IntegerMappedReferenced.isInstanceOf[AttributeReference])
      assert(col1IntegerMappedReferenced.exprId == col1IntegerMapped.exprId)
    }
  }

  test("Left branch: Single Alias") {
    val assigner = new ExpressionIdAssigner

    assigner.createMapping()

    val col1IntegerAliasMapped = assigner.mapExpression(col1IntegerAlias)
    assert(col1IntegerAliasMapped.isInstanceOf[Alias])
    assert(col1IntegerAliasMapped.exprId == col1IntegerAlias.exprId)

    val col1IntegerAliasReferenced = assigner.mapExpression(col1IntegerAlias.toAttribute)
    assert(col1IntegerAliasReferenced.isInstanceOf[AttributeReference])
    assert(col1IntegerAliasReferenced.exprId == col1IntegerAliasMapped.exprId)

    val col1IntegerAliasMappedReferenced =
      assigner.mapExpression(col1IntegerAliasMapped.toAttribute)
    assert(col1IntegerAliasMappedReferenced.isInstanceOf[AttributeReference])
    assert(col1IntegerAliasMappedReferenced.exprId == col1IntegerAliasMapped.exprId)

    val col1IntegerAliasMappedAgain = assigner.mapExpression(col1IntegerAlias)
    assert(col1IntegerAliasMappedAgain.isInstanceOf[Alias])
    assert(col1IntegerAliasMappedAgain.exprId != col1IntegerAlias.exprId)
    assert(col1IntegerAliasMappedAgain.exprId != col1IntegerAliasMapped.exprId)
  }

  test("Right branch: Single Alias") {
    val assigner = new ExpressionIdAssigner
    assigner.withNewMapping() {
      assigner.createMapping()

      val col1IntegerAliasMapped = assigner.mapExpression(col1IntegerAlias)
      assert(col1IntegerAliasMapped.isInstanceOf[Alias])
      assert(col1IntegerAliasMapped.exprId != col1IntegerAlias.exprId)

      val col1IntegerAliasReferenced = assigner.mapExpression(col1IntegerAlias.toAttribute)
      assert(col1IntegerAliasReferenced.isInstanceOf[AttributeReference])
      assert(col1IntegerAliasReferenced.exprId == col1IntegerAliasMapped.exprId)

      val col1IntegerAliasMappedReferenced =
        assigner.mapExpression(col1IntegerAliasMapped.toAttribute)
      assert(col1IntegerAliasMappedReferenced.isInstanceOf[AttributeReference])
      assert(col1IntegerAliasMappedReferenced.exprId == col1IntegerAliasMapped.exprId)

      val col1IntegerAliasMappedAgain = assigner.mapExpression(col1IntegerAlias)
      assert(col1IntegerAliasMappedAgain.isInstanceOf[Alias])
      assert(col1IntegerAliasMappedAgain.exprId != col1IntegerAlias.exprId)
      assert(col1IntegerAliasMappedAgain.exprId != col1IntegerAliasMapped.exprId)
    }
  }

  test("Left branch: Create mapping with new output") {
    val assigner = new ExpressionIdAssigner

    assigner.createMapping(newOutput = Seq(col1Integer, col2Integer))

    val col1IntegerReferenced = assigner.mapExpression(col1Integer)
    assert(col1IntegerReferenced.isInstanceOf[AttributeReference])
    assert(col1IntegerReferenced.exprId == col1Integer.exprId)

    val col2IntegerReferenced = assigner.mapExpression(col2Integer)
    assert(col2IntegerReferenced.isInstanceOf[AttributeReference])
    assert(col2IntegerReferenced.exprId == col2Integer.exprId)

    val col2IntegerAliasMapped = assigner.mapExpression(col2IntegerAlias)
    assert(col2IntegerAliasMapped.isInstanceOf[Alias])
    assert(col2IntegerAliasMapped.exprId == col2IntegerAlias.exprId)
    assert(col2IntegerAliasMapped.exprId != col2Integer.exprId)

    val col3IntegerMapped = assigner.mapExpression(col3Integer)
    assert(col3IntegerMapped.isInstanceOf[AttributeReference])
    assert(col3IntegerMapped.exprId != col3Integer.exprId)
  }

  test("Right branch: Create mapping with new output") {
    val assigner = new ExpressionIdAssigner
    assigner.withNewMapping() {
      assigner.createMapping(newOutput = Seq(col1Integer, col2Integer))

      val col1IntegerReferenced = assigner.mapExpression(col1Integer)
      assert(col1IntegerReferenced.isInstanceOf[AttributeReference])
      assert(col1IntegerReferenced.exprId == col1Integer.exprId)

      val col2IntegerReferenced = assigner.mapExpression(col2Integer)
      assert(col2IntegerReferenced.isInstanceOf[AttributeReference])
      assert(col2IntegerReferenced.exprId == col2Integer.exprId)

      val col2IntegerAliasMapped = assigner.mapExpression(col2IntegerAlias)
      assert(col2IntegerAliasMapped.isInstanceOf[Alias])
      assert(col2IntegerAliasMapped.exprId != col2IntegerAlias.exprId)
      assert(col2IntegerAliasMapped.exprId != col2Integer.exprId)

      val col3IntegerMapped = assigner.mapExpression(col3Integer)
      assert(col3IntegerMapped.isInstanceOf[AttributeReference])
      assert(col3IntegerMapped.exprId != col3Integer.exprId)
    }
  }

  test("Left branch: Create mapping with new output and old output") {
    val assigner = new ExpressionIdAssigner

    val col1IntegerNew = col1Integer.newInstance()
    assert(col1IntegerNew.exprId != col1Integer.exprId)

    val col2IntegerNew = col2Integer.newInstance()
    assert(col2IntegerNew.exprId != col2Integer.exprId)

    assigner.createMapping(
      newOutput = Seq(col1IntegerNew, col2IntegerNew),
      oldOutput = Some(Seq(col1Integer, col2Integer))
    )

    val col1IntegerReferenced = assigner.mapExpression(col1Integer)
    assert(col1IntegerReferenced.isInstanceOf[AttributeReference])
    assert(col1IntegerReferenced.exprId == col1IntegerNew.exprId)

    val col1IntegerNewReferenced = assigner.mapExpression(col1IntegerNew)
    assert(col1IntegerNewReferenced.isInstanceOf[AttributeReference])
    assert(col1IntegerNewReferenced.exprId == col1IntegerNew.exprId)

    val col2IntegerReferenced = assigner.mapExpression(col2Integer)
    assert(col2IntegerReferenced.isInstanceOf[AttributeReference])
    assert(col2IntegerReferenced.exprId == col2IntegerNew.exprId)

    val col2IntegerNewReferenced = assigner.mapExpression(col2IntegerNew)
    assert(col2IntegerNewReferenced.isInstanceOf[AttributeReference])
    assert(col2IntegerNewReferenced.exprId == col2IntegerNew.exprId)

    val col2IntegerAliasMapped = assigner.mapExpression(col2IntegerAlias)
    assert(col2IntegerAliasMapped.isInstanceOf[Alias])
    assert(col2IntegerAliasMapped.exprId == col2IntegerAlias.exprId)
    assert(col2IntegerAliasMapped.exprId != col2Integer.exprId)
    assert(col2IntegerAliasMapped.exprId != col2IntegerNew.exprId)

    val col3IntegerMapped = assigner.mapExpression(col3Integer)
    assert(col3IntegerMapped.isInstanceOf[AttributeReference])
    assert(col3IntegerMapped.exprId != col3Integer.exprId)
  }

  test("Right branch: Create mapping with new output and old output") {
    val assigner = new ExpressionIdAssigner
    assigner.withNewMapping() {
      val col1IntegerNew = col1Integer.newInstance()
      assert(col1IntegerNew.exprId != col1Integer.exprId)

      val col2IntegerNew = col2Integer.newInstance()
      assert(col2IntegerNew.exprId != col2Integer.exprId)

      assigner.createMapping(
        newOutput = Seq(col1IntegerNew, col2IntegerNew),
        oldOutput = Some(Seq(col1Integer, col2Integer))
      )

      val col1IntegerReferenced = assigner.mapExpression(col1Integer)
      assert(col1IntegerReferenced.isInstanceOf[AttributeReference])
      assert(col1IntegerReferenced.exprId == col1IntegerNew.exprId)

      val col1IntegerNewReferenced = assigner.mapExpression(col1IntegerNew)
      assert(col1IntegerNewReferenced.isInstanceOf[AttributeReference])
      assert(col1IntegerNewReferenced.exprId == col1IntegerNew.exprId)

      val col2IntegerReferenced = assigner.mapExpression(col2Integer)
      assert(col2IntegerReferenced.isInstanceOf[AttributeReference])
      assert(col2IntegerReferenced.exprId == col2IntegerNew.exprId)

      val col2IntegerNewReferenced = assigner.mapExpression(col2IntegerNew)
      assert(col2IntegerNewReferenced.isInstanceOf[AttributeReference])
      assert(col2IntegerNewReferenced.exprId == col2IntegerNew.exprId)

      val col2IntegerAliasMapped = assigner.mapExpression(col2IntegerAlias)
      assert(col2IntegerAliasMapped.isInstanceOf[Alias])
      assert(col2IntegerAliasMapped.exprId != col2IntegerAlias.exprId)
      assert(col2IntegerAliasMapped.exprId != col2Integer.exprId)
      assert(col2IntegerAliasMapped.exprId != col2IntegerNew.exprId)

      val col3IntegerMapped = assigner.mapExpression(col3Integer)
      assert(col3IntegerMapped.isInstanceOf[AttributeReference])
      assert(col3IntegerMapped.exprId != col3Integer.exprId)
    }
  }

  test("Several layers") {
    val assigner = new ExpressionIdAssigner
    val literalAlias1 = Alias(Literal(1), "a")()
    val literalAlias2 = Alias(Literal(2), "b")()

    val output1 = assigner.withNewMapping() {
      val output1 = assigner.withNewMapping() {
        assigner.createMapping()

        Seq(
          assigner.mapExpression(col1Integer).toAttribute,
          assigner.mapExpression(col2Integer).toAttribute
        )
      }

      val output2 = assigner.withNewMapping() {
        val col1IntegerNew = col1Integer.newInstance()
        val col2IntegerNew = col2Integer.newInstance()

        assigner.createMapping(newOutput = Seq(col1IntegerNew, col2IntegerNew))

        Seq(
          assigner.mapExpression(col1IntegerNew).toAttribute,
          assigner.mapExpression(col2IntegerNew).toAttribute
        )
      }

      val output3 = assigner.withNewMapping() {
        val col1IntegerNew = col1Integer.newInstance()
        val col2IntegerNew = col2Integer.newInstance()

        assigner.createMapping(
          newOutput = Seq(col1IntegerNew, col2IntegerNew),
          oldOutput = Some(Seq(col1Integer, col2Integer))
        )

        Seq(
          assigner.mapExpression(col1Integer).toAttribute,
          assigner.mapExpression(col2Integer).toAttribute
        )
      }

      output1.zip(output2).zip(output3).zip(Seq(col1Integer, col2Integer)).foreach {
        case (((attribute1, attribute2), attribute3), originalAttribute) =>
          assert(attribute1.exprId != originalAttribute.exprId)
          assert(attribute2.exprId != originalAttribute.exprId)
          assert(attribute3.exprId != originalAttribute.exprId)
          assert(attribute1.exprId != attribute2.exprId)
          assert(attribute1.exprId != attribute3.exprId)
          assert(attribute2.exprId != attribute3.exprId)
      }

      assigner.createMapping(newOutput = output2)

      val literalAlias1Remapped = assigner.mapExpression(literalAlias1)
      assert(literalAlias1Remapped.isInstanceOf[Alias])
      assert(literalAlias1Remapped.exprId != literalAlias1.exprId)

      val literalAlias2Remapped = assigner.mapExpression(literalAlias2)
      assert(literalAlias2Remapped.isInstanceOf[Alias])
      assert(literalAlias2Remapped.exprId != literalAlias2.exprId)

      Seq(literalAlias1Remapped.toAttribute, literalAlias2Remapped.toAttribute) ++ output2
    }

    val output2 = assigner.withNewMapping() {
      assigner.createMapping()

      val literalAlias1Remapped = assigner.mapExpression(literalAlias1)
      assert(literalAlias1Remapped.isInstanceOf[Alias])
      assert(literalAlias1Remapped.exprId != literalAlias1.exprId)

      val literalAlias2Remapped = assigner.mapExpression(literalAlias2)
      assert(literalAlias2Remapped.isInstanceOf[Alias])
      assert(literalAlias2Remapped.exprId != literalAlias2.exprId)

      Seq(literalAlias1Remapped.toAttribute, literalAlias2Remapped.toAttribute)
    }

    output1.zip(output2).foreach {
      case (aliasReference1, aliasReference2) =>
        assert(aliasReference1.exprId != aliasReference2.exprId)
    }

    assigner.createMapping(newOutput = output1)

    val aliasReferences = output1.map { aliasReference =>
      assigner.mapExpression(aliasReference)
    }

    aliasReferences.zip(output1).zip(output2).foreach {
      case ((aliasReference, aliasReference1), aliasReference2) =>
        assert(aliasReference.exprId == aliasReference1.exprId)
        assert(aliasReference.exprId != aliasReference2.exprId)
    }

    aliasReferences.map(_.toAttribute)
  }

  test("Simple select") {
    checkExpressionIdAssignment(
      spark
        .sql("""
        SELECT
          col1, 1 AS a, col1, 1 AS a, col2, 2 AS b, col3, 3 AS c
        FROM
          VALUES (1, 2, 3)
        """)
        .queryExecution
        .analyzed
    )
  }

  test("Simple select, aliases referenced") {
    checkExpressionIdAssignment(
      spark
        .sql("""
        SELECT
          col3, c, col2, b, col1, a, col1, a
        FROM (
          SELECT
            col1, 1 AS a, col1, col2, 2 AS b, col3, 3 AS c
          FROM
            VALUES (1, 2, 3)
        )""")
        .queryExecution
        .analyzed
    )
  }

  test("Simple select, aliases referenced and rewritten") {
    checkExpressionIdAssignment(
      spark
        .sql("""
        SELECT
          col3, 3 AS c, col2, 2 AS b, col1, 1 AS a, col1, 1 AS a
        FROM (
          SELECT
            col2, b, col1, a, col1, a, col3, c
          FROM (
            SELECT
              col1, 1 AS a, col1, col2, 2 AS b, col3, 3 AS c
            FROM
              VALUES (1, 2, 3)
          )
        )""")
        .queryExecution
        .analyzed
    )
  }

  test("SQL Union, same table") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      checkExpressionIdAssignment(
        spark
          .sql("""
          SELECT * FROM (
            SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            UNION ALL
            SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            UNION ALL
            SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
          )""")
          .queryExecution
          .analyzed
      )
    }
  }

  test("SQL Union, different tables") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")
      withTable("t2") {
        spark.sql("CREATE TABLE t2 (col1 INT, col2 INT, col3 INT)")
        withTable("t3") {
          spark.sql("CREATE TABLE t3 (col1 INT, col2 INT, col3 INT)")

          checkExpressionIdAssignment(
            spark
              .sql("""
          SELECT * FROM (
            SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            UNION ALL
            SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            UNION ALL
            SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
          )""")
              .queryExecution
              .analyzed
          )
        }
      }
    }
  }

  test("SQL Union, same table, several layers") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      checkExpressionIdAssignment(
        spark
          .sql("""
          SELECT * FROM (
            SELECT * FROM (
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
              UNION ALL
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            )
            UNION ALL
            SELECT * FROM (
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
              UNION ALL
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            )
          )
          UNION ALL
          SELECT * FROM (
            SELECT * FROM (
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
              UNION ALL
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            )
            UNION ALL
            SELECT * FROM (
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
              UNION ALL
              SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1
            )
          )""")
          .queryExecution
          .analyzed
      )
    }
  }

  test("DataFrame Union, same table") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      val df = spark.sql("SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1")
      checkExpressionIdAssignment(df.union(df).queryExecution.analyzed)
    }
  }

  test("DataFrame Union, different tables") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      withTable("t2") {
        spark.sql("CREATE TABLE t2 (col1 INT, col2 INT, col3 INT)")

        val df1 = spark.sql("SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1")
        val df2 = spark.sql("SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t2")
        checkExpressionIdAssignment(df1.union(df2).queryExecution.analyzed)
      }
    }
  }

  test("DataFrame Union, same table, several layers") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT, col3 INT)")

      val df = spark.sql("SELECT col1, 1 AS a, col2, 2 AS b, col3, 3 AS c FROM t1")
      checkExpressionIdAssignment(
        df.union(df)
          .select("*")
          .union(df.union(df).select("*"))
          .union(df.union(df).select("*"))
          .queryExecution
          .analyzed
      )
    }
  }

  test("The case of output attribute names is preserved") {
    val df = spark.sql("SELECT col1, COL1, cOl2, CoL2 FROM VALUES (1, 2)")

    checkExpressionIdAssignment(df.queryExecution.analyzed)
  }

  test("The metadata of output attributes is preserved") {
    val metadata1 = new MetadataBuilder().putString("m1", "1").putString("m2", "2").build()
    val metadata2 = new MetadataBuilder().putString("m2", "3").putString("m3", "4").build()
    val schema = new StructType().add("a", IntegerType, nullable = true, metadata = metadata2)
    val df =
      spark.sql("SELECT col1 FROM VALUES (1)").select(col("col1").as("a", metadata1)).to(schema)

    checkExpressionIdAssignment(df.queryExecution.analyzed)
  }

  test("Alias with the same ID in multiple Projects") {
    val t = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int),
      0.until(10).map(_ => Row(1, 2))
    )
    val alias = ("a".attr + 1).as("a")
    val plan = t.select(alias).select(alias).select(alias)

    checkExpressionIdAssignment(plan)
  }

  test("Raw union, same table") {
    val t = LocalRelation.fromExternalRows(
      Seq("col1".attr.int, "col2".attr.int),
      0.until(10).map(_ => Row(1, 2))
    )
    val query = t.select("col1".attr, Literal(1).as("a"), "col2".attr, Literal(2).as("b"))
    val plan = query.union(query)

    checkExpressionIdAssignment(plan)
  }

  test("DataFrame with binary arithmetic re-resolved") {
    val result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      val df = spark.sql("SELECT col1 + col2 AS a FROM VALUES (1, 2)")
      df.union(df)
    }
    checkAnswer(result, Array(Row(3), Row(3)))
  }

  test("Leftmost branch attributes are not regenerated in DataFrame") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 INT, col2 INT)")
      spark.sql("INSERT INTO t1 VALUES (0, 1), (2, 3)")

      var result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df1 = spark.table("t1")
        df1.select(col("col1"), col("col2")).filter(df1("col1") === 0)
      }
      checkAnswer(result, Array(Row(0, 1)))

      result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df1 = spark.table("t1").select(col("col1").as("a"), col("col2").as("b"))
        df1.select(col("a"), col("b")).filter(df1("a") === 0)
      }
      checkAnswer(result, Array(Row(0, 1)))

      result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df1 = spark.table("t1")
        df1.union(df1).filter(df1("col1") === 0)
      }
      checkAnswer(result, Array(Row(0, 1), Row(0, 1)))
    }
  }

  test("Attribute ID under ExtractValue is remapped correctly") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (col1 STRUCT<f1: INT, f2: STRUCT<f3: STRING, f4: INT>>)")
      spark.sql(
        "INSERT INTO t1 VALUES (named_struct('f1', 0, 'f2', named_struct('f3', 'a', 'f4', 1)))"
      )

      var result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df1 = spark.table("t1")
        val df2 = df1.select("col1.f1")
        df2.union(df2)
      }
      checkAnswer(result, Array(Row(0), Row(0)))

      result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        val df1 = spark.table("t1")
        val df2 = df1.select("col1.f2.f4")
        df2.union(df2)
      }
      checkAnswer(result, Array(Row(1), Row(1)))

      result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        spark.sql("SELECT col1.f1 FROM t1 UNION ALL SELECT col1.f1 FROM t1")
      }
      checkAnswer(result, Array(Row(0), Row(0)))

      result = withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        spark.sql("SELECT col1.f2.f4 FROM t1 UNION ALL SELECT col1.f2.f4 FROM t1")
      }
      checkAnswer(result, Array(Row(1), Row(1)))
    }
  }

  private def checkExpressionIdAssignment(originalPlan: LogicalPlan): Unit = {
    val resolver = new Resolver(
      catalogManager = spark.sessionState.catalogManager,
      extensions = spark.sessionState.analyzer.singlePassResolverExtensions
    )
    val newPlan = resolver.resolve(originalPlan)

    checkPlanConstraints(originalPlan, newPlan, leftmostBranch = true)
    checkSubtreeConstraints(originalPlan, newPlan, leftmostBranch = true)
  }

  private def checkPlanConstraints(
      originalPlan: LogicalPlan,
      newPlan: LogicalPlan,
      leftmostBranch: Boolean): Unit = {
    originalPlan.children.zip(newPlan.children).zipWithIndex.foreach {
      case ((originalChild, newChild), index) =>
        checkPlanConstraints(originalChild, newChild, leftmostBranch && index == 0)
    }

    if (originalPlan.children.length > 1) {
      ExpressionIdAssigner.assertOutputsHaveNoConflictingExpressionIds(
        newPlan.children.map(_.output)
      )
      originalPlan.children.zip(newPlan.children).zipWithIndex.foreach {
        case ((oldChild, newChild), index) =>
          checkSubtreeConstraints(oldChild, newChild, leftmostBranch && index == 0)
      }
    }
  }

  private def checkSubtreeConstraints(
      originalPlan: LogicalPlan,
      newPlan: LogicalPlan,
      leftmostBranch: Boolean): Unit = {
    val originalOperators = new ArrayBuffer[LogicalPlan]
    originalPlan.foreach {
      case operator if !operator.getTagValue(CONSTRAINTS_VALIDATED).getOrElse(false) =>
        originalOperators.append(operator)
      case _ =>
    }

    val newOperators = new ArrayBuffer[LogicalPlan]

    val leftmostOperators = new IdentityHashMap[LogicalPlan, Boolean]
    if (leftmostBranch) {
      leftmostOperators.put(newPlan, true)
    }

    newPlan.foreach {
      case operator if !operator.getTagValue(CONSTRAINTS_VALIDATED).getOrElse(false) =>
        newOperators.append(operator)

        if (operator.children.nonEmpty && leftmostOperators.containsKey(operator)) {
          leftmostOperators.put(operator.children.head, true)
        }
      case _ =>
    }

    val attributesByName = new HashMap[String, ArrayBuffer[AttributeReference]]
    val aliasesByName = new HashMap[String, ArrayBuffer[Alias]]
    originalOperators
      .zip(newOperators)
      .collect {
        case (originalProject: Project, newProject: Project) =>
          if (originalProject.resolved) {
            (originalProject.projectList, newProject.projectList, newProject)
          } else {
            (newProject.projectList, newProject)
          }
        case (originalOperator: LogicalPlan, newOperator: LogicalPlan) =>
          if (originalOperator.resolved) {
            (originalOperator.output, newOperator.output, newOperator)
          } else {
            (newOperator.output, newOperator)
          }
      }
      .foreach {
        case (
            originalExpressions: Seq[_],
            newExpressions: Seq[_],
            newOperator: LogicalPlan
            ) =>
          originalExpressions.zip(newExpressions).zipWithIndex.foreach {
            case (
                (originalAttribute: AttributeReference, newAttribute: AttributeReference),
                index
                ) =>
              if (leftmostOperators.containsKey(newOperator)) {
                assert(
                  originalAttribute.exprId == newAttribute.exprId,
                  s"Attribute at $index was regenerated: $originalAttribute, $newAttribute"
                )
              } else {
                assert(
                  originalAttribute.exprId != newAttribute.exprId,
                  s"Attribute at $index was not regenerated: $originalAttribute, $newAttribute"
                )
              }

              attributesByName
                .getOrElseUpdate(newAttribute.name, new ArrayBuffer[AttributeReference])
                .append(newAttribute)
            case ((originalAlias: Alias, newAlias: Alias), index) =>
              if (leftmostOperators.containsKey(newOperator)) {
                assert(
                  originalAlias.exprId == newAlias.exprId,
                  s"Alias at $index was regenerated: $originalAlias, $newAlias"
                )
              } else {
                assert(
                  originalAlias.exprId != newAlias.exprId,
                  s"Alias at $index was not regenerated: $originalAlias, $newAlias"
                )
              }

              aliasesByName.getOrElseUpdate(newAlias.name, new ArrayBuffer[Alias]).append(newAlias)
          }
        case (newExpressions: Seq[_], newOperator: LogicalPlan) =>
          newExpressions.foreach {
            case newAttribute: AttributeReference =>
              attributesByName
                .getOrElseUpdate(newAttribute.name, new ArrayBuffer[AttributeReference])
                .append(newAttribute)
            case newAlias: Alias =>
              aliasesByName.getOrElseUpdate(newAlias.name, new ArrayBuffer[Alias]).append(newAlias)
          }
      }

    attributesByName.values.foreach { attributes =>
      val ids = attributes.map(attribute => attribute.exprId).distinct
      assert(
        ids.length == 1,
        s"Different IDs for the same attribute in the plan: $attributes, $newPlan"
      )
    }
    aliasesByName.values.foreach { aliases =>
      val ids = aliases.map(alias => alias.exprId).distinct
      assert(
        ids.length == aliases.length,
        s"Duplicate IDs for aliases with the same name: $aliases"
      )
    }

    for (operator <- originalOperators) {
      operator.setTagValue(CONSTRAINTS_VALIDATED, true)
    }
    for (operator <- newOperators) {
      operator.setTagValue(CONSTRAINTS_VALIDATED, true)
    }

    if (originalPlan.resolved) {
      assert(newPlan.schema == originalPlan.schema)
    }
  }
}
