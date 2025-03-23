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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class AddMetadataColumnsSuite extends QueryTest with SharedSparkSession {

  test("Add only necessary metadata columns") {
    // For a query like:
    //
    // {{{
    // SELECT t1.k
    // FROM VALUES(1,2) AS t1(k,v1) FULL OUTER JOIN VALUES(1,2) AS t2(k,v2) USING (k)
    // }}}
    //
    // the analyzed plan would look like:
    // Project [k#0]
    // +- Project [coalesce(k#0, k#2) AS k#4, v1#1, v2#3, k#0, k#2]
    //    +- Join FullOuter, (k#0 = k#2)
    //       :- SubqueryAlias nt1
    //       :  +- LocalRelation [k#0, v1#1]
    //       +- SubqueryAlias nt2
    //          +- LocalRelation [k#2, v2#3]
    // The inner project in this case contains a reference to k#2, which is not needed in the
    // top-most project. With `spark.sql.analyzer.uniqueNecessaryMetadataColumns` set to false, we
    // will add k#2 to the project list because it is a metadata column. Otherwise, we don't need
    // it and can avoid adding it in AddMetadataColumns rule.
    withTable("t1", "t2") {
      sql("CREATE TABLE t1(k INT, v1 INT)")
      sql("CREATE TABLE t2(k INT, v2 INT)")
      val left = sql("select * from t1")
      val right = sql("select * from t2")
      val join = left.join(right, Seq("k"), "full_outer")

      val rightKeyExprId = right
        .select(right("k"))
        .queryExecution
        .analyzed
        .asInstanceOf[Project]
        .projectList
        .head
        .exprId

      withSQLConf(SQLConf.ONLY_NECESSARY_AND_UNIQUE_METADATA_COLUMNS.key -> "true") {
        // Inner project list shouldn't contain a reference to the right key.
        val analyzed = join.select(left("k")).queryExecution.analyzed
        analyzed match {
          case Project(_, Project(innerProjectList: Seq[NamedExpression], _)) =>
            assert(Seq("k", "v1", "v2", "k") == innerProjectList.map(_.name))
            assert(!innerProjectList.map(_.exprId).contains(rightKeyExprId))
        }
      }

      withSQLConf(SQLConf.ONLY_NECESSARY_AND_UNIQUE_METADATA_COLUMNS.key -> "false") {
        // Inner project list should contain a reference to the right key.
        val analyzed = join.select(left("k")).queryExecution.analyzed
        analyzed match {
          case Project(_, Project(innerProjectList: Seq[NamedExpression], _)) =>
            assert(Seq("k", "v1", "v2", "k", "k") == innerProjectList.map(_.name))
            assert(innerProjectList.map(_.exprId).contains(rightKeyExprId))
        }
      }
    }
  }

  test("Add only unique metadata columns") {
    // For a query like:
    // {{{
    // SELECT t1.k
    // FROM VALUES(1,2) AS t1(k, v1) FULL OUTER JOIN VALUES(1,2) AS t2(k,v2) USING (k)
    // WHERE t1.k IS NOT NULL
    // }}}
    //
    // the analyzed plan will look like:
    // Project [k#0]
    // +- Project [k#4, v1#1, v2#3, k#0, k#2]
    //    +- Filter isnotnull(k#0)
    //       +- Project [coalesce(k#0, k#2) AS k#4, v1#1, v2#3, k#0, k#0, k#2]
    //          +- Join FullOuter, (k#0 = k#2)
    //             :- SubqueryAlias t1
    //             :  +- LocalRelation [k#0, v1#1]
    //             +- SubqueryAlias t2
    //                +- LocalRelation [k#2, v2#3]
    //
    // In this case, the Project under Filter contains a duplicate #k#0 attribute reference as well
    // as an unnecessary k#2 attribute reference. Additionally, the second top-most Project has an
    // extra k#2 that can also be removed. Duplicate reference comes from the fact that this
    // attribute will first be added by ResolveReferences rule as missing input, but
    // AddMetadataColumns doesn't respect the fact that this attribute already exists in the
    // project list and duplicates it. With `spark.sql.analyzer.uniqueNecessaryMetadataColumns` set
    // to true, we remove this duplication and the unnecessary attribute.
    withTable("t1", "t2") {
      sql("CREATE TABLE t1(k INT, v1 INT)")
      sql("CREATE TABLE t2(k INT, v2 INT)")
      val left = sql("select * from t1")
      val right = sql("select * from t2")
      val join = left.join(right, Seq("k"), "full_outer")
      val filter = join.filter(left("k").isNull)

      val leftKeyExprId = left
        .select(left("k"))
        .queryExecution
        .analyzed
        .asInstanceOf[Project]
        .projectList
        .head
        .exprId
      val rightKeyExprId = right
        .select(right("k"))
        .queryExecution
        .analyzed
        .asInstanceOf[Project]
        .projectList
        .head
        .exprId

      withSQLConf(SQLConf.ONLY_NECESSARY_AND_UNIQUE_METADATA_COLUMNS.key -> "true") {
        // With conf on, no duplication of left key and no unnecessary right key.
        val analyzed = filter.select(left("k")).queryExecution.analyzed
        analyzed match {
          case Project(
              _,
              Project(_, Filter(_, Project(innerProjectList: Seq[NamedExpression], _)))
              ) =>
            assert(Seq("k", "v1", "v2", "k") == innerProjectList.map(_.name))
            assert(innerProjectList.map(_.exprId).count(_ == rightKeyExprId) == 0)
            assert(innerProjectList.map(_.exprId).count(_ == leftKeyExprId) == 1)
        }
      }

      withSQLConf(SQLConf.ONLY_NECESSARY_AND_UNIQUE_METADATA_COLUMNS.key -> "false") {
        // With conf off, duplication of left key and an unnecessary right key.
        val analyzed = filter.select(left("k")).queryExecution.analyzed
        analyzed match {
          case Project(
              _,
              Project(_, Filter(_, Project(innerProjectList: Seq[NamedExpression], _)))
              ) =>
            assert(Seq("k", "v1", "v2", "k", "k", "k") == innerProjectList.map(_.name))
            assert(innerProjectList.map(_.exprId).count(_ == rightKeyExprId) == 1)
            assert(innerProjectList.map(_.exprId).count(_ == leftKeyExprId) == 2)
        }
      }
    }
  }
}
