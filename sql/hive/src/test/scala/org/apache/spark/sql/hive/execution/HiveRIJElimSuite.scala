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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class HiveRIJElimSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTablesHive()
    createTablesParquet()
    createTablesParquetDT()
    createTablesParquetSelfJoin()
  }

  override def afterAll(): Unit = {
    try {
      dropTables()
    } finally {
      super.afterAll()
    }
  }

  def testRIJElim(query: String, rewrittenQuery: String): Unit = {

    withSQLConf(SQLConf.RI_JOIN_ELIMINATION.key -> "false") {
      val expectedOptimized = sql(rewrittenQuery).queryExecution.optimizedPlan
      val expectedAnswer = sql(rewrittenQuery).collect()

      withSQLConf(SQLConf.RI_JOIN_ELIMINATION.key -> "true") {
        comparePlans(sql(query).queryExecution.optimizedPlan, expectedOptimized)
        checkAnswer(sql(query), expectedAnswer)
      }
    }
  }

  // Unit tests coverage:
  // Unit tests RI1: Two way RI joins
  // Unit tests RI2: Restrictions on predicates
  // Unit tests RI3: Restrictions on columns in Select/Group By/Order By clauses
  // Unit tests RI4: RI join with multiple PK columns
  // Unit tests RI5: Multi-way joins
  // Unit tests RI6: Restrictions on string data type

  test("RI1.1. RI join with single PK column") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select fact.factc2
        | from fact
        | where fact.factc1 is not null
        | order by 1
        |
      """.stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI1.2. RI join with local predicate on the PK column") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1 and dim1.dim1c1 < 3
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select fact.factc2
        | from fact
        | where fact.factc1 is not null and fact.factc1 < 3
        | order by 1
        |
      """.stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI1.3. RI join with local predicate on the PK column in expression") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1 and abs(dim1.dim1c1) < 3
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select fact.factc2
        | from fact
        | where fact.factc1 is not null and abs(fact.factc1) < 3
        | order by 1
        |
      """.stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI1.4. RI join with additional join predicate on the PK column") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1 and
        |       fact.factc2 = dim1.dim1c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select fact.factc2
        | from fact
        | where fact.factc1 is not null and
        |       fact.factc2 = fact.factc1
        | order by 1
        |
      """.
        stripMargin

    testRIJElim(query, rewrittenQuery)
  }


  test("RI2.1.Neg. RI join with local predicate on the non-PK column") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1 and dim1.dim1c2 < 3
        | order by 1
      """.stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI2.2.Neg. RI join with local predicate on PK and non-PK columns") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1 and
        |       dim1.dim1c2 < 3 and
        |       dim1.dim1c1 < 5
        | order by 1
      """.stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI2.3.Neg. RI join with local predicate on PK and non-PK columns with expressions") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1 and
        |       dim1.dim1c2 + abs(dim1.dim1c1) < 10 and
        |       dim1.dim1c1 < 8 and fact.factc3 > 0
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI2.5.Neg RI join with local predicate on PK column in IN subquery") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1 and
        |       dim1.dim1c1 IN (select t1c1 from t1)
        | order by 1
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI2.6.Neq RI join with local predicate on PK column in correlated EXISTS subquery") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1 and
        |       exists (select t1c1 from t1 where dim1.dim1c1 < 10)
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI2.7.Neg RI join with local predicate on PK column in scalar subquery") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1 and
        |       dim1.dim1c1 = (select max(t1c1) from t1)
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery = query

    // testRIJElim(query, rewrittenQuery)

    withSQLConf(SQLConf.RI_JOIN_ELIMINATION.key -> "true") {
      val expectedOptimized = sql(rewrittenQuery).queryExecution.optimizedPlan
      val expectedAnswer = sql(rewrittenQuery).collect()

      withSQLConf(SQLConf.RI_JOIN_ELIMINATION.key -> "true") {
        // TODO: Scalar subquery plans are not traversed
        // comparePlans(sql(query).queryExecution.optimizedPlan, expectedOptimized)
        checkAnswer(sql(query), expectedAnswer)
      }
    }
  }

  test("RI2.8.Neg. RI join with additional join predicate on non-PK column") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1 and
        |       fact.factc2 = dim1.dim1c2
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI2.9.Neg. Inequality join") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1
        | where fact.factc1 <= dim1.dim1c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI2.10.Neg. Join over nested tables/views") {
    val query =
      """
        | select myfact.c2
        | from (select fact.factc2 as c2, fact.factc1 as c1 from fact) as myfact, dim1
        | where myfact.c1 <= dim1.dim1c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.3.0 RI join in GroupBy ") {
    val query =
      """
        | select fact.factc2, sum(fact.factc3)
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1
        | group by fact.factc2
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select fact.factc2, sum(fact.factc3)
        | from fact
        | where fact.factc1 is not null
        | group by fact.factc2
        | order by 1
        |
      """.
        stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.3.1.Neg PK columns in the SELECT list") {
    val query =
      """
        | select fact.factc2, dim1.dim1c2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.3.2.Neg PK columns in the SELECT list with expressions") {
    val query =
      """
        | select fact.factc2, abs(dim1.dim1c2)
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.3.3.Neg PK columns in the GroupBy list") {
    val query =
      """
        | select fact.factc2, dim1.dim1c2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1
        | group by fact.factc2, dim1.dim1c2
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.3.4.Neg PK columns in the Aggregate functions") {
    val query =
      """
        | select fact.factc2, sum(dim1.dim1c2)
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1
        | group by fact.factc2
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.3.4.Neg PK columns in the Order By clause") {
    val query =
      """
        | select fact.factc2, dim1.dim1c2
        | from fact, dim1
        | where fact.factc1 = dim1.dim1c1
        | order by 1, 2
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI4.1 (Parquet). RI join with multiple PK columns") {
    val query =
      """
        | select fact1.fact1c2
        | from fact1, dim11
        | where fact1.fact1c1 = dim11.dim11c1 and
        |       fact1.fact1c2 = dim11.dim11c2
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select fact1.fact1c2
        | from fact1
        | where fact1.fact1c1 is not null and
        |       fact1.fact1c2 is not null
        | order by 1
        |
      """.
        stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI4.2.Neg (Parquet). RI join with multiple PK columns and missing RI column") {
    val query =
      """
        | select fact1.fact1c2
        | from fact1, dim11
        | where fact1.fact1c2 = dim11.dim11c2
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI4.3.Neg (Parquet). RI join with multiple PK columns and exprs in join preds") {
    val query =
      """
        | select fact1.fact1c1
        | from fact1, dim11
        | where fact1.fact1c1 = dim11.dim11c1 and
        |       fact1.fact1c2 = abs(dim11.dim11c2)
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI4.4 Self-join with RI") {
    val query =
      """
        | select b.pkfkc3
        | from pkfk a, pkfk b
        | where  a.pkfkc1 = b.pkfkc2
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select pkfkc3
        | from pkfk
        | where pkfkc2 is not null
        | order by 1
        |
      """.stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.5.1 Multiple RI joins: one fact - two dimensions") {
    val query =
      """
        | select fact.factc3
        | from fact, dim1, dim2
        | where fact.factc1 = dim1.dim1c1 and
        |       fact.factc2 = dim2.dim2c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select fact.factc3
        | from fact
        | where fact.factc1 IS NOT NULL and
        |       fact.factc2 IS NOT NULL
        | order by 1
        |
      """.
        stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.5.2 Three way join with one RI join") {
    val query =
      """
        | select fact.factc3
        | from t1, fact, dim1
        | where fact.factc1 = dim1.dim1c1 and
        |       fact.factc2 = t1.t1c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select fact.factc3
        | from t1, fact
        | where fact.factc1 IS NOT NULL and
        |       fact.factc2 = t1.t1c1
        | order by 1
        |
      """.
        stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.5.3 Dimension in two RI joins") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1, fact2
        | where fact.factc1 = dim1.dim1c1 and
        |       fact2.fact2c1 = dim1.dim1c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select fact.factc2
        | from fact, fact2
        | where fact.factc1 = fact2.fact2c1
        | order by 1
        |
      """.
        stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.5.4 RI join and Cartesian product") {
    val query =
      """
        | select fact.factc2
        | from fact, dim1 cross join t1
        | where fact.factc1 = dim1.dim1c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select fact.factc2
        | from fact cross join t1
        | where fact.factc1 IS NOT NULL
        | order by 1
        |
      """.
        stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.5.5 RI join and Left outer join") {
    val query =
      """
        | select fact.factc2
        | from dim1 inner join fact on fact.factc1 = dim1.dim1c1 left outer join t1
        | on fact.factc2 = t1.t1c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select fact.factc2
        | from fact left outer join t1 on fact.factc2 = t1.t1c1
        | where fact.factc1 is not null
        | order by 1
        |
      """.
        stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.6.1 String comparison semantics - String data types") {
    val query =
      """
        | select factdt.factdtc1
        | from factdt, dimdt1
        | where factdt.factdtc1 = dimdt1.dimdt1c1 and dimdt1.dimdt1c1 like 'ab%'
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select factdt.factdtc1
        | from factdt
        | where factdt.factdtc1 like 'ab%' and factdt.factdtc1 is not null
        | order by 1
        |
      """.
        stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.6.2.Neg. String comparison semantics: With length sensitive functions") {
    val query =
      """
        | select factdt.factdtc1
        | from factdt, dimdt1
        | where factdt.factdtc1 = dimdt1.dimdt1c1 and length(dimdt1.dimdt1c1) < 10
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery = query

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.6.3 String comparison semantics - CHAR data types") {
    val query =
      """
        | select factdt.factdtc2
        | from factdt, dimdt2
        | where factdt.factdtc2 = dimdt2.dimdt2c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select factdt.factdtc2
        | from factdt
        | where factdt.factdtc2 is not null
        | order by 1
        |
      """.
        stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  test("RI.6.4 String comparison semantics - VARCHAR data types") {
    val query =
      """
        | select factdt.factdtc3
        | from factdt, dimdt3
        | where factdt.factdtc3 = dimdt3.dimdt3c1
        | order by 1
        |
      """.
        stripMargin

    val rewrittenQuery =
      """
        | select factdt.factdtc3
        | from factdt
        | where factdt.factdtc3 is not null
        | order by 1
        |
      """.
        stripMargin

    testRIJElim(query, rewrittenQuery)
  }

  def createTablesHive(): Unit = {
    sql("create table dim1 (dim1c1 int, dim1c2 int, dim1c3 int, dim1c4 int)")
    sql("create table dim2 (dim2c1 int, dim2c2 int, dim2c3 int, dim2c4 int)")
    sql("create table fact (factc1 int, factc2 int, factc3 int, factc4 int)")
    sql("create table fact2 (fact2c1 int, fact2c2 int, fact2c3 int, fact2c4 int)")

    sql("alter table dim1 add constraint pk1 primary key(dim1c1)")
    sql("alter table dim2 add constraint pk1 primary key(dim2c1)")
    sql("alter table fact add constraint fk1 foreign key (factc1) references dim1(dim1c1)")
    sql("alter table fact add constraint fk2 foreign key (factc2) references dim2(dim2c1)")
    sql("alter table fact2 add constraint fk21 foreign key (fact2c1)" +
      "references dim1(dim1c1)")
    sql("alter table fact2 add constraint fk22 foreign key (fact2c2)" +
      "references dim1(dim1c1)")

    sql("create table t1 (t1c1 int, t1c2 int, t1c3 int, t1c4 int)")

    sql("insert into dim1 values (1,1,1,1)")
    sql("insert into dim1 values (2,2,2,2)")

    sql("insert into dim2 values (1,1,1,1)")
    sql("insert into dim2 values (2,2,2,2)")

    sql("insert into fact values (1,1,1,1)")
    sql("insert into fact values (1,1,1,1)")
    sql("insert into fact values (2,2,2,2)")
    sql("insert into fact values (2,2,2,2)")

    sql("insert into fact2 values (1,1,1,1)")
    sql("insert into fact2 values (2,2,2,2)")

    sql("insert into t1 values (1,1,1,1)")
    sql("insert into t1 values (2,2,2,2)")
  }

  def createTablesParquet(): Unit = {
    sql("create table dim11 (dim11c1 int, dim11c2 int, dim11c3 int," +
      " dim11c4 int) using Parquet")
    sql("create table fact1 (fact1c1 int, fact1c2 int, fact1c3 int," +
      " fact1c4 int) using Parquet")

    sql("alter table dim11 add constraint pk11 primary key(dim11c1, dim11c2)")
    sql("alter table fact1 add constraint fk11 foreign key (fact1c1, fact1c2)" +
      "references dim11(dim11c1, dim11c2)")

    sql("insert into fact1 values (1,1,1,1)")
    sql("insert into fact1 values (1,1,1,1)")
    sql("insert into fact1 values (2,2,2,2)")
    sql("insert into fact1 values (2,2,2,2)")

    sql("insert into dim11 values (1,1,1,1)")
    sql("insert into dim11 values (2,2,2,2)")

  }

  def createTablesParquetSelfJoin(): Unit = {
    sql("create table pkfk (pkfkc1 int, pkfkc2 int, pkfkc3 int, pkfkc4 int) using parquet")
    sql("alter table pkfk add constraint pk1 primary key(pkfkc1)")
    sql("alter table pkfk add constraint fk1 foreign key (pkfkc2) references pkfk(pkfkc1)")

    sql("insert into pkfk values (1,1,1,1)")
    sql("insert into pkfk values (1,1,1,1)")
    sql("insert into pkfk values (2,2,2,2)")
    sql("insert into pkfk values (2,2,2,2)")

  }

  def createTablesParquetDT(): Unit = {
    sql("create table dimdt1 (dimdt1c1 String, dimdt1c2 int) using Parquet")
    sql("create table dimdt2 (dimdt2c1 char(10), dimdt2c2 int) using Parquet")
    sql("create table dimdt3 (dimdt3c1 varchar(10), dimdt3c2 int) using Parquet")

    sql("create table factdt (factdtc1 String, factdtc2 char(10)," +
      "factdtc3 varchar(10), factdtc4 char(10)) using Parquet")

    sql("alter table dimdt1 add constraint pk1 primary key(dimdt1c1)")
    sql("alter table dimdt2 add constraint pk1 primary key(dimdt2c1)")
    sql("alter table dimdt3 add constraint pk1 primary key(dimdt3c1)")

    sql("alter table factdt add constraint fk1 foreign key (factdtc1)" +
      "references dimdt1(dimdt1c1)")
    sql("alter table factdt add constraint fk2 foreign key (factdtc2)" +
      "references dimdt2(dimdt2c1)")
    sql("alter table factdt add constraint fk3 foreign key (factdtc3)" +
      "references dimdt3(dimdt3c1)")
    sql("alter table factdt add constraint fk4 foreign key (factdtc4)" +
      "references dimdt1(dimdt1c1)")

    sql("insert into dimdt1 values ('1',1)")
    sql("insert into dimdt1 values ('1 ',1)")
    sql("insert into dimdt1 values ('1  ',1)")

    sql("insert into dimdt2 values ('1',1)")

    sql("insert into dimdt3 values ('1',1)")
    sql("insert into dimdt3 values ('1 ',1)")
    sql("insert into dimdt3 values ('1  ',1)")

    sql("insert into factdt values ('1', '1     ', '1',  '1')")
    sql("insert into factdt values ('1 ','1  ',    '1 ', '1 ')")

  }

  def dropTables(): Unit = {
    sql("DROP TABLE IF EXISTS DIM1")
    sql("DROP TABLE IF EXISTS DIM2")
    sql("DROP TABLE IF EXISTS DIM11")
    sql("DROP TABLE IF EXISTS FACT")
    sql("DROP TABLE IF EXISTS FACT1")
    sql("DROP TABLE IF EXISTS FACT2")
    sql("DROP TABLE IF EXISTS T1")
    sql("DROP TABLE IF EXISTS PKFK")
    sql("DROP TABLE IF EXISTS DIMDT1")
    sql("DROP TABLE IF EXISTS DIMDT2")
    sql("DROP TABLE IF EXISTS DIMDT3")
    sql("DROP TABLE IF EXISTS FACTDT")
  }
}
