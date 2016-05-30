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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class WindowFunctionExcludeSuite extends QueryTest with SQLTestUtils with TestHiveSingleton{

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql("create table table1 (col1 int, col2 int, col3 int)")
    sql("insert into table1 select 6, 12, 10")
    sql("insert into table1 select 6, 11, 4")
    sql("insert into table1 select 6, 13, 11")
    sql("insert into table1 select 6, 9, 10")
    sql("insert into table1 select 6, 15, 8")
    sql("insert into table1 select 6, 10, 1")
    sql("insert into table1 select 6, 15, 8")
    sql("insert into table1 select 6, 7, 4")
    sql("insert into table1 select 6, 7, 8")

    // more than one partition
    sql("create table table2 (col1 int, col2 int, col3 int)")
    sql("insert into table2 select 6, 12, 10")
    sql("insert into table2 select 6, 11, 4")
    sql("insert into table2 select 6, 13, 11")
    sql("insert into table2 select 6, 9, 10")
    sql("insert into table2 select 6, 15, 8")
    sql("insert into table2 select 6, 10, 1")
    sql("insert into table2 select 6, 15, 8")
    sql("insert into table2 select 6, 7, 4")
    sql("insert into table2 select 6, 7, 8")
    sql("insert into table2 select 7, 12, 10")
    sql("insert into table2 select 7, 11, 4")
    sql("insert into table2 select 7, 13, 11")
    sql("insert into table2 select 7, 9, 10")
    sql("insert into table2 select 7, 15, 8")
    sql("insert into table2 select 7, 10, 1")
    sql("insert into table2 select 7, 15, 8")
    sql("insert into table2 select 7, 7, 4")
    sql("insert into table2 select 7, 7, 8")
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS table1")
      sql("DROP TABLE IF EXISTS table2")
    } finally {
      super.afterAll()
    }
  }

  test("Sliding frame with exclude current row") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between 2 preceding and 2 following exclude current row)
          |from table1 where col1 = 6
        """.stripMargin),
        Seq(
          Row(6, 10, 1, 18),
          Row(6, 11, 4, 32),
          Row(6, 7, 4, 51),
          Row(6, 15, 8, 40),
          Row(6, 15, 8, 41),
          Row(6, 7, 8, 51),
          Row(6, 12, 10, 44),
          Row(6, 9, 10, 32),
          Row(6, 13, 11, 21)
        ))
  }

  test("sliding frame with exclude group") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between 2 preceding and 2 following exclude group)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 18),
        Row(6, 11, 4, 25),
        Row(6, 7, 4, 40),
        Row(6, 15, 8, 18),
        Row(6, 15, 8, 19),
        Row(6, 7, 8, 21),
        Row(6, 12, 10, 35),
        Row(6, 9, 10, 20),
        Row(6, 13, 11, 21)
      )
    )
  }

  test("sliding frame with exclude ties") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between 2 preceding and 2 following exclude ties)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 28),
        Row(6, 11, 4, 36),
        Row(6, 7, 4, 47),
        Row(6, 15, 8, 33),
        Row(6, 15, 8, 34),
        Row(6, 7, 8, 28),
        Row(6, 12, 10, 47),
        Row(6, 9, 10, 29),
        Row(6, 13, 11, 34)
      )
    )
  }

  test("sliding frame with exclude no others") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between 2 preceding and 2 following exclude no others)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 28),
        Row(6, 11, 4, 43),
        Row(6, 7, 4, 58),
        Row(6, 15, 8, 55),
        Row(6, 15, 8, 56),
        Row(6, 7, 8, 58),
        Row(6, 12, 10, 56),
        Row(6, 9, 10, 41),
        Row(6, 13, 11, 34)
      )
    )
  }

  test("expanding frame with exclude current row") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between unbounded preceding and current row exclude current row)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, null),
        Row(6, 11, 4, 10),
        Row(6, 7, 4, 21),
        Row(6, 15, 8, 28),
        Row(6, 15, 8, 43),
        Row(6, 7, 8, 58),
        Row(6, 12, 10, 65),
        Row(6, 9, 10, 77),
        Row(6, 13, 11, 86)
      )
    )
  }

  test("expanding frame with exclude group") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between unbounded preceding and current row exclude group)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, null),
        Row(6, 11, 4, 10),
        Row(6, 7, 4, 10),
        Row(6, 15, 8, 28),
        Row(6, 15, 8, 28),
        Row(6, 7, 8, 28),
        Row(6, 12, 10, 65),
        Row(6, 9, 10, 65),
        Row(6, 13, 11, 86)
      )
    )
  }

  test("expanding frame with exclude ties") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between unbounded preceding and current row exclude ties)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 10),
        Row(6, 11, 4, 21),
        Row(6, 7, 4, 17),
        Row(6, 15, 8, 43),
        Row(6, 15, 8, 43),
        Row(6, 7, 8, 35),
        Row(6, 12, 10, 77),
        Row(6, 9, 10, 74),
        Row(6, 13, 11, 99)
      )
    )
  }

  test("expanding frame with exclude no others") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between unbounded preceding and current row)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 10),
        Row(6, 11, 4, 21),
        Row(6, 7, 4, 28),
        Row(6, 15, 8, 43),
        Row(6, 15, 8, 58),
        Row(6, 7, 8, 65),
        Row(6, 12, 10, 77),
        Row(6, 9, 10, 86),
        Row(6, 13, 11, 99)
      )
    )
  }

  test("shrinking frame with exclude current row") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between current row and unbounded following exclude current row)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 89),
        Row(6, 11, 4, 78),
        Row(6, 7, 4, 71),
        Row(6, 15, 8, 56),
        Row(6, 15, 8, 41),
        Row(6, 7, 8, 34),
        Row(6, 12, 10, 22),
        Row(6, 9, 10, 13),
        Row(6, 13, 11, null)
      )
    )
  }

  test("shrinking frame with exclude current group") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between current row and unbounded following exclude group)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 89),
        Row(6, 11, 4, 71),
        Row(6, 7, 4, 71),
        Row(6, 15, 8, 34),
        Row(6, 15, 8, 34),
        Row(6, 7, 8, 34),
        Row(6, 12, 10, 13),
        Row(6, 9, 10, 13),
        Row(6, 13, 11, null)
      )
    )
  }

  test("shrinking frame with exclude current ties") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between current row and unbounded following exclude ties)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 99),
        Row(6, 11, 4, 82),
        Row(6, 7, 4, 78),
        Row(6, 15, 8, 49),
        Row(6, 15, 8, 49),
        Row(6, 7, 8, 41),
        Row(6, 12, 10, 25),
        Row(6, 9, 10, 22),
        Row(6, 13, 11, 13)
      )
    )
  }

  test("shrinking frame with exclude current no others") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between current row and unbounded following)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 99),
        Row(6, 11, 4, 89),
        Row(6, 7, 4, 78),
        Row(6, 15, 8, 71),
        Row(6, 15, 8, 56),
        Row(6, 7, 8, 41),
        Row(6, 12, 10, 34),
        Row(6, 9, 10, 22),
        Row(6, 13, 11, 13)
      )
    )
  }

  test("whole partition frame with exclude current row") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between unbounded preceding and unbounded following exclude current row)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 89),
        Row(6, 11, 4, 88),
        Row(6, 7, 4, 92),
        Row(6, 15, 8, 84),
        Row(6, 15, 8, 84),
        Row(6, 7, 8, 92),
        Row(6, 12, 10, 87),
        Row(6, 9, 10, 90),
        Row(6, 13, 11, 86)
      )
    )
  }

  test("whole partition frame with exclude group") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between unbounded preceding and unbounded following exclude group)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 89),
        Row(6, 11, 4, 81),
        Row(6, 7, 4, 81),
        Row(6, 15, 8, 62),
        Row(6, 15, 8, 62),
        Row(6, 7, 8, 62),
        Row(6, 12, 10, 78),
        Row(6, 9, 10, 78),
        Row(6, 13, 11, 86)
      )
    )
  }

  test("whole partition frame with exclude ties") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between unbounded preceding and unbounded following exclude ties)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 99),
        Row(6, 11, 4, 92),
        Row(6, 7, 4, 88),
        Row(6, 15, 8, 77),
        Row(6, 15, 8, 77),
        Row(6, 7, 8, 69),
        Row(6, 12, 10, 90),
        Row(6, 9, 10, 87),
        Row(6, 13, 11, 99)
      )
    )
  }

  test("whole partition frame with exclude no others") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |rows between unbounded preceding and unbounded following exclude no others)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 99),
        Row(6, 11, 4, 99),
        Row(6, 7, 4, 99),
        Row(6, 15, 8, 99),
        Row(6, 15, 8, 99),
        Row(6, 7, 8, 99),
        Row(6, 12, 10, 99),
        Row(6, 9, 10, 99),
        Row(6, 13, 11, 99)
      )
    )
  }

  test("sliding range framing exclude no others") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between 3 preceding and 3 following exclude no others)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 28),
        Row(6, 11, 4, 28),
        Row(6, 7, 4, 28),
        Row(6, 15, 8, 71),
        Row(6, 15, 8, 71),
        Row(6, 7, 8, 71),
        Row(6, 12, 10, 71),
        Row(6, 9, 10, 71),
        Row(6, 13, 11, 71)
      )
    )
  }

  test("sliding range framing with exclude current row") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between 3 preceding and 3 following exclude current row)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 18),
        Row(6, 11, 4, 17),
        Row(6, 7, 4, 21),
        Row(6, 15, 8, 56),
        Row(6, 15, 8, 56),
        Row(6, 7, 8, 64),
        Row(6, 12, 10, 59),
        Row(6, 9, 10, 62),
        Row(6, 13, 11, 58)
      )
    )
  }

  test("sliding range framing with exclude group") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between 3 preceding and 3 following exclude group)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 18),
        Row(6, 11, 4, 10),
        Row(6, 7, 4, 10),
        Row(6, 15, 8, 34),
        Row(6, 15, 8, 34),
        Row(6, 7, 8, 34),
        Row(6, 12, 10, 50),
        Row(6, 9, 10, 50),
        Row(6, 13, 11, 58)
      )
    )
  }

  test("sliding range framing with exclude ties") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between 3 preceding and 3 following exclude ties)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 28),
        Row(6, 11, 4, 21),
        Row(6, 7, 4, 17),
        Row(6, 15, 8, 49),
        Row(6, 15, 8, 49),
        Row(6, 7, 8, 41),
        Row(6, 12, 10, 62),
        Row(6, 9, 10, 59),
        Row(6, 13, 11, 71)
      )
    )
  }

  test("expanding range framing exclude no others") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and current row exclude no others)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 10),
        Row(6, 11, 4, 28),
        Row(6, 7, 4, 28),
        Row(6, 15, 8, 65),
        Row(6, 15, 8, 65),
        Row(6, 7, 8, 65),
        Row(6, 12, 10, 86),
        Row(6, 9, 10, 86),
        Row(6, 13, 11, 99)
      )
    )
  }

  test("expanding range framing with exclude current row") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and current row exclude current row)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, null),
        Row(6, 11, 4, 17),
        Row(6, 7, 4, 21),
        Row(6, 15, 8, 50),
        Row(6, 15, 8, 50),
        Row(6, 7, 8, 58),
        Row(6, 12, 10, 74),
        Row(6, 9, 10, 77),
        Row(6, 13, 11, 86)
      )
    )
  }

  test("expanding range framing with exclude group") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and current row exclude group)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, null),
        Row(6, 11, 4, 10),
        Row(6, 7, 4, 10),
        Row(6, 15, 8, 28),
        Row(6, 15, 8, 28),
        Row(6, 7, 8, 28),
        Row(6, 12, 10, 65),
        Row(6, 9, 10, 65),
        Row(6, 13, 11, 86)
      )
    )
  }

  test("expanding range framing with exclude ties") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and current row exclude ties)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 10),
        Row(6, 11, 4, 21),
        Row(6, 7, 4, 17),
        Row(6, 15, 8, 43),
        Row(6, 15, 8, 43),
        Row(6, 7, 8, 35),
        Row(6, 12, 10, 77),
        Row(6, 9, 10, 74),
        Row(6, 13, 11, 99)
      )
    )
  }

  test("shrinking range framing exclude no others") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between current row and unbounded following exclude no others)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 99),
        Row(6, 11, 4, 89),
        Row(6, 7, 4, 89),
        Row(6, 15, 8, 71),
        Row(6, 15, 8, 71),
        Row(6, 7, 8, 71),
        Row(6, 12, 10, 34),
        Row(6, 9, 10, 34),
        Row(6, 13, 11, 13)
      )
    )
  }

  test("shrinking range framing with exclude current row") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between current row and unbounded following exclude current row)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 89),
        Row(6, 11, 4, 78),
        Row(6, 7, 4, 82),
        Row(6, 15, 8, 56),
        Row(6, 15, 8, 56),
        Row(6, 7, 8, 64),
        Row(6, 12, 10, 22),
        Row(6, 9, 10, 25),
        Row(6, 13, 11, null)
      )
    )
  }

  test("shrinking range framing with exclude group") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between current row and unbounded following exclude group)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 89),
        Row(6, 11, 4, 71),
        Row(6, 7, 4, 71),
        Row(6, 15, 8, 34),
        Row(6, 15, 8, 34),
        Row(6, 7, 8, 34),
        Row(6, 12, 10, 13),
        Row(6, 9, 10, 13),
        Row(6, 13, 11, null)
      )
    )
  }

  test("shrinking range framing with exclude ties") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between current row and unbounded following exclude ties)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 99),
        Row(6, 11, 4, 82),
        Row(6, 7, 4, 78),
        Row(6, 15, 8, 49),
        Row(6, 15, 8, 49),
        Row(6, 7, 8, 41),
        Row(6, 12, 10, 25),
        Row(6, 9, 10, 22),
        Row(6, 13, 11, 13)
      )
    )
  }

  test("whole partition range framing with no others") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and unbounded following )
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 99),
        Row(6, 11, 4, 99),
        Row(6, 7, 4, 99),
        Row(6, 15, 8, 99),
        Row(6, 15, 8, 99),
        Row(6, 7, 8, 99),
        Row(6, 12, 10, 99),
        Row(6, 9, 10, 99),
        Row(6, 13, 11, 99)
      )
    )
  }

  test("whole partition range framing with exclude current row") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and unbounded following exclude current row)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 89),
        Row(6, 11, 4, 88),
        Row(6, 7, 4, 92),
        Row(6, 15, 8, 84),
        Row(6, 15, 8, 84),
        Row(6, 7, 8, 92),
        Row(6, 12, 10, 87),
        Row(6, 9, 10, 90),
        Row(6, 13, 11, 86)
      )
    )
  }

  test("whole partition range framing with exclude group") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and unbounded following exclude group)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 89),
        Row(6, 11, 4, 81),
        Row(6, 7, 4, 81),
        Row(6, 15, 8, 62),
        Row(6, 15, 8, 62),
        Row(6, 7, 8, 62),
        Row(6, 12, 10, 78),
        Row(6, 9, 10, 78),
        Row(6, 13, 11, 86)
      )
    )
  }

  test("whole partition range framing with exclude ties") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and unbounded following exclude ties)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 99),
        Row(6, 11, 4, 92),
        Row(6, 7, 4, 88),
        Row(6, 15, 8, 77),
        Row(6, 15, 8, 77),
        Row(6, 7, 8, 69),
        Row(6, 12, 10, 90),
        Row(6, 9, 10, 87),
        Row(6, 13, 11, 99)
      )
    )
  }

  test("order by with component expression") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3+col2
          |range between unbounded preceding and current row exclude current row)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, 7),
        Row(6, 7, 4, 10),
        Row(6, 11, 4, 24),
        Row(6, 7, 8, 28),
        Row(6, 9, 10, 35),
        Row(6, 12, 10, 44),
        Row(6, 15, 8, 71),
        Row(6, 15, 8, 71),
        Row(6, 13, 11, 86)
      )
    )
  }

  test("trying on other aggregation functions, like AVG, MIN, MAX") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, AVG(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and current row exclude current row)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, null),
        Row(6, 11, 4, 8.5),
        Row(6, 7, 4, 10.5),
        Row(6, 15, 8, 10.0),
        Row(6, 15, 8, 10.0),
        Row(6, 7, 8, 11.6),
        Row(6, 12, 10, 10.571428571428571),
        Row(6, 9, 10, 11.0),
        Row(6, 13, 11, 10.75)
      )
    )

    checkAnswer(
      sql(
        """
          |select col1, col2, col3, MIN(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and current row exclude group)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, null),
        Row(6, 11, 4, 10),
        Row(6, 7, 4, 10),
        Row(6, 15, 8, 7),
        Row(6, 15, 8, 7),
        Row(6, 7, 8, 7),
        Row(6, 12, 10, 7),
        Row(6, 9, 10, 7),
        Row(6, 13, 11, 7)
      )
    )

    checkAnswer(
      sql(
        """
          |select col1, col2, col3, MAX(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and current row exclude current row)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, null),
        Row(6, 11, 4, 10),
        Row(6, 7, 4, 11),
        Row(6, 15, 8, 15),
        Row(6, 15, 8, 15),
        Row(6, 7, 8, 15),
        Row(6, 12, 10, 15),
        Row(6, 9, 10, 15),
        Row(6, 13, 11, 15)
      )
    )

    checkAnswer(
      sql(
        """
          |select col1, col2, col3, MAX(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and current row exclude group)
          |from table1 where col1 = 6
        """.stripMargin),
      Seq(
        Row(6, 10, 1, null),
        Row(6, 11, 4, 10),
        Row(6, 7, 4, 10),
        Row(6, 15, 8, 11),
        Row(6, 15, 8, 11),
        Row(6, 7, 8, 11),
        Row(6, 12, 10, 15),
        Row(6, 9, 10, 15),
        Row(6, 13, 11, 15)
      )
    )
  }

  test("multiple partitions") {
    checkAnswer(
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and current row exclude current row )
          |from table2 where col1 < 20 order by col1, col2
        """.stripMargin),
      Seq(
        Row(6, 7, 4, 21),
        Row(6, 7, 8, 58),
        Row(6, 9, 10, 77),
        Row(6, 10, 1, null),
        Row(6, 11, 4, 17),
        Row(6, 12, 10, 74),
        Row(6, 13, 11, 86),
        Row(6, 15, 8, 50),
        Row(6, 15, 8, 50),
        Row(7, 7, 4, 21),
        Row(7, 7, 8, 58),
        Row(7, 9, 10, 77),
        Row(7, 10, 1, null),
        Row(7, 11, 4, 17),
        Row(7, 12, 10, 74),
        Row(7, 13, 11, 86),
        Row(7, 15, 8, 50),
        Row(7, 15, 8, 50)
      )
    )
  }

  test("Non windowAggregation with exclude clause") {
    val e1 = intercept[AnalysisException](
      sql(
        """
          |select col1, cume_dist() over
          |(partition by col1 order by col3
          |range between unbounded preceding and current row exclude current row)
          |from table1
        """.stripMargin))
    assert(e1.getMessage.contains("does not support exclude clause"))

    val e2 = intercept[AnalysisException](
      sql(
        """
          |select col1, rank() over
          |(partition by col1 order by col3
          |range between unbounded preceding and current row exclude current row)
          |from table1
        """.stripMargin))
    assert(e2.getMessage.contains("does not support exclude clause"))


    val e3 = intercept[AnalysisException](
      sql(
        """
          |select col1, lead(2) over
          |(partition by col1 order by col3
          |range between unbounded preceding and current row exclude current row)
          |from table1
        """.stripMargin))
    assert(e3.getMessage.contains("does not support exclude clause"))
  }

  test("require ORDERBY for EXCLUDE GROUP/TIES") {
    val e1 = intercept[AnalysisException](
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1
          |rows current row exclude group)
          |from table1
        """.stripMargin))
    assert(e1.getMessage.contains("requires an ordered window frame"))

    val e2 = intercept[AnalysisException](
      sql(
        """
          |select col1, col2, col3, sum(col2) over
          |(partition by col1
          |range current row exclude ties)
          |from table1
        """.stripMargin))
    assert(e2.getMessage.contains("requires an ordered window frame"))
  }
}
