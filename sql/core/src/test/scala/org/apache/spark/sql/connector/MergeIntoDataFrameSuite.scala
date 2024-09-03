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

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.MergeIntoWriterImpl

class MergeIntoDataFrameSuite extends RowLevelOperationSuiteBase {

  import testImplicits._

  test("merge into empty table with NOT MATCHED clause") {
    withTempView("source") {
      createTable("pk INT NOT NULL, salary INT, dep STRING")

      val sourceRows = Seq(
        (1, 100, "hr"),
        (2, 200, "finance"),
        (3, 300, "hr"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenNotMatched()
        .insertAll()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // insert
          Row(2, 200, "finance"), // insert
          Row(3, 300, "hr"))) // insert
    }
  }

  test("merge into empty table with conditional NOT MATCHED clause") {
    withTempView("source") {
      createTable("pk INT NOT NULL, salary INT, dep STRING")

      val sourceRows = Seq(
        (1, 100, "hr"),
        (2, 200, "finance"),
        (3, 300, "hr"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenNotMatched($"source.pk" >= 2)
        .insertAll()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "finance"), // insert
          Row(3, 300, "hr"))) // insert
    }
  }

  test("merge into with conditional WHEN MATCHED clause (update)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "corrupted" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 100, "software"),
        (2, 200, "finance"),
        (3, 300, "software"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched($"source.pk" === 2)
        .updateAll()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // unchanged
          Row(2, 200, "finance"))) // update
    }
  }

  test("merge into with conditional WHEN MATCHED clause (delete)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "corrupted" }
          |""".stripMargin)

      Seq(1, 2, 3).toDF("pk").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched(col(tableNameAsString + ".salary") === 200)
        .delete()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(1, 100, "hr"))) // unchanged
    }
  }

  test("merge into with assignments to primary key in NOT MATCHED BY SOURCE") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "finance" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 100, "software"),
        (5, 500, "finance"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched()
        .update(Map(
           tableNameAsString + ".salary" -> lit(-1)
        ))
        .whenNotMatchedBySource()
        .update(Map(
          tableNameAsString + ".pk" -> lit(-1)
         ))
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr"), // update (matched)
          Row(-1, 200, "finance"))) // update (not matched by source)
    }
  }

  test("merge into with assignments to primary key in MATCHED") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "finance" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 100, "software"),
        (5, 500, "finance"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched()
        .update(Map(
          tableNameAsString + ".pk" -> lit(-1)
        ))
        .whenNotMatchedBySource()
        .update(Map(
          tableNameAsString + ".salary" -> lit(-1)
        ))
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(-1, 100, "hr"), // update (matched)
          Row(2, -1, "finance"))) // update (not matched by source)
    }
  }

  test("merge with all types of clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "salary": 400, "dep": "hr" }
          |{ "pk": 5, "salary": 500, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(3, 4, 5, 6).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched()
        .update(Map(
          tableNameAsString + ".salary" -> col("cat.ns1.test_table.salary").plus(lit(1))
        ))
        .whenNotMatched()
        .insert(Map(
          "pk" -> col("source.pk"),
          "salary" -> lit(0),
          "dep" -> lit("new")
        ))
        .whenNotMatchedBySource()
        .delete()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(3, 301, "hr"), // update
          Row(4, 401, "hr"), // update
          Row(5, 501, "hr"), // update
          Row(6, 0, "new"))) // insert
    }
  }

  test("merge with all types of clauses (update and insert star)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (2, 201, "support"),
        (4, 401, "support"),
        (5, 501, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched(col(tableNameAsString + ".pk") === 1)
        .updateAll()
        .whenNotMatched($"source.pk" === 4)
        .insertAll()
        .whenNotMatchedBySource(
          col(tableNameAsString + ".pk") === col(tableNameAsString + ".salary") / 100)
        .delete()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "support"), // update
          Row(2, 200, "software"), // unchanged
          Row(4, 401, "support"))) // insert
    }
  }

  test("merge with all types of conditional clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "salary": 400, "dep": "hr" }
          |{ "pk": 5, "salary": 500, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(3, 4, 5, 6, 7).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched(col(tableNameAsString + ".pk") === 4)
        .update(Map(
          tableNameAsString + ".salary" -> col(tableNameAsString + ".salary").plus(lit(1))
        ))
        .whenNotMatched($"pk" === 6)
        .insert(Map(
          "pk" -> col("source.pk"),
          "salary" -> lit(0),
          "dep" -> lit("new")
        ))
        .whenNotMatchedBySource($"salary" === 100)
        .delete()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "software"), // unchanged
          Row(3, 300, "hr"), // unchanged
          Row(4, 401, "hr"), // update
          Row(5, 500, "hr"), // unchanged
          Row(6, 0, "new"))) // insert
    }
  }

  test("merge with one NOT MATCHED BY SOURCE clause") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(1, 2).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenNotMatchedBySource()
        .delete()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // unchanged
          Row(2, 200, "software"))) // unchanged
    }
  }

  test("merge with one conditional NOT MATCHED BY SOURCE clause") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(2).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenNotMatchedBySource($"salary" === 100)
        .update(Map(
          "salary" -> lit(-1)
        ))
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr"), // updated
          Row(2, 200, "software"), // unchanged
          Row(3, 300, "hr"))) // unchanged
    }
  }

  test("merge with MATCHED and NOT MATCHED BY SOURCE clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(2).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched()
        .delete()
        .whenNotMatchedBySource($"salary" === 100)
        .update(Map(
          "salary" -> lit(-1)
        ))
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr"), // updated
          Row(3, 300, "hr"))) // unchanged
    }
  }

  test("merge with NOT MATCHED and NOT MATCHED BY SOURCE clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(2, 3, 4).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenNotMatched()
        .insert(Map(
          "pk" -> col("pk"),
          "salary" -> lit(-1),
          "dep" -> lit("new")
        ))
        .whenNotMatchedBySource()
        .delete()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "software"), // unchanged
          Row(3, 300, "hr"), // unchanged
          Row(4, -1, "new"))) // insert
    }
  }

  test("merge with multiple NOT MATCHED BY SOURCE clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(5, 6, 7).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenNotMatchedBySource($"salary" === 100)
        .update(Map(
          "salary" -> col("salary").plus(lit(1))
        ))
        .whenNotMatchedBySource($"salary" === 300)
        .delete()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "hr"), // update
          Row(2, 200, "software"))) // unchanged
    }
  }

  test("merge with MATCHED BY SOURCE clause and NULL values") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "id": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceRows = Seq(
        (2, 2, 201, "support"),
        (1, 1, 101, "support"),
        (3, 3, 301, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString,
          $"source.id" === col(tableNameAsString + ".id") && (col(tableNameAsString + ".id") < 3))
        .whenMatched()
        .updateAll()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, null, 100, "hr"), // unchanged
          Row(2, 2, 201, "support"), // update
          Row(3, 3, 300, "hr"))) // unchanged
    }
  }

  test("merge cardinality check with unconditional MATCHED clause (delete)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 6, "salary": 600, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (1, 102, "support"),
        (2, 201, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched()
        .delete()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(6, 600, "software"))) // unchanged
    }
  }

  test("merge cardinality check with only NOT MATCHED clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 6, "salary": 600, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (1, 102, "support"),
        (2, 201, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenNotMatched()
        .insertAll()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // unchanged
          Row(2, 201, "support"), // insert
          Row(6, 600, "software"))) // unchanged
    }
  }

  test("merge with extra columns in source") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, "smth", 101, "support"),
        (2, "smth", 201, "support"),
        (4, "smth", 401, "support"))
      sourceRows.toDF("pk", "extra", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched()
        .update(Map(
          "salary" -> col("source.salary").plus(lit(1))
        ))
        .whenNotMatched()
        .insert(Map(
          "pk" -> col("source.pk"),
          "salary" -> col("source.salary"),
          "dep" -> col("source.dep")
        ))
        .whenNotMatchedBySource()
        .delete()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 102, "hr"), // update
          Row(2, 202, "software"), // update
          Row(4, 401, "support"))) // insert
    }
  }

  test("merge with NULL values in target and source") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (5, None, 501, "support"),
        (6, Some(6), 601, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, null, 100, "hr"), // unchanged
          Row(2, 2, 200, "software"), // unchanged
          Row(5, null, 501, "support"), // insert
          Row(6, 6, 601, "support"))) // insert
    }
  }

  test("merge with <=>") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (5, None, 501, "support"),
        (6, Some(6), 601, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.id" <=> col(tableNameAsString + ".id"))
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 2, 200, "software"), // unchanged
          Row(5, null, 501, "support"), // updated
          Row(6, 6, 601, "support"))) // insert
    }
  }

  test("merge with NULL ON condition") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (5, None, 501, "support"),
        (6, Some(2), 201, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk") && lit(null))
        .whenMatched()
        .update(Map(
          "salary" -> col("source.salary")
        ))
        .whenNotMatched()
        .insertAll()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, null, 100, "hr"), // unchanged
          Row(2, 2, 200, "software"), // unchanged
          Row(5, null, 501, "support"), // new
          Row(6, 2, 201, "support"))) // new
    }
  }

  test("merge with NULL clause conditions") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (3, 301, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched(lit(null))
        .update(Map(
          "salary" -> col("source.salary")
        ))
        .whenNotMatched(lit(null))
        .insertAll()
        .whenNotMatchedBySource(lit(null))
        .delete()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // unchanged
          Row(2, 200, "software"))) // unchanged
    }
  }

  test("merge with multiple matching clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (3, 301, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched(col(tableNameAsString + ".pk") === 1)
        .update(Map(
          "salary" -> col(tableNameAsString + ".salary").plus(lit(5))
        ))
        .whenMatched(col(tableNameAsString + ".salary") === 100)
        .update(Map(
          "salary" -> col(tableNameAsString + ".salary").plus(lit(2))
        ))
        .whenNotMatchedBySource(col(tableNameAsString + ".pk") === 2)
        .update(Map(
          "salary" -> col("salary").minus(lit(1))
        ))
        .whenNotMatchedBySource(col(tableNameAsString + ".salary") === 200)
        .delete()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 105, "hr"), // updated (matched)
          Row(2, 199, "software"))) // updated (not matched by source)
    }
  }

  test("merge resolves and aligns columns by name") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        ("support", 1, 101),
        ("support", 3, 301))
      sourceRows.toDF("dep", "pk", "salary").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "support"), // update
          Row(2, 200, "software"), // unchanged
          Row(3, 301, "support"))) // insert
    }
  }

  test("merge refreshed relation cache") {
    withTempView("temp", "source") {
      withCache("temp") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 100, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        // define a view on top of the table
        val query = sql(s"SELECT * FROM $tableNameAsString WHERE salary = 100")
        query.createOrReplaceTempView("temp")

        // cache the view
        sql("CACHE TABLE temp")

        // verify the view returns expected results
        checkAnswer(
          sql("SELECT * FROM temp"),
          Row(1, 100, "hr") :: Row(2, 100, "software") :: Nil)

        val sourceRows = Seq(
          ("support", 1, 101),
          ("support", 3, 301))
        sourceRows.toDF("dep", "pk", "salary").createOrReplaceTempView("source")

        // merge changes into the table
        spark.table("source")
          .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .merge()

        // verify the merge was successful
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 101, "support"), // update
            Row(2, 100, "software"), // unchanged
            Row(3, 301, "support"))) // insert

        // verify the view reflects the changes in the table
        checkAnswer(sql("SELECT * FROM temp"), Row(2, 100, "software") :: Nil)
      }
    }
  }

  test("merge with updates to nested struct fields in MATCHED clauses") {
    withTempView("source") {
      createAndInitTable(
        s"""pk INT NOT NULL,
           |s STRUCT<c1: INT, c2: STRUCT<a: ARRAY<INT>, m: MAP<STRING, STRING>>>,
           |dep STRING""".stripMargin,
        """{ "pk": 1, "s": { "c1": 2, "c2": { "a": [1,2], "m": { "a": "b" } } }, "dep": "hr" }""")

      Seq(1, 3).toDF("pk").createOrReplaceTempView("source")

      // update primitive, array, map columns inside a struct
      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched()
        .update(Map(
          "s.c1" -> lit(-1),
          "s.c2.m" -> map(lit('k'), lit('v')),
          "s.c2.a" -> array(lit(-1))
        ))
        .merge()
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(1, Row(-1, Row(Seq(-1), Map("k" -> "v"))), "hr")))

      // set primitive, array, map columns to NULL (proper casts should be in inserted)
      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched()
        .update(Map(
          "s.c1" -> lit(null),
          "s.c2" -> lit(null)
        ))
        .merge()
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, Row(null, null), "hr") :: Nil)

      // assign an entire struct
      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched()
        .update(Map(
          "s" -> struct(
            lit(1).as("c1"),
            struct(array(lit(1)).as("a"), lit(null).as("m")).as("c2"))
        ))
        .merge()
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, Row(1, Row(Seq(1), null)), "hr") :: Nil)
    }
  }

  test("merge with updates to nested struct fields in NOT MATCHED BY SOURCE clauses") {
    withTempView("source") {
      createAndInitTable(
        s"""pk INT NOT NULL,
           |s STRUCT<c1: INT, c2: STRUCT<a: ARRAY<INT>, m: MAP<STRING, STRING>>>,
           |dep STRING""".stripMargin,
        """{ "pk": 1, "s": { "c1": 2, "c2": { "a": [1,2], "m": { "a": "b" } } }, "dep": "hr" }""")

      Seq(2, 4).toDF("pk").createOrReplaceTempView("source")

      // update primitive, array, map columns inside a struct
      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenNotMatchedBySource()
        .update(Map(
          "s.c1" -> lit(-1),
          "s.c2.m" -> map(lit('k'), lit('v')),
          "s.c2.a" -> array(lit(-1))
        ))
        .merge()
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(1, Row(-1, Row(Seq(-1), Map("k" -> "v"))), "hr")))

      // set primitive, array, map columns to NULL (proper casts should be in inserted)
      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenNotMatchedBySource()
        .update(Map(
          "s.c1" -> lit(null),
          "s.c2" -> lit(null)
        ))
        .merge()
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, Row(null, null), "hr") :: Nil)

      // assign an entire struct
      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenNotMatchedBySource()
        .update(Map(
          "s" -> struct(
            lit(1).as("c1"),
            struct(array(lit(1)).as("a"), lit(null).as("m")).as("c2"))
        ))
        .merge()
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, Row(1, Row(Seq(1), null)), "hr") :: Nil)
    }
  }

  test("merge with char/varchar columns") {
    withTempView("source") {
      createTable("pk INT NOT NULL, s STRUCT<n_c: CHAR(3), n_vc: VARCHAR(5)>, dep STRING")

      append("pk INT NOT NULL, s STRUCT<n_c: STRING, n_vc: STRING>, dep STRING",
        """{ "pk": 1, "s": { "n_c": "aaa", "n_vc": "aaa" }, "dep": "hr" }
          |{ "pk": 2, "s": { "n_c": "bbb", "n_vc": "bbb" }, "dep": "software" }
          |{ "pk": 3, "s": { "n_c": "ccc", "n_vc": "ccc" }, "dep": "hr" }
          |""".stripMargin)

      Seq(1, 2, 4).toDF("pk").createOrReplaceTempView("source")

      spark.table("source")
        .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
        .whenMatched()
        .update(Map(
          "s.n_c" -> lit("x1"),
          "s.n_vc" -> lit("x2")
        ))
        .whenNotMatchedBySource()
        .update(Map(
          "s.n_c" -> lit("y1"),
          "s.n_vc" -> lit("y2")
        ))
        .merge()

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, Row("x1 ", "x2"), "hr"), // update (matched)
          Row(2, Row("x1 ", "x2"), "software"), // update (matched)
          Row(3, Row("y1 ", "y2"), "hr"))) // update (not matched by source)
    }
  }

  test("withSchemaEvolution carries over existing when clauses") {
    withTempView("source") {
      Seq(1, 2, 4).toDF("pk").createOrReplaceTempView("source")

      // an arbitrary merge
      val writer1 = spark.table("source")
        .mergeInto("dummy", $"col" === $"col")
        .whenMatched(col("col") === 1)
        .updateAll()
        .whenMatched()
        .delete()
        .whenNotMatched(col("col") === 1)
        .insertAll()
        .whenNotMatchedBySource(col("col") === 1)
        .delete()
        .asInstanceOf[MergeIntoWriterImpl[Row]]
      val writer2 = writer1.withSchemaEvolution()
        .asInstanceOf[MergeIntoWriterImpl[Row]]

      assert(writer1.matchedActions.length === 2)
      assert(writer1.notMatchedActions.length === 1)
      assert(writer1.notMatchedBySourceActions.length === 1)

      assert(writer1.matchedActions === writer2.matchedActions)
      assert(writer1.notMatchedActions === writer2.notMatchedActions)
      assert(writer1.notMatchedBySourceActions === writer2.notMatchedBySourceActions)
      assert(writer2.schemaEvolutionEnabled)
    }
  }
}
