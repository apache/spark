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
package org.apache.spark.sql

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.{ConnectFunSuite, RemoteSparkSession}

class MergeIntoE2ETestSuite extends ConnectFunSuite with RemoteSparkSession {

  lazy val session: SparkSession = spark

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      "spark.sql.catalog.testcat",
      "org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog")
  }

  private val sourceRows = Seq((1, 100, "hr"), (2, 200, "finance"), (3, 300, "hr"))

  private def withSourceView(f: String => Unit): Unit = {
    import session.implicits._

    val name = "source"
    sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView(name)
    try {
      f(name)
    } finally {
      spark.sql(s"DROP VIEW IF EXISTS $name")
    }
  }

  private def withTargetTable(f: String => Unit): Unit =
    withTargetTable("pk INT NOT NULL, salary INT, dep STRING")(f)

  private def withTargetTable(schema: String)(f: String => Unit): Unit = {
    val name = "testcat.ns1.target"
    spark.sql(s"CREATE OR REPLACE TABLE $name ($schema)")
    try {
      f(name)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $name")
    }
  }

  private def checkTable(target: String, rows: Seq[(Int, Int, String)]): Unit = {
    val actual = spark.sql(s"select * from $target").collect().sortBy(_.getInt(0))
    val expected = rows.sortBy(_._1).map(Row.fromTuple).toArray
    assert(actual === expected)
  }

  test("insert and update and delete") {
    import session.implicits._

    withSourceView { source =>
      withTargetTable { target =>
        spark.sql(s"INSERT INTO $target VALUES (1, 100, 'hr'), (2, 200, 'finance')")
        spark
          .table("source")
          .mergeInto(target, $"$source.pk" === $"$target.pk")
          .whenMatched($"$source.pk" === 2)
          .delete()
          .whenMatched()
          .update(Map("salary" -> lit(999999)))
          .whenNotMatched()
          .insert(Map("pk" -> $"pk", "salary" -> $"salary", "dep" -> $"dep"))
          .merge()

        checkTable(target, Seq((1, 999999, "hr"), (3, 300, "hr")))
      }
    }
  }

  test("insertAll and updateAll") {
    import session.implicits._

    withSourceView { source =>
      withTargetTable { target =>
        spark.sql(s"INSERT INTO $target VALUES (1, 100, 'hr'), (2, 200, 'mgr')")
        spark
          .table("source")
          .mergeInto(target, $"$source.pk" === $"$target.pk")
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .merge()

        checkTable(target, Seq((1, 100, "hr"), (2, 200, "finance"), (3, 300, "hr")))
      }
    }
  }

  test("insertAll and delete") {
    import session.implicits._

    withSourceView { source =>
      withTargetTable { target =>
        spark.sql(s"INSERT INTO $target VALUES (1, 100, 'hr'), (2, 200, 'finance')")
        spark
          .table("source")
          .mergeInto(target, $"$source.pk" === $"$target.pk")
          .whenMatched()
          .delete()
          .whenNotMatched()
          .insertAll()
          .merge()

        checkTable(target, Seq((3, 300, "hr")))
      }
    }
  }

  test("insertAll and update with condition") {
    import session.implicits._

    withSourceView { source =>
      withTargetTable { target =>
        spark.sql(s"INSERT INTO $target VALUES (1, 100, 'hr'), (2, 200, 'finance')")
        spark
          .table("source")
          .mergeInto(target, $"$source.pk" === $"$target.pk")
          .whenMatched($"$source.pk" === 1)
          .update(Map("salary" -> lit(999999)))
          .whenNotMatched()
          .insertAll()
          .merge()

        checkTable(target, Seq((1, 999999, "hr"), (2, 200, "finance"), (3, 300, "hr")))
      }
    }
  }

  test("insertAll and delete with condition") {
    import session.implicits._

    withSourceView { source =>
      withTargetTable { target =>
        spark.sql(s"INSERT INTO $target VALUES (1, 100, 'hr'), (2, 200, 'finance')")
        spark
          .table("source")
          .mergeInto(target, $"$source.pk" === $"$target.pk")
          .whenMatched($"$source.pk" === 1)
          .delete()
          .whenNotMatched()
          .insertAll()
          .merge()

        checkTable(target, Seq((2, 200, "finance"), (3, 300, "hr")))
      }
    }
  }

  test("when not matched by source") {
    import session.implicits._

    withSourceView { source =>
      withTargetTable { target =>
        spark.sql(s"INSERT INTO $target VALUES (9, 99, 'eng')")
        spark
          .table("source")
          .mergeInto(target, $"$source.pk" === $"$target.pk")
          .whenNotMatched()
          .insertAll()
          .whenNotMatchedBySource()
          .delete()
          .merge()

        checkTable(target, Seq((1, 100, "hr"), (2, 200, "finance"), (3, 300, "hr")))
      }
    }
  }
}
