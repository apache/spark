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

import org.apache.spark.SparkConf
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * DML (INSERT / INSERT OVERWRITE / UPDATE / MERGE / DELETE) over the nanosecond timestamp types
 * (`TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)`, `p` in `[7, 9]`), checking the sub-microsecond
 * remainder survives the write path and drives row matching. The target is an in-memory row-level
 * V2 table; standalone `DELETE` uses a range predicate because the in-memory catalog's equality
 * delete only handles partition columns. The two subclasses run ANSI on and off.
 */
abstract class TimestampNanosDmlSuiteBase extends SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")
    .set("spark.sql.catalog.testcat", classOf[InMemoryRowLevelOperationTableCatalog].getName)

  // A time-zone family: SQL type, resolved nanos type, and a literal from a 9-digit fraction.
  private case class Family(label: String, typ: String, nanosType: DataType, lit: String => String)
  private val ntz = Family("NTZ", "timestamp_ntz(9)", TimestampNTZNanosType(9),
    frac => s"TIMESTAMP_NTZ '2020-01-01 00:00:00.$frac'")
  private val ltz = Family("LTZ", "timestamp_ltz(9)", TimestampLTZNanosType(9),
    frac => s"TIMESTAMP_LTZ '2020-01-01 00:00:00.$frac UTC'")
  private val families = Seq(ntz, ltz)

  private val t = "testcat.ns.tgt"

  private def withV2Table(schema: String)(f: => Unit): Unit = {
    spark.sql(s"CREATE TABLE $t ($schema) USING foo")
    try f finally spark.sql(s"DROP TABLE IF EXISTS $t")
  }

  private def rows(sqlText: String): Seq[Row] = spark.sql(sqlText).collect().toSeq

  // Expected two-row (c, n) set for a family.
  private def pair(fam: Family, f1: String, n1: Int, f2: String, n2: Int): Seq[Row] =
    rows(s"SELECT * FROM VALUES (${fam.lit(f1)}, $n1), (${fam.lit(f2)}, $n2) AS v(c, n)")

  // Seed two (c, n) rows keyed within the same microsecond.
  private def insert2(fam: Family, f1: String, n1: Int, f2: String, n2: Int): Unit =
    spark.sql(s"INSERT INTO $t VALUES (${fam.lit(f1)}, $n1), (${fam.lit(f2)}, $n2)")

  families.foreach { fam =>
    test(s"${fam.label}: INSERT/OVERWRITE keep the nanos type and sub-micro value") {
      withV2Table(s"c ${fam.typ}, n int") {
        spark.sql(s"INSERT INTO $t VALUES (${fam.lit("000000001")}, 1)")
        spark.sql(s"INSERT INTO $t SELECT ${fam.lit("000000999")}, 2")
        assert(spark.sql(s"SELECT c FROM $t").schema.head.dataType === fam.nanosType)
        checkAnswer(spark.sql(s"SELECT c, n FROM $t"), pair(fam, "000000001", 1, "000000999", 2))
        spark.sql(s"INSERT OVERWRITE $t VALUES (${fam.lit("000000500")}, 5)")
        checkAnswer(spark.sql(s"SELECT c, n FROM $t"), rows(s"SELECT ${fam.lit("000000500")}, 5"))
      }
    }

    test(s"${fam.label}: UPDATE ... SET targets the row at a sub-microsecond key") {
      withV2Table(s"c ${fam.typ}, n int") {
        insert2(fam, "000000001", 1, "000000999", 2)
        // Only the .000000999 row is updated.
        spark.sql(s"UPDATE $t SET n = 99 WHERE c = ${fam.lit("000000999")}")
        checkAnswer(spark.sql(s"SELECT c, n FROM $t"), pair(fam, "000000001", 1, "000000999", 99))
      }
    }

    test(s"${fam.label}: MERGE handles all three WHEN arms on a nanos key") {
      withV2Table(s"c ${fam.typ}, n int") {
        insert2(fam, "000000001", 1, "000000009", 9)
        spark.sql(
          s"""MERGE INTO $t t
             |USING (SELECT * FROM VALUES
             |    (${fam.lit("000000009")}, 900),
             |    (${fam.lit("000000123")}, 123) AS s(c, n)) s
             |ON t.c = s.c
             |WHEN MATCHED THEN UPDATE SET t.n = s.n
             |WHEN NOT MATCHED THEN INSERT (c, n) VALUES (s.c, s.n)
             |WHEN NOT MATCHED BY SOURCE THEN DELETE""".stripMargin)
        // .009 updated, .123 inserted, .001 deleted (not matched by source).
        checkAnswer(spark.sql(s"SELECT c, n FROM $t"),
          pair(fam, "000000009", 900, "000000123", 123))
      }
    }

    test(s"${fam.label}: DELETE FROM removes the sub-micro row via row-level rewrite") {
      withV2Table(s"c ${fam.typ}, n int") {
        insert2(fam, "000000001", 1, "000000999", 2)
        // Range predicate -> rewrite; .000000999 > .000000500 deleted, .000000001 kept.
        spark.sql(s"DELETE FROM $t WHERE c > ${fam.lit("000000500")}")
        checkAnswer(spark.sql(s"SELECT c, n FROM $t"), rows(s"SELECT ${fam.lit("000000001")}, 1"))
      }
    }
  }

  test("cross-precision write widens a p=7 source into a p=9 target") {
    withV2Table("c timestamp_ntz(9), n int") {
      spark.sql(s"INSERT INTO $t SELECT '2020-01-01 00:00:00.0000001' :: timestamp_ntz(7), 1")
      assert(spark.sql(s"SELECT c FROM $t").schema.head.dataType === TimestampNTZNanosType(9))
      // .0000001 at p=7 (100 ns) stores as .000000100 at p=9.
      checkAnswer(spark.sql(s"SELECT c, n FROM $t"),
        rows("SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000100', 1"))
    }
  }
}

class TimestampNanosDmlAnsiOnSuite extends TimestampNanosDmlSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

class TimestampNanosDmlAnsiOffSuite extends TimestampNanosDmlSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
