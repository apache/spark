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

import java.time.{Instant, LocalDateTime}

import org.apache.spark.{SparkArrayIndexOutOfBoundsException, SparkConf, SparkDateTimeException}
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.errors.DataTypeErrors.{toSQLConf, toSQLType}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Special datetime strings (`epoch`, `now`, `today`, `tomorrow`, `yesterday`) cast to
 * `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)`, compared with the micro types. The two subclasses
 * run ANSI on and off.
 */
abstract class TimestampNanosSpecialValuesSuiteBase extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)

  // A time-zone family: micro type name, nanos type, and its epoch value.
  private case class Family(micro: String, nanos: Int => DataType, epoch: Any)
  private val families = Seq(
    Family("TIMESTAMP_NTZ", TimestampNTZNanosType(_), LocalDateTime.of(1970, 1, 1, 0, 0)),
    Family("TIMESTAMP_LTZ", TimestampLTZNanosType(_), Instant.EPOCH))

  private def forAll(f: (Family, Int, String) => Unit): Unit = {
    for (fam <- families; p <- 7 to 9) f(fam, p, s"${fam.micro}($p)")
  }

  test("foldable casts of special strings equal the micro value") {
    forAll { (fam, p, t) =>
      Seq("epoch", " Epoch ", "today", "TOMORROW", "yesterday").foreach { s =>
        val df = sql(s"SELECT CAST('$s' AS $t), try_cast('$s' AS $t), " +
          s"CAST(CAST('$s' AS ${fam.micro}) AS $t)")
        assert(df.schema.map(_.dataType) === Seq.fill(3)(fam.nanos(p)), s"'$s' AS $t")
        val row = df.head()
        assert(!row.isNullAt(2), s"'$s' AS ${fam.micro}")
        assert(row.get(0) === row.get(2), s"'$s' AS $t")
        assert(row.get(1) === row.get(2), s"try_cast('$s' AS $t)")
      }
      checkAnswer(sql(s"SELECT CAST('epoch' AS $t)"), Row(fam.epoch))
    }
  }

  test("'now' is the current timestamp floored to the precision") {
    forAll { (fam, p, t) =>
      val current = if (fam.micro == "TIMESTAMP_NTZ") "localtimestamp()" else "current_timestamp()"
      val df = sql(s"SELECT CAST('now' AS $t) AS c, " +
        s"CAST('now' AS $t) BETWEEN $current - INTERVAL 1 MINUTE AND $current + INTERVAL 1 MINUTE")
      assert(df.schema.head.dataType === fam.nanos(p))
      val row = df.head()
      assert(row.getBoolean(1), s"'now' AS $t")
      val nano = row.get(0) match {
        case ldt: LocalDateTime => ldt.getNano
        case i: Instant => i.getNano
      }
      assert(nano % math.pow(10, 9 - p).toInt === 0, s"'now' AS $t")
    }
  }

  test("non-special and non-foldable strings are not converted") {
    import testImplicits._
    val df = Seq("epoch").toDF("s")
    forAll { (fam, p, t) =>
      Seq(
        "epochs" -> s"CAST('epochs' AS $t)",
        "now UTC" -> s"CAST('now UTC' AS $t)",
        "epoch" -> s"CAST(s AS $t)").foreach { case (s, expr) =>
        val res = df.selectExpr(expr)
        if (conf.ansiEnabled) {
          checkError(
            exception = intercept[SparkDateTimeException](res.collect()),
            condition = "CAST_INVALID_INPUT",
            parameters = Map(
              "expression" -> s"'$s'",
              "sourceType" -> toSQLType(StringType),
              "targetType" -> toSQLType(fam.nanos(p)),
              "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
            context = ExpectedContext(expr))
        } else {
          checkAnswer(res, Row(null))
        }
      }
      checkAnswer(sql(s"SELECT try_cast('epochs' AS $t)"), Row(null))
      checkAnswer(df.selectExpr(s"try_cast(s AS $t)"), Row(null))
    }
  }

  test("a failing foldable string fails the query only when it is evaluated") {
    forAll { (fam, p, t) =>
      Seq(t, fam.micro).foreach { typ =>
        val failing = s"CAST(element_at(array('a'), 2) AS $typ)"
        checkAnswer(
          sql(s"SELECT CASE WHEN id < 0 THEN $failing END FROM range(2)"),
          Seq(Row(null), Row(null)))
        if (conf.ansiEnabled) {
          checkError(
            exception = intercept[SparkArrayIndexOutOfBoundsException](
              sql(s"SELECT $failing").collect()),
            condition = "INVALID_ARRAY_INDEX_IN_ELEMENT_AT",
            parameters = Map("indexValue" -> "2", "arraySize" -> "1"),
            context = ExpectedContext("element_at(array('a'), 2)"))
        } else {
          checkAnswer(sql(s"SELECT $failing"), Row(null))
        }
      }
    }
  }

  test("implicit casts of special strings match the micro column") {
    forAll { (fam, p, t) =>
      withTable("t") {
        sql(s"CREATE TABLE t (c6 ${fam.micro}, c9 $t) USING parquet")
        sql(s"INSERT INTO t SELECT CAST(s AS ${fam.micro}), CAST(s AS $t) FROM VALUES " +
          "('1970-01-01 00:00:00Z'), ('2020-01-01 00:00:00.000000001Z') v(s)")
        Seq("= 'epoch'", "> ' Epoch '", "<= 'epoch'", "< 'tomorrow'", "IN ('epoch', 'today')",
            "BETWEEN 'epoch' AND 'today'").foreach { pred =>
          checkAnswer(
            sql(s"SELECT c6 FROM t WHERE c9 $pred"),
            sql(s"SELECT c6 FROM t WHERE c6 $pred"))
        }
        assert(sql("SELECT * FROM t WHERE c9 = 'epoch'").count() === 1)
      }
      checkAnswer(
        sql(s"SELECT CAST(coalesce(CAST(NULL AS $t), 'epoch') AS STRING)"),
        sql(s"SELECT CAST(coalesce(CAST(NULL AS ${fam.micro}), 'epoch') AS STRING)"))
    }
  }

  test("column defaults of special-string casts") {
    forAll { (fam, p, t) =>
      withTable("t") {
        sql(s"CREATE TABLE t (id INT, c $t DEFAULT CAST('epoch' AS $t), " +
          s"n $t DEFAULT CAST('now' AS $t)) USING json")
        sql("INSERT INTO t (id) VALUES (1)")
        sql(s"ALTER TABLE t ADD COLUMN d $t DEFAULT CAST(' Epoch ' AS $t)")
        checkAnswer(sql("SELECT c, d, n IS NOT NULL FROM t"), Row(fam.epoch, fam.epoch, true))
      }
    }
  }

  test("static partition values of special strings") {
    forAll { (fam, p, t) =>
      val tbl = "testcat.ns.t"
      withTable(tbl) {
        sql(s"CREATE TABLE $tbl (id INT, c $t) USING foo PARTITIONED BY (c)")
        sql(s"INSERT INTO $tbl PARTITION (c = 'epoch') VALUES (1)")
        checkAnswer(sql(s"SELECT id, c FROM $tbl"), Row(1, fam.epoch))
      }
    }
  }
}

class TimestampNanosSpecialValuesAnsiOnSuite extends TimestampNanosSpecialValuesSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

class TimestampNanosSpecialValuesAnsiOffSuite extends TimestampNanosSpecialValuesSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
