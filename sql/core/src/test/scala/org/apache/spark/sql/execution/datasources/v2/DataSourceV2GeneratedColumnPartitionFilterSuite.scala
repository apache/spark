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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.connector.DatasourceV2SQLBase
import org.apache.spark.sql.connector.catalog.InMemoryTableWithCatalystFilter
import org.apache.spark.sql.execution.ExplainUtils.stripAQEPlan

/**
 * End-to-end tests that create a generated-column-partitioned table, write rows, and query it,
 * verifying that partition filters derived from base-column filters are pushed to the data source,
 * prune partitions, and keep query results correct.
 *
 * Uses a data source whose scan builder implements
 * [[org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters]], so the derived
 * Catalyst filters (which may contain casts) are received without translation loss.
 */
class DataSourceV2GeneratedColumnPartitionFilterSuite extends QueryTest with DatasourceV2SQLBase {

  private type CatalystScan = InMemoryTableWithCatalystFilter#InMemoryCatalystFilterBatchScan

  private val cat = "catalystcat"

  private def withCatalystFilterCatalog(f: => Unit): Unit = {
    withSQLConf(
      s"spark.sql.catalog.$cat" -> classOf[
        org.apache.spark.sql.connector.catalog.InMemoryTableWithCatalystFilterCatalog].getName) {
      f
    }
  }

  private def scanOf(df: DataFrame): CatalystScan = {
    val batchScan = stripAQEPlan(df.queryExecution.executedPlan).collectFirst {
      case b: BatchScanExec => b
    }.getOrElse(throw new AssertionError("Expected a BatchScanExec in the plan"))
    batchScan.scan match {
      case s: CatalystScan => s
      case other => throw new AssertionError(s"Unexpected scan type: ${other.getClass.getName}")
    }
  }

  private def pushedPartitionFilterRefs(df: DataFrame): Set[String] =
    scanOf(df).pushedPartitionFilters.flatMap(_.references.map(_.name)).toSet

  private def numPartitionsRead(df: DataFrame): Int = scanOf(df)._data.size

  test("date-partitioned generated column: derive, push, and prune") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  eventTime TIMESTAMP,
             |  date DATE GENERATED ALWAYS AS (CAST(eventTime AS DATE))
             |) USING foo PARTITIONED BY (date)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, eventTime) VALUES
             |  (1, TIMESTAMP '2020-12-31 11:00:00'),
             |  (2, TIMESTAMP '2021-01-01 12:00:00'),
             |  (3, TIMESTAMP '2021-01-02 13:00:00')""".stripMargin)

        val query =
          s"""SELECT id FROM $cat.ns.events
             |WHERE eventTime >= TIMESTAMP '2021-01-01 12:00:00'
             |  AND eventTime <= TIMESTAMP '2021-01-01 18:00:00'""".stripMargin

        val df = sql(query)
        checkAnswer(df, Row(2))
        assert(pushedPartitionFilterRefs(df).contains("date"),
          "expected a derived partition filter on `date` to be pushed")
        assert(numPartitionsRead(df) == 1,
          "expected only the matching date partition to be read")
      }
    }
  }

  test("no partition filter is derived when the source does not opt in") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  eventTime TIMESTAMP,
             |  date DATE GENERATED ALWAYS AS (CAST(eventTime AS DATE))
             |) USING foo PARTITIONED BY (date)
             |TBLPROPERTIES (
             |  '${InMemoryTableWithCatalystFilter.INFER_GENERATED_COLUMN_PARTITION_FILTERS}'
             |    = 'false')""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, eventTime) VALUES
             |  (1, TIMESTAMP '2020-12-31 11:00:00'),
             |  (2, TIMESTAMP '2021-01-01 12:00:00'),
             |  (3, TIMESTAMP '2021-01-02 13:00:00')""".stripMargin)

        val df = sql(
          s"""SELECT id FROM $cat.ns.events
             |WHERE eventTime >= TIMESTAMP '2021-01-01 12:00:00'
             |  AND eventTime <= TIMESTAMP '2021-01-01 18:00:00'""".stripMargin)
        // Results are still correct, but no `date` partition filter is derived, so nothing is
        // pushed on the partition column and all partitions are read.
        checkAnswer(df, Row(2))
        assert(!pushedPartitionFilterRefs(df).contains("date"),
          "expected no derived partition filter on `date` when the source opts out")
        assert(numPartitionsRead(df) == 3, "expected all partitions to be read (no pruning)")
      }
    }
  }

  test("year/month/day-partitioned generated column: composite filter prunes") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  eventTime TIMESTAMP,
             |  year INT GENERATED ALWAYS AS (YEAR(eventTime)),
             |  month INT GENERATED ALWAYS AS (MONTH(eventTime)),
             |  day INT GENERATED ALWAYS AS (DAY(eventTime))
             |) USING foo PARTITIONED BY (year, month, day)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, eventTime) VALUES
             |  (1, TIMESTAMP '2021-06-14 10:00:00'),
             |  (2, TIMESTAMP '2021-06-15 10:00:00'),
             |  (3, TIMESTAMP '2021-06-16 10:00:00')""".stripMargin)

        val query =
          s"SELECT id FROM $cat.ns.events WHERE eventTime = TIMESTAMP '2021-06-15 10:00:00'"

        val df = sql(query)
        checkAnswer(df, Row(2))
        val refs = pushedPartitionFilterRefs(df)
        assert(Set("year", "month", "day").subsetOf(refs),
          s"expected derived partition filters on year/month/day, got $refs")
        assert(numPartitionsRead(df) == 1, "expected only the matching partition to be read")
      }
    }
  }

  test("substring-partitioned generated column handles multibyte characters") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  value STRING,
             |  prefix STRING GENERATED ALWAYS AS (SUBSTRING(value, 1, 2))
             |) USING foo PARTITIONED BY (prefix)""".stripMargin)
        // SUBSTRING counts characters, not bytes, so the generated partition value is the first
        // two characters of the multibyte string (\u4e00\u4e8c\u4e09\u56db -> \u4e00\u4e8c).
        sql(s"INSERT INTO $cat.ns.events (id, value) VALUES (1, '\u4e00\u4e8c\u4e09\u56db')")
        val df = sql(s"SELECT value FROM $cat.ns.events WHERE value > 'abcd'")
        checkAnswer(df, Row("\u4e00\u4e8c\u4e09\u56db"))
        assert(pushedPartitionFilterRefs(df).contains("prefix"),
          "expected a derived partition filter on `prefix` to be pushed")
      }
    }
  }

  test("five digit year in a year/month/day partition column") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  eventTime TIMESTAMP,
             |  year INT GENERATED ALWAYS AS (YEAR(eventTime)),
             |  month INT GENERATED ALWAYS AS (MONTH(eventTime)),
             |  day INT GENERATED ALWAYS AS (DAY(eventTime))
             |) USING foo PARTITIONED BY (year, month, day)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, eventTime) VALUES
             |  (1, CAST('12345-07-15 18:00:00' AS TIMESTAMP)),
             |  (2, TIMESTAMP '2021-06-15 10:00:00')""".stripMargin)

        val query = s"SELECT id, year, month, day FROM $cat.ns.events " +
          "WHERE eventTime = CAST('12345-07-15 18:00:00' AS TIMESTAMP)"

        val df = sql(query)
        checkAnswer(df, Row(1, 12345, 7, 15))
        assert(Set("year", "month", "day").subsetOf(pushedPartitionFilterRefs(df)))
        assert(numPartitionsRead(df) == 1, "expected only the five-digit-year partition to read")
      }
    }
  }

  test("five digit year in a date_format yyyy-MM partition column") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  eventTime TIMESTAMP,
             |  month STRING GENERATED ALWAYS AS (DATE_FORMAT(eventTime, 'yyyy-MM'))
             |) USING foo PARTITIONED BY (month)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, eventTime) VALUES
             |  (1, CAST('12345-07-15 18:00:00' AS TIMESTAMP)),
             |  (2, TIMESTAMP '2021-06-15 10:00:00')""".stripMargin)

        // A five-digit year is formatted with a leading '+' by date_format under the default
        // (CORRECTED) time parser policy.
        val query = s"SELECT id, month FROM $cat.ns.events " +
          "WHERE eventTime = CAST('12345-07-15 18:00:00' AS TIMESTAMP)"

        val df = sql(query)
        checkAnswer(df, Row(1, "+12345-07"))
        assert(pushedPartitionFilterRefs(df).contains("month"))
        assert(numPartitionsRead(df) == 1, "expected only the five-digit-year partition to read")
      }
    }
  }

  test("null generated partition value is not pruned by the derived filter") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  eventTime TIMESTAMP,
             |  date DATE GENERATED ALWAYS AS (CAST(eventTime AS DATE))
             |) USING foo PARTITIONED BY (date)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, eventTime) VALUES
             |  (1, TIMESTAMP '2021-01-01 12:00:00'),
             |  (2, TIMESTAMP '2021-01-02 12:00:00'),
             |  (3, CAST(NULL AS TIMESTAMP))""".stripMargin)

        val df = sql(s"SELECT id FROM $cat.ns.events WHERE " +
          "eventTime = TIMESTAMP '2021-01-01 12:00:00'")
        // Only the matching row is returned; the null-eventTime row is filtered post-scan.
        checkAnswer(df, Row(1))
        // The derived filter's `IS NULL` guard keeps the null partition, so it is not pruned:
        // the matching date partition and the null partition are read, but not '2021-01-02'.
        assert(numPartitionsRead(df) == 2,
          "expected the matching and the null partitions to be read, but not the third")
      }
    }
  }

  test("date_format partition column written and read across time parser policies") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  eventTime TIMESTAMP,
             |  month STRING GENERATED ALWAYS AS (DATE_FORMAT(eventTime, 'yyyy-MM'))
             |) USING foo PARTITIONED BY (month)""".stripMargin)
        // Write each row under a different policy so the stored partition value carries that
        // policy's format: LEGACY drops the leading '+' for 5-digit years, CORRECTED keeps it.
        withSQLConf("spark.sql.legacy.timeParserPolicy" -> "LEGACY") {
          sql(s"INSERT INTO $cat.ns.events (id, eventTime) VALUES " +
            "(1, CAST('+23456-07-20 18:30:00' AS TIMESTAMP))") // month = '23456-07'
        }
        withSQLConf("spark.sql.legacy.timeParserPolicy" -> "CORRECTED") {
          sql(s"INSERT INTO $cat.ns.events (id, eventTime) VALUES " +
            "(2, CAST('+30000-12-30 20:00:00' AS TIMESTAMP))") // month = '+30000-12'
        }

        val query = s"SELECT id FROM $cat.ns.events WHERE " +
          "eventTime >= CAST('20000-01-01 12:00:00' AS TIMESTAMP)"

        // Under each read policy, unix_timestamp() returns null for the partition written in the
        // other policy's format; the derived filter's IS NULL guard must keep it so no row is lost.
        Seq("CORRECTED", "LEGACY").foreach { policy =>
          withSQLConf(
            "spark.sql.legacy.timeParserPolicy" -> policy,
            "spark.sql.ansi.enabled" -> "false") {
            val df = sql(query)
            checkAnswer(df, Seq(Row(1), Row(2)))
            assert(pushedPartitionFilterRefs(df).contains("month"),
              "expected a derived partition filter on `month` to be pushed")
            assert(numPartitionsRead(df) == 2,
              "expected both partitions to be read (the null-yielding one must not be pruned)")
          }
        }
      }
    }
  }

  test("date_trunc-partitioned generated column: range filter prunes") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  eventTime TIMESTAMP,
             |  dt TIMESTAMP GENERATED ALWAYS AS (date_trunc('DAY', eventTime))
             |) USING foo PARTITIONED BY (dt)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, eventTime) VALUES
             |  (1, TIMESTAMP '2021-01-01 05:00:00'),
             |  (2, TIMESTAMP '2021-01-02 12:00:00'),
             |  (3, TIMESTAMP '2021-01-03 20:00:00')""".stripMargin)

        val df = sql(
          s"""SELECT id FROM $cat.ns.events
             |WHERE eventTime >= TIMESTAMP '2021-01-02 00:00:00'
             |  AND eventTime <= TIMESTAMP '2021-01-02 23:59:59'""".stripMargin)
        checkAnswer(df, Row(2))
        assert(pushedPartitionFilterRefs(df).contains("dt"),
          "expected a derived partition filter on `dt` to be pushed")
        assert(numPartitionsRead(df) == 1,
          "expected only the matching day partition to be read")
      }
    }
  }

  test("trunc-partitioned generated column: range filter prunes") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  d DATE,
             |  monthStart DATE GENERATED ALWAYS AS (trunc(d, 'MM'))
             |) USING foo PARTITIONED BY (monthStart)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, d) VALUES
             |  (1, DATE '2021-01-15'),
             |  (2, DATE '2021-02-15'),
             |  (3, DATE '2021-03-15')""".stripMargin)

        val df = sql(
          s"""SELECT id FROM $cat.ns.events
             |WHERE d >= DATE '2021-02-01' AND d <= DATE '2021-02-28'""".stripMargin)
        checkAnswer(df, Row(2))
        assert(pushedPartitionFilterRefs(df).contains("monthStart"),
          "expected a derived partition filter on `monthStart` to be pushed")
        assert(numPartitionsRead(df) == 1,
          "expected only the matching month partition to be read")
      }
    }
  }

  test("year/month/day/hour-partitioned generated column: composite filter prunes") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  eventTime TIMESTAMP,
             |  year INT GENERATED ALWAYS AS (YEAR(eventTime)),
             |  month INT GENERATED ALWAYS AS (MONTH(eventTime)),
             |  day INT GENERATED ALWAYS AS (DAY(eventTime)),
             |  hour INT GENERATED ALWAYS AS (HOUR(eventTime))
             |) USING foo PARTITIONED BY (year, month, day, hour)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, eventTime) VALUES
             |  (1, TIMESTAMP '2021-06-15 09:30:00'),
             |  (2, TIMESTAMP '2021-06-15 10:30:00'),
             |  (3, TIMESTAMP '2021-06-15 11:30:00')""".stripMargin)

        val df = sql(
          s"SELECT id FROM $cat.ns.events WHERE eventTime = TIMESTAMP '2021-06-15 10:30:00'")
        checkAnswer(df, Row(2))
        val refs = pushedPartitionFilterRefs(df)
        assert(Set("year", "month", "day", "hour").subsetOf(refs),
          s"expected derived partition filters on year/month/day/hour, got $refs")
        assert(numPartitionsRead(df) == 1, "expected only the matching partition to be read")
      }
    }
  }

  test("range < and > on a date partition column retain the boundary partition") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  eventTime TIMESTAMP,
             |  date DATE GENERATED ALWAYS AS (CAST(eventTime AS DATE))
             |) USING foo PARTITIONED BY (date)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, eventTime) VALUES
             |  (1, TIMESTAMP '2021-01-01 10:00:00'),
             |  (2, TIMESTAMP '2021-01-02 10:00:00'),
             |  (3, TIMESTAMP '2021-01-03 10:00:00')""".stripMargin)

        // `<` on eventTime becomes `<=` on the truncated `date`, so the boundary partition
        // 2021-01-02 is retained even though its only row is excluded from the answer.
        val ltDf = sql(
          s"SELECT id FROM $cat.ns.events WHERE eventTime < TIMESTAMP '2021-01-02 00:00:00'")
        checkAnswer(ltDf, Row(1))
        assert(pushedPartitionFilterRefs(ltDf).contains("date"))
        assert(numPartitionsRead(ltDf) == 2,
          "expected the matching and boundary partitions to be read, but not 2021-01-03")

        // `>` on eventTime becomes `>=` on `date`, so the boundary partition 2021-01-02 is kept.
        val gtDf = sql(
          s"SELECT id FROM $cat.ns.events WHERE eventTime > TIMESTAMP '2021-01-02 23:00:00'")
        checkAnswer(gtDf, Row(3))
        assert(pushedPartitionFilterRefs(gtDf).contains("date"))
        assert(numPartitionsRead(gtDf) == 2,
          "expected the matching and boundary partitions to be read, but not 2021-01-01")
      }
    }
  }

  test("substring with pos > 1 does not derive range partition filters") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  value STRING,
             |  mid STRING GENERATED ALWAYS AS (SUBSTRING(value, 2, 3))
             |) USING foo PARTITIONED BY (mid)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, value) VALUES
             |  (1, 'abcdef'),
             |  (2, 'xbcyz')""".stripMargin)

        // For a substring starting past the first character, the base column's ordering is not
        // preserved by the partition value, so range filters must not derive a partition filter.
        val df = sql(s"SELECT id FROM $cat.ns.events WHERE value > 'abcd'")
        checkAnswer(df, Seq(Row(1), Row(2)))
        assert(!pushedPartitionFilterRefs(df).contains("mid"),
          "expected no derived partition filter on `mid` for a range filter when pos > 1")
        assert(numPartitionsRead(df) == 2, "expected all partitions to be read (no pruning)")
      }
    }
  }

  test("empty string base value retains both the matching and null partitions") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  value STRING,
             |  prefix STRING GENERATED ALWAYS AS (SUBSTRING(value, 1, 4))
             |) USING foo PARTITIONED BY (prefix)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, value) VALUES
             |  (1, ''),
             |  (2, 'abcd'),
             |  (3, CAST(NULL AS STRING))""".stripMargin)

        // The derived equality filter is `prefix IS NULL OR prefix = SUBSTRING('', 1, 4)`. The
        // IS NULL guard keeps the null partition (a file source may store the empty-string value
        // of `prefix` as null, per SPARK-24438) so no matching row can be lost, while the answer
        // stays correct.
        val df = sql(s"SELECT id FROM $cat.ns.events WHERE value = ''")
        checkAnswer(df, Row(1))
        assert(pushedPartitionFilterRefs(df).contains("prefix"))
        assert(numPartitionsRead(df) == 2,
          "expected the empty-string and null partitions to be read, but not 'abcd'")
      }
    }
  }

  test("case-insensitive analysis matches differently-cased base column filters") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  eventTime TIMESTAMP,
             |  date DATE GENERATED ALWAYS AS (CAST(eventTime AS DATE))
             |) USING foo PARTITIONED BY (date)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, eventTime) VALUES
             |  (1, TIMESTAMP '2020-12-31 11:00:00'),
             |  (2, TIMESTAMP '2021-01-01 12:00:00'),
             |  (3, TIMESTAMP '2021-01-02 13:00:00')""".stripMargin)

        withSQLConf("spark.sql.caseSensitive" -> "false") {
          // Differently-cased base column in the filter must still derive after analysis.
          val df = sql(
            s"""SELECT id FROM $cat.ns.events
               |WHERE EVENTTIME >= TIMESTAMP '2021-01-01 12:00:00'
               |  AND EVENTTIME <= TIMESTAMP '2021-01-01 18:00:00'""".stripMargin)
          checkAnswer(df, Row(2))
          assert(pushedPartitionFilterRefs(df).contains("date"))
          assert(numPartitionsRead(df) == 1)
        }

        withSQLConf("spark.sql.caseSensitive" -> "true") {
          val df = sql(
            s"""SELECT id FROM $cat.ns.events
               |WHERE eventTime >= TIMESTAMP '2021-01-01 12:00:00'
               |  AND eventTime <= TIMESTAMP '2021-01-01 18:00:00'""".stripMargin)
          checkAnswer(df, Row(2))
          assert(pushedPartitionFilterRefs(df).contains("date"))
          assert(numPartitionsRead(df) == 1)
        }
      }
    }
  }

  test("filters on two independent generated partition columns both prune") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  eventTime TIMESTAMP,
             |  region STRING,
             |  date DATE GENERATED ALWAYS AS (CAST(eventTime AS DATE)),
             |  regionPart STRING GENERATED ALWAYS AS (region)
             |) USING foo PARTITIONED BY (date, regionPart)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, eventTime, region) VALUES
             |  (1, TIMESTAMP '2021-01-01 12:00:00', 'us'),
             |  (2, TIMESTAMP '2021-01-01 12:00:00', 'eu'),
             |  (3, TIMESTAMP '2021-01-02 12:00:00', 'us')""".stripMargin)

        val df = sql(
          s"""SELECT id FROM $cat.ns.events
             |WHERE eventTime = TIMESTAMP '2021-01-01 12:00:00' AND region = 'us'""".stripMargin)
        checkAnswer(df, Row(1))
        val refs = pushedPartitionFilterRefs(df)
        assert(Set("date", "regionPart").subsetOf(refs),
          s"expected derived filters on both partition columns, got $refs")
        assert(numPartitionsRead(df) == 1,
          "expected only the matching (date, region) partition to be read")
      }
    }
  }

  test("nested base column: cast(struct.field AS DATE) derives and prunes") {
    withCatalystFilterCatalog {
      withTable(s"$cat.ns.events") {
        sql(
          s"""CREATE TABLE $cat.ns.events (
             |  id INT,
             |  nested STRUCT<eventTime: TIMESTAMP>,
             |  date DATE GENERATED ALWAYS AS (CAST(nested.eventTime AS DATE))
             |) USING foo PARTITIONED BY (date)""".stripMargin)
        sql(
          s"""INSERT INTO $cat.ns.events (id, nested) VALUES
             |  (1, named_struct('eventTime', TIMESTAMP '2020-12-31 11:00:00')),
             |  (2, named_struct('eventTime', TIMESTAMP '2021-01-01 12:00:00')),
             |  (3, named_struct('eventTime', TIMESTAMP '2021-01-02 13:00:00'))""".stripMargin)

        val df = sql(
          s"""SELECT id FROM $cat.ns.events
             |WHERE nested.eventTime >= TIMESTAMP '2021-01-01 12:00:00'
             |  AND nested.eventTime <= TIMESTAMP '2021-01-01 18:00:00'""".stripMargin)
        checkAnswer(df, Row(2))
        assert(pushedPartitionFilterRefs(df).contains("date"))
        assert(numPartitionsRead(df) == 1)
      }
    }
  }
}
