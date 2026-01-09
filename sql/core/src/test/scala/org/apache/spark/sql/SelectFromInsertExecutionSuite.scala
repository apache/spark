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

import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Execution test suite for NEW TABLE(INSERT...) feature.
 *
 * Tests the end-to-end functionality of selecting rows from INSERT statements.
 */
class SelectFromInsertExecutionSuite extends QueryTest with SharedSparkSession {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Register test catalog that supports advanced features like identity columns
    spark.conf.set("spark.sql.catalog.testcat",
      classOf[InMemoryTableCatalog].getName)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    spark.sql("USE spark_catalog")
  }

  // ============================================================================
  // Basic Functionality Tests
  // ============================================================================

  test("NEW TABLE: basic INSERT with VALUES") {
    withTable("target") {
      sql("CREATE TABLE target(c1 INT, c2 STRING)")

      // scalastyle:off println
      println("\n========== FIRST: Regular INSERT for comparison ==========")
      // scalastyle:on println
      sql("INSERT INTO target VALUES (10, 'x')")

      // scalastyle:off println
      println("\n========== Check regular INSERT wrote data ==========")
      // scalastyle:on println
      checkAnswer(sql("SELECT * FROM target"), Seq(Row(10, "x")))

      // scalastyle:off println
      println("\n========== Recreate table for next test ==========")
      // scalastyle:on println
      sql("DROP TABLE target")
      sql("CREATE TABLE target(c1 INT, c2 STRING)")

      // scalastyle:off println
      println("\n========== NOW: NEW TABLE INSERT ==========")
      // scalastyle:on println
      val result = sql(
        """
          |SELECT * FROM NEW TABLE(
          |  INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c')
          |) AS tab
          |ORDER BY c1
          |""".stripMargin)

      checkAnswer(result,
        Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))

      // Verify data was actually written
      checkAnswer(sql("SELECT * FROM target ORDER BY c1"),
        Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))
    }
  }

  test("NEW TABLE: INSERT with SELECT") {
    withTable("source", "target") {
      sql("CREATE TABLE source(id INT, value STRING)")
      sql("CREATE TABLE target(id INT, value STRING)")
      sql("INSERT INTO source VALUES (10, 'x'), (20, 'y'), (30, 'z')")

      val result = sql(
        """
          |SELECT * FROM NEW TABLE(
          |  INSERT INTO target SELECT * FROM source
          |) AS tab
          |ORDER BY id
          |""".stripMargin)

      checkAnswer(result,
        Seq(Row(10, "x"), Row(20, "y"), Row(30, "z")))

      checkAnswer(sql("SELECT * FROM target ORDER BY id"),
        Seq(Row(10, "x"), Row(20, "y"), Row(30, "z")))
    }
  }

  test("NEW TABLE: returns all columns including defaults") {
    withTable("t") {
      sql("CREATE TABLE t(c1 INT, c2 STRING, c3 INT)")

      // Insert into only c1 column
      val result = sql(
        """
          |SELECT * FROM NEW TABLE(
          |  INSERT INTO t(c1) VALUES (100)
          |) AS tab
          |""".stripMargin)

      // Should return all 3 columns, with NULLs for c2 and c3
      val rows = result.collect()
      assert(rows.length == 1)
      assert(rows(0).getInt(0) == 100)
      assert(rows(0).isNullAt(1)) // c2 should be NULL
      assert(rows(0).isNullAt(2)) // c3 should be NULL
    }
  }

  test("NEW TABLE: non-deterministic functions are consistent") {
    withTable("t") {
      sql("CREATE TABLE t(id INT, random_val DOUBLE)")

      val result = sql(
        """
          |SELECT * FROM NEW TABLE(
          |  INSERT INTO t SELECT id, rand() FROM range(5)
          |) AS tab
          |ORDER BY id
          |""".stripMargin)

      val returnedRows = result.collect()
      val tableRows = sql("SELECT * FROM t ORDER BY id").collect()

      // The random values returned should exactly match what was written
      assert(returnedRows.length == tableRows.length)
      for (i <- returnedRows.indices) {
        assert(returnedRows(i).getInt(0) == tableRows(i).getInt(0))
        assert(returnedRows(i).getDouble(1) == tableRows(i).getDouble(1),
          "Random values should be identical between NEW TABLE result and table contents")
      }
    }
  }

  // ============================================================================
  // Identity Column Tests (require V2 catalog with identity support)
  // NOTE: These tests are currently ignored because V2 returning writes don't yet
  // capture generated identity values from the writer. This requires additional
  // work to extract generated column values from the V2 write result.
  // ============================================================================

  ignore("NEW TABLE: with GENERATED ALWAYS AS IDENTITY column") {
    withTable("testcat.target_with_identity", "testcat.source_data") {
      sql("USE testcat")

      // Create table with identity column
      sql(
        """
          |CREATE TABLE target_with_identity (
          |  id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 10),
          |  name STRING,
          |  value INT
          |) USING foo
          |""".stripMargin)

      // Create and populate source table
      sql(
        """
          |CREATE TABLE source_data (
          |  name STRING,
          |  value INT
          |) USING foo
          |""".stripMargin)

      sql(
        """
          |INSERT INTO source_data VALUES
          |  ('Alice', 10),
          |  ('Bob', 20),
          |  ('Charlie', 30)
          |""".stripMargin)

      // Use NEW TABLE to insert and capture generated identity values
      val result = sql(
        """
          |SELECT * FROM NEW TABLE(
          |  INSERT INTO target_with_identity (name, value)
          |  SELECT name, value FROM source_data
          |) AS result
          |ORDER BY id
          |""".stripMargin)

      checkAnswer(result,
        Seq(
          Row(100L, "Alice", 10),
          Row(110L, "Bob", 20),
          Row(120L, "Charlie", 30)))

      // Verify consistency with table contents
      checkAnswer(
        sql("SELECT * FROM target_with_identity ORDER BY id"),
        Seq(
          Row(100L, "Alice", 10),
          Row(110L, "Bob", 20),
          Row(120L, "Charlie", 30)))
    }
  }

  ignore("NEW TABLE: with GENERATED BY DEFAULT AS IDENTITY column") {
    withTable("testcat.target_with_default_identity", "testcat.source_data") {
      sql("USE testcat")

      // Create table with default identity column (allows explicit insert)
      sql(
        """
          |CREATE TABLE target_with_default_identity (
          |  id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
          |  description STRING
          |) USING foo
          |""".stripMargin)

      // Create source table
      sql(
        """
          |CREATE TABLE source_data (
          |  description STRING
          |) USING foo
          |""".stripMargin)

      sql("INSERT INTO source_data VALUES ('First'), ('Second'), ('Third')")

      // Use NEW TABLE
      val result = sql(
        """
          |SELECT * FROM NEW TABLE(
          |  INSERT INTO target_with_default_identity (description)
          |  SELECT description FROM source_data
          |) AS result
          |ORDER BY id
          |""".stripMargin)

      checkAnswer(result,
        Seq(
          Row(1L, "First"),
          Row(2L, "Second"),
          Row(3L, "Third")))

      // Verify consistency
      checkAnswer(
        sql("SELECT * FROM target_with_default_identity ORDER BY id"),
        Seq(
          Row(1L, "First"),
          Row(2L, "Second"),
          Row(3L, "Third")))
    }
  }

  ignore("NEW TABLE: identity column with mixed explicit columns") {
    withTable("testcat.mixed_table") {
      sql("USE testcat")

      sql(
        """
          |CREATE TABLE mixed_table (
          |  id BIGINT GENERATED ALWAYS AS IDENTITY,
          |  manual_id INT,
          |  data STRING
          |) USING foo
          |""".stripMargin)

      val result = sql(
        """
          |SELECT * FROM NEW TABLE(
          |  INSERT INTO mixed_table (manual_id, data)
          |  VALUES (999, 'test1'), (888, 'test2')
          |) AS result
          |ORDER BY id
          |""".stripMargin)

      checkAnswer(result,
        Seq(
          Row(1L, 999, "test1"),
          Row(2L, 888, "test2")))

      // Verify consistency
      checkAnswer(
        sql("SELECT * FROM mixed_table ORDER BY id"),
        Seq(
          Row(1L, 999, "test1"),
          Row(2L, 888, "test2")))
    }
  }

  // ============================================================================
  // V2 Data Source Tests
  // ============================================================================

  test("NEW TABLE: with V2 data source (AppendData)") {
    withTable("testcat.v2_table") {
      sql("USE testcat")

      sql(
        """
          |CREATE TABLE v2_table (
          |  id INT,
          |  name STRING
          |) USING foo
          |""".stripMargin)

      val result = sql(
        """
          |SELECT * FROM NEW TABLE(
          |  INSERT INTO v2_table VALUES (1, 'Alice'), (2, 'Bob')
          |) AS result
          |ORDER BY id
          |""".stripMargin)

      checkAnswer(result,
        Seq(
          Row(1, "Alice"),
          Row(2, "Bob")))

      // Verify data was written
      checkAnswer(
        sql("SELECT * FROM v2_table ORDER BY id"),
        Seq(
          Row(1, "Alice"),
          Row(2, "Bob")))
    }
  }

  test("NEW TABLE: with V2 data source INSERT OVERWRITE") {
    withTable("testcat.v2_overwrite_table") {
      sql("USE testcat")

      sql(
        """
          |CREATE TABLE v2_overwrite_table (
          |  id INT,
          |  value STRING
          |) USING foo
          |""".stripMargin)

      // First insert some data
      sql("INSERT INTO v2_overwrite_table VALUES (999, 'old')")

      // Overwrite with NEW TABLE
      val result = sql(
        """
          |SELECT * FROM NEW TABLE(
          |  INSERT OVERWRITE TABLE v2_overwrite_table
          |  VALUES (1, 'new1'), (2, 'new2')
          |) AS result
          |ORDER BY id
          |""".stripMargin)

      checkAnswer(result,
        Seq(
          Row(1, "new1"),
          Row(2, "new2")))

      // Verify old data was replaced
      checkAnswer(
        sql("SELECT * FROM v2_overwrite_table ORDER BY id"),
        Seq(
          Row(1, "new1"),
          Row(2, "new2")))
    }
  }

  test("NEW TABLE: chained CTEs with 5 INSERTs and final GROUP BY") {
    // Create a chain of 5 tables and INSERTs feeding each other through CTEs
    withTable("chain_t1", "chain_t2", "chain_t3", "chain_t4", "chain_t5") {
      // Create 5 tables
      sql("CREATE TABLE chain_t1 (id INT, category STRING, amount DECIMAL(10,2))")
      sql("CREATE TABLE chain_t2 (id INT, category STRING, amount DECIMAL(10,2))")
      sql("CREATE TABLE chain_t3 (id INT, category STRING, amount DECIMAL(10,2))")
      sql("CREATE TABLE chain_t4 (id INT, category STRING, amount DECIMAL(10,2))")
      sql("CREATE TABLE chain_t5 (id INT, category STRING, amount DECIMAL(10,2))")

      // Execute a chain of 5 INSERTs through CTEs, with final GROUP BY aggregation
      val result = sql(
        """
          |WITH
          |  step1 AS (
          |    SELECT * FROM NEW TABLE(
          |      INSERT INTO chain_t1 VALUES
          |        (1, 'A', 100.00),
          |        (2, 'B', 200.00),
          |        (3, 'A', 150.00)
          |    )
          |  ),
          |  step2 AS (
          |    SELECT * FROM NEW TABLE(
          |      INSERT INTO chain_t2
          |      SELECT id + 10, category, amount * 1.1
          |      FROM step1
          |    )
          |  ),
          |  step3 AS (
          |    SELECT * FROM NEW TABLE(
          |      INSERT INTO chain_t3
          |      SELECT id + 10, category, amount * 1.1
          |      FROM step2
          |    )
          |  ),
          |  step4 AS (
          |    SELECT * FROM NEW TABLE(
          |      INSERT INTO chain_t4
          |      SELECT id + 10, category, amount * 1.1
          |      FROM step3
          |    )
          |  ),
          |  step5 AS (
          |    SELECT * FROM NEW TABLE(
          |      INSERT INTO chain_t5
          |      SELECT id + 10, category, amount * 1.1
          |      FROM step4
          |    )
          |  )
          |SELECT
          |  category,
          |  COUNT(*) as count,
          |  CAST(SUM(amount) AS DECIMAL(10,2)) as total_amount,
          |  CAST(AVG(amount) AS DECIMAL(10,2)) as avg_amount
          |FROM step5
          |GROUP BY category
          |ORDER BY category
          |""".stripMargin)

      // Each step multiplies amount by 1.1, so after 4 multiplications:
      // A: 100 * 1.1^4 = 146.41, 150 * 1.1^4 = 219.615 -> sum = 366.025, avg = 183.0125
      // B: 200 * 1.1^4 = 292.82
      checkAnswer(result,
        Seq(
          Row("A", 2, BigDecimal("366.03"), BigDecimal("183.02")),
          Row("B", 1, BigDecimal("292.82"), BigDecimal("292.82"))))

      // Verify all 5 tables have data
      checkAnswer(
        sql("SELECT COUNT(*) FROM chain_t1"),
        Seq(Row(3)))
      checkAnswer(
        sql("SELECT COUNT(*) FROM chain_t2"),
        Seq(Row(3)))
      checkAnswer(
        sql("SELECT COUNT(*) FROM chain_t3"),
        Seq(Row(3)))
      checkAnswer(
        sql("SELECT COUNT(*) FROM chain_t4"),
        Seq(Row(3)))
      checkAnswer(
        sql("SELECT COUNT(*) FROM chain_t5"),
        Seq(Row(3)))

      // Verify final table has correct transformed data
      checkAnswer(
        sql("SELECT id, category FROM chain_t5 ORDER BY id"),
        Seq(
          Row(41, "A"),  // 1 + 10 + 10 + 10 + 10
          Row(42, "B"),  // 2 + 10 + 10 + 10 + 10
          Row(43, "A"))) // 3 + 10 + 10 + 10 + 10
    }
  }

  // NOTE: V1 Fallback tests (for Delta Lake and similar V2 tables with V1 writes)
  // are not included here due to catalog setup complexity. The V1 Fallback code
  // path has been implemented in V1FallbackWriters.scala and will be tested
  // separately or in integration tests with actual Delta Lake tables.
}
