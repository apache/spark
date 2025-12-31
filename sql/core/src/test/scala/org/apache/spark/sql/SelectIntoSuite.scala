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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for SELECT INTO functionality.
 * Tests various scenarios including CTEs, local variables, and session variables.
 */
class SelectIntoSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.variable.substitute", "false")
      .set(SQLConf.ANSI_ENABLED.key, "true")
  }

  test("SELECT INTO - Simple with local variables") {
    val script =
      """
        |BEGIN
        |  DECLARE emp_name STRING;
        |  DECLARE emp_salary INT;
        |
        |  CREATE OR REPLACE TEMP VIEW employees AS
        |  SELECT * FROM VALUES
        |    (1, 'Alice', 'Engineering', 95000),
        |    (2, 'Bob', 'Sales', 75000)
        |  AS t(emp_id, name, department, salary);
        |
        |  SELECT name, salary INTO emp_name, emp_salary
        |  FROM employees WHERE emp_id = 1;
        |
        |  SELECT emp_name, emp_salary;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "Alice")
    assert(result(0).getInt(1) == 95000)
  }

  test("SELECT INTO - With CTE and aggregation") {
    val script =
      """
        |BEGIN
        |  DECLARE top_dept STRING;
        |  DECLARE avg_sal DOUBLE;
        |
        |  CREATE OR REPLACE TEMP VIEW employees AS
        |  SELECT * FROM VALUES
        |    (1, 'Alice', 'Engineering', 95000),
        |    (2, 'Bob', 'Sales', 75000),
        |    (3, 'Charlie', 'Engineering', 105000),
        |    (4, 'Diana', 'HR', 65000)
        |  AS t(emp_id, name, department, salary);
        |
        |  WITH dept_stats AS (
        |    SELECT department, AVG(salary) as avg_salary
        |    FROM employees
        |    GROUP BY department
        |  )
        |  SELECT department, avg_salary INTO top_dept, avg_sal
        |  FROM dept_stats
        |  ORDER BY avg_salary DESC
        |  LIMIT 1;
        |
        |  SELECT top_dept, avg_sal;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "Engineering")
    assert(result(0).getDouble(1) == 100000.0)
  }

  test("SELECT INTO - With CTE and window functions") {
    val script =
      """
        |BEGIN
        |  DECLARE high_earner STRING;
        |  DECLARE salary_rank INT;
        |
        |  CREATE OR REPLACE TEMP VIEW employees AS
        |  SELECT * FROM VALUES
        |    (1, 'Alice', 95000),
        |    (2, 'Bob', 75000),
        |    (3, 'Charlie', 105000)
        |  AS t(emp_id, name, salary);
        |
        |  WITH ranked_employees AS (
        |    SELECT name, salary,
        |           RANK() OVER (ORDER BY salary DESC) as rank
        |    FROM employees
        |  )
        |  SELECT name, CAST(rank AS INT) INTO high_earner, salary_rank
        |  FROM ranked_employees
        |  WHERE rank = 1;
        |
        |  SELECT high_earner, salary_rank;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "Charlie")
    assert(result(0).getInt(1) == 1)
  }

  test("SELECT INTO - Session variables") {
    // Declare session variables
    spark.sql("DECLARE VARIABLE top_earner STRING")
    spark.sql("DECLARE VARIABLE top_salary INT")

    val script1 =
      """
        |BEGIN
        |  CREATE OR REPLACE TEMP VIEW employees AS
        |  SELECT * FROM VALUES
        |    (1, 'Alice', 95000),
        |    (2, 'Bob', 75000),
        |    (3, 'Charlie', 105000)
        |  AS t(emp_id, name, salary);
        |
        |  SELECT name, salary INTO top_earner, top_salary
        |  FROM employees
        |  ORDER BY salary DESC
        |  LIMIT 1;
        |
        |  SELECT top_earner, top_salary;
        |END;
      """.stripMargin

    val result1 = spark.sql(script1).collect()
    assert(result1.length == 1)
    assert(result1(0).getString(0) == "Charlie")
    assert(result1(0).getInt(1) == 105000)

    // Verify session variables persist outside the block
    val result2 = spark.sql("SELECT top_earner, top_salary").collect()
    assert(result2.length == 1)
    assert(result2(0).getString(0) == "Charlie")
    assert(result2(0).getInt(1) == 105000)

    // Update session variable
    val script2 =
      """
        |BEGIN
        |  SELECT 'Updated' INTO top_earner;
        |  SELECT top_earner;
        |END;
      """.stripMargin

    val result3 = spark.sql(script2).collect()
    assert(result3.length == 1)
    assert(result3(0).getString(0) == "Updated")
  }

  test("SELECT INTO - Nested CTEs") {
    val script =
      """
        |BEGIN
        |  DECLARE result_name STRING;
        |  DECLARE result_sal INT;
        |
        |  CREATE OR REPLACE TEMP VIEW employees AS
        |  SELECT * FROM VALUES
        |    (1, 'Alice', 'Engineering', 95000),
        |    (2, 'Bob', 'Sales', 75000),
        |    (3, 'Charlie', 'Engineering', 105000),
        |    (4, 'Diana', 'HR', 65000)
        |  AS t(emp_id, name, department, salary);
        |
        |  WITH eng_employees AS (
        |    SELECT * FROM employees WHERE department = 'Engineering'
        |  ),
        |  high_paid_eng AS (
        |    SELECT * FROM eng_employees WHERE salary > 100000
        |  )
        |  SELECT name, salary INTO result_name, result_sal
        |  FROM high_paid_eng
        |  LIMIT 1;
        |
        |  SELECT result_name, result_sal;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "Charlie")
    assert(result(0).getInt(1) == 105000)
  }

  test("SELECT INTO - CTE with aggregation and grouping") {
    val script =
      """
        |BEGIN
        |  DECLARE dept_name STRING;
        |  DECLARE total_payroll BIGINT;
        |
        |  CREATE OR REPLACE TEMP VIEW employees AS
        |  SELECT * FROM VALUES
        |    (1, 'Alice', 'Engineering', 95000),
        |    (2, 'Bob', 'Sales', 75000),
        |    (3, 'Charlie', 'Engineering', 105000),
        |    (4, 'Diana', 'HR', 65000)
        |  AS t(emp_id, name, department, salary);
        |
        |  WITH payroll AS (
        |    SELECT department, SUM(salary) as total
        |    FROM employees
        |    GROUP BY department
        |  )
        |  SELECT department, total INTO dept_name, total_payroll
        |  FROM payroll
        |  ORDER BY total DESC
        |  LIMIT 1;
        |
        |  SELECT dept_name, total_payroll;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "Engineering")
    assert(result(0).getLong(1) == 200000L)
  }

  test("SELECT INTO - Struct unpacking with CTE") {
    val script =
      """
        |BEGIN
        |  DECLARE emp_info STRUCT<id INT, name STRING, salary INT>;
        |
        |  CREATE OR REPLACE TEMP VIEW employees AS
        |  SELECT * FROM VALUES
        |    (1, 'Alice', 'Sales', 95000),
        |    (2, 'Bob', 'Sales', 75000),
        |    (3, 'Charlie', 'Sales', 82000)
        |  AS t(emp_id, name, department, salary);
        |
        |  WITH filtered_emps AS (
        |    SELECT emp_id, name, salary
        |    FROM employees
        |    WHERE department = 'Sales'
        |  )
        |  SELECT emp_id, name, salary INTO emp_info
        |  FROM filtered_emps
        |  ORDER BY salary DESC
        |  LIMIT 1;
        |
        |  SELECT emp_info.id, emp_info.name, emp_info.salary;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 1)
    assert(result(0).getString(1) == "Alice")
    assert(result(0).getInt(2) == 95000)
  }

  test("SELECT INTO - Multiple CTEs with JOIN") {
    val script =
      """
        |BEGIN
        |  DECLARE result STRING;
        |  DECLARE result_sal INT;
        |
        |  CREATE OR REPLACE TEMP VIEW employees AS
        |  SELECT * FROM VALUES
        |    (1, 'Alice', 'Engineering', 95000),
        |    (2, 'Bob', 'Sales', 75000),
        |    (3, 'Charlie', 'Engineering', 105000)
        |  AS t(emp_id, name, department, salary);
        |
        |  WITH dept_avg AS (
        |    SELECT department, AVG(salary) as avg_sal
        |    FROM employees
        |    GROUP BY department
        |  ),
        |  emp_with_avg AS (
        |    SELECT e.name, e.salary, d.avg_sal
        |    FROM employees e
        |    JOIN dept_avg d ON e.department = d.department
        |  )
        |  SELECT name, CAST(salary AS INT) INTO result, result_sal
        |  FROM emp_with_avg
        |  WHERE salary > avg_sal
        |  ORDER BY salary DESC
        |  LIMIT 1;
        |
        |  SELECT result, result_sal;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "Charlie")
    assert(result(0).getInt(1) == 105000)
  }

  test("SELECT INTO - Zero rows with CTE (variables remain unchanged)") {
    val script =
      """
        |BEGIN
        |  DECLARE test_id INT = 999;
        |  DECLARE test_name STRING = 'unchanged';
        |
        |  CREATE OR REPLACE TEMP VIEW employees AS
        |  SELECT * FROM VALUES
        |    (1, 'Alice', 95000),
        |    (2, 'Bob', 75000)
        |  AS t(emp_id, name, salary);
        |
        |  WITH filtered AS (
        |    SELECT emp_id, name FROM employees WHERE emp_id > 100
        |  )
        |  SELECT emp_id, name INTO test_id, test_name FROM filtered;
        |
        |  SELECT test_id, test_name;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 999)
    assert(result(0).getString(1) == "unchanged")
  }

  test("SELECT INTO - CTE with HAVING clause") {
    val script =
      """
        |BEGIN
        |  DECLARE high_dept STRING;
        |  DECLARE emp_count BIGINT;
        |
        |  CREATE OR REPLACE TEMP VIEW employees AS
        |  SELECT * FROM VALUES
        |    (1, 'Alice', 'Engineering'),
        |    (2, 'Bob', 'Sales'),
        |    (3, 'Charlie', 'Engineering'),
        |    (4, 'Diana', 'HR')
        |  AS t(emp_id, name, department);
        |
        |  WITH dept_counts AS (
        |    SELECT department, COUNT(*) as cnt
        |    FROM employees
        |    GROUP BY department
        |    HAVING COUNT(*) >= 2
        |  )
        |  SELECT department, cnt INTO high_dept, emp_count
        |  FROM dept_counts
        |  ORDER BY cnt DESC
        |  LIMIT 1;
        |
        |  SELECT high_dept, emp_count;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "Engineering")
    assert(result(0).getLong(1) == 2L)
  }

  test("SELECT INTO - CTE with expression calculations") {
    val script =
      """
        |BEGIN
        |  DECLARE name_result STRING;
        |  DECLARE bonus INT;
        |
        |  CREATE OR REPLACE TEMP VIEW employees AS
        |  SELECT * FROM VALUES
        |    (1, 'Alice', 95000),
        |    (2, 'Bob', 75000),
        |    (3, 'Charlie', 105000)
        |  AS t(emp_id, name, salary);
        |
        |  WITH bonus_calc AS (
        |    SELECT name, CAST(salary * 0.1 AS INT) as bonus_amt
        |    FROM employees
        |  )
        |  SELECT name, bonus_amt INTO name_result, bonus
        |  FROM bonus_calc
        |  ORDER BY bonus_amt DESC
        |  LIMIT 1;
        |
        |  SELECT name_result, bonus;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "Charlie")
    assert(result(0).getInt(1) == 10500)
  }

  test("SELECT INTO - CTE with DISTINCT") {
    val script =
      """
        |BEGIN
        |  DECLARE dept_result STRING;
        |
        |  CREATE OR REPLACE TEMP VIEW employees AS
        |  SELECT * FROM VALUES
        |    (1, 'Alice', 'Engineering'),
        |    (2, 'Bob', 'Engineering'),
        |    (3, 'Charlie', 'Sales')
        |  AS t(emp_id, name, department);
        |
        |  WITH unique_depts AS (
        |    SELECT DISTINCT department
        |    FROM employees
        |  )
        |  SELECT department INTO dept_result
        |  FROM unique_depts
        |  ORDER BY department
        |  LIMIT 1;
        |
        |  SELECT dept_result;
        |END;
      """.stripMargin

    val result = spark.sql(script).collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "Engineering")
  }
}
