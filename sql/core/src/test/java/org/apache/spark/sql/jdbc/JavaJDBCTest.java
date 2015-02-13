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

package org.apache.spark.sql.jdbc;

import org.junit.*;
import static org.junit.Assert.*;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.test.TestSQLContext$;

public class JavaJDBCTest {
  static String url = "jdbc:h2:mem:testdb1";

  static Connection conn = null;

  // This variable will always be null if TestSQLContext is intact when running
  // these tests.  Some Java tests do not play nicely with others, however;
  // they create a SparkContext of their own at startup and stop it at exit.
  // This renders TestSQLContext inoperable, meaning we have to do the same
  // thing.  If this variable is nonnull, that means we allocated a
  // SparkContext of our own and that we need to stop it at teardown.
  static JavaSparkContext localSparkContext = null;

  static SQLContext sql = TestSQLContext$.MODULE$;

  @Before
  public void beforeTest() throws Exception {
    if (SparkEnv.get() == null) { // A previous test destroyed TestSQLContext.
      localSparkContext = new JavaSparkContext("local", "JavaAPISuite");
      sql = new SQLContext(localSparkContext);
    }
    Class.forName("org.h2.Driver");
    conn = DriverManager.getConnection(url);
    conn.prepareStatement("create schema test").executeUpdate();
    conn.prepareStatement("create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate();
    conn.prepareStatement("insert into test.people values ('fred', 1)").executeUpdate();
    conn.prepareStatement("insert into test.people values ('mary', 2)").executeUpdate();
    conn.prepareStatement("insert into test.people values ('joe', 3)").executeUpdate();
    conn.commit();
  }

  @After
  public void afterTest() throws Exception {
    if (localSparkContext != null) {
      localSparkContext.stop();
      localSparkContext = null;
    }
    try {
      conn.close();
    } finally {
      conn = null;
    }
  }

  @Test
  public void basicTest() throws Exception {
    DataFrame rdd = JDBCUtils.jdbcRDD(sql, url, "TEST.PEOPLE");
    Row[] rows = rdd.collect();
    assertEquals(rows.length, 3);
  }

  @Test
  public void partitioningTest() throws Exception {
    String[] parts = new String[2];
    parts[0] = "THEID < 2";
    parts[1] = "THEID = 2"; // Deliberately forget about one of them.
    DataFrame rdd = JDBCUtils.jdbcRDD(sql, url, "TEST.PEOPLE", parts);
    Row[] rows = rdd.collect();
    assertEquals(rows.length, 2);
  }

  @Test
  public void writeTest() throws Exception {
    DataFrame rdd = JDBCUtils.jdbcRDD(sql, url, "TEST.PEOPLE");
    JDBCUtils.createJDBCTable(rdd, url, "TEST.PEOPLECOPY", false);
    DataFrame rdd2 = JDBCUtils.jdbcRDD(sql, url, "TEST.PEOPLECOPY");
    Row[] rows = rdd2.collect();
    assertEquals(rows.length, 3);
  }
}
