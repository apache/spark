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

import org.apache.spark.Partition;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;

public class JDBCUtils {
  /**
   * Construct a DataFrame representing the JDBC table at the database
   * specified by url with table name table.
   */
  public static DataFrame jdbcRDD(SQLContext sql, String url, String table) {
    Partition[] parts = new Partition[1];
    parts[0] = new JDBCPartition(null, 0);
    return sql.baseRelationToDataFrame(
        new JDBCRelation(url, table, parts, sql));
  }

  /**
   * Construct a DataFrame representing the JDBC table at the database
   * specified by url with table name table partitioned by parts.
   * Here, parts is an array of expressions suitable for insertion into a WHERE
   * clause; each one defines one partition.
   */
  public static DataFrame jdbcRDD(SQLContext sql, String url, String table, String[] parts) {
    Partition[] partitions = new Partition[parts.length];
    for (int i = 0; i < parts.length; i++)
      partitions[i] = new JDBCPartition(parts[i], i);
    return sql.baseRelationToDataFrame(
        new JDBCRelation(url, table, partitions, sql));
  }

  private static JavaJDBCTrampoline trampoline = new JavaJDBCTrampoline();

  public static void createJDBCTable(DataFrame rdd, String url, String table, boolean allowExisting) {
    trampoline.createJDBCTable(rdd, url, table, allowExisting);
  }

  public static void insertIntoJDBC(DataFrame rdd, String url, String table, boolean overwrite) {
    trampoline.insertIntoJDBC(rdd, url, table, overwrite);
  }
}
