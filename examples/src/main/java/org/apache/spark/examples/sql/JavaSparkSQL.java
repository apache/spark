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

package org.apache.spark.examples.sql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.JavaRow;

public final class JavaSparkSQL {
  public static void main(String[] args) throws Exception {
    JavaSparkContext ctx = new JavaSparkContext("local", "JavaSparkSQL",
        System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(JavaSparkSQL.class));
    JavaSQLContext sqlCtx = new JavaSQLContext(ctx);

    JavaSchemaRDD parquetFile = sqlCtx.parquetFile("pair.parquet");
    parquetFile.registerAsTable("parquet");

    JavaSchemaRDD queryResult = sqlCtx.sql("SELECT * FROM parquet");
    queryResult.foreach(new VoidFunction<JavaRow>() {
        @Override
        public void call(JavaRow row) throws Exception {
            System.out.println(row.get(0) + " " + row.get(1));
        }
    });
  }
}
