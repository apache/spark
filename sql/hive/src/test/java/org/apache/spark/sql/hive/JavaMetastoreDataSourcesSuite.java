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

package org.apache.spark.sql.hive;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.QueryTest$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.test.TestHive$;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.util.Utils;

public class JavaMetastoreDataSourcesSuite {
  private transient JavaSparkContext sc;
  private transient SQLContext sqlContext;

  File path;
  Path hiveManagedPath;
  FileSystem fs;
  Dataset<Row> df;

  @Before
  public void setUp() throws IOException {
    sqlContext = TestHive$.MODULE$;
    sc = new JavaSparkContext(sqlContext.sparkContext());

    path =
      Utils.createTempDir(System.getProperty("java.io.tmpdir"), "datasource").getCanonicalFile();
    if (path.exists()) {
      path.delete();
    }
    HiveSessionCatalog catalog = (HiveSessionCatalog) sqlContext.sessionState().catalog();
    hiveManagedPath = new Path(catalog.defaultTablePath(new TableIdentifier("javaSavedTable")));
    fs = hiveManagedPath.getFileSystem(sc.hadoopConfiguration());
    fs.delete(hiveManagedPath, true);

    List<String> jsonObjects = new ArrayList<>(10);
    for (int i = 0; i < 10; i++) {
      jsonObjects.add("{\"a\":" + i + ", \"b\":\"str" + i + "\"}");
    }
    Dataset<String> ds = sqlContext.createDataset(jsonObjects, Encoders.STRING());
    df = sqlContext.read().json(ds);
    df.createOrReplaceTempView("jsonTable");
  }

  @After
  public void tearDown() throws IOException {
    // Clean up tables.
    if (sqlContext != null) {
      sqlContext.sql("DROP TABLE IF EXISTS javaSavedTable");
      sqlContext.sql("DROP TABLE IF EXISTS externalTable");
    }
  }

  @Test
  public void saveTableAndQueryIt() {
    Map<String, String> options = new HashMap<>();
    df.write()
      .format("org.apache.spark.sql.json")
      .mode(SaveMode.Append)
      .options(options)
      .saveAsTable("javaSavedTable");

    QueryTest$.MODULE$.checkAnswer(
      sqlContext.sql("SELECT * FROM javaSavedTable"),
      df.collectAsList());
  }
}
