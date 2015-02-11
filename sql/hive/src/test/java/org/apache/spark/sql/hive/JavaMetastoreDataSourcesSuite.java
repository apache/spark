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
import org.apache.spark.sql.sources.SaveMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.QueryTest$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.test.TestHive$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;

public class JavaMetastoreDataSourcesSuite {
  private transient JavaSparkContext sc;
  private transient HiveContext sqlContext;

  String originalDefaultSource;
  File path;
  Path hiveManagedPath;
  FileSystem fs;
  DataFrame df;

  private void checkAnswer(DataFrame actual, List<Row> expected) {
    String errorMessage = QueryTest$.MODULE$.checkAnswer(actual, expected);
    if (errorMessage != null) {
      Assert.fail(errorMessage);
    }
  }

  @Before
  public void setUp() throws IOException {
    sqlContext = TestHive$.MODULE$;
    sc = new JavaSparkContext(sqlContext.sparkContext());

    originalDefaultSource = sqlContext.conf().defaultDataSourceName();
    path =
      Utils.createTempDir(System.getProperty("java.io.tmpdir"), "datasource").getCanonicalFile();
    if (path.exists()) {
      path.delete();
    }
    hiveManagedPath = new Path(sqlContext.catalog().hiveDefaultTableFilePath("javaSavedTable"));
    fs = hiveManagedPath.getFileSystem(sc.hadoopConfiguration());
    if (fs.exists(hiveManagedPath)){
      fs.delete(hiveManagedPath, true);
    }

    List<String> jsonObjects = new ArrayList<String>(10);
    for (int i = 0; i < 10; i++) {
      jsonObjects.add("{\"a\":" + i + ", \"b\":\"str" + i + "\"}");
    }
    JavaRDD<String> rdd = sc.parallelize(jsonObjects);
    df = sqlContext.jsonRDD(rdd);
    df.registerTempTable("jsonTable");
  }

  @After
  public void tearDown() throws IOException {
    // Clean up tables.
    sqlContext.sql("DROP TABLE IF EXISTS javaSavedTable");
    sqlContext.sql("DROP TABLE IF EXISTS externalTable");
  }

  @Test
  public void saveExternalTableAndQueryIt() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("path", path.toString());
    df.saveAsTable("javaSavedTable", "org.apache.spark.sql.json", SaveMode.Append, options);

    checkAnswer(
      sqlContext.sql("SELECT * FROM javaSavedTable"),
      df.collectAsList());

    DataFrame loadedDF =
      sqlContext.createExternalTable("externalTable", "org.apache.spark.sql.json", options);

    checkAnswer(loadedDF, df.collectAsList());
    checkAnswer(
      sqlContext.sql("SELECT * FROM externalTable"),
      df.collectAsList());
  }

  @Test
  public void saveExternalTableWithSchemaAndQueryIt() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("path", path.toString());
    df.saveAsTable("javaSavedTable", "org.apache.spark.sql.json", SaveMode.Append, options);

    checkAnswer(
      sqlContext.sql("SELECT * FROM javaSavedTable"),
      df.collectAsList());

    List<StructField> fields = new ArrayList<>();
    fields.add(DataTypes.createStructField("b", DataTypes.StringType, true));
    StructType schema = DataTypes.createStructType(fields);
    DataFrame loadedDF =
      sqlContext.createExternalTable("externalTable", "org.apache.spark.sql.json", schema, options);

    checkAnswer(
      loadedDF,
      sqlContext.sql("SELECT b FROM javaSavedTable").collectAsList());
    checkAnswer(
      sqlContext.sql("SELECT * FROM externalTable"),
      sqlContext.sql("SELECT b FROM javaSavedTable").collectAsList());
  }

  @Test
  public void saveTableAndQueryIt() {
    Map<String, String> options = new HashMap<String, String>();
    df.saveAsTable("javaSavedTable", "org.apache.spark.sql.json", SaveMode.Append, options);

    checkAnswer(
      sqlContext.sql("SELECT * FROM javaSavedTable"),
      df.collectAsList());
  }
}
