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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.test.TestHive$;
import org.apache.spark.sql.sources.SaveModes;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JavaMetastoreDataSourcesSuite {
  private transient JavaSparkContext sc;
  private transient HiveContext sqlContext;

  String originalDefaultSource;
  File path;
  DataFrame df;

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

    List<String> jsonObjects = new ArrayList<String>(10);
    for (int i = 0; i < 10; i++) {
      jsonObjects.add("{\"a\":" + i + ", \"b\":\"str" + i + "\"}");
    }
    JavaRDD<String> rdd = sc.parallelize(jsonObjects);
    df = sqlContext.jsonRDD(rdd);
    df.registerTempTable("jsonTable");
  }

  @Test
  public void saveTableAndQueryIt() {
    Map<String, String> options = new HashMap<String, String>();
    df.saveAsTable("javaSavedTable", "org.apache.spark.sql.json", true, options);

    Assert.assertEquals(
      df.collectAsList(),
      sqlContext.sql("SELECT * FROM javaSavedTable").collectAsList());

    sqlContext.sql("DROP TABLE javaSavedTable");
  }

  @Test
  public void saveExternalTableAndQueryIt() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("path", path.toString());
    df.saveAsTable("javaSavedTable", "org.apache.spark.sql.json", true, options);

    Assert.assertEquals(
      df.collectAsList(),
      sqlContext.sql("SELECT * FROM javaSavedTable").collectAsList());

    DataFrame loadedDF =
      sqlContext.createExternalTable("externalTable", "org.apache.spark.sql.json", options);

    Assert.assertEquals(df.collectAsList(), loadedDF.collectAsList());
    Assert.assertEquals(
      df.collectAsList(),
      sqlContext.sql("SELECT * FROM externalTable").collectAsList());

    sqlContext.sql("DROP TABLE javaSavedTable");
    sqlContext.sql("DROP TABLE externalTable");
  }

  @Test
  public void saveExternalTableWithSchemaAndQueryIt() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("path", path.toString());
    df.saveAsTable("javaSavedTable", "org.apache.spark.sql.json", true, options);

    Assert.assertEquals(
      df.collectAsList(),
      sqlContext.sql("SELECT * FROM javaSavedTable").collectAsList());

    List<StructField> fields = new ArrayList<>();
    fields.add(DataTypes.createStructField("b", DataTypes.StringType, true));
    StructType schema = DataTypes.createStructType(fields);
    DataFrame loadedDF =
      sqlContext.createExternalTable("externalTable", "org.apache.spark.sql.json", schema, options);

    Assert.assertEquals(
      sqlContext.sql("SELECT b FROM javaSavedTable").collectAsList(),
      loadedDF.collectAsList());
    Assert.assertEquals(
      sqlContext.sql("SELECT b FROM javaSavedTable").collectAsList(),
      sqlContext.sql("SELECT * FROM externalTable").collectAsList());

    sqlContext.sql("DROP TABLE javaSavedTable");
    sqlContext.sql("DROP TABLE externalTable");
  }
}
