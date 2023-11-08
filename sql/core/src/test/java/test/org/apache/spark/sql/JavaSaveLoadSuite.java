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

package test.org.apache.spark.sql;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;

public class JavaSaveLoadSuite {

  private transient SparkSession spark;

  File path;
  Dataset<Row> df;

  private static void checkAnswer(Dataset<Row> actual, List<Row> expected) {
    QueryTest$.MODULE$.checkAnswer(actual, expected);
  }

  @BeforeEach
  public void setUp() throws IOException {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("testing")
      .getOrCreate();

    path =
      Utils.createTempDir(System.getProperty("java.io.tmpdir"), "datasource").getCanonicalFile();
    if (path.exists()) {
      path.delete();
    }

    List<String> jsonObjects = new ArrayList<>(10);
    for (int i = 0; i < 10; i++) {
      jsonObjects.add("{\"a\":" + i + ", \"b\":\"str" + i + "\"}");
    }
    Dataset<String> ds = spark.createDataset(jsonObjects, Encoders.STRING());
    df = spark.read().json(ds);
    df.createOrReplaceTempView("jsonTable");
  }

  @AfterEach
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void saveAndLoad() {
    Map<String, String> options = new HashMap<>();
    options.put("path", path.toString());
    df.write().mode(SaveMode.ErrorIfExists).format("json").options(options).save();
    Dataset<Row> loadedDF = spark.read().format("json").options(options).load();
    checkAnswer(loadedDF, df.collectAsList());
  }

  @Test
  public void saveAndLoadWithSchema() {
    Map<String, String> options = new HashMap<>();
    options.put("path", path.toString());
    df.write().format("json").mode(SaveMode.ErrorIfExists).options(options).save();

    List<StructField> fields = new ArrayList<>();
    fields.add(DataTypes.createStructField("b", DataTypes.StringType, true));
    StructType schema = DataTypes.createStructType(fields);
    Dataset<Row> loadedDF = spark.read().format("json").schema(schema).options(options).load();

    checkAnswer(loadedDF, spark.sql("SELECT b FROM jsonTable").collectAsList());
  }
}
