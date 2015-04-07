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

package test.org.apache.spark.sql.sources;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.test.TestSQLContext$;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;

public class JavaSaveLoadSuite {

  private transient JavaSparkContext sc;
  private transient SQLContext sqlContext;

  String originalDefaultSource;
  File path;
  DataFrame df;

  private void checkAnswer(DataFrame actual, List<Row> expected) {
    String errorMessage = QueryTest$.MODULE$.checkAnswer(actual, expected);
    if (errorMessage != null) {
      Assert.fail(errorMessage);
    }
  }

  @Before
  public void setUp() throws IOException {
    sqlContext = TestSQLContext$.MODULE$;
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
  public void saveAndLoad() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("path", path.toString());
    df.save("org.apache.spark.sql.json", SaveMode.ErrorIfExists, options);

    DataFrame loadedDF = sqlContext.load("org.apache.spark.sql.json", options);

    checkAnswer(loadedDF, df.collectAsList());
  }

  @Test
  public void saveAndLoadWithSchema() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("path", path.toString());
    df.save("org.apache.spark.sql.json", SaveMode.ErrorIfExists, options);

    List<StructField> fields = new ArrayList<StructField>();
    fields.add(DataTypes.createStructField("b", DataTypes.StringType, true));
    StructType schema = DataTypes.createStructType(fields);
    DataFrame loadedDF = sqlContext.load("org.apache.spark.sql.json", schema, options);

    checkAnswer(loadedDF, sqlContext.sql("SELECT b FROM jsonTable").collectAsList());
  }
}
