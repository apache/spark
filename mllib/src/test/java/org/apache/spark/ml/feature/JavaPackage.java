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

package org.apache.spark.ml.feature;

// Standard imports we leave out of the example
import org.apache.spark.sql.SQLContext;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

// Test specific imports
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.spark.util.Utils;
import java.io.File;

// Imports we have in the example
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
// Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
// Import StructType and StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Row;

import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.PipelineModel;

class JavaPackage {
  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaBucketizerSuite");
    jsql = new SQLContext(jsc);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void javaPackageSampleCodeTest() {
    // a DataFrame with three columns: id (integer), text (string), and rating (double).
    List<StructField> fields = new ArrayList<StructField>();
    fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
    fields.add(DataTypes.createStructField("text", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("rating", DataTypes.DoubleType, false));
    StructType schema = DataTypes.createStructType(fields);
    JavaRDD<Row> rowRDD = jsc.parallelize(
        Arrays.asList(
            RowFactory.create(0, "Hi I heard about Spark", 3.0),
            RowFactory.create(1, "I wish Java could use case classes", 4.0),
            RowFactory.create(2, "Logistic regression models are neat", 4.0)));
    DataFrame df = jsql.createDataFrame(rowRDD, schema);
    // define feature transformers
    RegexTokenizer tok = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words");
    StopWordsRemover sw = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered_words");
    HashingTF tf = new HashingTF()
      .setInputCol("filtered_words")
      .setOutputCol("tf")
      .setNumFeatures(10000);
    IDF idf = new IDF()
      .setInputCol("tf")
      .setOutputCol("tf_idf");
    VectorAssembler assembler = new VectorAssembler()
      .setInputCols(new String[] {"tf_idf", "rating"})
      .setOutputCol("features");

    // assemble and fit the feature transformation pipeline
    Pipeline pipeline = new Pipeline()
      .setStages(new PipelineStage[] {tok, sw, tf, idf, assembler});
    PipelineModel model = pipeline.fit(df);

    // save transformed features with raw data
    // we leave out the try/finally from the package example since the tmp dir is only
    // for tests.
    File tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    String path = tempDir.toURI().toString();
    try {
      model.transform(df)
        .select("id", "text", "rating", "features")
        .write().format("parquet").save(path);
    } finally {
      Utils.deleteRecursively(tempDir);
    }
  }
}
