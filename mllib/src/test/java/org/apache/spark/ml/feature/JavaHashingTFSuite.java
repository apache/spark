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

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class JavaHashingTFSuite {
  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaHashingTFSuite");
    jsql = new SQLContext(jsc);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void hashingTF() {
    List<Row> data = Arrays.asList(
      RowFactory.create(0.0, "Hi I heard about Spark"),
      RowFactory.create(0.0, "I wish Java could use case classes"),
      RowFactory.create(1.0, "Logistic regression models are neat")
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
    });

    DataFrame sentenceData = jsql.createDataFrame(data, schema);
    Tokenizer tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words");
    DataFrame wordsData = tokenizer.transform(sentenceData);
    int numFeatures = 20;
    HashingTF hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(numFeatures);
    DataFrame featurizedData = hashingTF.transform(wordsData);
    IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
    IDFModel idfModel = idf.fit(featurizedData);
    DataFrame rescaledData = idfModel.transform(featurizedData);
    for (Row r : rescaledData.select("features", "label").take(3)) {
      Vector features = r.getAs(0);
      Assert.assertEquals(features.size(), numFeatures);
    }
  }
}
