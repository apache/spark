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

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

public class JavaWord2VecSuite extends SharedSparkSession {

  @Test
  public void testJavaWord2Vec() {
    StructType schema = new StructType(new StructField[]{
      new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
    });
    Dataset<Row> documentDF = spark.createDataFrame(
      Arrays.asList(
        RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
        RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
        RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))),
      schema);

    Word2Vec word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0);
    Word2VecModel model = word2Vec.fit(documentDF);
    Dataset<Row> result = model.transform(documentDF);

    for (Row r : result.select("result").collectAsList()) {
      double[] polyFeatures = ((Vector) r.get(0)).toArray();
      Assert.assertEquals(3, polyFeatures.length);
    }
  }
}
