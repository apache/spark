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
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class JavaTokenizerSuite {
  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaTokenizerSuite");
    jsql = new SQLContext(jsc);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void RegexTokenizer() {
    RegexTokenizer myRegExTokenizer = new RegexTokenizer()
      .setInputCol("rawText")
      .setOutputCol("tokens")
      .setPattern("\\s")
      .setGaps(true)
      .setMinTokenLength(0);

    List<String> t = Arrays.asList(
      "{\"rawText\": \"Test of tok.\", \"wantedTokens\": [\"Test\", \"of\", \"tok.\"]}",
      "{\"rawText\": \"Te,st.  punct\", \"wantedTokens\": [\"Te,st.\", \"\", \"punct\"]}");

    JavaRDD<String> myRdd = jsc.parallelize(t);
    DataFrame dataset = jsql.jsonRDD(myRdd);

    Row[] pairs = myRegExTokenizer.transform(dataset)
      .select("tokens","wantedTokens")
      .collect();

    Assert.assertEquals(pairs[0].get(0), pairs[0].get(1));
    Assert.assertEquals(pairs[1].get(0), pairs[1].get(1));
  }
}
