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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.types.DataTypes.*;

public class JavaStringIndexerSuite {
  private transient JavaSparkContext jsc;
  private transient SQLContext sqlContext;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaStringIndexerSuite");
    sqlContext = new SQLContext(jsc);
  }

  @After
  public void tearDown() {
    jsc.stop();
    sqlContext = null;
  }

  @Test
  public void testStringIndexer() {
    StructType schema = createStructType(new StructField[] {
      createStructField("id", IntegerType, false),
      createStructField("label", StringType, false)
    });
    List<Row> data = Arrays.asList(
      c(0, "a"), c(1, "b"), c(2, "c"), c(3, "a"), c(4, "a"), c(5, "c"));
    DataFrame dataset = sqlContext.createDataFrame(data, schema);

    StringIndexer indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex");
    DataFrame output = indexer.fit(dataset).transform(dataset);

    Assert.assertArrayEquals(
      new Row[] { c(0, 0.0), c(1, 2.0), c(2, 1.0), c(3, 0.0), c(4, 0.0), c(5, 1.0) },
      output.orderBy("id").select("id", "labelIndex").collect());
  }

  /** An alias for RowFactory.create. */
  private Row c(Object... values) {
    return RowFactory.create(values);
  }
}
