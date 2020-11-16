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

package test.org.apache.spark.sql.streaming;

import java.io.File;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.util.Utils;

public class JavaDataStreamReaderWriterSuite {
  private SparkSession spark;
  private String input;

  @Before
  public void setUp() {
    spark = new TestSparkSession();
    input = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "input").toString();
  }

  @After
  public void tearDown() {
    try {
      Utils.deleteRecursively(new File(input));
    } finally {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void testForeachBatchAPI() throws TimeoutException {
    StreamingQuery query = spark
      .readStream()
      .textFile(input)
      .writeStream()
      .foreachBatch(new VoidFunction2<Dataset<String>, Long>() {
        @Override
        public void call(Dataset<String> v1, Long v2) throws Exception {}
      })
      .start();
    query.stop();
  }

  @Test
  public void testForeachAPI() throws TimeoutException {
    StreamingQuery query = spark
      .readStream()
      .textFile(input)
      .writeStream()
      .foreach(new ForeachWriter<String>() {
        @Override
        public boolean open(long partitionId, long epochId) {
          return true;
        }

        @Override
        public void process(String value) {}

        @Override
        public void close(Throwable errorOrNull) {}
      })
      .start();
    query.stop();
  }
}
