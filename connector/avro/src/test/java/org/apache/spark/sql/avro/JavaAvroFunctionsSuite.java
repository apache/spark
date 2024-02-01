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

package org.apache.spark.sql.avro;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.QueryTest$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.test.TestSparkSession;

import static org.apache.spark.sql.avro.functions.to_avro;
import static org.apache.spark.sql.avro.functions.from_avro;


public class JavaAvroFunctionsSuite {
  private transient TestSparkSession spark;

  @BeforeEach
  public void setUp() {
    spark = new TestSparkSession();
  }

  @AfterEach
  public void tearDown() {
    spark.stop();
  }

  @Test
  public void testToAvroFromAvro() {
    Dataset<Long> rangeDf = spark.range(10);
    Dataset<Row> df = rangeDf.select(
      rangeDf.col("id"), rangeDf.col("id").cast("string").as("str"));

    Dataset<Row> avroDF =
      df.select(
        to_avro(df.col("id")).as("a"),
        to_avro(df.col("str")).as("b"));

    String avroTypeLong = "{\"type\": \"int\", \"name\": \"id\"}";
    String avroTypeStr = "{\"type\": \"string\", \"name\": \"str\"}";

    Dataset<Row> actual = avroDF.select(
      from_avro(avroDF.col("a"), avroTypeLong),
      from_avro(avroDF.col("b"), avroTypeStr));

    QueryTest$.MODULE$.checkAnswer(actual, df.collectAsList());
  }
}
