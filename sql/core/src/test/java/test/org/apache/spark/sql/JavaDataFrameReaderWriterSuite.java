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
import java.util.HashMap;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JavaDataFrameReaderWriterSuite {
  private SparkSession spark = new TestSparkSession();
  private StructType schema = new StructType().add("s", "string");
  private transient String input;
  private transient String output;

  @BeforeEach
  public void setUp() {
    input = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "input").toString();
    File f = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "output");
    f.delete();
    output = f.toString();
  }

  @AfterEach
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void testFormatAPI() {
    spark
        .read()
        .format("org.apache.spark.sql.test")
        .load()
        .write()
        .format("org.apache.spark.sql.test")
        .save();
  }

  @Test
  public void testOptionsAPI() {
    HashMap<String, String> map = new HashMap<>();
    map.put("e", "1");
    spark
        .read()
        .option("a", "1")
        .option("b", 1)
        .option("c", 1.0)
        .option("d", true)
        .options(map)
        .text()
        .write()
        .option("a", "1")
        .option("b", 1)
        .option("c", 1.0)
        .option("d", true)
        .options(map)
        .format("org.apache.spark.sql.test")
        .save();
  }

  @Test
  public void testSaveModeAPI() {
    spark
        .range(10)
        .write()
        .format("org.apache.spark.sql.test")
        .mode(SaveMode.ErrorIfExists)
        .save();
  }

  @Test
  public void testLoadAPI() {
    spark.read().format("org.apache.spark.sql.test").load();
    spark.read().format("org.apache.spark.sql.test").load(input);
    spark.read().format("org.apache.spark.sql.test").load(input, input, input);
    spark.read().format("org.apache.spark.sql.test").load(new String[]{input, input});
  }

  @Test
  public void testTextAPI() {
    spark.read().text();
    spark.read().text(input);
    spark.read().text(input, input, input);
    spark.read().text(new String[]{input, input})
        .write().text(output);
  }

  @Test
  public void testTextFileAPI() {
    spark.read().textFile();
    spark.read().textFile(input);
    spark.read().textFile(input, input, input);
    spark.read().textFile(new String[]{input, input});
  }

  @Test
  public void testCsvAPI() {
    spark.read().schema(schema).csv();
    spark.read().schema(schema).csv(input);
    spark.read().schema(schema).csv(input, input, input);
    spark.read().schema(schema).csv(new String[]{input, input})
        .write().csv(output);
  }

  @Test
  public void testJsonAPI() {
    spark.read().schema(schema).json();
    spark.read().schema(schema).json(input);
    spark.read().schema(schema).json(input, input, input);
    spark.read().schema(schema).json(new String[]{input, input})
        .write().json(output);
  }

  @Test
  public void testParquetAPI() {
    spark.read().schema(schema).parquet();
    spark.read().schema(schema).parquet(input);
    spark.read().schema(schema).parquet(input, input, input);
    spark.read().schema(schema).parquet(new String[] { input, input })
        .write().parquet(output);
  }

  @Test
  public void testOrcAPI() {
    spark.read().schema(schema).orc();
    spark.read().schema(schema).orc(input);
    spark.read().schema(schema).orc(input, input, input);
    spark.read().schema(schema).orc(new String[]{input, input})
        .write().orc(output);
  }
}
