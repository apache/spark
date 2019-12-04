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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.InMemoryTableCatalog;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.spark.sql.functions.*;

public class JavaDataFrameWriterV2Suite {
  private static StructType schema = new StructType().add("s", "string");
  private SparkSession spark = null;

  public Dataset<Row> df() {
    return spark.read().schema(schema).text();
  }

  @Before
  public void createTestTable() {
    this.spark = new TestSparkSession();
    spark.conf().set("spark.sql.catalog.testcat", InMemoryTableCatalog.class.getName());
    spark.sql("CREATE TABLE testcat.t (s string) USING foo");
  }

  @After
  public void dropTestTable() {
    spark.sql("DROP TABLE testcat.t");
    spark.stop();
  }

  @Test
  public void testAppendAPI() throws NoSuchTableException {
    df().writeTo("testcat.t").append();
    df().writeTo("testcat.t").option("property", "value").append();
  }

  @Test
  public void testOverwritePartitionsAPI() throws NoSuchTableException {
    df().writeTo("testcat.t").overwritePartitions();
    df().writeTo("testcat.t").option("property", "value").overwritePartitions();
  }

  @Test
  public void testOverwriteAPI() throws NoSuchTableException {
    df().writeTo("testcat.t").overwrite(lit(true));
    df().writeTo("testcat.t").option("property", "value").overwrite(lit(true));
  }

  @Test
  public void testCreateAPI() throws TableAlreadyExistsException {
    df().writeTo("testcat.t2").create();
    spark.sql("DROP TABLE testcat.t2");

    df().writeTo("testcat.t2").option("property", "value").create();
    spark.sql("DROP TABLE testcat.t2");

    df().writeTo("testcat.t2").tableProperty("property", "value").create();
    spark.sql("DROP TABLE testcat.t2");

    df().writeTo("testcat.t2").using("v2format").create();
    spark.sql("DROP TABLE testcat.t2");

    df().writeTo("testcat.t2").partitionedBy(col("s")).create();
    spark.sql("DROP TABLE testcat.t2");
  }

  @Test
  public void testReplaceAPI() throws CannotReplaceMissingTableException {
    df().writeTo("testcat.t").replace();
    df().writeTo("testcat.t").option("property", "value").replace();
    df().writeTo("testcat.t").tableProperty("property", "value").replace();
    df().writeTo("testcat.t").using("v2format").replace();
    df().writeTo("testcat.t").partitionedBy(col("s")).replace();
  }

  @Test
  public void testCreateOrReplaceAPI() {
    df().writeTo("testcat.t").createOrReplace();
    df().writeTo("testcat.t").option("property", "value").createOrReplace();
    df().writeTo("testcat.t").tableProperty("property", "value").createOrReplace();
    df().writeTo("testcat.t").using("v2format").createOrReplace();
    df().writeTo("testcat.t").partitionedBy(col("s")).createOrReplace();
  }
}
