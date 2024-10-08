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

package org.apache.spark.ml.source.libsvm;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.google.common.io.Files;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.Utils;


/**
 * Test LibSVMRelation in Java.
 */
public class JavaLibSVMRelationSuite extends SharedSparkSession {

  private File tempDir;
  private String path;

  @Override
  @BeforeEach
  public void setUp() throws IOException {
    super.setUp();
    tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "datasource");
    File file = new File(tempDir, "part-00000");
    String s = "1 1:1.0 3:2.0 5:3.0\n0\n0 2:4.0 4:5.0 6:6.0";
    Files.asCharSink(file, StandardCharsets.UTF_8).write(s);
    path = tempDir.toURI().toString();
  }

  @Override
  public void tearDown() {
    super.tearDown();
    Utils.deleteRecursively(tempDir);
  }

  @Test
  public void verifyLibSVMDF() {
    Dataset<Row> dataset = spark.read().format("libsvm").option("vectorType", "dense")
      .load(path);
    Assertions.assertEquals("label", dataset.columns()[0]);
    Assertions.assertEquals("features", dataset.columns()[1]);
    Row r = dataset.first();
    Assertions.assertEquals(1.0, r.getDouble(0), 1e-15);
    DenseVector v = r.getAs(1);
    Assertions.assertEquals(Vectors.dense(1.0, 0.0, 2.0, 0.0, 3.0, 0.0), v);
  }
}
