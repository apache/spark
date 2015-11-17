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

package org.apache.spark.ml.util;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.util.Utils;

public class JavaDefaultReadWriteSuite {

  JavaSparkContext jsc = null;
  SQLContext sqlContext = null;
  File tempDir = null;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local[2]", "JavaDefaultReadWriteSuite");
    SQLContext.clearActive();
    sqlContext = new SQLContext(jsc);
    SQLContext.setActive(sqlContext);
    tempDir = Utils.createTempDir(
      System.getProperty("java.io.tmpdir"), "JavaDefaultReadWriteSuite");
  }

  @After
  public void tearDown() {
    sqlContext = null;
    SQLContext.clearActive();
    if (jsc != null) {
      jsc.stop();
      jsc = null;
    }
    Utils.deleteRecursively(tempDir);
  }

  @Test
  public void testDefaultReadWrite() throws IOException {
    String uid = "my_params";
    MyParams instance = new MyParams(uid);
    instance.set(instance.intParam(), 2);
    String outputPath = new File(tempDir, uid).getPath();
    instance.save(outputPath);
    try {
      instance.save(outputPath);
      Assert.fail(
        "Write without overwrite enabled should fail if the output directory already exists.");
    } catch (IOException e) {
      // expected
    }
    instance.write().context(sqlContext).overwrite().save(outputPath);
    MyParams newInstance = MyParams.load(outputPath);
    Assert.assertEquals("UID should match.", instance.uid(), newInstance.uid());
    Assert.assertEquals("Params should be preserved.",
      2, newInstance.getOrDefault(newInstance.intParam()));
  }
}
