/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.yarn;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

/**
 * This test suite is to be run as an integration test, after packaging.
 * It verifies that the shaded jackson artifacts can be loaded
 */
public class ShadedJacksonIT {

  private static final Logger log =
      LoggerFactory.getLogger(ShadedJacksonIT.class);

  private static final String SHADED_JACKSON_PACKAGE = "org.spark-project.com.fasterxml.jackson.";

  @Test
  public void testJacksonShadedAnnotation() throws Throwable {
    loadClassAsResource(SHADED_JACKSON_PACKAGE + "annotation.JacksonAnnotation");
  }

  @Test
  public void testJacksonShadedCore() throws Throwable {
    loadClassAsResource(SHADED_JACKSON_PACKAGE + "core.JsonParser");
  }

  @Test
  public void testJacksonShadedDatabind() throws Throwable {
    loadClassAsResource(SHADED_JACKSON_PACKAGE + "databind.JsonSerializer");
  }

  @Test
  public void testJacksonShadedScala() throws Throwable {
    loadClassAsResource(SHADED_JACKSON_PACKAGE + "scala.SetModule");
  }

  private void loadClassAsResource(String classname) throws Throwable {
    String fullClassName = classname + ".class";
    URL url = this.getClass().getClassLoader().getResource(fullClassName);
    Assert.assertNotNull("Failed to find " + fullClassName, null);
    log.info("Loaded {} at {}", classname, url);
  }
}
