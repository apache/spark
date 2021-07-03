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

package test.org.apache.spark;

import java.io.*;

import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

import org.junit.Test;

import org.apache.spark.api.java.*;
import org.apache.spark.*;
import org.apache.spark.util.Utils;

/**
 * Java apps can use both Java-friendly JavaSparkContext and Scala SparkContext.
 */
public class JavaSparkContextSuite implements Serializable {

  @Test
  public void javaSparkContext() throws IOException {
    File tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    String dummyJarFile = File.createTempFile(tempDir.toString(), "jarFile").toString();
    String[] jars = new String[] {};
    java.util.Map<String, String> environment = new java.util.HashMap<>();

    new JavaSparkContext(new SparkConf().setMaster("local").setAppName("name")).stop();
    new JavaSparkContext("local", "name", new SparkConf()).stop();
    new JavaSparkContext("local", "name").stop();
    new JavaSparkContext("local", "name", "sparkHome", dummyJarFile).stop();
    new JavaSparkContext("local", "name", "sparkHome", jars).stop();
    new JavaSparkContext("local", "name", "sparkHome", jars, environment).stop();
  }

  @Test
  public void scalaSparkContext() {
    List<String> jars = List$.MODULE$.empty();
    Map<String, String> environment = Map$.MODULE$.empty();

    new SparkContext(new SparkConf().setMaster("local").setAppName("name")).stop();
    new SparkContext("local", "name", new SparkConf()).stop();
    new SparkContext("local", "name").stop();
    new SparkContext("local", "name", "sparkHome").stop();
    new SparkContext("local", "name", "sparkHome", jars).stop();
    new SparkContext("local", "name", "sparkHome", jars, environment).stop();
  }
}
