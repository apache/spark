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
package test.org.apache.spark.sql.execution.datasources.xml;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.xml.XmlOptions;

public final class JavaXmlSuite {

    private static final int numBooks = 12;
    private static final String booksFile = "src/test/resources/test-data/xml-resources/books.xml";
    private static final String booksFileTag = "book";

    private SparkSession spark;
    private Path tempDir;

    private static void setEnv(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.put(key, value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e);
        }
    }

    @BeforeEach
    public void setUp() throws IOException {
        setEnv("SPARK_LOCAL_IP", "127.0.0.1");
        spark = SparkSession.builder()
            .master("local[2]")
            .appName("XmlSuite")
            .config("spark.ui.enabled", false)
            .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        tempDir = Files.createTempDirectory("JavaXmlSuite");
        tempDir.toFile().deleteOnExit();
    }

    @AfterEach
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    private Path getEmptyTempDir() throws IOException {
        return Files.createTempDirectory(tempDir, "test");
    }

    @Test
    public void testXmlParser() {
        Map<String, String> options = new HashMap<>();
        options.put("rowTag", booksFileTag);
        Dataset<Row> df = spark.read().options(options).xml(booksFile);
        String prefix = XmlOptions.DEFAULT_ATTRIBUTE_PREFIX();
        long result = df.select(prefix + "id").count();
        Assertions.assertEquals(result, numBooks);
    }

    @Test
    public void testLoad() {
        Map<String, String> options = new HashMap<>();
        options.put("rowTag", booksFileTag);
        Dataset<Row> df = spark.read().options(options).xml(booksFile);
        long result = df.select("description").count();
        Assertions.assertEquals(result, numBooks);
    }

    @Test
    public void testSave() throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("rowTag", booksFileTag);
        Path booksPath = getEmptyTempDir().resolve("booksFile");

        Dataset<Row> df = spark.read().options(options).xml(booksFile);
        df.select("price", "description").write().options(options).xml(booksPath.toString());

        Dataset<Row> newDf = spark.read().options(options).xml(booksPath.toString());
        long result = newDf.select("price").count();
        Assertions.assertEquals(result, numBooks);
    }

}
