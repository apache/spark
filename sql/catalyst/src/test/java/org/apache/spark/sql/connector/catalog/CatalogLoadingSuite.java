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

package org.apache.spark.sql.connector.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkException;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.Utils;
public class CatalogLoadingSuite {
  @Test
  public void testLoad() throws SparkException {
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.catalog.test-name", TestCatalogPlugin.class.getCanonicalName());

    CatalogPlugin plugin = Catalogs.load("test-name", conf);
    Assertions.assertNotNull(plugin, "Should instantiate a non-null plugin");
    Assertions.assertEquals(TestCatalogPlugin.class, plugin.getClass(),
      "Plugin should have correct implementation");

    TestCatalogPlugin testPlugin = (TestCatalogPlugin) plugin;
    Assertions.assertEquals(0, testPlugin.options.size(),
      "Options should contain no keys");
    Assertions.assertEquals("test-name", testPlugin.name(), "Catalog should have correct name");
  }

  @Test
  public void testIllegalCatalogName() {
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.catalog.test.name", TestCatalogPlugin.class.getCanonicalName());

    SparkException exc = Assertions.assertThrows(SparkException.class,
      () -> Catalogs.load("test.name", conf));
    Assertions.assertTrue(exc.getMessage().contains("Invalid catalog name: test.name"),
      "Catalog name should not contain '.'");
  }

  @Test
  public void testInitializationOptions() throws SparkException {
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.catalog.test-name", TestCatalogPlugin.class.getCanonicalName());
    conf.setConfString("spark.sql.catalog.test-name.name", "not-catalog-name");
    conf.setConfString("spark.sql.catalog.test-name.kEy", "valUE");

    CatalogPlugin plugin = Catalogs.load("test-name", conf);
    Assertions.assertNotNull(plugin,"Should instantiate a non-null plugin");
    Assertions.assertEquals(TestCatalogPlugin.class, plugin.getClass(),
      "Plugin should have correct implementation");

    TestCatalogPlugin testPlugin = (TestCatalogPlugin) plugin;

    Assertions.assertEquals(2, testPlugin.options.size(), "Options should contain only two keys");
    Assertions.assertEquals("not-catalog-name", testPlugin.options.get("name"),
      "Options should contain correct value for name (not overwritten)");
    Assertions.assertEquals("valUE", testPlugin.options.get("key"),
      "Options should contain correct value for key");
  }

  @Test
  public void testLoadWithoutConfig() {
    SQLConf conf = new SQLConf();

    SparkException exc = Assertions.assertThrows(CatalogNotFoundException.class,
        () -> Catalogs.load("missing", conf));

    Assertions.assertEquals(exc.getErrorClass(), "CATALOG_NOT_FOUND");
    Assertions.assertEquals(exc.getMessageParameters().get("catalogName"), "`missing`");
  }

  @Test
  public void testLoadMissingClass() {
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.catalog.missing", "com.example.NoSuchCatalogPlugin");

    SparkException exc = Assertions.assertThrows(SparkException.class,
      () -> Catalogs.load("missing", conf));

    Assertions.assertTrue(exc.getMessage().contains("Cannot find catalog plugin class"),
      "Should complain that the class is not found");
    Assertions.assertTrue(exc.getMessage().contains("missing"),
      "Should identify the catalog by name");
    Assertions.assertTrue(exc.getMessage().contains("com.example.NoSuchCatalogPlugin"),
      "Should identify the missing class");
  }

  @Test
  public void testLoadMissingDependentClasses() {
    SQLConf conf = new SQLConf();
    String catalogClass = ClassFoundCatalogPlugin.class.getCanonicalName();
    conf.setConfString("spark.sql.catalog.missing", catalogClass);

    SparkException exc =
      Assertions.assertThrows(SparkException.class, () -> Catalogs.load("missing", conf));

    Assertions.assertTrue(exc.getCause() instanceof ClassNotFoundException);
    Assertions.assertTrue(exc.getCause().getMessage().contains(catalogClass + "Dep"));
  }

  @Test
  public void testLoadNonCatalogPlugin() {
    SQLConf conf = new SQLConf();
    String invalidClassName = InvalidCatalogPlugin.class.getCanonicalName();
    conf.setConfString("spark.sql.catalog.invalid", invalidClassName);

    SparkException exc = Assertions.assertThrows(SparkException.class,
      () -> Catalogs.load("invalid", conf));

    Assertions.assertTrue(exc.getMessage().contains("does not implement CatalogPlugin"),
      "Should complain that class does not implement CatalogPlugin");
    Assertions.assertTrue(exc.getMessage().contains("invalid"),
      "Should identify the catalog by name");
    Assertions.assertTrue(exc.getMessage().contains(invalidClassName),
      "Should identify the class");
  }

  @Test
  public void testLoadConstructorFailureCatalogPlugin() {
    SQLConf conf = new SQLConf();
    String invalidClassName = ConstructorFailureCatalogPlugin.class.getCanonicalName();
    conf.setConfString("spark.sql.catalog.invalid", invalidClassName);

    SparkException exc = Assertions.assertThrows(SparkException.class,
      () -> Catalogs.load("invalid", conf));

    Assertions.assertTrue(
      exc.getMessage().contains("Failed during instantiating constructor for catalog"),
      "Should identify the constructor error");
    Assertions.assertTrue(exc.getCause().getMessage().contains("Expected failure"),
      "Should have expected error message");
  }

  @Test
  public void testLoadAccessErrorCatalogPlugin() {
    SQLConf conf = new SQLConf();
    String invalidClassName = AccessErrorCatalogPlugin.class.getCanonicalName();
    conf.setConfString("spark.sql.catalog.invalid", invalidClassName);

    SparkException exc = Assertions.assertThrows(SparkException.class,
      () -> Catalogs.load("invalid", conf));

    Assertions.assertTrue(
      exc.getMessage().contains("Failed to call public no-arg constructor for catalog"),
      "Should complain that no public constructor is provided");
    Assertions.assertTrue(exc.getMessage().contains("invalid"),
      "Should identify the catalog by name");
    Assertions.assertTrue(exc.getMessage().contains(invalidClassName),
      "Should identify the class");
  }
}

class TestCatalogPlugin implements CatalogPlugin {
  String name = null;
  CaseInsensitiveStringMap options = null;

  TestCatalogPlugin() {
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;
    this.options = options;
  }

  @Override
  public String name() {
    return name;
  }
}

class ConstructorFailureCatalogPlugin implements CatalogPlugin { // fails in its constructor
  ConstructorFailureCatalogPlugin() {
    throw new RuntimeException("Expected failure.");
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
  }

  @Override
  public String name() {
    return null;
  }
}

class AccessErrorCatalogPlugin implements CatalogPlugin { // no public constructor
  private AccessErrorCatalogPlugin() {
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
  }

  @Override
  public String name() {
    return null;
  }
}

class InvalidCatalogPlugin { // doesn't implement CatalogPlugin
  public void initialize(CaseInsensitiveStringMap options) {
  }
}

class ClassFoundCatalogPlugin implements CatalogPlugin {

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    Utils.classForName(this.getClass().getCanonicalName() + "Dep", true, true);
  }

  @Override
  public String name() {
    return null;
  }
}
