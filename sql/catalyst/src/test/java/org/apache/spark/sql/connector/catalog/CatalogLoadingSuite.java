/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.SparkException;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.Utils;

import org.junit.Assert;
import org.junit.Test;

public class CatalogLoadingSuite {
  @Test
  public void testLoad() throws SparkException {
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.catalog.test-name", TestCatalogPlugin.class.getCanonicalName());

    CatalogPlugin plugin = Catalogs.load("test-name", conf);
    Assert.assertNotNull("Should instantiate a non-null plugin", plugin);
    Assert.assertEquals("Plugin should have correct implementation",
        TestCatalogPlugin.class, plugin.getClass());

    TestCatalogPlugin testPlugin = (TestCatalogPlugin) plugin;
    Assert.assertEquals("Options should contain no keys", 0, testPlugin.options.size());
    Assert.assertEquals("Catalog should have correct name", "test-name", testPlugin.name());
  }

  @Test
  public void testIllegalCatalogName() {
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.catalog.test.name", TestCatalogPlugin.class.getCanonicalName());

    SparkException exc = Assert.assertThrows(SparkException.class,
            () -> Catalogs.load("test.name", conf));
    Assert.assertTrue("Catalog name should not contain '.'", exc.getMessage().contains(
            "Invalid catalog name: test.name"));
  }

  @Test
  public void testInitializationOptions() throws SparkException {
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.catalog.test-name", TestCatalogPlugin.class.getCanonicalName());
    conf.setConfString("spark.sql.catalog.test-name.name", "not-catalog-name");
    conf.setConfString("spark.sql.catalog.test-name.kEy", "valUE");

    CatalogPlugin plugin = Catalogs.load("test-name", conf);
    Assert.assertNotNull("Should instantiate a non-null plugin", plugin);
    Assert.assertEquals("Plugin should have correct implementation",
        TestCatalogPlugin.class, plugin.getClass());

    TestCatalogPlugin testPlugin = (TestCatalogPlugin) plugin;

    Assert.assertEquals("Options should contain only two keys", 2, testPlugin.options.size());
    Assert.assertEquals("Options should contain correct value for name (not overwritten)",
        "not-catalog-name", testPlugin.options.get("name"));
    Assert.assertEquals("Options should contain correct value for key",
        "valUE", testPlugin.options.get("key"));
  }

  @Test
  public void testLoadWithoutConfig() {
    SQLConf conf = new SQLConf();

    SparkException exc = Assert.assertThrows(CatalogNotFoundException.class,
        () -> Catalogs.load("missing", conf));

    Assert.assertTrue("Should complain that implementation is not configured",
        exc.getMessage()
            .contains("plugin class not found: spark.sql.catalog.missing is not defined"));
    Assert.assertTrue("Should identify the catalog by name",
        exc.getMessage().contains("missing"));
  }

  @Test
  public void testLoadMissingClass() {
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.catalog.missing", "com.example.NoSuchCatalogPlugin");

    SparkException exc =
      Assert.assertThrows(SparkException.class, () -> Catalogs.load("missing", conf));

    Assert.assertTrue("Should complain that the class is not found",
        exc.getMessage().contains("Cannot find catalog plugin class"));
    Assert.assertTrue("Should identify the catalog by name",
        exc.getMessage().contains("missing"));
    Assert.assertTrue("Should identify the missing class",
        exc.getMessage().contains("com.example.NoSuchCatalogPlugin"));
  }

  @Test
  public void testLoadMissingDependentClasses() {
    SQLConf conf = new SQLConf();
    String catalogClass = ClassFoundCatalogPlugin.class.getCanonicalName();
    conf.setConfString("spark.sql.catalog.missing", catalogClass);

    SparkException exc =
        Assert.assertThrows(SparkException.class, () -> Catalogs.load("missing", conf));

    Assert.assertTrue(exc.getCause() instanceof ClassNotFoundException);
    Assert.assertTrue(exc.getCause().getMessage().contains(catalogClass + "Dep"));
  }

  @Test
  public void testLoadNonCatalogPlugin() {
    SQLConf conf = new SQLConf();
    String invalidClassName = InvalidCatalogPlugin.class.getCanonicalName();
    conf.setConfString("spark.sql.catalog.invalid", invalidClassName);

    SparkException exc =
      Assert.assertThrows(SparkException.class, () -> Catalogs.load("invalid", conf));

    Assert.assertTrue("Should complain that class does not implement CatalogPlugin",
        exc.getMessage().contains("does not implement CatalogPlugin"));
    Assert.assertTrue("Should identify the catalog by name",
        exc.getMessage().contains("invalid"));
    Assert.assertTrue("Should identify the class",
        exc.getMessage().contains(invalidClassName));
  }

  @Test
  public void testLoadConstructorFailureCatalogPlugin() {
    SQLConf conf = new SQLConf();
    String invalidClassName = ConstructorFailureCatalogPlugin.class.getCanonicalName();
    conf.setConfString("spark.sql.catalog.invalid", invalidClassName);

    SparkException exc =
      Assert.assertThrows(SparkException.class, () -> Catalogs.load("invalid", conf));

    Assert.assertTrue("Should identify the constructor error",
        exc.getMessage().contains("Failed during instantiating constructor for catalog"));
    Assert.assertTrue("Should have expected error message",
        exc.getCause().getMessage().contains("Expected failure"));
  }

  @Test
  public void testLoadAccessErrorCatalogPlugin() {
    SQLConf conf = new SQLConf();
    String invalidClassName = AccessErrorCatalogPlugin.class.getCanonicalName();
    conf.setConfString("spark.sql.catalog.invalid", invalidClassName);

    SparkException exc =
      Assert.assertThrows(SparkException.class, () -> Catalogs.load("invalid", conf));

    Assert.assertTrue("Should complain that no public constructor is provided",
        exc.getMessage().contains("Failed to call public no-arg constructor for catalog"));
    Assert.assertTrue("Should identify the catalog by name",
        exc.getMessage().contains("invalid"));
    Assert.assertTrue("Should identify the class",
        exc.getMessage().contains(invalidClassName));
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
