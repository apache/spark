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

package org.apache.spark.sql.catalog.v2;

import org.apache.spark.SparkException;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;

public class CatalogLoadingSuite {
  @Test
  public void testLoad() throws SparkException {
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.catalog.test-name", TestCatalogProvider.class.getCanonicalName());

    CatalogProvider provider = Catalogs.load("test-name", conf);
    Assert.assertNotNull("Should instantiate a non-null provider", provider);
    Assert.assertEquals("Provider should have correct implementation",
        TestCatalogProvider.class, provider.getClass());

    TestCatalogProvider testProvider = (TestCatalogProvider) provider;
    Assert.assertEquals("Options should contain only one key", 1, testProvider.options.size());
    Assert.assertEquals("Options should contain correct catalog name",
        "test-name", testProvider.options.get("name"));
  }

  @Test
  public void testInitializationOptions() throws SparkException {
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.catalog.test-name", TestCatalogProvider.class.getCanonicalName());
    conf.setConfString("spark.sql.catalog.test-name.name", "overwritten");
    conf.setConfString("spark.sql.catalog.test-name.kEy", "valUE");

    CatalogProvider provider = Catalogs.load("test-name", conf);
    Assert.assertNotNull("Should instantiate a non-null provider", provider);
    Assert.assertEquals("Provider should have correct implementation",
        TestCatalogProvider.class, provider.getClass());

    TestCatalogProvider testProvider = (TestCatalogProvider) provider;

    Assert.assertEquals("Options should contain only two keys", 2, testProvider.options.size());
    Assert.assertEquals("Options should contain correct catalog name",
        "test-name", testProvider.options.get("name"));
    Assert.assertEquals("Options should contain correct value for key",
        "valUE", testProvider.options.get("key"));
  }

  @Test
  public void testLoadWithoutConfig() {
    SQLConf conf = new SQLConf();

    SparkException exc = intercept(SparkException.class, () -> Catalogs.load("missing", conf));

    Assert.assertTrue("Should complain that implementation is not configured",
        exc.getMessage().contains("provider not found: spark.sql.catalog.missing is not defined"));
    Assert.assertTrue("Should identify the catalog by name", exc.getMessage().contains("missing"));
  }

  @Test
  public void testLoadMissingClass() {
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.catalog.missing", "com.example.NoSuchCatalogProvider");

    SparkException exc = intercept(SparkException.class, () -> Catalogs.load("missing", conf));

    Assert.assertTrue("Should complain that the class is not found",
        exc.getMessage().contains("Cannot find catalog provider class"));
    Assert.assertTrue("Should identify the catalog by name", exc.getMessage().contains("missing"));
    Assert.assertTrue("Should identify the missing class",
        exc.getMessage().contains("com.example.NoSuchCatalogProvider"));
  }

  @Test
  public void testLoadNonCatalogProvider() {
    SQLConf conf = new SQLConf();
    String invalidClassName = InvalidCatalogProvider.class.getCanonicalName();
    conf.setConfString("spark.sql.catalog.invalid", invalidClassName);

    SparkException exc = intercept(SparkException.class, () -> Catalogs.load("invalid", conf));

    Assert.assertTrue("Should complain that class does not implement CatalogProvider",
        exc.getMessage().contains("does not implement CatalogProvider"));
    Assert.assertTrue("Should identify the catalog by name", exc.getMessage().contains("invalid"));
    Assert.assertTrue("Should identify the class", exc.getMessage().contains(invalidClassName));
  }

  @Test
  public void testLoadConstructorFailureCatalogProvider() {
    SQLConf conf = new SQLConf();
    String invalidClassName = ConstructorFailureCatalogProvider.class.getCanonicalName();
    conf.setConfString("spark.sql.catalog.invalid", invalidClassName);

    RuntimeException exc = intercept(RuntimeException.class, () -> Catalogs.load("invalid", conf));

    Assert.assertTrue("Should have expected error message",
        exc.getMessage().contains("Expected failure"));
  }

  @Test
  public void testLoadAccessErrorCatalogProvider() {
    SQLConf conf = new SQLConf();
    String invalidClassName = AccessErrorCatalogProvider.class.getCanonicalName();
    conf.setConfString("spark.sql.catalog.invalid", invalidClassName);

    SparkException exc = intercept(SparkException.class, () -> Catalogs.load("invalid", conf));

    Assert.assertTrue("Should complain that no public constructor is provided",
        exc.getMessage().contains("Failed to call public no-arg constructor for catalog"));
    Assert.assertTrue("Should identify the catalog by name", exc.getMessage().contains("invalid"));
    Assert.assertTrue("Should identify the class", exc.getMessage().contains(invalidClassName));
  }

  @SuppressWarnings("unchecked")
  public static <E extends Exception> E intercept(Class<E> expected, Callable<?> callable) {
    try {
      callable.call();
      Assert.fail("No exception was thrown, expected: " +
          expected.getName());
    } catch (Exception actual) {
      try {
        Assert.assertEquals(expected, actual.getClass());
        return (E) actual;
      } catch (AssertionError e) {
        e.addSuppressed(actual);
        throw e;
      }
    }
    // Compiler doesn't catch that Assert.fail will always throw an exception.
    throw new UnsupportedOperationException("[BUG] Should not reach this statement");
  }
}

class TestCatalogProvider implements CatalogProvider {
  CaseInsensitiveStringMap options = null;

  TestCatalogProvider() {
  }

  @Override
  public void initialize(CaseInsensitiveStringMap options) {
    this.options = options;
  }
}

class ConstructorFailureCatalogProvider implements CatalogProvider { // fails in its constructor
  ConstructorFailureCatalogProvider() {
    throw new RuntimeException("Expected failure.");
  }

  @Override
  public void initialize(CaseInsensitiveStringMap options) {
  }
}

class AccessErrorCatalogProvider implements CatalogProvider { // no public constructor
  private AccessErrorCatalogProvider() {
  }

  @Override
  public void initialize(CaseInsensitiveStringMap options) {
  }
}

class InvalidCatalogProvider { // doesn't implement CatalogProvider
  public void initialize(CaseInsensitiveStringMap options) {
  }
}
