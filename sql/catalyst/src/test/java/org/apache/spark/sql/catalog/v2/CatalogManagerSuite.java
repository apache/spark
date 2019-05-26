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
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

public class CatalogManagerSuite {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  CatalogManager catalogManager = new CatalogManager(new SQLConf());

  @Test
  public void testAdd() throws SparkException {
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(
        new HashMap<String, String>() {{
          put("option1", "value1");
          put("option2", "value2");
        }});
    catalogManager.add("testcat", TestCatalogPlugin.class.getCanonicalName(), options);
    CatalogPlugin catalogPlugin = catalogManager.load("testcat");
    assertThat(catalogPlugin.name(), is("testcat"));
    assertThat(catalogPlugin, instanceOf(TestCatalogPlugin.class));
    assertThat(((TestCatalogPlugin) catalogPlugin).options, is(options));
  }

  @Test
  public void testAddWithOption() throws SparkException {
    catalogManager.add("testcat", TestCatalogPlugin.class.getCanonicalName());
    CatalogPlugin catalogPlugin = catalogManager.load("testcat");
    assertThat(catalogPlugin.name(), is("testcat"));
    assertThat(catalogPlugin, instanceOf(TestCatalogPlugin.class));
    assertThat(((TestCatalogPlugin) catalogPlugin).options, is(CaseInsensitiveStringMap.empty()));
  }

  @Test
  public void testRemove() throws SparkException {
    catalogManager.add("testcat", TestCatalogPlugin.class.getCanonicalName());
    catalogManager.load("testcat");
    catalogManager.remove("testcat");
    exception.expect(CatalogNotFoundException.class);
    catalogManager.load("testcat");
  }
}
