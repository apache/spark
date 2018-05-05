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
import org.apache.spark.util.Utils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;

public class Catalogs {
  private Catalogs() {
  }

  /**
   * Load and configure a catalog by name.
   * <p>
   * This loads, instantiates, and initializes the catalog provider for each call; it does not
   * cache or reuse instances.
   *
   * @param name a String catalog name
   * @param conf a SQLConf
   * @return an initialized CatalogProvider
   * @throws SparkException If the provider class cannot be found or instantiated
   */
  public static CatalogProvider load(String name, SQLConf conf) throws SparkException {
    String providerClassName = conf.getConfString("spark.sql.catalog." + name, null);
    if (providerClassName == null) {
      throw new SparkException(String.format(
          "Catalog '%s' provider not found: spark.sql.catalog.%s is not defined", name, name));
    }

    ClassLoader loader = Utils.getContextOrSparkClassLoader();

    try {
      Class<?> providerClass = loader.loadClass(providerClassName);

      if (!CatalogProvider.class.isAssignableFrom(providerClass)) {
        throw new SparkException(String.format(
            "Provider class for catalog '%s' does not implement CatalogProvider: %s",
            name, providerClassName));
      }

      CatalogProvider provider = CatalogProvider.class.cast(providerClass.newInstance());

      provider.initialize(catalogOptions(name, conf));

      return provider;

    } catch (ClassNotFoundException e) {
      throw new SparkException(String.format(
          "Cannot find catalog provider class for catalog '%s': %s", name, providerClassName));

    } catch (IllegalAccessException e) {
      throw new SparkException(String.format(
          "Failed to call public no-arg constructor for catalog '%s': %s", name, providerClassName),
          e);

    } catch (InstantiationException e) {
      throw new SparkException(String.format(
          "Failed while instantiating provider for catalog '%s': %s", name, providerClassName),
          e.getCause());
    }
  }

  /**
   * Extracts a named catalog's configuration from a SQLConf.
   *
   * @param name a catalog name
   * @param conf a SQLConf
   * @return a case insensitive string map of options starting with spark.sql.catalog.(name).
   */
  private static CaseInsensitiveStringMap catalogOptions(String name, SQLConf conf) {
    Map<String, String> allConfs = mapAsJavaMapConverter(conf.getAllConfs()).asJava();
    Pattern prefix = Pattern.compile("^spark\\.sql\\.catalog\\." + name + "\\.(.+)");

    CaseInsensitiveStringMap options = CaseInsensitiveStringMap.empty();
    for (Map.Entry<String, String> entry : allConfs.entrySet()) {
      Matcher matcher = prefix.matcher(entry.getKey());
      if (matcher.matches() && matcher.groupCount() > 0) {
        options.put(matcher.group(1), entry.getValue());
      }
    }

    // add name last to ensure it overwrites any conflicting options
    options.put("name", name);

    return options;
  }
}
