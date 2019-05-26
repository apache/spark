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
import org.apache.spark.annotation.Private;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.apache.spark.sql.catalog.v2.Catalogs.classKey;
import static org.apache.spark.sql.catalog.v2.Catalogs.isOptionKey;
import static org.apache.spark.sql.catalog.v2.Catalogs.optionKeyPrefix;
import static scala.collection.JavaConverters.mapAsJavaMapConverter;

@Private
public class CatalogManager {

  private final SQLConf conf;

  public CatalogManager(SQLConf conf) {
    this.conf = conf;
  }

  /**
   * Load a catalog.
   *
   * @param name a catalog name
   * @return a catalog plugin
   */
  public CatalogPlugin load(String name) throws SparkException {
    return Catalogs.load(name, conf);
  }

  /**
   * Add a catalog.
   *
   * @param name a catalog name
   * @param pluginClassName a catalog plugin class name
   * @param options catalog options
   */
  public void add(
      String name,
      String pluginClassName,
      CaseInsensitiveStringMap options) {
    options.entrySet().stream()
        .forEach(e -> conf.setConfString(optionKeyPrefix(name) + e.getKey(), e.getValue()));
    conf.setConfString(classKey(name), pluginClassName);
  }

  /**
   * Add a catalog without option.
   *
   * @param name a catalog name
   * @param pluginClassName a catalog plugin class name
   */
  public void add(
      String name,
      String pluginClassName) {
    add(name, pluginClassName, CaseInsensitiveStringMap.empty());
  }

  /**
   * Remove a catalog.
   *
   * @param name a catalog name
   */
  public void remove(String name) {
    conf.unsetConf(classKey(name));
    mapAsJavaMapConverter(conf.getAllConfs()).asJava().keySet().stream()
        .filter(key -> isOptionKey(name, key))
        .forEach(conf::unsetConf);
  }
}
