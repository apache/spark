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

package org.apache.spark.sql.connector.catalog

import java.lang.reflect.InvocationTargetException
import java.util
import java.util.NoSuchElementException
import java.util.regex.Pattern

import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

private[sql] object Catalogs {
  /**
   * Load and configure a catalog by name.
   * <p>
   * This loads, instantiates, and initializes the catalog plugin for each call; it does not cache
   * or reuse instances.
   *
   * @param name a String catalog name
   * @param conf a SQLConf
   * @return an initialized CatalogPlugin
   * @throws CatalogNotFoundException if the plugin class cannot be found
   * @throws org.apache.spark.SparkException           if the plugin class cannot be instantiated
   */
  @throws[CatalogNotFoundException]
  @throws[SparkException]
  def load(name: String, conf: SQLConf): CatalogPlugin = {
    val pluginClassName = try {
      conf.getConfString("spark.sql.catalog." + name)
    } catch {
      case _: NoSuchElementException =>
        throw new CatalogNotFoundException(
          s"Catalog '$name' plugin class not found: spark.sql.catalog.$name is not defined")
    }
    val loader = Utils.getContextOrSparkClassLoader
    try {
      val pluginClass = loader.loadClass(pluginClassName)
      if (!classOf[CatalogPlugin].isAssignableFrom(pluginClass)) {
        throw new SparkException(
          s"Plugin class for catalog '$name' does not implement CatalogPlugin: $pluginClassName")
      }
      val plugin = pluginClass.getDeclaredConstructor().newInstance().asInstanceOf[CatalogPlugin]
      plugin.initialize(name, catalogOptions(name, conf))
      plugin
    } catch {
      case _: ClassNotFoundException =>
        throw new SparkException(
          s"Cannot find catalog plugin class for catalog '$name': $pluginClassName")
      case e: NoSuchMethodException =>
        throw new SparkException(
          s"Failed to find public no-arg constructor for catalog '$name': $pluginClassName)", e)
      case e: IllegalAccessException =>
        throw new SparkException(
          s"Failed to call public no-arg constructor for catalog '$name': $pluginClassName)", e)
      case e: InstantiationException =>
        throw new SparkException("Cannot instantiate abstract catalog plugin class for " +
          s"catalog '$name': $pluginClassName", e.getCause)
      case e: InvocationTargetException =>
        throw new SparkException("Failed during instantiating constructor for catalog " +
          s"'$name': $pluginClassName", e.getCause)
    }
  }

  /**
   * Extracts a named catalog's configuration from a SQLConf.
   *
   * @param name a catalog name
   * @param conf a SQLConf
   * @return a case insensitive string map of options starting with spark.sql.catalog.(name).
   */
  private def catalogOptions(name: String, conf: SQLConf) = {
    val prefix = Pattern.compile("^spark\\.sql\\.catalog\\." + name + "\\.(.+)")
    val options = new util.HashMap[String, String]
    conf.getAllConfs.foreach {
      case (key, value) =>
        val matcher = prefix.matcher(key)
        if (matcher.matches && matcher.groupCount > 0) options.put(matcher.group(1), value)
    }
    new CaseInsensitiveStringMap(options)
  }
}
