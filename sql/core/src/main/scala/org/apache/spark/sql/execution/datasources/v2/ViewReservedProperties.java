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

package org.apache.spark.sql.execution.datasources.v2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public final class ViewReservedProperties {

  private static final String PROP_COMMENT = "comment";
  private static final String PROP_OWNER = "owner";
  private static final String PROP_CREATE_ENGINE_VERSION = "create_engine_version";

  private static final List<String> RESERVED_PROPERTIES = Arrays.asList(
      PROP_COMMENT,
      PROP_OWNER,
      PROP_CREATE_ENGINE_VERSION);

  private final Map<String, String> properties;

  public static class Updater {

    private final ImmutableMap.Builder<String, String> builder;

    Updater(Map<String, String> properties) {
      Preconditions.checkNotNull(properties);
      builder = ImmutableMap.builder();
      properties.entrySet().stream()
          .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
          .forEach(builder::put);
    }

    public Updater comment(String comment) {
      Preconditions.checkNotNull(comment);
      builder.put(PROP_COMMENT, comment);
      return this;
    }

    public Updater createEngineVersion(String createEngineVersion) {
      Preconditions.checkNotNull(createEngineVersion);
      builder.put(PROP_CREATE_ENGINE_VERSION, createEngineVersion);
      return this;
    }

    public Map<String, String> update() {
      return builder.build();
    }
  }

  public static Updater updater(Map<String, String> properties) {
    return new Updater(properties);
  }

  public static Map<String, String> removeReserved(Map<String, String> properties) {
      return properties.entrySet().stream()
          .filter(e -> !RESERVED_PROPERTIES.contains(e.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static ViewReservedProperties extract(Map<String, String> properties) {
    return new ViewReservedProperties(properties);
  }

  private ViewReservedProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public String comment() {
    return properties.getOrDefault(PROP_COMMENT, "");
  }

  public String owner() {
    return properties.getOrDefault(PROP_OWNER, "");
  }

  public String createEngineVersion() {
    return properties.getOrDefault(PROP_CREATE_ENGINE_VERSION, "");
  }
}
