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

package org.apache.spark.kvstore;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

/**
 * Wrapper around types managed in a KVStore, providing easy access to their indexed fields.
 */
public class KVTypeInfo {

  private final Class<?> type;
  private final Collection<KVIndex> indices;
  private final Map<String, Accessor> accessors;

  public KVTypeInfo(Class<?> type) throws Exception {
    this.type = type;
    this.indices = new ArrayList<>();
    this.accessors = new HashMap<>();

    for (Field f : type.getFields()) {
      KVIndex idx = f.getAnnotation(KVIndex.class);
      if (idx != null) {
        checkIndex(idx);
        indices.add(idx);
        accessors.put(idx.value(), new FieldAccessor(f));
      }
    }

    for (Method m : type.getMethods()) {
      KVIndex idx = m.getAnnotation(KVIndex.class);
      if (idx != null) {
        checkIndex(idx);
        Preconditions.checkArgument(m.getParameterTypes().length == 0,
          "Annotated method %s::%s should not have any parameters.", type.getName(), m.getName());
        indices.add(idx);
        accessors.put(idx.value(), new MethodAccessor(m));
      }
    }

    Preconditions.checkArgument(accessors.containsKey(KVIndex.NATURAL_INDEX_NAME),
        "No natural index defined for type %s.", type.getName());
  }

  private void checkIndex(KVIndex idx) {
    Preconditions.checkArgument(idx.value() != null && !idx.value().isEmpty(),
      "No name provided for index in type %s.", type.getName());
    Preconditions.checkArgument(
      !idx.value().startsWith("_") || idx.value().equals(KVIndex.NATURAL_INDEX_NAME),
      "Index name %s (in type %s) is not allowed.", idx.value(), type.getName());
    Preconditions.checkArgument(!indices.contains(idx.value()),
      "Duplicate index %s for type %s.", idx.value(), type.getName());
  }

  public Class<?> getType() {
    return type;
  }

  public Object getIndexValue(String indexName, Object instance) throws Exception {
    return getAccessor(indexName).get(instance);
  }

  public Stream<KVIndex> indices() {
    return indices.stream();
  }

  Accessor getAccessor(String indexName) {
    Accessor a = accessors.get(indexName);
    Preconditions.checkArgument(a != null, "No index %s.", indexName);
    return a;
  }

  /**
   * Abstracts the difference between invoking a Field and a Method.
   */
  interface Accessor {

    Object get(Object instance) throws Exception;

  }

  private class FieldAccessor implements Accessor {

    private final Field field;

    FieldAccessor(Field field) {
      this.field = field;
    }

    @Override
    public Object get(Object instance) throws Exception {
      return field.get(instance);
    }

  }

  private class MethodAccessor implements Accessor {

    private final Method method;

    MethodAccessor(Method method) {
      this.method = method;
    }

    @Override
    public Object get(Object instance) throws Exception {
      return method.invoke(instance);
    }

  }

}
