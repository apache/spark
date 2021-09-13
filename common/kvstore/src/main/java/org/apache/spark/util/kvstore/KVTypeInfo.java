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

package org.apache.spark.util.kvstore;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

import org.apache.spark.annotation.Private;

/**
 * Wrapper around types managed in a KVStore, providing easy access to their indexed fields.
 */
@Private
public class KVTypeInfo {

  private final Class<?> type;
  private final Map<String, KVIndex> indices;
  private final Map<String, Accessor> accessors;

  public KVTypeInfo(Class<?> type) {
    this.type = type;
    this.accessors = new HashMap<>();
    this.indices = new HashMap<>();

    for (Field f : type.getDeclaredFields()) {
      KVIndex idx = f.getAnnotation(KVIndex.class);
      if (idx != null) {
        checkIndex(idx, indices);
        f.setAccessible(true);
        indices.put(idx.value(), idx);
        f.setAccessible(true);
        accessors.put(idx.value(), new FieldAccessor(f));
      }
    }

    for (Method m : type.getDeclaredMethods()) {
      KVIndex idx = m.getAnnotation(KVIndex.class);
      if (idx != null) {
        checkIndex(idx, indices);
        Preconditions.checkArgument(m.getParameterTypes().length == 0,
          "Annotated method %s::%s should not have any parameters.", type.getName(), m.getName());
        m.setAccessible(true);
        indices.put(idx.value(), idx);
        m.setAccessible(true);
        accessors.put(idx.value(), new MethodAccessor(m));
      }
    }

    Preconditions.checkArgument(indices.containsKey(KVIndex.NATURAL_INDEX_NAME),
        "No natural index defined for type %s.", type.getName());

    for (KVIndex idx : indices.values()) {
      if (!idx.parent().isEmpty()) {
        KVIndex parent = indices.get(idx.parent());
        Preconditions.checkArgument(parent != null,
          "Cannot find parent %s of index %s.", idx.parent(), idx.value());
        Preconditions.checkArgument(parent.parent().isEmpty(),
          "Parent index %s of index %s cannot be itself a child index.", idx.parent(), idx.value());
      }
    }
  }

  private void checkIndex(KVIndex idx, Map<String, KVIndex> indices) {
    Preconditions.checkArgument(idx.value() != null && !idx.value().isEmpty(),
      "No name provided for index in type %s.", type.getName());
    Preconditions.checkArgument(
      !idx.value().startsWith("_") || idx.value().equals(KVIndex.NATURAL_INDEX_NAME),
      "Index name %s (in type %s) is not allowed.", idx.value(), type.getName());
    Preconditions.checkArgument(idx.parent().isEmpty() || !idx.parent().equals(idx.value()),
      "Index %s cannot be parent of itself.", idx.value());
    Preconditions.checkArgument(!indices.containsKey(idx.value()),
      "Duplicate index %s for type %s.", idx.value(), type.getName());
  }

  public Class<?> type() {
    return type;
  }

  public Object getIndexValue(String indexName, Object instance) throws Exception {
    return getAccessor(indexName).get(instance);
  }

  public Stream<KVIndex> indices() {
    return indices.values().stream();
  }

  Accessor getAccessor(String indexName) {
    Accessor a = accessors.get(indexName);
    Preconditions.checkArgument(a != null, "No index %s.", indexName);
    return a;
  }

  Accessor getParentAccessor(String indexName) {
    KVIndex index = indices.get(indexName);
    return index.parent().isEmpty() ? null : getAccessor(index.parent());
  }

  String getParentIndexName(String indexName) {
    KVIndex index = indices.get(indexName);
    return index.parent();
  }

  /**
   * Abstracts the difference between invoking a Field and a Method.
   */
  interface Accessor {

    Object get(Object instance) throws ReflectiveOperationException;

    Class<?> getType();
  }

  private static class FieldAccessor implements Accessor {

    private final Field field;

    FieldAccessor(Field field) {
      this.field = field;
    }

    @Override
    public Object get(Object instance) throws ReflectiveOperationException {
      return field.get(instance);
    }

    @Override
    public Class<?> getType() {
      return field.getType();
    }
  }

  private static class MethodAccessor implements Accessor {

    private final Method method;

    MethodAccessor(Method method) {
      this.method = method;
    }

    @Override
    public Object get(Object instance) throws ReflectiveOperationException {
      return method.invoke(instance);
    }

    @Override
    public Class<?> getType() {
      return method.getReturnType();
    }
  }

}
