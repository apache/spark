/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io.serializer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * <p>
 * A factory for {@link Serialization}s.
 * </p>
 */
public class SerializationFactory extends Configured {
  
  private static final Log LOG =
    LogFactory.getLog(SerializationFactory.class.getName());

  private List<Serialization<?>> serializations = new ArrayList<Serialization<?>>();
  
  /**
   * <p>
   * Serializations are found by reading the <code>io.serializations</code>
   * property from <code>conf</code>, which is a comma-delimited list of
   * classnames. 
   * </p>
   */
  public SerializationFactory(Configuration conf) {
    super(conf);
    for (String serializerName : conf.getStrings("io.serializations", 
      new String[]{"org.apache.hadoop.io.serializer.WritableSerialization"})) {
      add(conf, serializerName);
    }
  }
  
  @SuppressWarnings("unchecked")
  private void add(Configuration conf, String serializationName) {
    try {
      
      Class<? extends Serialization> serializionClass =
        (Class<? extends Serialization>) conf.getClassByName(serializationName);
      serializations.add((Serialization)
          ReflectionUtils.newInstance(serializionClass, getConf()));
    } catch (ClassNotFoundException e) {
      LOG.warn("Serilization class not found: " +
          StringUtils.stringifyException(e));
    }
  }

  public <T> Serializer<T> getSerializer(Class<T> c) {
    return getSerialization(c).getSerializer(c);
  }

  public <T> Deserializer<T> getDeserializer(Class<T> c) {
    return getSerialization(c).getDeserializer(c);
  }

  @SuppressWarnings("unchecked")
  public <T> Serialization<T> getSerialization(Class<T> c) {
    for (Serialization serialization : serializations) {
      if (serialization.accept(c)) {
        return (Serialization<T>) serialization;
      }
    }
    return null;
  }
}
