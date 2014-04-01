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

package org.apache.hadoop.mapred;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.GenericsUtil;

public class TestWritableJobConf extends TestCase {

  private static final Configuration CONF = new Configuration();

  private <K> K serDeser(K conf) throws Exception {
    SerializationFactory factory = new SerializationFactory(CONF);
    Serializer<K> serializer =
      factory.getSerializer(GenericsUtil.getClass(conf));
    Deserializer<K> deserializer =
      factory.getDeserializer(GenericsUtil.getClass(conf));

    DataOutputBuffer out = new DataOutputBuffer();
    serializer.open(out);
    serializer.serialize(conf);
    serializer.close();

    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    deserializer.open(in);
    K after = deserializer.deserialize(null);
    deserializer.close();
    return after;
  }

  private void assertEquals(Configuration conf1, Configuration conf2) {
    assertEquals(conf1.size(), conf2.size());

    Iterator<Map.Entry<String, String>> iterator1 = conf1.iterator();
    Map<String, String> map1 = new HashMap<String,String>();
    while (iterator1.hasNext()) {
      Map.Entry<String, String> entry = iterator1.next();
      map1.put(entry.getKey(), entry.getValue());
    }

    Iterator<Map.Entry<String, String>> iterator2 = conf1.iterator();
    Map<String, String> map2 = new HashMap<String,String>();
    while (iterator2.hasNext()) {
      Map.Entry<String, String> entry = iterator2.next();
      map2.put(entry.getKey(), entry.getValue());
    }

    assertEquals(map1, map2);
  }

  public void testEmptyConfiguration() throws Exception {
    JobConf conf = new JobConf();
    Configuration deser = serDeser(conf);
    assertEquals(conf, deser);
  }

  public void testNonEmptyConfiguration() throws Exception {
    JobConf conf = new JobConf();
    conf.set("a", "A");
    conf.set("b", "B");
    Configuration deser = serDeser(conf);
    assertEquals(conf, deser);
  }

  public void testConfigurationWithDefaults() throws Exception {
    JobConf conf = new JobConf(false);
    conf.set("a", "A");
    conf.set("b", "B");
    Configuration deser = serDeser(conf);
    assertEquals(conf, deser);
  }
  
}
