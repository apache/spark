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

import static org.apache.hadoop.io.TestGenericWritable.CONF_TEST_KEY;
import static org.apache.hadoop.io.TestGenericWritable.CONF_TEST_VALUE;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TestGenericWritable.Baz;
import org.apache.hadoop.io.TestGenericWritable.FooGenericWritable;
import org.apache.hadoop.util.GenericsUtil;

public class TestWritableSerialization extends TestCase {

  private static final Configuration conf = new Configuration();
  
  static {
    conf.set("io.serializations"
        , "org.apache.hadoop.io.serializer.WritableSerialization");
  }
  
  public void testWritableSerialization() throws Exception {
    Text before = new Text("test writable"); 
    testSerialization(conf, before);
  }
  
  
  public void testWritableConfigurable() throws Exception {
    
    //set the configuration parameter
    conf.set(CONF_TEST_KEY, CONF_TEST_VALUE);

    //reuse TestGenericWritable inner classes to test 
    //writables that also implement Configurable.
    FooGenericWritable generic = new FooGenericWritable();
    generic.setConf(conf);
    Baz baz = new Baz();
    generic.set(baz);
    Baz result = testSerialization(conf, baz);
    assertNotNull(result.getConf());
  }
  
  /**
   * A utility that tests serialization/deserialization. 
   * @param <K> the class of the item
   * @param conf configuration to use, "io.serializations" is read to 
   * determine the serialization
   * @param before item to (de)serialize
   * @return deserialized item
   */
  public static<K> K testSerialization(Configuration conf, K before) 
    throws Exception {
    
    SerializationFactory factory = new SerializationFactory(conf);
    Serializer<K> serializer 
      = factory.getSerializer(GenericsUtil.getClass(before));
    Deserializer<K> deserializer 
      = factory.getDeserializer(GenericsUtil.getClass(before));
   
    DataOutputBuffer out = new DataOutputBuffer();
    serializer.open(out);
    serializer.serialize(before);
    serializer.close();
    
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    deserializer.open(in);
    K after = deserializer.deserialize(null);
    deserializer.close();
    
    assertEquals(before, after);
    return after;
  }
  
}
