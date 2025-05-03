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

package org.apache.hadoop.hive.contrib.udtf.example;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


/**
 * GenericUDTFCount3 outputs the number of rows seen, three times. It's output three times
 * to test outputting of rows on close with lateral view.
 *
 */
@Description(name = "udtf_count3",
    value = "_FUNC_(col) - UDF outputs the number of rows seen, three times.")
public class GenericUDTFCount3 extends GenericUDTF {

  private transient Integer count = Integer.valueOf(0);
  private transient Object forwardObj[] = new Object[1];

  @Override
  public void close() throws HiveException {
    forwardObj[0] = count;
    forward(forwardObj);
    forward(forwardObj);
    forward(forwardObj);
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    fieldNames.add("col1");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
        fieldOIs);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    count = Integer.valueOf(count.intValue() + 1);
  }

}