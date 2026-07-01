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

package org.apache.spark.sql.hive.execution;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.IntWritable;

public class UDTFStack2 extends GenericUDTF {
  private transient List<ObjectInspector> argOIs = new ArrayList<>();
  private transient Object[] forwardObj = null;
  private transient ArrayList<GenericUDFUtils.ReturnObjectInspectorResolver>
      returnOIResolvers = new ArrayList<>();
  IntWritable numRows = null;
  Integer numCols = null;

  @Override
  public void close() throws HiveException {}

  @Override
  public StructObjectInspector initialize(StructObjectInspector argOI)
      throws UDFArgumentException {
    List<? extends StructField> fields = argOI.getAllStructFieldRefs();
    ObjectInspector[] args = new ObjectInspector[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      args[i] = fields.get(i).getFieldObjectInspector();
    }
    return initialize2(args);
  }

  public StructObjectInspector initialize2(ObjectInspector[] args)
      throws UDFArgumentException {
    if (args.length < 2) {
      throw new UDFArgumentException(
          "STACK() expects at least two arguments.");
    }
    if (!(args[0] instanceof ConstantObjectInspector)) {
      throw new UDFArgumentException(
          "The first argument to STACK() must be a constant integer (got"
              + args[0].getTypeName() + " instead).");
    }
    numRows = (IntWritable)
        ((ConstantObjectInspector) args[0]).getWritableConstantValue();
    if (numRows == null || numRows.get() < 1) {
      throw new UDFArgumentException(
          "STACK() expects its first argument to be >= 1.");
    }
    numCols = (args.length - 1 + numRows.get() - 1) / numRows.get();
    for (int i = 0; i < numCols; i++) {
      returnOIResolvers.add(
          new GenericUDFUtils.ReturnObjectInspectorResolver());
      for (int j = 0; j < numRows.get(); j++) {
        int pos = j * numCols + i + 1;
        if (pos < args.length) {
          if (!returnOIResolvers.get(i).update(args[pos])) {
            throw new UDFArgumentException(
                "Argument" + (i + 1) + "'s type ("
                    + args[i + 1].getTypeName()
                    + ") should be equal to argument" + pos
                    + "'s type (" + args[pos].getTypeName() + ")");
          }
        }
      }
    }
    forwardObj = new Object[numCols];
    for (int i = 0; i < args.length; i++) {
      argOIs.add(args[i]);
    }
    ArrayList<String> fieldNames = new ArrayList<>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
    for (int i = 0; i < numCols; i++) {
      fieldNames.add("col" + i);
      fieldOIs.add(returnOIResolvers.get(i).get());
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] args)
      throws HiveException, UDFArgumentException {
    for (int i = 0; i < numRows.get(); i++) {
      for (int j = 0; j < numCols; j++) {
        int pos = i * numCols + j + 1;
        if (pos < args.length) {
          forwardObj[j] = returnOIResolvers.get(j).convertIfNecessary(
              args[pos], (ObjectInspector) argOIs.get(pos));
        } else {
          forwardObj[i] = null;
        }
      }
      forward(forwardObj);
    }
  }

  @Override
  public String toString() {
    return "stack";
  }
}
