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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * This class implements a mapper/reducer class that can be used to perform
 * field selections in a manner similar to unix cut. The input data is treated
 * as fields separated by a user specified separator (the default value is
 * "\t"). The user can specify a list of fields that form the map output keys,
 * and a list of fields that form the map output values. If the inputformat is
 * TextInputFormat, the mapper will ignore the key to the map function. and the
 * fields are from the value only. Otherwise, the fields are the union of those
 * from the key and those from the value.
 * 
 * The field separator is under attribute "mapred.data.field.separator"
 * 
 * The map output field list spec is under attribute "map.output.key.value.fields.spec".
 * The value is expected to be like "keyFieldsSpec:valueFieldsSpec"
 * key/valueFieldsSpec are comma (,) separated field spec: fieldSpec,fieldSpec,fieldSpec ...
 * Each field spec can be a simple number (e.g. 5) specifying a specific field, or a range
 * (like 2-5) to specify a range of fields, or an open range (like 3-) specifying all 
 * the fields starting from field 3. The open range field spec applies value fields only.
 * They have no effect on the key fields.
 * 
 * Here is an example: "4,3,0,1:6,5,1-3,7-". It specifies to use fields 4,3,0 and 1 for keys,
 * and use fields 6,5,1,2,3,7 and above for values.
 * 
 * The reduce output field list spec is under attribute "reduce.output.key.value.fields.spec".
 * 
 * The reducer extracts output key/value pairs in a similar manner, except that
 * the key is never ignored.
 * 
 */
public class FieldSelectionMapReduce<K, V>
    implements Mapper<K, V, Text, Text>, Reducer<Text, Text, Text, Text> {

  private String mapOutputKeyValueSpec;

  private boolean ignoreInputKey;

  private String fieldSeparator = "\t";

  private int[] mapOutputKeyFieldList = null;

  private int[] mapOutputValueFieldList = null;

  private int allMapValueFieldsFrom = -1;

  private String reduceOutputKeyValueSpec;

  private int[] reduceOutputKeyFieldList = null;

  private int[] reduceOutputValueFieldList = null;

  private int allReduceValueFieldsFrom = -1;

  private static Text emptyText = new Text("");

  public static final Log LOG = LogFactory.getLog("FieldSelectionMapReduce");

  private String specToString() {
    StringBuffer sb = new StringBuffer();
    sb.append("fieldSeparator: ").append(fieldSeparator).append("\n");

    sb.append("mapOutputKeyValueSpec: ").append(mapOutputKeyValueSpec).append(
        "\n");
    sb.append("reduceOutputKeyValueSpec: ").append(reduceOutputKeyValueSpec)
        .append("\n");

    sb.append("allMapValueFieldsFrom: ").append(allMapValueFieldsFrom).append(
        "\n");

    sb.append("allReduceValueFieldsFrom: ").append(allReduceValueFieldsFrom)
        .append("\n");

    int i = 0;

    sb.append("mapOutputKeyFieldList.length: ").append(
        mapOutputKeyFieldList.length).append("\n");
    for (i = 0; i < mapOutputKeyFieldList.length; i++) {
      sb.append("\t").append(mapOutputKeyFieldList[i]).append("\n");
    }
    sb.append("mapOutputValueFieldList.length: ").append(
        mapOutputValueFieldList.length).append("\n");
    for (i = 0; i < mapOutputValueFieldList.length; i++) {
      sb.append("\t").append(mapOutputValueFieldList[i]).append("\n");
    }

    sb.append("reduceOutputKeyFieldList.length: ").append(
        reduceOutputKeyFieldList.length).append("\n");
    for (i = 0; i < reduceOutputKeyFieldList.length; i++) {
      sb.append("\t").append(reduceOutputKeyFieldList[i]).append("\n");
    }
    sb.append("reduceOutputValueFieldList.length: ").append(
        reduceOutputValueFieldList.length).append("\n");
    for (i = 0; i < reduceOutputValueFieldList.length; i++) {
      sb.append("\t").append(reduceOutputValueFieldList[i]).append("\n");
    }
    return sb.toString();
  }

  /**
   * The identify function. Input key/value pair is written directly to output.
   */
  public void map(K key, V val,
                  OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    String valStr = val.toString();
    String[] inputValFields = valStr.split(this.fieldSeparator);
    String[] inputKeyFields = null;
    String[] fields = null;
    if (this.ignoreInputKey) {
      fields = inputValFields;
    } else {
      inputKeyFields = key.toString().split(this.fieldSeparator);
      fields = new String[inputKeyFields.length + inputValFields.length];
      int i = 0;
      for (i = 0; i < inputKeyFields.length; i++) {
        fields[i] = inputKeyFields[i];
      }
      for (i = 0; i < inputValFields.length; i++) {
        fields[inputKeyFields.length + i] = inputValFields[i];
      }
    }
    String newKey = selectFields(fields, mapOutputKeyFieldList, -1,
        fieldSeparator);
    String newVal = selectFields(fields, mapOutputValueFieldList,
        allMapValueFieldsFrom, fieldSeparator);

    if (newKey == null) {
      newKey = newVal;
      newVal = null;
    }
    Text newTextKey = emptyText;
    if (newKey != null) {
      newTextKey = new Text(newKey);
    }
    Text newTextVal = emptyText;
    if (newTextVal != null) {
      newTextVal = new Text(newVal);
    }
    output.collect(newTextKey, newTextVal);
  }

  /**
   * Extract the actual field numbers from the given field specs.
   * If a field spec is in the form of "n-" (like 3-), then n will be the 
   * return value. Otherwise, -1 will be returned.  
   * @param fieldListSpec an array of field specs
   * @param fieldList an array of field numbers extracted from the specs.
   * @return number n if some field spec is in the form of "n-", -1 otherwise.
   */
  private int extractFields(String[] fieldListSpec,
                            ArrayList<Integer> fieldList) {
    int allFieldsFrom = -1;
    int i = 0;
    int j = 0;
    int pos = -1;
    String fieldSpec = null;
    for (i = 0; i < fieldListSpec.length; i++) {
      fieldSpec = fieldListSpec[i];
      if (fieldSpec.length() == 0) {
        continue;
      }
      pos = fieldSpec.indexOf('-');
      if (pos < 0) {
        Integer fn = new Integer(fieldSpec);
        fieldList.add(fn);
      } else {
        String start = fieldSpec.substring(0, pos);
        String end = fieldSpec.substring(pos + 1);
        if (start.length() == 0) {
          start = "0";
        }
        if (end.length() == 0) {
          allFieldsFrom = Integer.parseInt(start);
          continue;
        }
        int startPos = Integer.parseInt(start);
        int endPos = Integer.parseInt(end);
        for (j = startPos; j <= endPos; j++) {
          fieldList.add(j);
        }
      }
    }
    return allFieldsFrom;
  }

  private void parseOutputKeyValueSpec() {
    String[] mapKeyValSpecs = mapOutputKeyValueSpec.split(":", -1);
    String[] mapKeySpec = mapKeyValSpecs[0].split(",");
    String[] mapValSpec = new String[0];
    if (mapKeyValSpecs.length > 1) {
      mapValSpec = mapKeyValSpecs[1].split(",");
    }

    int i = 0;
    ArrayList<Integer> fieldList = new ArrayList<Integer>();
    extractFields(mapKeySpec, fieldList);
    this.mapOutputKeyFieldList = new int[fieldList.size()];
    for (i = 0; i < fieldList.size(); i++) {
      this.mapOutputKeyFieldList[i] = fieldList.get(i).intValue();
    }

    fieldList = new ArrayList<Integer>();
    allMapValueFieldsFrom = extractFields(mapValSpec, fieldList);
    this.mapOutputValueFieldList = new int[fieldList.size()];
    for (i = 0; i < fieldList.size(); i++) {
      this.mapOutputValueFieldList[i] = fieldList.get(i).intValue();
    }

    String[] reduceKeyValSpecs = reduceOutputKeyValueSpec.split(":", -1);
    String[] reduceKeySpec = reduceKeyValSpecs[0].split(",");
    String[] reduceValSpec = new String[0];
    if (reduceKeyValSpecs.length > 1) {
      reduceValSpec = reduceKeyValSpecs[1].split(",");
    }

    fieldList = new ArrayList<Integer>();
    extractFields(reduceKeySpec, fieldList);
    this.reduceOutputKeyFieldList = new int[fieldList.size()];
    for (i = 0; i < fieldList.size(); i++) {
      this.reduceOutputKeyFieldList[i] = fieldList.get(i).intValue();
    }

    fieldList = new ArrayList<Integer>();
    allReduceValueFieldsFrom = extractFields(reduceValSpec, fieldList);
    this.reduceOutputValueFieldList = new int[fieldList.size()];
    for (i = 0; i < fieldList.size(); i++) {
      this.reduceOutputValueFieldList[i] = fieldList.get(i).intValue();
    }
  }

  public void configure(JobConf job) {
    this.fieldSeparator = job.get("mapred.data.field.separator", "\t");
    this.mapOutputKeyValueSpec = job.get("map.output.key.value.fields.spec",
        "0-:");
    this.ignoreInputKey = TextInputFormat.class.getCanonicalName().equals(
        job.getInputFormat().getClass().getCanonicalName());
    this.reduceOutputKeyValueSpec = job.get(
        "reduce.output.key.value.fields.spec", "0-:");
    parseOutputKeyValueSpec();
    LOG.info(specToString());
  }

  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  private static String selectFields(String[] fields, int[] fieldList,
      int allFieldsFrom, String separator) {
    String retv = null;
    int i = 0;
    StringBuffer sb = null;
    if (fieldList != null && fieldList.length > 0) {
      if (sb == null) {
        sb = new StringBuffer();
      }
      for (i = 0; i < fieldList.length; i++) {
        if (fieldList[i] < fields.length) {
          sb.append(fields[fieldList[i]]);
        }
        sb.append(separator);
      }
    }
    if (allFieldsFrom >= 0) {
      if (sb == null) {
        sb = new StringBuffer();
      }
      for (i = allFieldsFrom; i < fields.length; i++) {
        sb.append(fields[i]).append(separator);
      }
    }
    if (sb != null) {
      retv = sb.toString();
      if (retv.length() > 0) {
        retv = retv.substring(0, retv.length() - 1);
      }
    }
    return retv;
  }

  public void reduce(Text key, Iterator<Text> values,
                     OutputCollector<Text, Text> output, Reporter reporter)
    throws IOException {

    String keyStr = key.toString() + this.fieldSeparator;
    while (values.hasNext()) {
      String valStr = values.next().toString();
      valStr = keyStr + valStr;
      String[] fields = valStr.split(this.fieldSeparator);
      String newKey = selectFields(fields, reduceOutputKeyFieldList, -1,
          fieldSeparator);
      String newVal = selectFields(fields, reduceOutputValueFieldList,
          allReduceValueFieldsFrom, fieldSeparator);
      Text newTextKey = null;
      if (newKey != null) {
        newTextKey = new Text(newKey);
      }
      Text newTextVal = null;
      if (newVal != null) {
        newTextVal = new Text(newVal);
      }
      output.collect(newTextKey, newTextVal);
    }
  }
}
