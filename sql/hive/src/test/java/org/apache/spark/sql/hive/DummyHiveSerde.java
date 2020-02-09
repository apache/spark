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

package org.apache.spark.sql.hive;

import java.util.*;


import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.STRING;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DummyHiveSerde implements SerDe {

  private List<String> columnNames;
  private ObjectInspector objectInspector;
  private Map<String, String> rowMap;
  private List<String> rowFields;
  private long deserializedByteCount;
  private SerDeStats stats;

  @Override
  public void initialize(Configuration conf, Properties tableProperties) throws SerDeException {

    final List<TypeInfo> columnTypes =
        TypeInfoUtils.getTypeInfosFromTypeString(tableProperties.getProperty(LIST_COLUMN_TYPES));

    // verify types
    for (TypeInfo type : columnTypes) {
      if (!type.getCategory().equals(PRIMITIVE)
          || !((PrimitiveTypeInfo) type).getPrimitiveCategory().equals(STRING)) {
        throw new SerDeException("This SerDe only supports strings.");
      }
    }

    columnNames = Arrays.asList(tableProperties.getProperty(LIST_COLUMNS).split(","));

    List<ObjectInspector> columnObjectInspectors =
        Collections.nCopies(columnNames.size(), javaStringObjectInspector);

    objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, columnObjectInspectors);

    rowMap = new HashMap<String, String>(columnNames.size());
    rowFields = new ArrayList<String>(columnNames.size());
    stats = new SerDeStats();
    deserializedByteCount = 0;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objectInspector) throws SerDeException {

    StringBuilder builder = new StringBuilder();
    StructObjectInspector structOI = (StructObjectInspector) objectInspector;

    List<? extends StructField> structFields = structOI.getAllStructFieldRefs();

    for (int i = 0; i < structFields.size(); i++) {
      StructField structField = structFields.get(i);
      Object fieldData = structOI.getStructFieldData(obj, structField);
      StringObjectInspector fieldOI = (StringObjectInspector) structField.getFieldObjectInspector();
      String fieldContent = fieldOI.getPrimitiveJavaObject(fieldData);
      if (fieldContent != null) {
        String fieldName = columnNames.get(i);

        if (builder.length() > 0) {
          builder.append("\001");
        }
        builder.append(fieldName);
        builder.append("\002");
        builder.append(fieldContent);
      }
    }
    return new Text(builder.toString());
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    // reset internal state
    rowMap.clear();
    rowFields.clear();

    Text text = (Text) writable;
    String content = text.toString();

    // update stats
    deserializedByteCount += text.getBytes().length;

    // parse map
    String[] pairs = content.split("\001");
    for (String pair : pairs) {
      int delimiterIndex = pair.indexOf('\002');

      if (delimiterIndex >= 0) {
        String key = pair.substring(0, delimiterIndex);
        String value = pair.substring(delimiterIndex + 1);
        rowMap.put(key, value);
      }
    }

    for (String columnName : columnNames) {
      rowFields.add(rowMap.get(columnName));
    }

    return rowFields;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public SerDeStats getSerDeStats() {
    stats.setRawDataSize(deserializedByteCount);
    return stats;
  }
}
