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
package org.apache.hadoop.vaidya.statistics.job;

import java.util.Hashtable;
import java.util.Map;

/**
 *
 */
public class TaskStatistics {
  
  /*
   * Stores task statistics as Enum/String key,value pairs.
   */
  private Hashtable<Enum, String>  _task = new Hashtable<Enum, String>();
  
  /* 
   * Get Long key value
   */
  public long getLongValue(Enum key) {
    if (this._task.get(key) == null) {
      return (long)0;
    }
    else {
      return Long.parseLong(this._task.get(key));
    }
  } 

  /*
   * Get key type Double
   */
  public double getDoubleValue(Enum key) {
    if (this._task.get(key) == null) {
      return (double)0;
    } else {
      return Double.parseDouble(this._task.get(key));
    }
  }
 
  /*
   * Get key of type String
   */
  public String getStringValue(Enum key) {
    if (this._task.get(key) == null) {
      return "";
    } else {
     return this._task.get(key);
    }
  }

  /*
   * Set long key value 
   */
  public void setValue(Enum key, long value) {
    this._task.put(key, Long.toString(value));
  }
  
  /*
   * Set double key value
   */
  public void setValue(Enum key, double value) {
    this._task.put(key, Double.toString(value));
  }
  
  /*
   * Set String key value
   */
  public void setValue(Enum key, String value) {
    this._task.put(key, value);
  }
  
  /*
   * Print the key/values pairs for a task 
   */
  public void  printKeys () {
    java.util.Set<Map.Entry<Enum, String>> task = this._task.entrySet();
    int size = task.size();
    java.util.Iterator<Map.Entry<Enum, String>> kv = task.iterator();
    for (int i = 0; i < size; i++)
    {
      Map.Entry<Enum, String> entry = (Map.Entry<Enum, String>) kv.next();
      Enum key = entry.getKey();
      String value = entry.getValue();
      System.out.println("Key:<" + key.name() + ">, value:<"+ value +">"); 
    }
  }
}
