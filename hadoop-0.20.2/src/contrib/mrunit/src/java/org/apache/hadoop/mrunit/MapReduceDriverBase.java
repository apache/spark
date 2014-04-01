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
package org.apache.hadoop.mrunit;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Harness that allows you to test a Mapper and a Reducer instance together
 * You provide the input key and value that should be sent to the Mapper, and
 * outputs you expect to be sent by the Reducer to the collector for those
 * inputs. By calling runTest(), the harness will deliver the input to the
 * Mapper, feed the intermediate results to the Reducer (without checking
 * them), and will check the Reducer's outputs against the expected results.
 * This is designed to handle a single (k, v)* -> (k, v)* case from the
 * Mapper/Reducer pair, representing a single unit test.
 */
public abstract class MapReduceDriverBase<K1, V1, K2 extends Comparable, V2, K3, V3>
    extends TestDriver<K1, V1, K3, V3> {

  public static final Log LOG = LogFactory.getLog(MapReduceDriverBase.class);

  protected List<Pair<K1, V1>> inputList;
  
  /** Key group comparator */
  protected Comparator<K2> keyGroupComparator;
  
  /** Key value order comparator */
  protected Comparator<K2> keyValueOrderComparator;

  public MapReduceDriverBase() {
    inputList = new ArrayList<Pair<K1, V1>>();
    
    // create a simple comparator for key grouping and ordering
    Comparator<K2> simpleComparator = new Comparator<K2>() {
      @Override
      public int compare(K2 o1, K2 o2) {
        return o1.compareTo(o2);
      }
    };
    
    // assign simple key grouping and ordering comparator
    keyGroupComparator = simpleComparator;
    keyValueOrderComparator = null;
  }

  /**
   * Adds an input to send to the mapper
   * @param key
   * @param val
   */
  public void addInput(K1 key, V1 val) {
    inputList.add(new Pair<K1, V1>(key, val));
  }

  /**
   * Adds an input to send to the Mapper
   * @param input The (k, v) pair to add to the input list.
   */
  public void addInput(Pair<K1, V1> input) {
    if (null == input) {
      throw new IllegalArgumentException("Null input in addInput()");
    }

    inputList.add(input);
  }

  /**
   * Adds an output (k, v) pair we expect from the Reducer
   * @param outputRecord The (k, v) pair to add
   */
  public void addOutput(Pair<K3, V3> outputRecord) {
    if (null != outputRecord) {
      expectedOutputs.add(outputRecord);
    } else {
      throw new IllegalArgumentException("Tried to add null outputRecord");
    }
  }

  /**
   * Adds a (k, v) pair we expect as output from the Reducer
   * @param key
   * @param val
   */
  public void addOutput(K3 key, V3 val) {
    addOutput(new Pair<K3, V3>(key, val));
  }

  /**
   * Expects an input of the form "key \t val"
   * Forces the Mapper input types to Text.
   * @param input A string of the form "key \t val". Trims any whitespace.
   */
  public void addInputFromString(String input) {
    if (null == input) {
      throw new IllegalArgumentException("null input given to setInput");
    } else {
      Pair<Text, Text> inputPair = parseTabbedPair(input);
      if (null != inputPair) {
        // I know this is not type-safe, but I don't
        // know a better way to do this.
        addInput((Pair<K1, V1>) inputPair);
      } else {
        throw new IllegalArgumentException("Could not parse input pair in addInput");
      }
    }
  }

  /**
   * Expects an input of the form "key \t val"
   * Forces the Reducer output types to Text.
   * @param output A string of the form "key \t val". Trims any whitespace.
   */
  public void addOutputFromString(String output) {
    if (null == output) {
      throw new IllegalArgumentException("null input given to setOutput");
    } else {
      Pair<Text, Text> outputPair = parseTabbedPair(output);
      if (null != outputPair) {
        // I know this is not type-safe,
        // but I don't know a better way to do this.
        addOutput((Pair<K3, V3>) outputPair);
      } else {
        throw new IllegalArgumentException(
            "Could not parse output pair in setOutput");
      }
    }
  }

  public abstract List<Pair<K3, V3>> run() throws IOException;

  @Override
  public void runTest() throws RuntimeException {
    List<Pair<K3, V3>> reduceOutputs = null;
    boolean succeeded;

    try {
      reduceOutputs = run();
      validate(reduceOutputs);
    } catch (IOException ioe) {
      LOG.error("IOException: " + ioe.toString());
      LOG.debug("Setting success to false based on IOException");
      throw new RuntimeException();
    }
  }

  /** Take the outputs from the Mapper, combine all values for the
   *  same key, and sort them by key.
   * @param mapOutputs An unordered list of (key, val) pairs from the mapper
   * @return the sorted list of (key, list(val))'s to present to the reducer
   */
  public List<Pair<K2, List<V2>>> shuffle(List<Pair<K2, V2>> mapOutputs) {
    // step 1 - use the key group comparator to organise map outputs
    final TreeMap<K2, List<Pair<K2,V2>>> groupedByKey = 
        new TreeMap<K2, List<Pair<K2,V2>>>(keyGroupComparator);
    
    List<Pair<K2,V2>> groupedKeyList;
    for (Pair<K2, V2> mapOutput : mapOutputs) {
      groupedKeyList = groupedByKey.get(mapOutput.getFirst());
      
      if (groupedKeyList == null) {
        groupedKeyList = new ArrayList<Pair<K2,V2>>();
        groupedByKey.put(mapOutput.getFirst(), groupedKeyList);
      }
      
      groupedKeyList.add(mapOutput);
    }
    
    // step 2 - sort each key group using the key order comparator (if set)
    Comparator<Pair<K2,V2>> pairKeyComparator = new Comparator<Pair<K2, V2>>() {
      @Override
      public int compare(Pair<K2, V2> o1, Pair<K2, V2> o2) {
        return keyValueOrderComparator.compare(o1.getFirst(), o2.getFirst());
      }
    };
    
    // create shuffle stage output list
    List<Pair<K2, List<V2>>> outputKeyValuesList = 
        new ArrayList<Pair<K2,List<V2>>>();
    
    // populate output list
    for (Entry<K2, List<Pair<K2, V2>>> groupedByKeyEntry : 
          groupedByKey.entrySet()) {
      if (keyValueOrderComparator != null) {
        // sort the key/value pairs using the key order comparator (if set)
        Collections.sort(groupedByKeyEntry.getValue(), pairKeyComparator);
      }
      
      // create list to hold values for the grouped key
      List<V2> valuesList = new ArrayList<V2>();
      for (Pair<K2, V2> pair : groupedByKeyEntry.getValue()) {
        valuesList.add(pair.getSecond());
      }
      
      // add key and values to output list
      outputKeyValuesList.add(
          new Pair<K2,List<V2>>(groupedByKeyEntry.getKey(), valuesList));
    }
    
    // return output list
    return outputKeyValuesList;
  }
  
  /**
   * Set the key grouping comparator, similar to calling the following API 
   * calls but passing a real instance rather than just the class:
   * <UL>
   * <LI>pre 0.20.1 API: {@link JobConf#setOutputValueGroupingComparator(Class)}
   * <LI>0.20.1+ API: {@link Job#setGroupingComparatorClass(Class)}
   * </UL>
   * @param groupingComparator
   */
  public void setKeyGroupingComparator(
        RawComparator<K2> groupingComparator) {
    keyGroupComparator = groupingComparator;
  }
  
  /**
   * Set the key value order comparator, similar to calling the following API 
   * calls but passing a real instance rather than just the class:
   * <UL>
   * <LI>pre 0.20.1 API: {@link JobConf#setOutputKeyComparatorClass(Class)}
   * <LI>0.20.1+ API: {@link Job#setSortComparatorClass(Class)}
   * </UL>
   * @param orderComparator
   */
  public void setKeyOrderComparator(
        RawComparator<K2> orderComparator) {
    keyValueOrderComparator = orderComparator;
  }
}
