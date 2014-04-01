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

package org.apache.hadoop.contrib.utils.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This abstract class serves as the base class for the mapper class of a data
 * join job. This class expects its subclasses to implement methods for the
 * following functionalities:
 * 
 * 1. Compute the source tag of input values 2. Compute the map output value
 * object 3. Compute the map output key object
 * 
 * The source tag will be used by the reducer to determine from which source
 * (which table in SQL terminology) a value comes. Computing the map output
 * value object amounts to performing projecting/filtering work in a SQL
 * statement (through the select/where clauses). Computing the map output key
 * amounts to choosing the join key. This class provides the appropriate plugin
 * points for the user defined subclasses to implement the appropriate logic.
 * 
 */
public abstract class DataJoinMapperBase extends JobBase {

  protected String inputFile = null;

  protected JobConf job = null;

  protected Text inputTag = null;

  protected Reporter reporter = null;

  public void configure(JobConf job) {
    super.configure(job);
    this.job = job;
    this.inputFile = job.get("map.input.file");
    this.inputTag = generateInputTag(this.inputFile);
  }

  /**
   * Determine the source tag based on the input file name.
   * 
   * @param inputFile
   * @return the source tag computed from the given file name.
   */
  protected abstract Text generateInputTag(String inputFile);

  /**
   * Generate a tagged map output value. The user code can also perform
   * projection/filtering. If it decides to discard the input record when
   * certain conditions are met,it can simply return a null.
   * 
   * @param value
   * @return an object of TaggedMapOutput computed from the given value.
   */
  protected abstract TaggedMapOutput generateTaggedMapOutput(Object value);

  /**
   * Generate a map output key. The user code can compute the key
   * programmatically, not just selecting the values of some fields. In this
   * sense, it is more general than the joining capabilities of SQL.
   * 
   * @param aRecord
   * @return the group key for the given record
   */
  protected abstract Text generateGroupKey(TaggedMapOutput aRecord);

  public void map(Object key, Object value,
                  OutputCollector output, Reporter reporter) throws IOException {
    if (this.reporter == null) {
      this.reporter = reporter;
    }
    addLongValue("totalCount", 1);
    TaggedMapOutput aRecord = generateTaggedMapOutput(value);
    if (aRecord == null) {
      addLongValue("discardedCount", 1);
      return;
    }
    Text groupKey = generateGroupKey(aRecord);
    if (groupKey == null) {
      addLongValue("nullGroupKeyCount", 1);
      return;
    }
    output.collect(groupKey, aRecord);
    addLongValue("collectedCount", 1);
  }

  public void close() throws IOException {
    if (this.reporter != null) {
      this.reporter.setStatus(super.getReport());
    }
  }

  public void reduce(Object arg0, Iterator arg1,
                     OutputCollector arg2, Reporter arg3) throws IOException {
    // TODO Auto-generated method stub

  }
}
