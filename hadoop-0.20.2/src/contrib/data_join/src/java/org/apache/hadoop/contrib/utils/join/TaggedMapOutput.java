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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;

/**
 * This abstract class serves as the base class for the values that 
 * flow from the mappers to the reducers in a data join job. 
 * Typically, in such a job, the mappers will compute the source
 * tag of an input record based on its attributes or based on the 
 * file name of the input file. This tag will be used by the reducers
 * to re-group the values of a given key according to their source tags.
 * 
 */
public abstract class TaggedMapOutput implements Writable {
  protected Text tag;

  public TaggedMapOutput() {
    this.tag = new Text("");
  }

  public Text getTag() {
    return tag;
  }

  public void setTag(Text tag) {
    this.tag = tag;
  }

  public abstract Writable getData();
  
  public TaggedMapOutput clone(JobConf job) {
    return (TaggedMapOutput) WritableUtils.clone(this, job);
  }

}
