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

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.KeyFieldHelper.KeyDescription;

 /**   
  *  Defines a way to partition keys based on certain key fields (also see
  *  {@link KeyFieldBasedComparator}.
  *  The key specification supported is of the form -k pos1[,pos2], where,
  *  pos is of the form f[.c][opts], where f is the number
  *  of the key field to use, and c is the number of the first character from
  *  the beginning of the field. Fields and character posns are numbered 
  *  starting with 1; a character position of zero in pos2 indicates the
  *  field's last character. If '.c' is omitted from pos1, it defaults to 1
  *  (the beginning of the field); if omitted from pos2, it defaults to 0 
  *  (the end of the field).
  * 
  */
public class KeyFieldBasedPartitioner<K2, V2> implements Partitioner<K2, V2> {

  private static final Log LOG = LogFactory.getLog(KeyFieldBasedPartitioner.class.getName());
  private int numOfPartitionFields;
  
  private KeyFieldHelper keyFieldHelper = new KeyFieldHelper();

  public void configure(JobConf job) {
    String keyFieldSeparator = job.get("map.output.key.field.separator", "\t");
    keyFieldHelper.setKeyFieldSeparator(keyFieldSeparator);
    if (job.get("num.key.fields.for.partition") != null) {
      LOG.warn("Using deprecated num.key.fields.for.partition. " +
      		"Use mapred.text.key.partitioner.options instead");
      this.numOfPartitionFields = job.getInt("num.key.fields.for.partition",0);
      keyFieldHelper.setKeyFieldSpec(1,numOfPartitionFields);
    } else {
      String option = job.getKeyFieldPartitionerOption();
      keyFieldHelper.parseOption(option);
    }
  }

  public int getPartition(K2 key, V2 value,
      int numReduceTasks) {
    byte[] keyBytes;

    List <KeyDescription> allKeySpecs = keyFieldHelper.keySpecs();
    if (allKeySpecs.size() == 0) {
      return getPartition(key.toString().hashCode(), numReduceTasks);
    }

    try {
      keyBytes = key.toString().getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("The current system does not " +
          "support UTF-8 encoding!", e);
    }
    // return 0 if the key is empty
    if (keyBytes.length == 0) {
      return 0;
    }
    
    int []lengthIndicesFirst = keyFieldHelper.getWordLengths(keyBytes, 0, 
        keyBytes.length);
    int currentHash = 0;
    for (KeyDescription keySpec : allKeySpecs) {
      int startChar = keyFieldHelper.getStartOffset(keyBytes, 0, keyBytes.length, 
          lengthIndicesFirst, keySpec);
       // no key found! continue
      if (startChar < 0) {
        continue;
      }
      int endChar = keyFieldHelper.getEndOffset(keyBytes, 0, keyBytes.length, 
          lengthIndicesFirst, keySpec);
      currentHash = hashCode(keyBytes, startChar, endChar, 
          currentHash);
    }
    return getPartition(currentHash, numReduceTasks);
  }
  
  protected int hashCode(byte[] b, int start, int end, int currentHash) {
    for (int i = start; i <= end; i++) {
      currentHash = 31*currentHash + b[i];
    }
    return currentHash;
  }

  protected int getPartition(int hash, int numReduceTasks) {
    return (hash & Integer.MAX_VALUE) % numReduceTasks;
  }
}
