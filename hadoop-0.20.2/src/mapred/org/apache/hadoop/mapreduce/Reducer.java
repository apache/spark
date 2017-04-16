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

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.RawKeyValueIterator;

/** 
 * Reduces a set of intermediate values which share a key to a smaller set of
 * values.  
 * 
 * <p><code>Reducer</code> implementations 
 * can access the {@link Configuration} for the job via the 
 * {@link JobContext#getConfiguration()} method.</p>

 * <p><code>Reducer</code> has 3 primary phases:</p>
 * <ol>
 *   <li>
 *   
 *   <h4 id="Shuffle">Shuffle</h4>
 *   
 *   <p>The <code>Reducer</code> copies the sorted output from each 
 *   {@link Mapper} using HTTP across the network.</p>
 *   </li>
 *   
 *   <li>
 *   <h4 id="Sort">Sort</h4>
 *   
 *   <p>The framework merge sorts <code>Reducer</code> inputs by 
 *   <code>key</code>s 
 *   (since different <code>Mapper</code>s may have output the same key).</p>
 *   
 *   <p>The shuffle and sort phases occur simultaneously i.e. while outputs are
 *   being fetched they are merged.</p>
 *      
 *   <h5 id="SecondarySort">SecondarySort</h5>
 *   
 *   <p>To achieve a secondary sort on the values returned by the value 
 *   iterator, the application should extend the key with the secondary
 *   key and define a grouping comparator. The keys will be sorted using the
 *   entire key, but will be grouped using the grouping comparator to decide
 *   which keys and values are sent in the same call to reduce.The grouping 
 *   comparator is specified via 
 *   {@link Job#setGroupingComparatorClass(Class)}. The sort order is
 *   controlled by 
 *   {@link Job#setSortComparatorClass(Class)}.</p>
 *   
 *   
 *   For example, say that you want to find duplicate web pages and tag them 
 *   all with the url of the "best" known example. You would set up the job 
 *   like:
 *   <ul>
 *     <li>Map Input Key: url</li>
 *     <li>Map Input Value: document</li>
 *     <li>Map Output Key: document checksum, url pagerank</li>
 *     <li>Map Output Value: url</li>
 *     <li>Partitioner: by checksum</li>
 *     <li>OutputKeyComparator: by checksum and then decreasing pagerank</li>
 *     <li>OutputValueGroupingComparator: by checksum</li>
 *   </ul>
 *   </li>
 *   
 *   <li>   
 *   <h4 id="Reduce">Reduce</h4>
 *   
 *   <p>In this phase the 
 *   {@link #reduce(Object, Iterable, Context)}
 *   method is called for each <code>&lt;key, (collection of values)></code> in
 *   the sorted inputs.</p>
 *   <p>The output of the reduce task is typically written to a 
 *   {@link RecordWriter} via 
 *   {@link Context#write(Object, Object)}.</p>
 *   </li>
 * </ol>
 * 
 * <p>The output of the <code>Reducer</code> is <b>not re-sorted</b>.</p>
 * 
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class IntSumReducer<Key> extends Reducer<Key,IntWritable,
 *                                                 Key,IntWritable> {
 *   private IntWritable result = new IntWritable();
 * 
 *   public void reduce(Key key, Iterable<IntWritable> values, 
 *                      Context context) throws IOException {
 *     int sum = 0;
 *     for (IntWritable val : values) {
 *       sum += val.get();
 *     }
 *     result.set(sum);
 *     context.collect(key, result);
 *   }
 * }
 * </pre></blockquote></p>
 * 
 * @see Mapper
 * @see Partitioner
 */
public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

  public class Context 
    extends ReduceContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    public Context(Configuration conf, TaskAttemptID taskid,
                   RawKeyValueIterator input, 
                   Counter inputKeyCounter,
                   Counter inputValueCounter,
                   RecordWriter<KEYOUT,VALUEOUT> output,
                   OutputCommitter committer,
                   StatusReporter reporter,
                   RawComparator<KEYIN> comparator,
                   Class<KEYIN> keyClass,
                   Class<VALUEIN> valueClass
                   ) throws IOException, InterruptedException {
      super(conf, taskid, input, inputKeyCounter, inputValueCounter,
            output, committer, reporter, 
            comparator, keyClass, valueClass);
    }
  }

  /**
   * Called once at the start of the task.
   */
  protected void setup(Context context
                       ) throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * This method is called once for each key. Most applications will define
   * their reduce class by overriding this method. The default implementation
   * is an identity function.
   */
  @SuppressWarnings("unchecked")
  protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context
                        ) throws IOException, InterruptedException {
    for(VALUEIN value: values) {
      context.write((KEYOUT) key, (VALUEOUT) value);
    }
  }

  /**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context
                         ) throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * Advanced application writers can use the 
   * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
   * control how the reduce task works.
   */
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKey()) {
      reduce(context.getCurrentKey(), context.getValues(), context);
    }
    cleanup(context);
  }
}
