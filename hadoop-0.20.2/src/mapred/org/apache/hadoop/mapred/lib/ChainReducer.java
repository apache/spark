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

import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * The ChainReducer class allows to chain multiple Mapper classes after a
 * Reducer within the Reducer task.
 * <p/>
 * For each record output by the Reducer, the Mapper classes are invoked in a
 * chained (or piped) fashion, the output of the first becomes the input of the
 * second, and so on until the last Mapper, the output of the last Mapper will
 * be written to the task's output.
 * <p/>
 * The key functionality of this feature is that the Mappers in the chain do not
 * need to be aware that they are executed after the Reducer or in a chain.
 * This enables having reusable specialized Mappers that can be combined to
 * perform composite operations within a single task.
 * <p/>
 * Special care has to be taken when creating chains that the key/values output
 * by a Mapper are valid for the following Mapper in the chain. It is assumed
 * all Mappers and the Reduce in the chain use maching output and input key and
 * value classes as no conversion is done by the chaining code.
 * <p/>
 * Using the ChainMapper and the ChainReducer classes is possible to compose
 * Map/Reduce jobs that look like <code>[MAP+ / REDUCE MAP*]</code>. And
 * immediate benefit of this pattern is a dramatic reduction in disk IO.
 * <p/>
 * IMPORTANT: There is no need to specify the output key/value classes for the
 * ChainReducer, this is done by the setReducer or the addMapper for the last
 * element in the chain.
 * <p/>
 * ChainReducer usage pattern:
 * <p/>
 * <pre>
 * ...
 * conf.setJobName("chain");
 * conf.setInputFormat(TextInputFormat.class);
 * conf.setOutputFormat(TextOutputFormat.class);
 * <p/>
 * JobConf mapAConf = new JobConf(false);
 * ...
 * ChainMapper.addMapper(conf, AMap.class, LongWritable.class, Text.class,
 *   Text.class, Text.class, true, mapAConf);
 * <p/>
 * JobConf mapBConf = new JobConf(false);
 * ...
 * ChainMapper.addMapper(conf, BMap.class, Text.class, Text.class,
 *   LongWritable.class, Text.class, false, mapBConf);
 * <p/>
 * JobConf reduceConf = new JobConf(false);
 * ...
 * ChainReducer.setReducer(conf, XReduce.class, LongWritable.class, Text.class,
 *   Text.class, Text.class, true, reduceConf);
 * <p/>
 * ChainReducer.addMapper(conf, CMap.class, Text.class, Text.class,
 *   LongWritable.class, Text.class, false, null);
 * <p/>
 * ChainReducer.addMapper(conf, DMap.class, LongWritable.class, Text.class,
 *   LongWritable.class, LongWritable.class, true, null);
 * <p/>
 * FileInputFormat.setInputPaths(conf, inDir);
 * FileOutputFormat.setOutputPath(conf, outDir);
 * ...
 * <p/>
 * JobClient jc = new JobClient(conf);
 * RunningJob job = jc.submitJob(conf);
 * ...
 * </pre>
 */
public class ChainReducer implements Reducer {

  /**
   * Sets the Reducer class to the chain job's JobConf.
   * <p/>
   * It has to be specified how key and values are passed from one element of
   * the chain to the next, by value or by reference. If a Reducer leverages the
   * assumed semantics that the key and values are not modified by the collector
   * 'by value' must be used. If the Reducer does not expect this semantics, as
   * an optimization to avoid serialization and deserialization 'by reference'
   * can be used.
   * <p/>
   * For the added Reducer the configuration given for it,
   * <code>reducerConf</code>, have precedence over the job's JobConf. This
   * precedence is in effect when the task is running.
   * <p/>
   * IMPORTANT: There is no need to specify the output key/value classes for the
   * ChainReducer, this is done by the setReducer or the addMapper for the last
   * element in the chain.
   *
   * @param job              job's JobConf to add the Reducer class.
   * @param klass            the Reducer class to add.
   * @param inputKeyClass    reducer input key class.
   * @param inputValueClass  reducer input value class.
   * @param outputKeyClass   reducer output key class.
   * @param outputValueClass reducer output value class.
   * @param byValue          indicates if key/values should be passed by value
   * to the next Mapper in the chain, if any.
   * @param reducerConf      a JobConf with the configuration for the Reducer
   * class. It is recommended to use a JobConf without default values using the
   * <code>JobConf(boolean loadDefaults)</code> constructor with FALSE.
   */
  public static <K1, V1, K2, V2> void setReducer(JobConf job,
                           Class<? extends Reducer<K1, V1, K2, V2>> klass,
                           Class<? extends K1> inputKeyClass,
                           Class<? extends V1> inputValueClass,
                           Class<? extends K2> outputKeyClass,
                           Class<? extends V2> outputValueClass,
                           boolean byValue, JobConf reducerConf) {
    job.setReducerClass(ChainReducer.class);
    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);
    Chain.setReducer(job, klass, inputKeyClass, inputValueClass, outputKeyClass,
                     outputValueClass, byValue, reducerConf);
  }

  /**
   * Adds a Mapper class to the chain job's JobConf.
   * <p/>
   * It has to be specified how key and values are passed from one element of
   * the chain to the next, by value or by reference. If a Mapper leverages the
   * assumed semantics that the key and values are not modified by the collector
   * 'by value' must be used. If the Mapper does not expect this semantics, as
   * an optimization to avoid serialization and deserialization 'by reference'
   * can be used.
   * <p/>
   * For the added Mapper the configuration given for it,
   * <code>mapperConf</code>, have precedence over the job's JobConf. This
   * precedence is in effect when the task is running.
   * <p/>
   * IMPORTANT: There is no need to specify the output key/value classes for the
   * ChainMapper, this is done by the addMapper for the last mapper in the chain
   * .
   *
   * @param job              chain job's JobConf to add the Mapper class.
   * @param klass            the Mapper class to add.
   * @param inputKeyClass    mapper input key class.
   * @param inputValueClass  mapper input value class.
   * @param outputKeyClass   mapper output key class.
   * @param outputValueClass mapper output value class.
   * @param byValue          indicates if key/values should be passed by value
   * to the next Mapper in the chain, if any.
   * @param mapperConf       a JobConf with the configuration for the Mapper
   * class. It is recommended to use a JobConf without default values using the
   * <code>JobConf(boolean loadDefaults)</code> constructor with FALSE.
   */
  public static <K1, V1, K2, V2> void addMapper(JobConf job,
                           Class<? extends Mapper<K1, V1, K2, V2>> klass,
                           Class<? extends K1> inputKeyClass,
                           Class<? extends V1> inputValueClass,
                           Class<? extends K2> outputKeyClass,
                           Class<? extends V2> outputValueClass,
                           boolean byValue, JobConf mapperConf) {
    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);
    Chain.addMapper(false, job, klass, inputKeyClass, inputValueClass,
                    outputKeyClass, outputValueClass, byValue, mapperConf);
  }

  private Chain chain;

  /**
   * Constructor.
   */
  public ChainReducer() {
    chain = new Chain(false);
  }

  /**
   * Configures the ChainReducer, the Reducer and all the Mappers in the chain.
   * <p/>
   * If this method is overriden <code>super.configure(...)</code> should be
   * invoked at the beginning of the overwriter method.
   */
  public void configure(JobConf job) {
    chain.configure(job);
  }

  /**
   * Chains the <code>reduce(...)</code> method of the Reducer with the
   * <code>map(...) </code> methods of the Mappers in the chain.
   */
  @SuppressWarnings({"unchecked"})
  public void reduce(Object key, Iterator values, OutputCollector output,
                     Reporter reporter) throws IOException {
    Reducer reducer = chain.getReducer();
    if (reducer != null) {
      reducer.reduce(key, values, chain.getReducerCollector(output, reporter),
                     reporter);
    }
  }

  /**
   * Closes  the ChainReducer, the Reducer and all the Mappers in the chain.
   * <p/>
   * If this method is overriden <code>super.close()</code> should be
   * invoked at the end of the overwriter method.
   */
  public void close() throws IOException {
    chain.close();
  }

}
