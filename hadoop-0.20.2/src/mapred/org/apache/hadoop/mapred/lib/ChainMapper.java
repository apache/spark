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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * The ChainMapper class allows to use multiple Mapper classes within a single
 * Map task.
 * <p/>
 * The Mapper classes are invoked in a chained (or piped) fashion, the output of
 * the first becomes the input of the second, and so on until the last Mapper,
 * the output of the last Mapper will be written to the task's output.
 * <p/>
 * The key functionality of this feature is that the Mappers in the chain do not
 * need to be aware that they are executed in a chain. This enables having
 * reusable specialized Mappers that can be combined to perform composite
 * operations within a single task.
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
 * ChainMapper, this is done by the addMapper for the last mapper in the chain.
 * <p/>
 * ChainMapper usage pattern:
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
public class ChainMapper implements Mapper {

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
   * <p/>
   *
   * @param job              job's JobConf to add the Mapper class.
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
    job.setMapperClass(ChainMapper.class);
    job.setMapOutputKeyClass(outputKeyClass);
    job.setMapOutputValueClass(outputValueClass);
    Chain.addMapper(true, job, klass, inputKeyClass, inputValueClass,
                    outputKeyClass, outputValueClass, byValue, mapperConf);
  }

  private Chain chain;

  /**
   * Constructor.
   */
  public ChainMapper() {
    chain = new Chain(true);
  }

  /**
   * Configures the ChainMapper and all the Mappers in the chain.
   * <p/>
   * If this method is overriden <code>super.configure(...)</code> should be
   * invoked at the beginning of the overwriter method.
   */
  public void configure(JobConf job) {
    chain.configure(job);
  }

  /**
   * Chains the <code>map(...)</code> methods of the Mappers in the chain.
   */
  @SuppressWarnings({"unchecked"})
  public void map(Object key, Object value, OutputCollector output,
                  Reporter reporter) throws IOException {
    Mapper mapper = chain.getFirstMap();
    if (mapper != null) {
      mapper.map(key, value, chain.getMapperCollector(0, output, reporter),
                 reporter);
    }
  }

  /**
   * Closes  the ChainMapper and all the Mappers in the chain.
   * <p/>
   * If this method is overriden <code>super.close()</code> should be
   * invoked at the end of the overwriter method.
   */
  public void close() throws IOException {
    chain.close();
  }

}
