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

package org.apache.hadoop.mapred.lib.aggregate;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This is the main class for creating a map/reduce job using Aggregate
 * framework. The Aggregate is a specialization of map/reduce framework,
 * specilizing for performing various simple aggregations.
 * 
 * Generally speaking, in order to implement an application using Map/Reduce
 * model, the developer is to implement Map and Reduce functions (and possibly
 * combine function). However, a lot of applications related to counting and
 * statistics computing have very similar characteristics. Aggregate abstracts
 * out the general patterns of these functions and implementing those patterns.
 * In particular, the package provides generic mapper/redducer/combiner classes,
 * and a set of built-in value aggregators, and a generic utility class that
 * helps user create map/reduce jobs using the generic class. The built-in
 * aggregators include:
 * 
 * sum over numeric values count the number of distinct values compute the
 * histogram of values compute the minimum, maximum, media,average, standard
 * deviation of numeric values
 * 
 * The developer using Aggregate will need only to provide a plugin class
 * conforming to the following interface:
 * 
 * public interface ValueAggregatorDescriptor { public ArrayList<Entry>
 * generateKeyValPairs(Object key, Object value); public void
 * configure(JobConfjob); }
 * 
 * The package also provides a base class, ValueAggregatorBaseDescriptor,
 * implementing the above interface. The user can extend the base class and
 * implement generateKeyValPairs accordingly.
 * 
 * The primary work of generateKeyValPairs is to emit one or more key/value
 * pairs based on the input key/value pair. The key in an output key/value pair
 * encode two pieces of information: aggregation type and aggregation id. The
 * value will be aggregated onto the aggregation id according the aggregation
 * type.
 * 
 * This class offers a function to generate a map/reduce job using Aggregate
 * framework. The function takes the following parameters: input directory spec
 * input format (text or sequence file) output directory a file specifying the
 * user plugin class
 * 
 */
public class ValueAggregatorJob {

  public static JobControl createValueAggregatorJobs(String args[]
    , Class<? extends ValueAggregatorDescriptor>[] descriptors) throws IOException {
    
    JobControl theControl = new JobControl("ValueAggregatorJobs");
    ArrayList<Job> dependingJobs = new ArrayList<Job>();
    JobConf aJobConf = createValueAggregatorJob(args);
    if(descriptors != null)
      setAggregatorDescriptors(aJobConf, descriptors);
    Job aJob = new Job(aJobConf, dependingJobs);
    theControl.addJob(aJob);
    return theControl;
  }

  public static JobControl createValueAggregatorJobs(String args[]) throws IOException {
    return createValueAggregatorJobs(args, null);
  }
  
  /**
   * Create an Aggregate based map/reduce job.
   * 
   * @param args the arguments used for job creation. Generic hadoop
   * arguments are accepted.
   * @return a JobConf object ready for submission.
   * 
   * @throws IOException
   * @see GenericOptionsParser
   */
  public static JobConf createValueAggregatorJob(String args[])
    throws IOException {

    Configuration conf = new Configuration();
    
    GenericOptionsParser genericParser 
      = new GenericOptionsParser(conf, args);
    args = genericParser.getRemainingArgs();
    
    if (args.length < 2) {
      System.out.println("usage: inputDirs outDir "
          + "[numOfReducer [textinputformat|seq [specfile [jobName]]]]");
      GenericOptionsParser.printGenericCommandUsage(System.out);
      System.exit(1);
    }
    String inputDir = args[0];
    String outputDir = args[1];
    int numOfReducers = 1;
    if (args.length > 2) {
      numOfReducers = Integer.parseInt(args[2]);
    }

    Class<? extends InputFormat> theInputFormat =
      TextInputFormat.class;
    if (args.length > 3 && 
        args[3].compareToIgnoreCase("textinputformat") == 0) {
      theInputFormat = TextInputFormat.class;
    } else {
      theInputFormat = SequenceFileInputFormat.class;
    }

    Path specFile = null;

    if (args.length > 4) {
      specFile = new Path(args[4]);
    }

    String jobName = "";
    
    if (args.length > 5) {
      jobName = args[5];
    }
    
    JobConf theJob = new JobConf(conf);
    if (specFile != null) {
      theJob.addResource(specFile);
    }
    String userJarFile = theJob.get("user.jar.file");
    if (userJarFile == null) {
      theJob.setJarByClass(ValueAggregator.class);
    } else {
      theJob.setJar(userJarFile);
    }
    theJob.setJobName("ValueAggregatorJob: " + jobName);

    FileInputFormat.addInputPaths(theJob, inputDir);

    theJob.setInputFormat(theInputFormat);
    
    theJob.setMapperClass(ValueAggregatorMapper.class);
    FileOutputFormat.setOutputPath(theJob, new Path(outputDir));
    theJob.setOutputFormat(TextOutputFormat.class);
    theJob.setMapOutputKeyClass(Text.class);
    theJob.setMapOutputValueClass(Text.class);
    theJob.setOutputKeyClass(Text.class);
    theJob.setOutputValueClass(Text.class);
    theJob.setReducerClass(ValueAggregatorReducer.class);
    theJob.setCombinerClass(ValueAggregatorCombiner.class);
    theJob.setNumMapTasks(1);
    theJob.setNumReduceTasks(numOfReducers);
    return theJob;
  }

  public static JobConf createValueAggregatorJob(String args[]
    , Class<? extends ValueAggregatorDescriptor>[] descriptors)
  throws IOException {
    JobConf job = createValueAggregatorJob(args);
    setAggregatorDescriptors(job, descriptors);
    return job;
  }
  
  public static void setAggregatorDescriptors(JobConf job
      , Class<? extends ValueAggregatorDescriptor>[] descriptors) {
    job.setInt("aggregator.descriptor.num", descriptors.length);
    //specify the aggregator descriptors
    for(int i=0; i< descriptors.length; i++) {
      job.set("aggregator.descriptor." + i, "UserDefined," + descriptors[i].getName());
    }    
  }
  
  /**
   * create and run an Aggregate based map/reduce job.
   * 
   * @param args the arguments used for job creation
   * @throws IOException
   */
  public static void main(String args[]) throws IOException {
    JobConf job = ValueAggregatorJob.createValueAggregatorJob(args);
    JobClient.runJob(job);
  }
}
