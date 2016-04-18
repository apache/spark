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

package org.apache.spark.examples;

import org.apache.spark.scheduler.eagle.EagleTriple;
import org.apache.spark.scheduler.eagle.EagleProperties$;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.concurrent.*;

import java.io.BufferedReader;
import java.io.FileReader;

public class JavaSleep {

  public static class JobToSubmit{
	public int waitingTime;
	public int nrTasks;
	public ArrayList<Integer> taskRTs;
	public int estimate;

	public JobToSubmit(int waitingTime, int nrTasks, ArrayList<Integer> taskRTs, int estimate){
		this.waitingTime=waitingTime;
		this.nrTasks=nrTasks;
		this.taskRTs=taskRTs;
		this.estimate=estimate;
	}
  }

  public static void submitNewJob(JobToSubmit jts, JavaSparkContext ctx, String jobType){

    int tasks=jts.nrTasks;

    List<EagleTriple<Integer,Boolean,Double>> l = new ArrayList<EagleTriple<Integer,Boolean,Double>>(tasks);
    for (int i = 0; i < tasks; i++)
    	l.add(new EagleTriple(jts.taskRTs.get(i),jobType.equals("small"), (double)jts.estimate));
    
    ctx.setLocalProperty(EagleProperties$.MODULE$.EAGLE_JOB_ESTIMATED_RUNTIME(),Integer.toString(jts.estimate));
    ctx.setLocalProperty(EagleProperties$.MODULE$.EAGLE_JOB_SHORT(),""+jobType.equals("small"));

    JavaRDD<EagleTriple<Integer,Boolean,Double>> dataSet = ctx.parallelize(l, tasks);

    int counter=(int)dataSet.map(new Function<EagleTriple<Integer,Boolean,Double>, Integer>() {
      @Override
      public Integer call(EagleTriple<Integer,Boolean,Double> triple) throws Exception {
       	Thread.sleep(triple.getFirst()); 
        return triple.getFirst();
      }
    }).count();

  }


  public static void main(String[] args) throws Exception {
    if (args.length < 6) {
      System.err.println("Usage: JavaWordCount <master> <sleep_sec> <nr_tasks> <hostname> <small> <job_desc>");
      System.exit(1);
    }
    final String  master=args[0];
    final Integer sleep_tout=Integer.valueOf(args[1]);
    final Integer tasks=Integer.valueOf(args[2]);
    final String  hostname=args[3];
    final String  jobType=args[4];
    final String  jobDescription=args[5]; 

    ArrayList<JobToSubmit> jobs = new ArrayList<JobToSubmit> ();

    final JavaSparkContext ctx = new JavaSparkContext(master,hostname,System.getenv("SPARK_HOME"), System.getenv("SPARK_EXAMPLES_JAR"));

	System.out.println("SparkContext created, waiting for Executors to get started!!");
    Thread.sleep(60000);

    BufferedReader br = new BufferedReader(new FileReader(jobDescription));
    String line="";
    int line_ctr=0;
    while((line=br.readLine())!=null){
                 line_ctr++;
                 String [] splits = line.split("\\s+");
	  	 int wait = Integer.valueOf(splits[0]);
		 int nrtsk = Integer.valueOf(splits[1]);
		 int estimate = Integer.valueOf(splits[2]);
		 ArrayList<Integer> taskrts =  new ArrayList<Integer> ();		 

		 for (int i=0;i<nrtsk;i++){
			taskrts.add(Integer.valueOf(splits[3+i]));	
		 }		
		 System.out.println("Adding job nr: "+line_ctr+" nr task: "+nrtsk+"  estimate: "+estimate);
		 jobs.add(new JobToSubmit(wait,nrtsk,taskrts,estimate)); 
    } 

 
    ExecutorService executorService = Executors.newFixedThreadPool(line_ctr+1);

    while(true){
		final JobToSubmit newJob=jobs.remove(0);
		try{
			Thread.sleep(newJob.waitingTime);
		}catch(Exception e){}

		Future<Long> future1 = executorService.submit(new Callable<Long>() {
        
                public Long call() throws Exception {
			submitNewJob(newJob,ctx,jobType);
			return (long)1;
        	}
   		});

		if(jobs.size()==0) break;
    }



    executorService.shutdown();
    try {
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }   
    executorService.shutdownNow();
    System.exit(0);
  }
}
