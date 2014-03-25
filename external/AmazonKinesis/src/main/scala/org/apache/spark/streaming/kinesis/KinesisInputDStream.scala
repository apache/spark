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

package org.apache.spark.streaming.kinesis

import org.apache.spark.streaming.StreamingContext
import scala.reflect.ClassTag
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.NetworkReceiver
import com.amazonaws.auth.AWSCredentialsProvider
import java.util.UUID
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import java.net.UnknownHostException
import java.net.InetAddress
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import java.nio.charset.Charset
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import org.apache.spark.streaming.dstream.NetworkInputDStream
import scala.collection.JavaConversions._
import java.util.List


private[streaming]
class KinesisInputDStream[T: ClassTag](
    @transient ssc_ : StreamingContext,
    accesskey:String,
    accessSecretKey:String,
    kinesisStream:String,
    kinesisEndpoint:String,
    storageLevel: StorageLevel
  ) extends NetworkInputDStream[String](ssc_)  {
  
  
  override def getReceiver(): NetworkReceiver[String] = {
    new KinesisReceiver(accesskey,accessSecretKey,kinesisStream,kinesisEndpoint,storageLevel)
  }
}


object AllDone extends Exception { }

private[streaming]
class KinesisReceiver[T: ClassTag](
    accesskey:String,
    accessSecretKey:String,
    kinesisStream:String,
    kinesisEndpoint:String,
    storageLevel: StorageLevel
  ) extends NetworkReceiver[String] {

  val NUM_RETRIES =5
  val BACKOFF_TIME_IN_MILLIS =2000
  var workerId = UUID.randomUUID().toString()
  
  lazy val credentialsProvider = new AWSCredentialsProvider {
           
       def getCredentials():AWSCredentials = {
         if (accesskey.isEmpty()||accessSecretKey.isEmpty) {
           new InstanceProfileCredentialsProvider().getCredentials()
         }else{
           new BasicAWSCredentials(accesskey,accessSecretKey)
         }
       }
      
       def refresh() {}
   }
  
    try {
      workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID()
    } catch {
      case e:UnknownHostException => e.printStackTrace()
    }

  private lazy val decoder = Charset.forName("UTF-8").newDecoder();
  private lazy val kinesisClientLibConfiguration =  new KinesisClientLibConfiguration(kinesisStream, kinesisStream, credentialsProvider,workerId).withKinesisEndpoint(kinesisEndpoint)
  private lazy val blockGenerator = new BlockGenerator(storageLevel)
  
  protected override def onStart() {
    
    blockGenerator.start()
     lazy val recordProcessorFactory:IRecordProcessorFactory = new IRecordProcessorFactory{
	      def createProcessor():IRecordProcessor= new IRecordProcessor {
		   
	         def initialize(shardId:String){
	          logInfo("starting with shardId: "+shardId)
		       }
		      
		       def processRecords(records: List[Record], checkpointer : IRecordProcessorCheckpointer) {	
		         records.toList.foreach(record=>{
		           blockGenerator+=decoder.decode(record.getData()).toString();
		         })
		          checkpoint(checkpointer);
		       }
		     
		        def shutdown(checkpointer : IRecordProcessorCheckpointer, reason : ShutdownReason){
		          logInfo("Shutting Down Kinesis Receiver: "+reason)
		        }
	        }	      
	      }
     val worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);
     worker.run()
  }
  
  private def checkpoint(checkpointer : IRecordProcessorCheckpointer) {
    
    for (i<-1 to NUM_RETRIES) {
        try {
                checkpointer.checkpoint();
                throw AllDone;
        } catch {
          case  se:ShutdownException =>logInfo("Caught shutdown exception, skipping checkpoint.", se)
          case  e:ThrottlingException => {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                        logInfo("Checkpoint failed after " + (i + 1) + "attempts.", e)
                        throw AllDone;
                } else {
                        logInfo("Transient issue when checkpointing - attempt " 
                            + (i + 1) + " of "+ NUM_RETRIES, e)
                }
          }
          case e:InvalidStateException => {
            logInfo("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e)
            throw AllDone
          }
          case AllDone=>
        }
        try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS)
        } catch {
          case e:InterruptedException => logInfo("Interrupted sleep", e)
        }
    }
  }

  protected override def onStop() {
    blockGenerator.stop()
    logInfo("Amazon Kinesis receiver stopped")
  }
}
