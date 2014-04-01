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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;

/** An {@link OutputFormat} that writes {@link SequenceFile}s. */
public class SequenceFileOutputFormat <K,V> extends FileOutputFormat<K, V> {

  public RecordWriter<K, V> 
         getRecordWriter(TaskAttemptContext context
                         ) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    if (getCompressOutput(context)) {
      // find the kind of compression to do
      compressionType = getOutputCompressionType(context);

      // find the right codec
      Class<?> codecClass = getOutputCompressorClass(context, 
                                                     DefaultCodec.class);
      codec = (CompressionCodec) 
        ReflectionUtils.newInstance(codecClass, conf);
    }
    // get the path of the temporary output file 
    Path file = getDefaultWorkFile(context, "");
    FileSystem fs = file.getFileSystem(conf);
    final SequenceFile.Writer out = 
      SequenceFile.createWriter(fs, conf, file,
                                context.getOutputKeyClass(),
                                context.getOutputValueClass(),
                                compressionType,
                                codec,
                                context);

    return new RecordWriter<K, V>() {

        public void write(K key, V value)
          throws IOException {

          out.append(key, value);
        }

        public void close(TaskAttemptContext context) throws IOException { 
          out.close();
        }
      };
  }

  /**
   * Get the {@link CompressionType} for the output {@link SequenceFile}.
   * @param job the {@link Job}
   * @return the {@link CompressionType} for the output {@link SequenceFile}, 
   *         defaulting to {@link CompressionType#RECORD}
   */
  public static CompressionType getOutputCompressionType(JobContext job) {
    String val = job.getConfiguration().get("mapred.output.compression.type", 
                                            CompressionType.RECORD.toString());
    return CompressionType.valueOf(val);
  }
  
  /**
   * Set the {@link CompressionType} for the output {@link SequenceFile}.
   * @param job the {@link Job} to modify
   * @param style the {@link CompressionType} for the output
   *              {@link SequenceFile} 
   */
  public static void setOutputCompressionType(Job job, 
		                                          CompressionType style) {
    setCompressOutput(job, true);
    job.getConfiguration().set("mapred.output.compression.type", 
                               style.toString());
  }

}

