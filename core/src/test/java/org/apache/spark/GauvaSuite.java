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

package org.apache.spark;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.collection.JavaConverters;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.apache.spark.rdd.RDD;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.StatCounter;
import org.apache.spark.util.testing.LocalJavaSparkContext;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class GauvaSuite implements Serializable {
  /**
   * Test for SPARK-3647. This test needs to use the maven-built assembly to trigger the issue,
   * since that's the only artifact where Guava classes have been relocated.
   */
  @Test
  public void testGuavaOptional() {
    JavaSparkContext localCluster = new JavaSparkContext("local-cluster[1,1,1024]", "JavaAPISuite");
    try {
      JavaRDD<Integer> rdd1 = localCluster.parallelize(Arrays.asList(1, 2, null), 3);
      JavaRDD<Optional<Integer>> rdd2 = rdd1.map(
        new Function<Integer, Optional<Integer>>() {
          @Override
          public Optional<Integer> call(Integer i) {
            return Optional.fromNullable(i);
          }
        });
      rdd2.collect();
    } finally {
      localCluster.stop();
    }
  }

}
