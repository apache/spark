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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * An {@link InputFormat} that delegates behaviour of paths to multiple other
 * InputFormats.
 * 
 * @see MultipleInputs#addInputPath(JobConf, Path, Class, Class)
 */
public class DelegatingInputFormat<K, V> implements InputFormat<K, V> {

  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {

    JobConf confCopy = new JobConf(conf);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    Map<Path, InputFormat> formatMap = MultipleInputs.getInputFormatMap(conf);
    Map<Path, Class<? extends Mapper>> mapperMap = MultipleInputs
       .getMapperTypeMap(conf);
    Map<Class<? extends InputFormat>, List<Path>> formatPaths
        = new HashMap<Class<? extends InputFormat>, List<Path>>();

    // First, build a map of InputFormats to Paths
    for (Entry<Path, InputFormat> entry : formatMap.entrySet()) {
      if (!formatPaths.containsKey(entry.getValue().getClass())) {
       formatPaths.put(entry.getValue().getClass(), new LinkedList<Path>());
      }

      formatPaths.get(entry.getValue().getClass()).add(entry.getKey());
    }

    for (Entry<Class<? extends InputFormat>, List<Path>> formatEntry : 
        formatPaths.entrySet()) {
      Class<? extends InputFormat> formatClass = formatEntry.getKey();
      InputFormat format = (InputFormat) ReflectionUtils.newInstance(
         formatClass, conf);
      List<Path> paths = formatEntry.getValue();

      Map<Class<? extends Mapper>, List<Path>> mapperPaths
          = new HashMap<Class<? extends Mapper>, List<Path>>();

      // Now, for each set of paths that have a common InputFormat, build
      // a map of Mappers to the paths they're used for
      for (Path path : paths) {
       Class<? extends Mapper> mapperClass = mapperMap.get(path);
       if (!mapperPaths.containsKey(mapperClass)) {
         mapperPaths.put(mapperClass, new LinkedList<Path>());
       }

       mapperPaths.get(mapperClass).add(path);
      }

      // Now each set of paths that has a common InputFormat and Mapper can
      // be added to the same job, and split together.
      for (Entry<Class<? extends Mapper>, List<Path>> mapEntry : mapperPaths
         .entrySet()) {
       paths = mapEntry.getValue();
       Class<? extends Mapper> mapperClass = mapEntry.getKey();

       if (mapperClass == null) {
         mapperClass = conf.getMapperClass();
       }

       FileInputFormat.setInputPaths(confCopy, paths.toArray(new Path[paths
           .size()]));

       // Get splits for each input path and tag with InputFormat
       // and Mapper types by wrapping in a TaggedInputSplit.
       InputSplit[] pathSplits = format.getSplits(confCopy, numSplits);
       for (InputSplit pathSplit : pathSplits) {
         splits.add(new TaggedInputSplit(pathSplit, conf, format.getClass(),
             mapperClass));
       }
      }
    }

    return splits.toArray(new InputSplit[splits.size()]);
  }

  @SuppressWarnings("unchecked")
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf conf,
      Reporter reporter) throws IOException {

    // Find the InputFormat and then the RecordReader from the
    // TaggedInputSplit.

    TaggedInputSplit taggedInputSplit = (TaggedInputSplit) split;
    InputFormat<K, V> inputFormat = (InputFormat<K, V>) ReflectionUtils
       .newInstance(taggedInputSplit.getInputFormatClass(), conf);
    return inputFormat.getRecordReader(taggedInputSplit.getInputSplit(), conf,
       reporter);
  }
}
