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

package org.apache.spark.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 */
public class SimpleCombiner<K, V> implements InputFormat<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleCombiner.class);

  // apply only if the input format is file-based
  public static boolean accepts(Class<? extends InputFormat> inputFormatter) {
    return FileInputFormat.class.isAssignableFrom(inputFormatter) ||
        org.apache.hadoop.mapred.FileInputFormat.class.isAssignableFrom(inputFormatter);
  }

  private final InputFormat<K, V> delegate;
  private final long threshold;

  public SimpleCombiner(InputFormat<K, V> delegate, long threshold) {
    this.delegate = delegate;
    this.threshold = threshold;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] original = delegate.getSplits(job, numSplits);
    LOG.info("Start combining " + original.length + " splits with threshold " + threshold);

    long start = System.currentTimeMillis();
    List<TaggedPair<Set<String>, List<InputSplit>>> splits = new ArrayList<>();
    for (InputSplit split : original) {
      final long length = split.getLength();
      final String[] locations = split.getLocations();

      boolean added = false;
      for (TaggedPair<Set<String>, List<InputSplit>> entry : splits) {
        if (entry.t > threshold) {
          continue;
        }
        Set<String> set = entry.l;
        if (containsAny(set, locations)) {
          set.retainAll(Arrays.asList(locations));
          entry.r.add(split);
          entry.t += length;
          added = true;
          break;
        }
      }
      if (!added) {
        splits.add(new TaggedPair<Set<String>, List<InputSplit>>(
            length,
            new HashSet<>(Arrays.asList(locations)),
            new ArrayList<>(Arrays.asList(split))));
      }
    }

    List<InputSplit> combined = new ArrayList<>();
    Iterator<TaggedPair<Set<String>, List<InputSplit>>> iterator = splits.iterator();
    while (iterator.hasNext()) {
      TaggedPair<Set<String>, List<InputSplit>> entry = iterator.next();
      if (entry.t >= threshold) {
        combined.add(
            new InputSplits(entry.t,
                entry.l.toArray(new String[entry.l.size()]),
                entry.r.toArray(new InputSplit[entry.r.size()])));
        iterator.remove();
      }
    }

    TaggedPair<Set<String>, List<InputSplit>> current = null;
    iterator = splits.iterator();
    while (iterator.hasNext()) {
      TaggedPair<Set<String>, List<InputSplit>> entry = iterator.next();
      if (current == null) {
        iterator.remove();
        current = entry;
        continue;
      }
      if (containsAny(current.l, entry.l)) {
        iterator.remove();
        current.t += entry.t;
        current.r.addAll(entry.r);
        current.l.retainAll(entry.l);
      }
      if (current.t > threshold) {
        combined.add(
            new InputSplits(current.t,
                current.l.toArray(new String[current.l.size()]),
                current.r.toArray(new InputSplit[current.r.size()])));
        current = null;
      }
    }
    if (current != null) {
      combined.add(
          new InputSplits(current.t,
              current.l.toArray(new String[current.l.size()]),
              current.r.toArray(new InputSplit[current.r.size()])));
    }
    for (TaggedPair<Set<String>, List<InputSplit>> entry : splits) {
      combined.add(new InputSplits(entry.t,
          entry.l.toArray(new String[entry.l.size()]),
          entry.r.toArray(new InputSplit[entry.r.size()])));
    }
    LOG.info("Combined to " + combined.size() + " splits, took " + (System.currentTimeMillis() - start) + " msec");
    return combined.toArray(new InputSplit[combined.size()]);
  }

  private boolean containsAny(Set<String> set, String[] targets) {
    return containsAny(set, Arrays.asList(targets));
  }

  private boolean containsAny(Set<String> set, Collection<String> targets) {
    for (String target : targets) {
      if (set.contains(target)) {
        return true;
      }
    }
    return set.isEmpty();
  }

  @Override
  public RecordReader<K, V> getRecordReader(final InputSplit split, final JobConf job, final Reporter reporter)
      throws IOException {

    final InputSplit[] splits = ((InputSplits) split).splits;

    return new RecordReader<K, V>() {

      private int index;
      private long pos;
      private RecordReader<K, V> reader = nextReader();

      private RecordReader<K, V> nextReader() throws IOException {
        return delegate.getRecordReader(splits[index++], job, reporter);
      }

      @Override
      @SuppressWarnings("unchecked")
      public boolean next(K key, V value) throws IOException {
        while (!reader.next(key, value)) {
          if (index < splits.length) {
            pos = reader.getPos();
            reader.close();
            reader = nextReader();
            continue;
          }
          return false;
        }
        return true;
      }

      @Override
      public K createKey() {
        return reader.createKey();
      }

      @Override
      public V createValue() {
        return reader.createValue();
      }

      @Override
      public long getPos() throws IOException {
        return pos + reader.getPos();
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

      @Override
      public float getProgress() throws IOException {
        return (index - 1 + reader.getProgress()) / splits.length;
      }
    };
  }

  public static class InputSplits implements InputSplit, Configurable {

    private long length;
    private InputSplit[] splits;
    private String[] locations;

    private transient Configuration conf;

    public InputSplits() {
    }

    public InputSplits(long length, String[] locations, InputSplit[] splits) {
      this.length = length;
      this.locations = locations;
      this.splits = splits;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public long getLength() throws IOException {
      return length;
    }

    @Override
    public String[] getLocations() throws IOException {
      return locations;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(length);
      out.writeInt(locations.length);
      for (String location : locations) {
        Text.writeString(out, location);
      }
      out.writeInt(splits.length);
      for (InputSplit split : splits) {
        Text.writeString(out, split.getClass().getName());
        split.write(out);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      length = in.readLong();
      locations = new String[in.readInt()];
      for (int i = 0; i < locations.length; i++) {
        locations[i] = Text.readString(in);
      }
      splits = new InputSplit[in.readInt()];
      try {
        for (int i = 0; i < splits.length; i++) {
          Class<?> clazz = ObjectWritable.loadClass(conf, Text.readString(in));
          splits[i] = (InputSplit) ReflectionUtils.newInstance(clazz, conf);
          splits[i].readFields(in);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public String toString() {
      return "length = " + length + ", locations = " + Arrays.toString(locations) + ", splits = " + Arrays.toString(splits);
    }
  }

  private static class TaggedPair<L, R> {
    private long t;
    private final L l;
    private final R r;

    private TaggedPair(long t, L l, R r) {
      this.t = t;
      this.l = l;
      this.r = r;
    }

    static <L, R> TaggedPair<L, R> of(long t, L l, R r) {
      return new TaggedPair<>(t, l, r);
    }
  }
}
