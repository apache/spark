#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from org.apache.hadoop.fs import Path
from org.apache.hadoop.io import *
from org.apache.hadoop.mapred import *

import sys
import getopt

class WordCountMap(Mapper, MapReduceBase):
    one = IntWritable(1)
    def map(self, key, value, output, reporter):
        for w in value.toString().split():
            output.collect(Text(w), self.one)

class Summer(Reducer, MapReduceBase):
    def reduce(self, key, values, output, reporter):
        sum = 0
        while values.hasNext():
            sum += values.next().get()
        output.collect(key, IntWritable(sum))

def printUsage(code):
    print "wordcount [-m <maps>] [-r <reduces>] <input> <output>"
    sys.exit(code)

def main(args):
    conf = JobConf(WordCountMap);
    conf.setJobName("wordcount");
 
    conf.setOutputKeyClass(Text);
    conf.setOutputValueClass(IntWritable);
    
    conf.setMapperClass(WordCountMap);        
    conf.setCombinerClass(Summer);
    conf.setReducerClass(Summer);
    try:
        flags, other_args = getopt.getopt(args[1:], "m:r:")
    except getopt.GetoptError:
        printUsage(1)
    if len(other_args) != 2:
        printUsage(1)
    
    for f,v in flags:
        if f == "-m":
            conf.setNumMapTasks(int(v))
        elif f == "-r":
            conf.setNumReduceTasks(int(v))
    conf.setInputPath(Path(other_args[0]))
    conf.setOutputPath(Path(other_args[1]))
    JobClient.runJob(conf);

if __name__ == "__main__":
    main(sys.argv)
