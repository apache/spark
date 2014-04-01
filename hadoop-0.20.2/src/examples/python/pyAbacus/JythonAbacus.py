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

from org.apache.hadoop.abacus import *

from java.util import *;

import sys

class AbacusMapper(ValueAggregatorMapper):
    def map(self, key, value, output, reporter):
        ValueAggregatorMapper.map(self, key, value, output, reporter);

class AbacusReducer(ValueAggregatorReducer):
    def reduce(self, key, values, output, reporter):
        ValueAggregatorReducer.reduce(self, key, values, output, reporter);

class AbacusCombiner(ValueAggregatorCombiner):
    def reduce(self, key, values, output, reporter):
        ValueAggregatorCombiner.reduce(self, key, values, output, reporter);

def printUsage(code):
    print "Abacus <input> <output> <numOfReducers> <inputformat> <specfile>"
    sys.exit(code)

def main(args):
    if len(args) < 6:
        printUsage(1);

    inDir = args[1];
    outDir = args[2];
    numOfReducers = int(args[3]);
    theInputFormat = args[4];
    specFile = args[5];
                                        
    print "numOfReducers: ", numOfReducers, "theInputFormat: ", theInputFormat, "specFile: ", specFile

    conf = JobConf(AbacusMapper);
    conf.setJobName("recordcount");
    conf.addDefaultResource(Path(specFile));
 
    if theInputFormat=="textinputformat":
        conf.setInputFormat(TextInputFormat);
    else:
        conf.setInputFormat(SequenceFileInputFormat);
    conf.setOutputFormat(TextOutputFormat);
    conf.setMapOutputKeyClass(Text);
    conf.setMapOutputValueClass(Text);
    conf.setOutputKeyClass(Text);
    conf.setOutputValueClass(Text);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(numOfReducers);

    conf.setMapperClass(AbacusMapper);        
    conf.setCombinerClass(AbacusCombiner);
    conf.setReducerClass(AbacusReducer);
    conf.setInputPath(Path(args[1]))
    conf.setOutputPath(Path(args[2]))

    JobClient.runJob(conf);

if __name__ == "__main__":
    main(sys.argv)
