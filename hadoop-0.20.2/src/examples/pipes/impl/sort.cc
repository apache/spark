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

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"

class SortMap: public HadoopPipes::Mapper {
private:
  /* the fraction 0.0 to 1.0 of records to keep */
  float keepFraction;
  /* the number of records kept so far */
  long long keptRecords;
  /* the total number of records */
  long long totalRecords;
  static const std::string MAP_KEEP_PERCENT;
public:
  /*
   * Look in the config to find the fraction of records to keep.
   */
  SortMap(HadoopPipes::TaskContext& context){
    const HadoopPipes::JobConf* conf = context.getJobConf();
    if (conf->hasKey(MAP_KEEP_PERCENT)) {
      keepFraction = conf->getFloat(MAP_KEEP_PERCENT) / 100.0;
    } else {
      keepFraction = 1.0;
    }
    keptRecords = 0;
    totalRecords = 0;
  }

  void map(HadoopPipes::MapContext& context) {
    totalRecords += 1;
    while ((float) keptRecords / totalRecords < keepFraction) {
      keptRecords += 1;
      context.emit(context.getInputKey(), context.getInputValue());
    }
  }
};

const std::string SortMap::MAP_KEEP_PERCENT("hadoop.sort.map.keep.percent");

class SortReduce: public HadoopPipes::Reducer {
private:
  /* the fraction 0.0 to 1.0 of records to keep */
  float keepFraction;
  /* the number of records kept so far */
  long long keptRecords;
  /* the total number of records */
  long long totalRecords;
  static const std::string REDUCE_KEEP_PERCENT;
public:
  SortReduce(HadoopPipes::TaskContext& context){
    const HadoopPipes::JobConf* conf = context.getJobConf();
    if (conf->hasKey(REDUCE_KEEP_PERCENT)) {
      keepFraction = conf->getFloat(REDUCE_KEEP_PERCENT) / 100.0;
    } else {
      keepFraction = 1.0;
    }
    keptRecords = 0;
    totalRecords = 0;
  }

  void reduce(HadoopPipes::ReduceContext& context) {
    while (context.nextValue()) {
      totalRecords += 1;
      while ((float) keptRecords / totalRecords < keepFraction) {
        keptRecords += 1;
        context.emit(context.getInputKey(), context.getInputValue());
      }
    }
  }
};

const std::string 
  SortReduce::REDUCE_KEEP_PERCENT("hadoop.sort.reduce.keep.percent");

int main(int argc, char *argv[]) {
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<SortMap,
                                                           SortReduce>());
}

