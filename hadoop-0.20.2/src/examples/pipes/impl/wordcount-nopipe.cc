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
#include "hadoop/StringUtils.hh"
#include "hadoop/SerialUtils.hh"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>

const std::string WORDCOUNT = "WORDCOUNT";
const std::string INPUT_WORDS = "INPUT_WORDS";
const std::string OUTPUT_WORDS = "OUTPUT_WORDS";

class WordCountMap: public HadoopPipes::Mapper {
public:
  HadoopPipes::TaskContext::Counter* inputWords;
  
  WordCountMap(HadoopPipes::TaskContext& context) {
    inputWords = context.getCounter(WORDCOUNT, INPUT_WORDS);
  }
  
  void map(HadoopPipes::MapContext& context) {
    std::vector<std::string> words = 
      HadoopUtils::splitString(context.getInputValue(), " ");
    for(unsigned int i=0; i < words.size(); ++i) {
      context.emit(words[i], "1");
    }
    context.incrementCounter(inputWords, words.size());
  }
};

class WordCountReduce: public HadoopPipes::Reducer {
public:
  HadoopPipes::TaskContext::Counter* outputWords;

  WordCountReduce(HadoopPipes::TaskContext& context) {
    outputWords = context.getCounter(WORDCOUNT, OUTPUT_WORDS);
  }

  void reduce(HadoopPipes::ReduceContext& context) {
    int sum = 0;
    while (context.nextValue()) {
      sum += HadoopUtils::toInt(context.getInputValue());
    }
    context.emit(context.getInputKey(), HadoopUtils::toString(sum));
    context.incrementCounter(outputWords, 1); 
  }
};

class WordCountReader: public HadoopPipes::RecordReader {
private:
  int64_t bytesTotal;
  int64_t bytesRead;
  FILE* file;
public:
  WordCountReader(HadoopPipes::MapContext& context) {
    std::string filename;
    HadoopUtils::StringInStream stream(context.getInputSplit());
    HadoopUtils::deserializeString(filename, stream);
    struct stat statResult;
    stat(filename.c_str(), &statResult);
    bytesTotal = statResult.st_size;
    bytesRead = 0;
    file = fopen(filename.c_str(), "rt");
    HADOOP_ASSERT(file != NULL, "failed to open " + filename);
  }

  ~WordCountReader() {
    fclose(file);
  }

  virtual bool next(std::string& key, std::string& value) {
    key = HadoopUtils::toString(ftell(file));
    int ch = getc(file);
    bytesRead += 1;
    value.clear();
    while (ch != -1 && ch != '\n') {
      value += ch;
      ch = getc(file);
      bytesRead += 1;
    }
    return ch != -1;
  }

  /**
   * The progress of the record reader through the split as a value between
   * 0.0 and 1.0.
   */
  virtual float getProgress() {
    if (bytesTotal > 0) {
      return (float)bytesRead / bytesTotal;
    } else {
      return 1.0f;
    }
  }
};

class WordCountWriter: public HadoopPipes::RecordWriter {
private:
  FILE* file;
public:
  WordCountWriter(HadoopPipes::ReduceContext& context) {
    const HadoopPipes::JobConf* job = context.getJobConf();
    int part = job->getInt("mapred.task.partition");
    std::string outDir = job->get("mapred.work.output.dir");
    // remove the file: schema substring
    std::string::size_type posn = outDir.find(":");
    HADOOP_ASSERT(posn != std::string::npos, 
                  "no schema found in output dir: " + outDir);
    outDir.erase(0, posn+1);
    mkdir(outDir.c_str(), 0777);
    std::string outFile = outDir + "/part-" + HadoopUtils::toString(part);
    file = fopen(outFile.c_str(), "wt");
    HADOOP_ASSERT(file != NULL, "can't open file for writing: " + outFile);
  }

  ~WordCountWriter() {
    fclose(file);
  }

  void emit(const std::string& key, const std::string& value) {
    fprintf(file, "%s -> %s\n", key.c_str(), value.c_str());
  }
};

int main(int argc, char *argv[]) {
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<WordCountMap, 
                              WordCountReduce, void, void, WordCountReader,
                              WordCountWriter>());
}

